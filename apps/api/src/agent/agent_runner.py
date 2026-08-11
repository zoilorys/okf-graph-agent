import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any, cast

from chat.utils import (
    langchain_message_to_model,
    make_presence_event,
    message_model_to_langchain_message,
    message_model_to_outbox,
    message_model_to_redis_event,
)
from common import get_next_attempt_at
from db import Message
from db.models import AgentRun
from event import OutboxEventTypeEnum
from langchain.agents.middleware.types import InputAgentState
from langchain.messages import AIMessageChunk, AnyMessage
from outbox.utils import make_event_stream_name
from redis.asyncio import Redis
from sqlalchemy import and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent.schemas import AgentRunStatusEnum, MessagesChunkMessage, UpdatesChunkMessage

from .agent import make_agent

logger = logging.getLogger(__name__)


MAX_ATTEMPTS: int = 3

agent = make_agent()


async def claim_pending_runs(
    session_factory: async_sessionmaker[AsyncSession], limit: int = 5
):
    async with session_factory.begin() as session:
        stmt = (
            select(AgentRun)
            .where(
                and_(
                    AgentRun.status == AgentRunStatusEnum.PENDING,
                    AgentRun.next_attempt_at <= func.now(),
                )
            )
            .limit(limit)
            .order_by(AgentRun.created_at.asc())
            .with_for_update(skip_locked=True)
        )

        runs = (await session.execute(stmt)).scalars().all()

        for run in runs:
            run.status = AgentRunStatusEnum.RUNNING

    return [(run.id, run.conversation_id) for run in runs]


async def run_agent(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    await redis.xadd(
        make_event_stream_name(conversation_id),
        make_presence_event(typing=True),
    )

    async with session_factory.begin() as session:
        msg_stmt = (
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.seq.asc())
        )

        stored_messages = (await session.execute(msg_stmt)).scalars().all()

    agent_messages: list[AnyMessage | dict[str, Any]] = [
        message_model_to_langchain_message(msg) for msg in stored_messages
    ]

    message_id_map = dict[str, uuid.UUID]()

    async for mode, chunk in agent.astream(
        InputAgentState(messages=agent_messages), stream_mode=["messages", "updates"]
    ):
        async with session_factory.begin() as session:
            check_run = await session.get_one(AgentRun, run_id)

            if check_run.status == AgentRunStatusEnum.INTERRUPT_REQUESTED:
                raise asyncio.CancelledError

        match mode:
            case "messages":
                chunk = cast(MessagesChunkMessage, chunk)
                chunk_message, _metadata = chunk

                message = langchain_message_to_model(chunk_message, conversation_id)

                message_id = message_id_map.get(str(chunk_message.id), None)

                if message_id is None:
                    msg_uuid = uuid.uuid7()
                    message_id_map[str(chunk_message.id)] = msg_uuid
                    message_id = msg_uuid

                if message_id in incomplete_messages:
                    incomplete_messages[message_id] += chunk_message
                else:
                    incomplete_messages[message_id] = chunk_message

                message.id = message_id
                message.created_at = datetime.now(UTC)

                await redis.xadd(
                    make_event_stream_name(conversation_id),
                    message_model_to_redis_event(
                        OutboxEventTypeEnum.MESSAGE_DELTA, message
                    ),
                )

            case "updates":
                chunk = cast(UpdatesChunkMessage, chunk)
                chunk_messages = chunk["model"]["messages"]

                async with session_factory.begin() as session:
                    messages: list[Message] = []

                    for chunk_message in chunk_messages:
                        message = langchain_message_to_model(
                            chunk_message, conversation_id
                        )

                        message_id = message_id_map.get(str(chunk_message.id), None)

                        if message_id is None:
                            msg_uuid = uuid.uuid7()
                            message_id_map[str(chunk_message.id)] = msg_uuid
                            message_id = msg_uuid

                        message.id = message_id

                        messages.append(message)

                    for message in messages:
                        if message.id in incomplete_messages:
                            del incomplete_messages[message.id]

                    session.add_all(messages)
                    await session.flush()

                    session.add_all(
                        [message_model_to_outbox(message) for message in messages]
                    )

                    stmt = (
                        update(AgentRun)
                        .where(
                            and_(
                                AgentRun.id == run_id,
                                AgentRun.status == AgentRunStatusEnum.RUNNING,
                            )
                        )
                        .values(
                            status=AgentRunStatusEnum.COMPLETED,
                            completed_at=func.now(),
                        )
                    )

                    await session.execute(stmt)

            case _:
                pass

    await redis.xadd(
        make_event_stream_name(conversation_id),
        make_presence_event(typing=False),
    )


async def mark_interrupted(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    async with session_factory.begin() as session:
        messages: list[Message] = []

        for message_id, chunk_message in incomplete_messages.items():
            message = langchain_message_to_model(chunk_message, conversation_id)
            message.id = message_id

            messages.append(message)

        session.add_all(messages)
        await session.flush()

        session.add_all([message_model_to_outbox(message) for message in messages])

        stmt = (
            update(AgentRun)
            .where(
                and_(
                    AgentRun.id == run_id,
                    AgentRun.status == AgentRunStatusEnum.INTERRUPT_REQUESTED,
                )
            )
            .values(
                status=AgentRunStatusEnum.INTERRUPTED,
                completed_at=None,
            )
        )

        await session.execute(stmt)

    await redis.xadd(
        make_event_stream_name(conversation_id),
        make_presence_event(typing=False),
    )


async def mark_retry_or_failed(
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
):
    async with session_factory.begin() as session:
        run = await session.get_one(AgentRun, run_id)

        attempt = run.attempt + 1

        if attempt > MAX_ATTEMPTS:
            run.status = AgentRunStatusEnum.FAILED
        else:
            run.status = AgentRunStatusEnum.PENDING
            run.attempt = attempt
            run.next_attempt_at = get_next_attempt_at(attempt)


async def run_agent_safe(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
):
    incomplete_messages: dict[uuid.UUID, AIMessageChunk] = {}

    try:
        await run_agent(
            redis,
            session_factory,
            run_id,
            conversation_id,
            incomplete_messages,
        )

    except asyncio.CancelledError:
        logger.exception("Agent run %s interrupted", run_id)

        await asyncio.shield(
            mark_interrupted(
                redis,
                session_factory,
                run_id,
                conversation_id,
                incomplete_messages,
            )
        )
        raise

    except Exception:
        logger.exception("Agent run %s failed", run_id)

        await mark_retry_or_failed(
            session_factory,
            run_id,
        )


async def agent_runner(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
):
    while True:
        claimed_run_ids = await claim_pending_runs(session_factory)

        if len(claimed_run_ids) == 0:
            await asyncio.sleep(0.2)
            continue

        async with asyncio.TaskGroup() as tg:
            for run_id, conversation_id in claimed_run_ids:
                tg.create_task(
                    run_agent_safe(
                        redis,
                        session_factory,
                        run_id,
                        conversation_id,
                    )
                )


async def agent_runner_supervisor(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
):
    while True:
        try:
            await agent_runner(redis, session_factory)

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("Agent runner crashed")
            await asyncio.sleep(1)
