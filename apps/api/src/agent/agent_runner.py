import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any, cast

from background import make_event_stream_name
from chat.utils import (
    langchain_message_to_model,
    make_presence_event,
    message_model_to_outbox,
    message_model_to_redis_event,
)
from common import get_next_attempt_at
from db import Message
from db.models import AgentRun
from event import OutboxEventTypeEnum
from langchain.agents.middleware.types import InputAgentState
from langchain.messages import AIMessage, AIMessageChunk, AnyMessage, HumanMessage
from redis.asyncio import Redis
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent.schemas import AgentRunStatusEnum, MessagesChunkMessage, UpdatesChunkMessage

from .agent import make_agent

logger = logging.getLogger(__name__)


MAX_ATTEMPTS: int = 3


async def claim_pending_runs(
    session_factory: async_sessionmaker[AsyncSession], limit: int = 5
):
    session = session_factory()

    async with session.begin():
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

        runs = list((await session.execute(stmt)).scalars().all())

        for run in runs:
            run.status = AgentRunStatusEnum.RUNNING

    return runs


async def run_agent(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    run: AgentRun,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    agent = make_agent()

    stored_messages: list[Message] = []

    session = session_factory()
    async with session.begin():
        msg_stmt = (
            select(Message)
            .where(Message.conversation_id == run.conversation_id)
            .order_by(Message.seq.asc())
        )

        stored_messages = list((await session.execute(msg_stmt)).scalars().all())

    agent_messages: list[AnyMessage | dict[str, Any]] = [
        HumanMessage(content=[dict(block) for block in msg.content])
        if msg.role == "user"
        else AIMessage(content=[dict(block) for block in msg.content])
        for msg in stored_messages
    ]

    message_id_map = dict[str, uuid.UUID]()

    await redis.xadd(
        make_event_stream_name(
            run.conversation_id,
        ),
        make_presence_event(typing=True),
    )

    async for mode, chunk in agent.astream(
        InputAgentState(messages=agent_messages), stream_mode=["messages", "updates"]
    ):
        session = session_factory()
        async with session.begin():
            check_run = await session.get(AgentRun, run.id)

            if (
                check_run is not None
                and check_run.status == AgentRunStatusEnum.INTERRUPT_REQUESTED
            ):
                raise asyncio.CancelledError

        match mode:
            case "messages":
                chunk = cast(MessagesChunkMessage, chunk)
                chunk_message, _metadata = chunk

                message = langchain_message_to_model(chunk_message, run.conversation_id)

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
                    make_event_stream_name(
                        run.conversation_id,
                    ),
                    message_model_to_redis_event(
                        OutboxEventTypeEnum.MESSAGE_DELTA, message
                    ),
                )

            case "updates":
                chunk = cast(UpdatesChunkMessage, chunk)
                chunk_messages = chunk["model"]["messages"]

                messages: list[Message] = []

                for chunk_message in chunk_messages:
                    message = langchain_message_to_model(
                        chunk_message, run.conversation_id
                    )

                    message_id = message_id_map.get(str(chunk_message.id), None)

                    if message_id is None:
                        msg_uuid = uuid.uuid7()
                        message_id_map[str(chunk_message.id)] = msg_uuid
                        message_id = msg_uuid

                    message.id = message_id

                    messages.append(message)

                session = session_factory()
                async with session.begin():
                    session.add_all(messages)

                for message in messages:
                    if message.id in incomplete_messages:
                        del incomplete_messages[message.id]

                    await redis.xadd(
                        make_event_stream_name(
                            run.conversation_id,
                        ),
                        message_model_to_redis_event(
                            OutboxEventTypeEnum.MESSAGE_CREATED, message
                        ),
                    )

            case _:
                pass

    await redis.xadd(
        make_event_stream_name(
            run.conversation_id,
        ),
        make_presence_event(typing=False),
    )


async def mark_interrupted(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    run: AgentRun,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    messages: list[Message] = []

    for message_id, chunk_message in incomplete_messages.items():
        message = langchain_message_to_model(chunk_message, run.conversation_id)
        message.id = message_id

        messages.append(message)

    session = session_factory()
    async with session.begin():
        session.add_all(messages)
        await session.flush()

        session.add_all([message_model_to_outbox(message) for message in messages])

        run.status = AgentRunStatusEnum.INTERRUPTED
        run.completed_at = func.now()

    await redis.xadd(
        make_event_stream_name(run.conversation_id),
        make_presence_event(typing=False),
    )


async def mark_completed(
    session_factory: async_sessionmaker[AsyncSession],
    run: AgentRun,
):
    session = session_factory()

    async with session.begin():
        run.status = AgentRunStatusEnum.COMPLETED
        run.completed_at = func.now()


async def mark_retry_or_failed(
    session_factory: async_sessionmaker[AsyncSession],
    run: AgentRun,
):
    session = session_factory()

    async with session.begin():
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
    run: AgentRun,
):
    incomplete_messages: dict[uuid.UUID, AIMessageChunk] = {}

    try:
        await run_agent(redis, session_factory, run, incomplete_messages)

    except asyncio.CancelledError:
        logger.exception("Agent run %s interrupted", run.id)

        await asyncio.shield(
            mark_interrupted(redis, session_factory, run, incomplete_messages)
        )
        raise

    except Exception:
        logger.exception("Agent run %s failed", run.id)

        await mark_retry_or_failed(session_factory, run)
        return

    await mark_completed(session_factory, run)


async def agent_runner(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
):
    while True:
        claimed_runs = await claim_pending_runs(session_factory)

        if len(claimed_runs) == 0:
            await asyncio.sleep(0.2)
            continue

        async with asyncio.TaskGroup() as tg:
            for run in claimed_runs:
                tg.create_task(run_agent_safe(redis, session_factory, run))


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
