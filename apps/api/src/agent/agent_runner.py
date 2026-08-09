import asyncio
import json
import logging
from typing import cast

from background import make_event_stream_name
from common import get_next_attempt_at
from db import Message
from db.models import AgentRun
from event import OutboxEventTypeEnum
from langchain.messages import AIMessage, AnyMessage, HumanMessage
from redis.asyncio import Redis
from redis.typing import EncodableT, FieldT
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent.schemas import AgentRunStatusEnum

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
):
    agent = make_agent()

    session = session_factory()

    msg_stmt = (
        select(Message)
        .where(Message.conversation_id == run.conversation_id)
        .order_by(Message.seq.asc())
    )

    stored_messages = list((await session.execute(msg_stmt)).scalars().all())

    agent_messages: list[AnyMessage] = [
        HumanMessage(content=[dict(block) for block in msg.content])
        if msg.role == "user"
        else AIMessage(content=[dict(block) for block in msg.content])
        for msg in stored_messages
    ]

    async for mode, chunk in agent.astream(
        {"messages": agent_messages}, stream_mode=["messages", "updates"]
    ):
        match mode:
            case "messages":
                print(("messages", chunk))
            case "updates":
                print(("updates", chunk))
                chunk_messages = chunk["model"]["messages"]
                chunk_messages = cast(list[AIMessage], chunk_messages)

                messages = [
                    Message(
                        conversation_id=run.conversation_id,
                        role="assistant",
                        content=[{"type": "text", "text": str(chunk_message.content)}],
                    )
                    for chunk_message in chunk_messages
                ]

                session = session_factory()
                async with session.begin():
                    session.add_all(messages)

                for message in messages:
                    await redis.xadd(
                        make_event_stream_name(
                            run.conversation_id,
                        ),
                        cast(
                            dict[FieldT, EncodableT],
                            {
                                "id": str(message.id),
                                "event_type": OutboxEventTypeEnum.MESSAGE_CREATED,
                                "payload": json.dumps(
                                    {
                                        "message_id": str(message.id),
                                        "role": "assistant",
                                        "content": message.content,
                                        "created_at": message.created_at.isoformat(),
                                    }
                                ),
                            },
                        ),
                    )

            case _:
                pass


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
    try:
        await run_agent(redis, session_factory, run)

    except asyncio.CancelledError:
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
