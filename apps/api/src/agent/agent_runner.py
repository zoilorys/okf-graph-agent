import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
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

from agent.active_runs import ActiveRuns, RunControl
from agent.schemas import AgentRunStatusEnum, MessagesChunkMessage, UpdatesChunkMessage

from .agent import make_agent

logger = logging.getLogger(__name__)


MAX_ATTEMPTS: int = 3
MAX_CONCURRENT_RUNS = 5
RUN_POLL_INTERVAL_SECONDS = 0.2
INTERRUPT_RECONCILE_INTERVAL_SECONDS = 0.5
INTERRUPT_CHANNEL = "agent_runs:interrupt"

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


async def get_run_for_update(
    session: AsyncSession,
    run_id: uuid.UUID,
):
    stmt = select(AgentRun).where(AgentRun.id == run_id).with_for_update()

    return (await session.execute(stmt)).scalar_one()


async def get_run_status(
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
) -> str:
    async with session_factory.begin() as session:
        run = await session.get_one(AgentRun, run_id)
        return run.status


class AgentInterruptedException(Exception):
    pass


async def interruptible_stream(
    stream: AsyncIterator[Any],
    interrupted: asyncio.Event,
) -> AsyncIterator[Any]:
    iterator = aiter(stream)
    interrupt_task = asyncio.create_task(interrupted.wait())
    next_chunk_task: asyncio.Task[Any] | None = None

    try:
        while True:
            next_chunk_task = asyncio.ensure_future(anext(iterator))

            done, _ = await asyncio.wait(
                {next_chunk_task, interrupt_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if interrupt_task in done:
                next_chunk_task.cancel()

                try:
                    await next_chunk_task
                except asyncio.CancelledError:
                    raise AgentInterruptedException

            try:
                yield next_chunk_task.result()
            except StopAsyncIteration:
                return

            next_chunk_task = None

    finally:
        interrupt_task.cancel()

        tasks: list[asyncio.Task[Any]] = [interrupt_task]

        if next_chunk_task is not None and not next_chunk_task.done():
            next_chunk_task.cancel()

            tasks.append(next_chunk_task)

        await asyncio.gather(*tasks, return_exceptions=True)

        close = getattr(iterator, "aclose", None)
        if close is not None:
            await close()


async def persist_update_message(
    session_factory: async_sessionmaker[AsyncSession],
    conversation_id: uuid.UUID,
    chunk: UpdatesChunkMessage,
    message_id_map: dict[str, uuid.UUID],
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    chunk_messages = chunk["model"]["messages"]

    messages: list[Message] = []

    for chunk_message in chunk_messages:
        message = langchain_message_to_model(chunk_message, conversation_id)

        chunk_message_id = str(chunk_message.id)
        message_id = message_id_map.get(chunk_message_id, None)

        if message_id is None:
            message_id = uuid.uuid7()
            message_id_map[chunk_message_id] = message_id

        message.id = message_id
        messages.append(message)

    async with session_factory.begin() as session:
        session.add_all(messages)
        await session.flush()

        session.add_all([message_model_to_outbox(message) for message in messages])

    for message in messages:
        incomplete_messages.pop(message.id, None)


async def run_agent(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    control: RunControl,
    conversation_id: uuid.UUID,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
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

    stream = agent.astream(
        InputAgentState(messages=agent_messages),
        stream_mode=["messages", "updates"],
    )

    async for mode, chunk in interruptible_stream(stream, control.interrupted):
        match mode:
            case "messages":
                chunk = cast(MessagesChunkMessage, chunk)
                chunk_message, _metadata = chunk

                message = langchain_message_to_model(chunk_message, conversation_id)

                chunk_message_id = str(chunk_message.id)
                message_id = message_id_map.get(chunk_message_id, None)

                if message_id is None:
                    message_id = uuid.uuid7()
                    message_id_map[chunk_message_id] = message_id

                existing_chunk = incomplete_messages.get(message_id)

                if existing_chunk is None:
                    incomplete_messages[message_id] = chunk_message
                else:
                    incomplete_messages[message_id] = existing_chunk + chunk_message

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

                await persist_update_message(
                    session_factory,
                    conversation_id,
                    chunk,
                    message_id_map,
                    incomplete_messages,
                )

            case _:
                logger.debug("Ignoring agent stream mode %s", mode)


async def persist_incomplete_messages(
    session: AsyncSession,
    conversation_id: uuid.UUID,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    messages: list[Message] = []

    for message_id, chunk_message in incomplete_messages.items():
        message = langchain_message_to_model(chunk_message, conversation_id)
        message.id = message_id
        messages.append(message)

    session.add_all(messages)
    await session.flush()

    session.add_all([message_model_to_outbox(message) for message in messages])


async def mark_completed(
    session_factory: async_sessionmaker[AsyncSession], run_id: uuid.UUID
):
    async with session_factory.begin() as session:
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
            .returning(AgentRun.id)
        )

        return (await session.execute(stmt)).scalar_one_or_none() is not None


async def mark_interrupted(
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    async with session_factory.begin() as session:
        run = await get_run_for_update(session, run_id)

        if run.status != AgentRunStatusEnum.INTERRUPT_REQUESTED:
            return False

        await persist_incomplete_messages(
            session,
            conversation_id,
            incomplete_messages,
        )

        run.status = AgentRunStatusEnum.INTERRUPTED
        run.completed_at = func.now()

        return True


async def mark_retry_failed_or_interrupted(
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    async with session_factory.begin() as session:
        run = await get_run_for_update(session, run_id)

        if run.status == AgentRunStatusEnum.INTERRUPT_REQUESTED:
            await persist_incomplete_messages(
                session,
                conversation_id,
                incomplete_messages,
            )

            run.status = AgentRunStatusEnum.INTERRUPTED
            run.completed_at = func.now()
            return

        if run.status != AgentRunStatusEnum.RUNNING:
            return

        next_attempt = run.attempt + 1

        if next_attempt > MAX_ATTEMPTS:
            run.status = AgentRunStatusEnum.FAILED
            run.completed_at = func.now()
        else:
            run.status = AgentRunStatusEnum.PENDING
            run.attempt = next_attempt
            run.next_attempt_at = get_next_attempt_at(next_attempt)


async def settle_cancelled_run(
    session_factory: async_sessionmaker[AsyncSession],
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
    incomplete_messages: dict[uuid.UUID, AIMessageChunk],
):
    async with session_factory.begin() as session:
        run = await get_run_for_update(session, run_id)

        if run.status == AgentRunStatusEnum.INTERRUPT_REQUESTED:
            await persist_incomplete_messages(
                session,
                conversation_id,
                incomplete_messages,
            )

            run.status = AgentRunStatusEnum.INTERRUPTED
            run.completed_at = func.now()
            return

        if run.status == AgentRunStatusEnum.RUNNING:
            run.status = AgentRunStatusEnum.PENDING
            run.next_attempt_at = func.now()


async def publish_presence(
    redis: Redis,
    conversation_id: uuid.UUID,
    *,
    typing: bool,
):
    await redis.xadd(
        make_event_stream_name(conversation_id),
        make_presence_event(typing=typing),
    )


async def publish_presence_best_effort(
    redis: Redis,
    conversation_id: uuid.UUID,
    *,
    typing: bool,
):
    try:
        await publish_presence(
            redis,
            conversation_id,
            typing=typing,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "Could not publish presence for conversation %s",
            conversation_id,
        )


async def run_agent_safe(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    active_runs: ActiveRuns,
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
):
    incomplete_messages: dict[uuid.UUID, AIMessageChunk] = {}
    control = active_runs.register(run_id)
    presence_started = False

    try:
        status = await get_run_status(session_factory, run_id)

        if status == AgentRunStatusEnum.INTERRUPT_REQUESTED:
            raise AgentInterruptedException

        if status != AgentRunStatusEnum.RUNNING:
            raise RuntimeError(f"Cannot execute agent run {run_id} is status {status}")

        await publish_presence(
            redis,
            conversation_id,
            typing=True,
        )
        presence_started = True

        await run_agent(
            redis,
            session_factory,
            control,
            conversation_id,
            incomplete_messages,
        )

        if control.interrupted.is_set():
            raise AgentInterruptedException

        if not await mark_completed(session_factory, run_id):
            status = await get_run_status(session_factory, run_id)

            if status == AgentRunStatusEnum.INTERRUPT_REQUESTED:
                raise AgentInterruptedException

            raise RuntimeError(
                f"Could not complete agent run {run_id}: status is {status}"
            )

    except AgentInterruptedException:
        logger.info("Agent run %s was interrupted", run_id)
        await mark_interrupted(
            session_factory,
            run_id,
            conversation_id,
            incomplete_messages,
        )

    except asyncio.CancelledError:
        logger.exception("Agent run %s interrupted", run_id)

        await asyncio.shield(
            settle_cancelled_run(
                session_factory,
                run_id,
                conversation_id,
                incomplete_messages,
            )
        )
        raise

    except Exception:
        logger.exception("Agent run %s failed", run_id)

        await mark_retry_failed_or_interrupted(
            session_factory,
            run_id,
            conversation_id,
            incomplete_messages,
        )

    finally:
        active_runs.unregister(run_id)

        if presence_started:
            await asyncio.shield(
                publish_presence_best_effort(
                    redis,
                    conversation_id,
                    typing=False,
                )
            )


async def listen_for_interrupts(
    redis: Redis,
    active_runs: ActiveRuns,
):
    while True:
        try:
            async with redis.pubsub(ignore_subscribe_messages=True) as pubsub:
                await pubsub.subscribe(INTERRUPT_CHANNEL)

                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue

                    try:
                        run_id = uuid.UUID(str(message["data"]))
                    except (TypeError, ValueError):
                        logger.warning(
                            "Ignoring malformed interrupt message: %r",
                            message["data"],
                        )
                        continue

                    active_runs.interrupt(run_id)

            raise RuntimeError("Redis interrupt subscription ended")

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Redis interrupt listener failed")
            await asyncio.sleep(1)


async def reconcile_interrupts(
    session_factory: async_sessionmaker[AsyncSession],
    active_runs: ActiveRuns,
):
    while True:
        try:
            run_ids = active_runs.run_ids()

            if run_ids:
                async with session_factory.begin() as session:
                    stmt = select(AgentRun.id).where(
                        and_(
                            AgentRun.id.in_(run_ids),
                            AgentRun.status == AgentRunStatusEnum.INTERRUPT_REQUESTED,
                        )
                    )

                    interrupted_run_ids = (await session.execute(stmt)).scalars().all()

                for run_id in interrupted_run_ids:
                    active_runs.interrupt(run_id)

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("Interrupt reconciliation failed")

        await asyncio.sleep(INTERRUPT_RECONCILE_INTERVAL_SECONDS)


async def run_with_semaphore(
    semaphore: asyncio.Semaphore,
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    active_runs: ActiveRuns,
    run_id: uuid.UUID,
    conversation_id: uuid.UUID,
):
    try:
        await run_agent_safe(
            redis,
            session_factory,
            active_runs,
            run_id,
            conversation_id,
        )
    finally:
        semaphore.release()


async def agent_runner(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    active_runs: ActiveRuns,
):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_RUNS)
    active_tasks: set[asyncio.Task[None]] = set()

    def observe_task(task: asyncio.Task[None]):
        active_tasks.discard(task)

        if task.cancelled():
            return

        exception = task.exception()

        if exception is not None:
            logger.error(
                "Agent execution task crashed",
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    try:
        while True:
            await semaphore.acquire()

            try:
                claimed_runs = await claim_pending_runs(session_factory, limit=1)
            except BaseException:
                semaphore.release()
                raise

            if not claimed_runs:
                semaphore.release()
                await asyncio.sleep(RUN_POLL_INTERVAL_SECONDS)
                continue

            run_id, conversation_id = claimed_runs[0]

            task = asyncio.create_task(
                run_with_semaphore(
                    semaphore,
                    redis,
                    session_factory,
                    active_runs,
                    run_id,
                    conversation_id,
                )
            )
            active_tasks.add(task)
            task.add_done_callback(observe_task)

    finally:
        tasks = list(active_tasks)

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)


async def supervise_agent_runner(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    active_runs: ActiveRuns,
):
    while True:
        try:
            await agent_runner(
                redis,
                session_factory,
                active_runs,
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("Agent runner crashed")
            await asyncio.sleep(1)


async def agent_runner_supervisor(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
):
    active_runs = ActiveRuns()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            supervise_agent_runner(
                redis,
                session_factory,
                active_runs,
            )
        )
        tg.create_task(
            listen_for_interrupts(
                redis,
                active_runs,
            )
        )
        tg.create_task(
            reconcile_interrupts(
                session_factory,
                active_runs,
            )
        )
