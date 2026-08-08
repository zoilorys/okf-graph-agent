import asyncio
import json
import logging
import random
from datetime import UTC, datetime, timedelta

from chat.schemas import OutboxEventStatusType
from db import OutboxEvent
from redis.asyncio import Redis
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from background.utils import make_event_stream_name

logger = logging.getLogger(__name__)


MAX_RETRIES: int = 3


def get_next_attempt_at(
    attempt: int,
    *,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> datetime:
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    delay *= random.uniform(0.8, 1.2)

    return datetime.now(UTC) + timedelta(seconds=delay)


async def claim_pending_events(
    session_factory: async_sessionmaker[AsyncSession],
    limit: int = 5,
) -> list[OutboxEvent]:
    session = session_factory()

    async with session.begin():
        stmt = (
            select(OutboxEvent)
            .where(
                and_(
                    OutboxEvent.status == OutboxEventStatusType.PENDING,
                    OutboxEvent.next_attempt_at <= func.now(),
                )
            )
            .limit(limit)
            .order_by(OutboxEvent.seq.asc())
            .with_for_update(skip_locked=True)
        )

        select_result = await session.execute(stmt)

        events = list(select_result.scalars().all())

        for event in events:
            event.status = OutboxEventStatusType.PUBLISHING

    return events


async def publish(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    event: OutboxEvent,
):
    session = session_factory()

    stream_name = make_event_stream_name(event.conversation_id)

    await redis.xadd(
        stream_name,
        {
            "id": str(event.id),
            "event_type": event.event_type,
            "payload": json.dumps(event.payload),
        },
    )

    async with session.begin():
        event.status = OutboxEventStatusType.PUBLISHED
        event.published_at = func.now()


async def mark_retry_or_failed(
    session_factory: async_sessionmaker[AsyncSession],
    event: OutboxEvent,
):
    session = session_factory()

    async with session.begin():
        attempt = event.attempt + 1

        if attempt > MAX_RETRIES:
            event.status = OutboxEventStatusType.FAILED
        else:
            event.status = OutboxEventStatusType.PENDING
            event.attempt = attempt
            event.next_attempt_at = get_next_attempt_at(attempt)


async def publish_one(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    event: OutboxEvent,
):
    try:
        await publish(redis, session_factory, event)

    except asyncio.CancelledError:
        raise

    except Exception:
        logger.exception(
            "Failed to publish outbox event %s",
            event.id,
        )

        await mark_retry_or_failed(session_factory, event)


async def outbox_publisher(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
):
    while True:
        claimed_events = await claim_pending_events(session_factory)

        if len(claimed_events) == 0:
            await asyncio.sleep(0.2)
            continue

        async with asyncio.TaskGroup() as tg:
            for event in claimed_events:
                tg.create_task(publish_one(redis, session_factory, event))


async def outbox_publisher_supervisor(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
):
    while True:
        try:
            await outbox_publisher(redis, session_factory)

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("Outbox publisher crashed")
            await asyncio.sleep(1)
