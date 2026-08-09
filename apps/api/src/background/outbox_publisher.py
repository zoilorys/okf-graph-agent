import asyncio
import json
import logging

from common import get_next_attempt_at
from db import OutboxEvent
from event import OutboxEventStatusEnum
from redis.asyncio import Redis
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from background.utils import make_event_stream_name

logger = logging.getLogger(__name__)


MAX_RETRIES: int = 3


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
                    OutboxEvent.status == OutboxEventStatusEnum.PENDING,
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
            event.status = OutboxEventStatusEnum.PUBLISHING

    return events


async def publish(
    redis: Redis,
    event: OutboxEvent,
):
    stream_name = make_event_stream_name(event.conversation_id)

    await redis.xadd(
        stream_name,
        {
            "id": str(event.id),
            "event_type": event.event_type,
            "payload": json.dumps(event.payload),
        },
    )


async def mark_published(
    session_factory: async_sessionmaker[AsyncSession],
    event: OutboxEvent,
):
    session = session_factory()

    async with session.begin():
        event.status = OutboxEventStatusEnum.PUBLISHED
        event.published_at = func.now()


async def mark_retry_or_failed(
    session_factory: async_sessionmaker[AsyncSession],
    event: OutboxEvent,
):
    session = session_factory()

    async with session.begin():
        attempt = event.attempt + 1

        if attempt > MAX_RETRIES:
            event.status = OutboxEventStatusEnum.FAILED
        else:
            event.status = OutboxEventStatusEnum.PENDING
            event.attempt = attempt
            event.next_attempt_at = get_next_attempt_at(attempt)


async def publish_one(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    event: OutboxEvent,
):
    try:
        await publish(redis, event)

    except asyncio.CancelledError:
        raise

    except Exception:
        logger.exception(
            "Failed to publish outbox event %s",
            event.id,
        )

        await mark_retry_or_failed(session_factory, event)
        return

    await mark_published(session_factory, event)


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
