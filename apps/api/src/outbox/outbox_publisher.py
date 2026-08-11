import asyncio
import logging
import uuid

from chat.utils import event_model_to_redis_event
from common import get_next_attempt_at
from db import OutboxEvent
from event import OutboxEventStatusEnum
from redis.asyncio import Redis
from sqlalchemy import and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from outbox.utils import make_event_stream_name

logger = logging.getLogger(__name__)


MAX_RETRIES: int = 3


async def claim_pending_events(
    session_factory: async_sessionmaker[AsyncSession],
    limit: int = 5,
):
    async with session_factory.begin() as session:
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

        events = (await session.execute(stmt)).scalars().all()

        for event in events:
            event.status = OutboxEventStatusEnum.PUBLISHING

    return [(event.id, event.conversation_id) for event in events]


async def publish(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
    event_id: uuid.UUID,
    conversation_id: uuid.UUID,
):
    async with session_factory.begin() as session:
        event = await session.get_one(OutboxEvent, event_id)

    await redis.xadd(
        make_event_stream_name(conversation_id),
        event_model_to_redis_event(event),
    )


async def mark_published(
    session_factory: async_sessionmaker[AsyncSession],
    event_id: uuid.UUID,
):
    async with session_factory.begin() as session:
        stmt = (
            update(OutboxEvent)
            .where(
                and_(
                    OutboxEvent.id == event_id,
                    OutboxEvent.status == OutboxEventStatusEnum.PUBLISHING,
                )
            )
            .values(status=OutboxEventStatusEnum.PUBLISHED, published_at=func.now())
        )

        await session.execute(stmt)


async def mark_retry_or_failed(
    session_factory: async_sessionmaker[AsyncSession],
    event_id: uuid.UUID,
):
    async with session_factory.begin() as session:
        event = await session.get_one(OutboxEvent, event_id)

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
    event_id: uuid.UUID,
    conversation_id: uuid.UUID,
):
    try:
        await publish(redis, session_factory, event_id, conversation_id)

    except asyncio.CancelledError:
        raise

    except Exception:
        logger.exception(
            "Failed to publish outbox event %s",
            event_id,
        )

        await mark_retry_or_failed(session_factory, event_id)
        return

    await mark_published(session_factory, event_id)


async def outbox_publisher(
    redis: Redis,
    session_factory: async_sessionmaker[AsyncSession],
):
    while True:
        claimed_event_ids = await claim_pending_events(session_factory)

        if len(claimed_event_ids) == 0:
            await asyncio.sleep(0.2)
            continue

        async with asyncio.TaskGroup() as tg:
            for event_id, conversation_id in claimed_event_ids:
                tg.create_task(
                    publish_one(redis, session_factory, event_id, conversation_id)
                )


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
