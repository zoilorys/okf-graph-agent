from pathlib import Path

from agent.schemas import AgentRunStatusEnum
from chat.utils import message_model_to_outbox
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[4]
load_dotenv(REPO_ROOT / ".env")

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, TypedDict, cast

from agent.agent_runner import agent_runner_supervisor
from background import make_event_stream_name, outbox_publisher_supervisor
from chat.schemas import (
    ConversationRead,
    MessageCreate,
    MessageRead,
    ResponseData,
    ResponsePagination,
)
from db import DATABASE_URL, REDIS_URL, AgentRun
from db.models import Conversation, Message
from fastapi import Depends, FastAPI, Request, status
from redis.asyncio import Redis
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sse_starlette import EventSourceResponse, ServerSentEvent


class AppState(TypedDict):
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[AppState]:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=True,
    )

    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )

    redis = Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=None,
    )

    outbox_task_supervisor = asyncio.create_task(
        outbox_publisher_supervisor(redis, session_factory)
    )
    agent_runner_task_supervisor = asyncio.create_task(
        agent_runner_supervisor(redis, session_factory)
    )

    yield {
        "engine": engine,
        "session_factory": session_factory,
        "redis": redis,
    }

    outbox_task_supervisor.cancel()
    agent_runner_task_supervisor.cancel()

    await engine.dispose()
    await redis.close()


async def get_redis(request: Request) -> Redis:
    return request.state.redis


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = request.state.session_factory

    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


app = FastAPI(lifespan=lifespan)


@app.post(
    "/api/conversations",
    status_code=status.HTTP_201_CREATED,
)
async def post_conversation(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ConversationRead:
    async with session.begin():
        conversation = Conversation()

        session.add(conversation)
        await session.flush()

        return ConversationRead.model_validate(conversation)


@app.get("/api/conversations/{conversation_id}/messages")
async def get_conversation_messages(
    conversation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ResponseData[MessageRead, int]:
    stmt = (
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.seq)
    )

    result = await session.scalars(stmt)
    messages = [MessageRead.model_validate(message) for message in result]
    return ResponseData(
        data=messages,
        pagination=ResponsePagination(total=len(messages)),
    )


@app.post(
    "/api/conversations/{conversation_id}/messages",
    status_code=status.HTTP_201_CREATED,
)
async def post_conversation_message(
    conversation_id: str,
    payload: MessageCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> MessageRead:
    async with session.begin():
        message = Message(
            conversation_id=conversation_id,
            role="user",
            content=payload.model_dump(mode="json")["content"],
        )

        session.add(message)
        await session.flush()

        event = message_model_to_outbox(message)

        session.add(event)

        agent_run = AgentRun(
            conversation_id=message.conversation_id,
            trigger_type="message",
            trigger_id=message.id,
        )

        session.add(agent_run)

        return MessageRead.model_validate(message)


@app.post(
    "/api/conversations/{conversation_id}/interrupt",
    status_code=status.HTTP_200_OK,
)
async def interrupt_conversation(
    conversation_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    async with session.begin():
        stmt = select(AgentRun).where(
            and_(
                AgentRun.conversation_id == conversation_id,
                AgentRun.status == AgentRunStatusEnum.RUNNING,
            )
        )

        runs = list((await session.execute(stmt)).scalars().all())

        for run in runs:
            run.status = AgentRunStatusEnum.INTERRUPT_REQUESTED


@app.get("/api/conversations/{conversation_id}/events")
async def get_conversation_events(
    conversation_id: str,
    request: Request,
    redis: Annotated[Redis, Depends(get_redis)],
):
    stream_name: str = make_event_stream_name(conversation_id)

    last_event_id: str = request.headers.get("last-event-id", "$")

    async def generate() -> AsyncGenerator[ServerSentEvent, None]:
        cursor = last_event_id

        while not await request.is_disconnected():
            result = await redis.xread(
                {stream_name: cursor},
                count=100,
                block=5_000,
            )

            if not result:
                continue

            for _, messages in result:
                for message_id, data in cast(
                    list[tuple[str, dict[str, str]]], messages
                ):
                    cursor = message_id

                    yield ServerSentEvent(
                        id=message_id,
                        event=data["event_type"],
                        data=data["payload"],
                    )

    return EventSourceResponse(generate())
