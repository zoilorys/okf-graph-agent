import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, TypedDict

from chat.schemas import (
    ConversationRead,
    MessageCreate,
    MessageRead,
    ResponseData,
    ResponsePagination,
)
from db import DATABASE_URL, REDIS_URL
from db.models import Conversation, Message, OutboxEvent
from fastapi import Depends, FastAPI, Request, status
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


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
    )

    yield {
        "engine": engine,
        "session_factory": session_factory,
        "redis": redis,
    }

    await engine.dispose()
    await redis.close()


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

        event = OutboxEvent(
            conversation_id=message.conversation_id,
            aggregate_type="message",
            aggregate_id=message.id,
            event_type="message.created",
            payload={
                "role": message.role,
                "content": message.content,
                "created_at": message.created_at.isoformat(),
            },
        )

        session.add(event)

        return MessageRead.model_validate(message)
