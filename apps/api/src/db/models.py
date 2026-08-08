from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    UUID,
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]


class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__: str = "conversations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid7,
    )

    next_seq: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class Message(Base):
    __tablename__: str = "messages"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid7,
    )

    seq: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        nullable=False,
        unique=True,
    )

    conversation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("conversations.id", ondelete="CASCADE"),
        nullable=False,
    )

    role: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    content: Mapped[list[JsonObject]] = mapped_column(
        JSONB,
        nullable=False,
    )

    tool_call_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    tool_call_args: Mapped[JsonObject | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__: tuple[Index, ...] = (
        Index("ix_messages_conversation_id", "conversation_id", "seq"),
    )


class OutboxEvent(Base):
    __tablename__: str = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid7,
    )

    seq: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        nullable=False,
        unique=True,
    )

    conversation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("conversations.id", ondelete="CASCADE"),
        nullable=False,
    )

    aggregate_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )

    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")

    event_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    payload: Mapped[JsonObject] = mapped_column(
        JSONB,
        nullable=False,
    )

    attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="1",
    )

    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__: tuple[Index, ...] = (
        Index(
            "ix_outbox_unpublished_seq",
            "seq",
            postgresql_where=published_at.is_(None),
        ),
        Index(
            "ix_outbox_aggregate",
            "aggregate_type",
            "aggregate_id",
            "seq",
        ),
    )
