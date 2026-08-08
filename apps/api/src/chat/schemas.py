import uuid
from datetime import datetime
from typing import ClassVar, Literal

from db.models import JsonObject
from pydantic import BaseModel, ConfigDict


class ORMModel(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(from_attributes=True)


class ResponsePagination[C = str](BaseModel):
    total: int
    prev_cursor: C | None = None
    next_cursor: C | None = None


class ResponseData[T, C = str](BaseModel):
    data: list[T]
    pagination: ResponsePagination[C]


class ConversationRead(ORMModel):
    id: uuid.UUID
    next_seq: int
    created_at: datetime


class ContentBlockText(ORMModel):
    type: Literal["text"]
    text: str


ContentBlock = ContentBlockText


class MessageCreate(ORMModel):
    content: list[ContentBlock]


class MessageRead(MessageCreate):
    id: uuid.UUID
    seq: int
    conversation_id: uuid.UUID
    role: Literal["user", "assistant"]
    tool_call_id: str | None
    tool_call_args: JsonObject | None
    created_at: datetime
