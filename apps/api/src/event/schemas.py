from enum import StrEnum
from typing import Literal, TypedDict

from chat.schemas import MessageContentBlock
from redis.typing import EncodableT, FieldT


class OutboxEventStatusEnum(StrEnum):
    PENDING = "pending"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FAILED = "failed"


class OutboxEventTypeEnum(StrEnum):
    MESSAGE_CREATED = "message.created"
    MESSAGE_DELTA = "message.delta"


class OutboxEventAggregateEnum(StrEnum):
    MESSAGE = "message"
    CONVERSATION = "conversation"


RedisEvent = dict[FieldT, EncodableT]


class RedisEventPayloadMessage(TypedDict):
    message_id: str
    role: Literal["user", "assistant", "tool", "system"]
    content: list[MessageContentBlock]
    created_at: str


class RedisEventPayloadMessagePresence(TypedDict):
    typing: bool


RedisEventPayload = RedisEventPayloadMessage | RedisEventPayloadMessagePresence
