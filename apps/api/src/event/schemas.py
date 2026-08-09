from enum import StrEnum


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
