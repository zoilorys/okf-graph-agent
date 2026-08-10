import json
import uuid

from db import Message
from db.models import OutboxEvent
from event import OutboxEventTypeEnum
from event.schemas import (
    RedisEvent,
    RedisEventPayload,
    RedisEventPayloadMessage,
)
from langchain.messages import (
    AIMessage,
    AIMessageChunk,
    AnyMessage,
    HumanMessage,
    ToolMessage,
)


class UnhandledLangchainMessageTypeException(Exception):
    """Error returned by `langchain_message_to_model` for unhandled message types"""


def langchain_message_to_model(
    message: AnyMessage, conversation_id: uuid.UUID
) -> Message:
    if isinstance(message, AIMessage):
        return Message(
            conversation_id=conversation_id,
            role="assistant",
            content=[{"type": "text", "text": str(message.content)}],
        )
    elif isinstance(message, HumanMessage):
        return Message(
            conversation_id=conversation_id,
            role="user",
            content=[{"type": "text", "text": str(message.content)}],
        )
    elif isinstance(message, ToolMessage):
        return Message(
            conversation_id=conversation_id,
            role="tool",
            content=[],
            tool_call_id=message.tool_call_id,
        )
    elif isinstance(message, AIMessageChunk):
        return Message(
            conversation_id=conversation_id,
            role="assistant",
            content=[{"type": "text", "text": str(message.content)}],
        )
    else:
        raise UnhandledLangchainMessageTypeException(
            f"Unhandled langchain message type! {message.__class__.__name__}"
        )


def message_model_to_redis_event_payload(message: Message) -> RedisEventPayload:
    return RedisEventPayloadMessage(
        message_id=str(message.id),
        role=message.role,
        content=message.content,
        created_at=message.created_at.isoformat(),
    )


def message_model_to_redis_event(
    event_type: OutboxEventTypeEnum, message: Message
) -> RedisEvent:
    return {
        "id": str(message.id),
        "event_type": event_type,
        "payload": json.dumps(message_model_to_redis_event_payload(message)),
    }


def event_model_to_redis_event(event: OutboxEvent) -> RedisEvent:
    return {
        "id": str(event.aggregate_id),
        "event_type": event.event_type,
        "payload": json.dumps(event.payload),
    }


def make_presence_event(typing: bool = False) -> RedisEvent:
    return {
        "id": str(uuid.uuid7()),
        "event_type": OutboxEventTypeEnum.PRESENCE,
        "payload": json.dumps({"typing": typing}),
    }
