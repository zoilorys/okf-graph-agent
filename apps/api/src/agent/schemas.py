from enum import StrEnum
from typing import Any, TypedDict

from langchain.messages import AIMessageChunk, AnyMessage


class AgentRunStatusEnum(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPT_REQUESTED = "interrupt_requested"
    INTERRUPTED = "interrupted"


class UpdatesChunkMessageModel(TypedDict):
    messages: list[AnyMessage]


class UpdatesChunkMessage(TypedDict):
    model: UpdatesChunkMessageModel


MessagesChunkMessage = tuple[AIMessageChunk, dict[str, Any]]
