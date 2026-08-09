import os

from .models import AgentRun, Base, Conversation, Message, OutboxEvent

DATABASE_URL_ENV = os.getenv("DATABASE_URL")
DATABASE_URL = (
    DATABASE_URL_ENV
    if DATABASE_URL_ENV != None
    else "postgresql+psycopg://postgres:postgres@localhost:5432/okf-agent-db"
)
REDIS_URL_ENV = os.getenv("REDIS_URL")
REDIS_URL = REDIS_URL_ENV if REDIS_URL_ENV != None else "redis://localhost:6379"

__all__ = [
    "DATABASE_URL",
    "REDIS_URL",
    "AgentRun",
    "Base",
    "Conversation",
    "Message",
    "OutboxEvent",
]
