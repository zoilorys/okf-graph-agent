import uuid


def make_event_stream_name(conversation_id: uuid.UUID | str) -> str:
    return f"chat:conversations:{conversation_id}:events"
