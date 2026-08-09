import random
from datetime import UTC, datetime, timedelta

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]


def get_next_attempt_at(
    attempt: int,
    *,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> datetime:
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    delay *= random.uniform(0.8, 1.2)

    return datetime.now(UTC) + timedelta(seconds=delay)
