from datetime import UTC, datetime, timedelta

MAX_WINDOW = timedelta(hours=1)


def parse_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    moment = value.astimezone(UTC)
    if moment.microsecond:
        text = moment.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".")
        return f"{text}Z"
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def require_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(UTC)


def preceding_window(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start = require_aware(start)
    end = require_aware(end)
    if start >= end:
        raise ValueError("window start must be before end")
    duration = end - start
    if duration > MAX_WINDOW:
        raise ValueError("window exceeds 1 hour")
    return start - duration, start
