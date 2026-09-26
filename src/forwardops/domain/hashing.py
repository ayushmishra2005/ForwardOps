import hashlib
import json
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forwardops.domain.time import format_utc


def to_canonical(value: Any) -> Any:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        raise TypeError("canonical JSON does not allow floats")
    if isinstance(value, datetime):
        return format_utc(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, BaseModel):
        return to_canonical(value.model_dump(mode="python"))
    if isinstance(value, dict):
        return {str(key): to_canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_canonical(item) for item in value]
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        to_canonical(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def sha256_canonical(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()
