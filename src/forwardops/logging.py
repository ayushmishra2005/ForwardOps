import json
import logging
import os
from datetime import UTC, datetime


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "tenant_id",
            "investigation_id",
            "tool_name",
            "event",
            "provider",
            "model",
            "request_id",
            "duration_ms",
            "token_usage",
            "finish_reason",
            "http_status",
            "openai_error_code",
            "openai_error_type",
            "openai_request_id",
            "cluster",
            "rpc_operation",
            "result_category",
            "source_id",
            "query_capability",
        ):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if hasattr(record, "error_category"):
            payload["error_category"] = record.error_category
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception_type"] = record.exc_info[0].__name__
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(os.environ.get("FORWARDOPS_LOG_LEVEL", "INFO"))
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
