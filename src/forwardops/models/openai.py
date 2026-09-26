"""OpenAI chat-completions adapter.

The provider sees tool definitions and returns either tool requests or analysis.
It does not execute tools. Provider tool-call ids are discarded here.
"""

import asyncio
import json
import logging
import re
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from pydantic import ValidationError

from forwardops.domain.errors import ModelError
from forwardops.models.contracts import (
    ModelReply,
    ModelRequest,
    ProposedAnalysis,
    ProposedToolCall,
    TokenUsage,
)

_ANALYSIS_TOOL = "submit_analysis"
_HTTP_BODY_LIMIT = 8192
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
logger = logging.getLogger(__name__)
Transport = Callable[[str, dict[str, Any], dict[str, str], float], dict[str, Any]]


class ProviderResponseError(Exception):
    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def classify_http_status(status: int) -> str:
    if status == 429:
        return "rate_limited"
    if status in {401, 403, 408} or status >= 500:
        return "model_unavailable"
    return "provider_rejected"


class OpenAIModelProvider:
    provider_name = "openai"

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str = "https://api.openai.com/v1",
        timeout_seconds: float = 30,
        transport: Transport | None = None,
    ) -> None:
        if not base_url.startswith("https://"):
            raise ModelError("provider_rejected", "model provider base URL must use https")
        self.api_key = api_key
        self.model_name = model
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._transport = transport or post_json

    async def complete(self, request: ModelRequest) -> ModelReply:
        url = f"{self.base_url}/chat/completions"
        payload = _completion_payload(request, self.model_name)
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            body = await asyncio.to_thread(
                self._transport,
                url,
                payload,
                headers,
                self.timeout_seconds,
            )
        except ProviderResponseError as exc:
            raise ModelError(exc.category, "model provider request failed") from None
        except ModelError:
            raise
        except Exception:
            raise ModelError("model_unavailable", "model provider request failed") from None
        return map_completion(body, model=self.model_name)


def post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
) -> dict[str, Any]:
    encoded = json.dumps(payload).encode("utf-8")
    outgoing = urllib.request.Request(url, data=encoded, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(outgoing, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        category = classify_http_status(exc.code)
        _log_openai_http_error(exc, category=category, secret=_bearer_secret(headers))
        raise ProviderResponseError(category) from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise ProviderResponseError("model_unavailable") from None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        raise ProviderResponseError("malformed_output") from None
    if not isinstance(parsed, dict):
        raise ProviderResponseError("malformed_output")
    return parsed


def map_completion(body: dict[str, Any], *, model: str) -> ModelReply:
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise ModelError("malformed_output", "structured output was malformed")
    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        raise ModelError("malformed_output", "structured output was malformed")
    finish = choice.get("finish_reason")
    finish_reason = finish if isinstance(finish, str) and finish else "stop"
    usage = _usage(body.get("usage"))
    tool_calls = message.get("tool_calls") or []
    if tool_calls:
        return _from_tool_calls(
            tool_calls,
            model=model,
            finish_reason=finish_reason,
            usage=usage,
        )
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise ModelError("malformed_output", "structured output was malformed")
    return _from_content(content, model=model, finish_reason=finish_reason, usage=usage)


def _completion_payload(request: ModelRequest, model: str) -> dict[str, Any]:
    public = request.model_dump(mode="json")
    instructions = str(public.pop("instructions"))
    public["allowed_tools"] = [
        {"name": item["name"], "description": item["description"]}
        for item in public["allowed_tools"]
    ]
    tools = [
        {
            "type": "function",
            "function": {
                "name": item.name,
                "description": item.description,
                "parameters": _object_schema(item.parameters_schema),
            },
        }
        for item in request.allowed_tools
    ]
    tools.append(
        {
            "type": "function",
            "function": {
                "name": _ANALYSIS_TOOL,
                "description": (
                    "Return the structured analysis when the requested evidence is collected. "
                    "This does not execute a tool or a remediation."
                ),
                "parameters": _object_schema(request.output_schema),
            },
        }
    )
    return {
        "model": model,
        "temperature": 0,
        "max_completion_tokens": 2500,
        "parallel_tool_calls": True,
        "messages": [
            {"role": "system", "content": instructions},
            {"role": "user", "content": json.dumps(public, sort_keys=True)},
        ],
        "tools": tools,
        "tool_choice": "auto",
    }


def _object_schema(schema: dict[str, Any]) -> dict[str, Any]:
    copied = dict(schema)
    copied.pop("$schema", None)
    copied.pop("title", None)
    copied.setdefault("type", "object")
    return copied


def _from_tool_calls(
    tool_calls: list[Any],
    *,
    model: str,
    finish_reason: str,
    usage: TokenUsage | None,
) -> ModelReply:
    real: list[ProposedToolCall] = []
    analysis_args: dict[str, Any] | None = None
    for call in tool_calls:
        if not isinstance(call, dict):
            raise ModelError("malformed_output", "structured output was malformed")
        function = call.get("function")
        if not isinstance(function, dict):
            raise ModelError("malformed_output", "structured output was malformed")
        name = function.get("name")
        if not isinstance(name, str) or not name:
            raise ModelError("malformed_output", "structured output was malformed")
        arguments = _parse_arguments(function.get("arguments"))
        # The provider id on `call` is intentionally ignored.
        if name == _ANALYSIS_TOOL:
            analysis_args = arguments
            continue
        real.append(ProposedToolCall(name=name, arguments=arguments))
    if real:
        return ModelReply(
            kind="tool_requests",
            tool_requests=real,
            provider="openai",
            model=model,
            finish_reason=finish_reason or "tool_calls",
            token_usage=usage,
        )
    if analysis_args is None:
        raise ModelError("malformed_output", "structured output was malformed")
    return ModelReply(
        kind="analysis",
        analysis=_analysis(analysis_args),
        provider="openai",
        model=model,
        finish_reason=finish_reason,
        token_usage=usage,
    )


def _from_content(
    content: str,
    *,
    model: str,
    finish_reason: str,
    usage: TokenUsage | None,
) -> ModelReply:
    payload = _parse_json_object(_strip_fence(content))
    requests = payload.get("tool_requests")
    if isinstance(requests, list) and requests:
        parsed: list[ProposedToolCall] = []
        for item in requests:
            if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                raise ModelError("malformed_output", "structured output was malformed")
            arguments = item.get("arguments") or {}
            if not isinstance(arguments, dict):
                raise ModelError("malformed_output", "structured output was malformed")
            parsed.append(ProposedToolCall(name=item["name"], arguments=arguments))
        return ModelReply(
            kind="tool_requests",
            tool_requests=parsed,
            provider="openai",
            model=model,
            finish_reason=finish_reason,
            token_usage=usage,
        )
    analysis_payload = (
        payload.get("analysis") if isinstance(payload.get("analysis"), dict) else payload
    )
    if not isinstance(analysis_payload, dict) or "findings" not in analysis_payload:
        raise ModelError("malformed_output", "structured output was malformed")
    return ModelReply(
        kind="analysis",
        analysis=_analysis(analysis_payload),
        provider="openai",
        model=model,
        finish_reason=finish_reason,
        token_usage=usage,
    )


def _analysis(arguments: dict[str, Any]) -> ProposedAnalysis:
    payload = (
        arguments.get("analysis") if isinstance(arguments.get("analysis"), dict) else arguments
    )
    if not isinstance(payload, dict):
        raise ModelError("malformed_output", "structured output was malformed")
    try:
        return ProposedAnalysis.model_validate(payload)
    except ValidationError:
        raise ModelError("malformed_output", "structured output was malformed") from None


def _parse_arguments(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise ModelError("malformed_output", "structured output was malformed")
    parsed = _parse_json_object(value)
    return parsed


def _parse_json_object(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        raise ModelError("malformed_output", "structured output was malformed") from None
    if not isinstance(parsed, dict):
        raise ModelError("malformed_output", "structured output was malformed")
    return parsed


def _strip_fence(content: str) -> str:
    text = content.strip()
    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _usage(value: Any) -> TokenUsage | None:
    if not isinstance(value, dict):
        return None
    total = _token(value, "total_tokens")
    input_tokens = _token(value, "input_tokens")
    if input_tokens is None:
        input_tokens = _token(value, "prompt_tokens")
    output_tokens = _token(value, "output_tokens")
    if output_tokens is None:
        output_tokens = _token(value, "completion_tokens")
    if total is None and input_tokens is None and output_tokens is None:
        return None
    return TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total,
    )


def _log_openai_http_error(
    exc: urllib.error.HTTPError,
    *,
    category: str,
    secret: str | None,
) -> None:
    """Log provider diagnostics. The error body and request are not logged."""
    code, error_type = _openai_error_tokens(_read_error_body(exc), secret=secret)
    logger.warning(
        "openai provider request failed",
        extra={
            "event": "openai_provider_error",
            "provider": "openai",
            "http_status": exc.code,
            "openai_error_code": code,
            "openai_error_type": error_type,
            "openai_request_id": _openai_request_id(exc.headers, secret=secret),
            "error_category": category,
        },
    )


def _bearer_secret(headers: dict[str, str]) -> str | None:
    authorization = headers.get("Authorization")
    if not isinstance(authorization, str) or not authorization.startswith("Bearer "):
        return None
    secret = authorization.removeprefix("Bearer ").strip()
    return secret or None


def _read_error_body(exc: urllib.error.HTTPError) -> bytes:
    try:
        raw = exc.read(_HTTP_BODY_LIMIT)
    except Exception:
        return b""
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, str):
        return raw.encode("utf-8", errors="replace")
    return b""


def _openai_error_tokens(raw: bytes, *, secret: str | None) -> tuple[str | None, str | None]:
    try:
        parsed = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None, None
    if not isinstance(parsed, dict):
        return None, None
    error = parsed.get("error")
    if not isinstance(error, dict):
        return None, None
    return _safe_token(error.get("code"), secret=secret), _safe_token(
        error.get("type"), secret=secret
    )


def _openai_request_id(headers: Any, *, secret: str | None) -> str | None:
    if headers is None or not hasattr(headers, "get"):
        return None
    for name in ("x-request-id", "openai-request-id"):
        token = _safe_token(headers.get(name), secret=secret)
        if token is not None:
            return token
    return None


def _safe_token(value: Any, *, secret: str | None) -> str | None:
    if isinstance(value, bool) or not isinstance(value, str):
        return None
    text = value.strip()
    if _SAFE_TOKEN.fullmatch(text) is None:
        return None
    lowered = text.lower()
    if lowered.startswith("sk-") or "bearer" in lowered or "authorization" in lowered:
        return None
    if secret and secret in text:
        return None
    return text


def _token(value: dict[str, Any], key: str) -> int | None:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None
    return raw
