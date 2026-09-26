"""Read-only Grafana Tempo adapter.

The model supplies a logical source id and a trace id. The Tempo URL comes from
ForwardOps configuration. Span text is untrusted source data.
"""

import asyncio
import base64
import json
import logging
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from pydantic import ValidationError

from forwardops.domain.errors import ConfigError, ToolFailedError
from forwardops.domain.investigation import DATABASE_POOL_SCENARIO, InvestigationScope
from forwardops.domain.time import format_utc
from forwardops.integrations.replay import HandlerResult, Observation
from forwardops.tools.contracts import GetTraceInput, TraceSpanView, TraceView

logger = logging.getLogger(__name__)

GET_TRACE = "get_trace"
QUERY_VERSION = "v1"
_SOURCE_SYSTEM = "tempo"
_SOURCE_TYPE = "observability"
_TRACE_ID = re.compile(r"^[0-9a-f]{32}$")
_HEX_ID = re.compile(r"^[0-9a-fA-F]+$")
_ATTR_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,63}$")
_ERROR_CODE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_SENSITIVE = re.compile(
    r"(password|secret|token|authorization|cookie|dsn|sql|statement|credential|api[_-]?key)",
    re.IGNORECASE,
)
_MAX_ATTRS = 16
_MAX_ATTR_CHARS = 200
_MAX_NAME = 128
_REDACTED = "[redacted]"

Transport = Callable[[str, float, int], bytes]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        raise ToolFailedError("FORBIDDEN_URL", "the trace backend must not redirect")


def require_tempo_base(url: str) -> str:
    """Accept only a configured HTTP origin. No userinfo, query, or path."""
    if not isinstance(url, str) or any(character in url for character in "\r\n\t "):
        raise ConfigError("Tempo URL must be an http origin")
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ConfigError("Tempo URL must be an http origin")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ConfigError("Tempo URL must be an http origin")
    if parts.path not in {"", "/"}:
        raise ConfigError("Tempo URL must be an http origin")
    return url


def trace_request_url(base_url: str, trace_id: str) -> str:
    require_tempo_base(base_url)
    if _TRACE_ID.fullmatch(trace_id) is None:
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "trace id must be 32 hexadecimal characters")
    parts = urlsplit(base_url)
    netloc = parts.netloc
    return urlunsplit((parts.scheme, netloc, f"/api/traces/{trace_id}", "", ""))


def fetch_tempo(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 1:
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "trace response limit is outside policy")
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json"},
        method="GET",
    )
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            status = getattr(response, "status", 200)
            if status == 404:
                raise ToolFailedError("NOT_FOUND", "Tempo returned no trace for this id")
            if status != 200:
                raise ToolFailedError("TRACE_HTTP", "Tempo trace request failed", retryable=True)
            raw = response.read(max_bytes + 1)
    except ToolFailedError:
        raise
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise ToolFailedError("NOT_FOUND", "Tempo returned no trace for this id") from None
        raise ToolFailedError("TRACE_HTTP", "Tempo trace request failed", retryable=True) from None
    except TimeoutError:
        raise ToolFailedError(
            "TRACE_TIMEOUT", "Tempo trace request timed out", retryable=True
        ) from None
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            raise ToolFailedError(
                "TRACE_TIMEOUT", "Tempo trace request timed out", retryable=True
            ) from None
        raise ToolFailedError("TRACE_HTTP", "Tempo trace request failed", retryable=True) from None
    except OSError:
        raise ToolFailedError("TRACE_HTTP", "Tempo trace request failed", retryable=True) from None
    if len(raw) > max_bytes:
        raise ToolFailedError("RESPONSE_TOO_LARGE", "Tempo trace response exceeded the read limit")
    return raw


def normalize_tempo_payload(
    raw: bytes,
    *,
    trace_id: str,
    source_id: str,
    max_spans: int,
) -> TraceView:
    if not isinstance(max_spans, int) or isinstance(max_spans, bool) or not 1 <= max_spans <= 200:
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "span limit is outside policy")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed") from None
    if not isinstance(payload, dict):
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    batches = payload.get("batches", payload.get("resourceSpans"))
    if not isinstance(batches, list) or not batches:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    spans = _flatten(batches, trace_id)
    if not spans:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    spans.sort(key=lambda item: (item.start_time, item.span_id))
    truncated = len(spans) > max_spans
    kept = spans[:max_spans]
    root = _root_span(kept)
    try:
        return TraceView(
            source_ref=source_id,
            trace_id=trace_id,
            root_service=root.service_name,
            root_operation=root.span_name,
            start_time=root.start_time,
            duration_ms=root.duration_ms,
            status=root.status,
            spans=kept,
            service_names=sorted({item.service_name for item in kept}),
            truncated=truncated,
            span_count=len(kept),
        )
    except ValidationError:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed") from None


class TempoHandlers:
    """One configured Tempo origin. The URL is not a tool argument."""

    def __init__(
        self,
        source_id: str,
        base_url: str,
        *,
        timeout_seconds: float = 8,
        max_spans: int = 32,
        max_bytes: int = 65_536,
        transport: Transport | None = None,
    ) -> None:
        if (
            not isinstance(source_id, str)
            or re.fullmatch(r"[a-z][a-z0-9-]{0,62}", source_id) is None
        ):
            raise ConfigError("Tempo source id must be a logical identifier")
        self.source_id = source_id
        self._base_url = require_tempo_base(base_url)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int | float)
            or not 1 <= float(timeout_seconds) <= 30
        ):
            raise ConfigError("Tempo timeout must be between 1 and 30 seconds")
        self.timeout_seconds = float(timeout_seconds)
        self.max_spans = max_spans
        self.max_bytes = max_bytes
        self._transport = transport or (
            lambda url, timeout, limit: fetch_tempo(url, timeout, limit)
        )

    def __repr__(self) -> str:
        return f"TempoHandlers(source_id={self.source_id!r})"

    def serves(self, source_ref: str) -> bool:
        return source_ref == self.source_id

    async def get_trace(
        self,
        scope: InvestigationScope,
        arguments: GetTraceInput,
    ) -> HandlerResult:
        started = time.perf_counter()
        category = "failed"
        try:
            result = await self._read(scope, arguments)
            category = "succeeded"
            return result
        except ToolFailedError as exc:
            category = exc.code.lower()
            raise
        finally:
            logger.info(
                "tempo read finished",
                extra={
                    "event": "tempo_read",
                    "source_id": self.source_id,
                    "query_capability": GET_TRACE,
                    "result_category": category,
                    "duration_ms": max(0, int((time.perf_counter() - started) * 1000)),
                    **({} if category == "succeeded" else {"error_category": category}),
                },
            )

    async def _read(self, scope: InvestigationScope, arguments: GetTraceInput) -> HandlerResult:
        if scope.scenario_id != DATABASE_POOL_SCENARIO:
            raise ToolFailedError("FORBIDDEN_RESOURCE", "tool is outside the selected playbook")
        if scope.trace_source_ref != self.source_id or arguments.trace_source_ref != self.source_id:
            raise ToolFailedError(
                "FORBIDDEN_RESOURCE",
                "trace source is outside the investigation scope",
            )
        url = trace_request_url(self._base_url, arguments.trace_id)
        try:
            raw = await asyncio.to_thread(
                self._transport, url, self.timeout_seconds, self.max_bytes
            )
        except ToolFailedError:
            raise
        except TimeoutError:
            raise ToolFailedError(
                "TRACE_TIMEOUT", "Tempo trace request timed out", retryable=True
            ) from None
        except Exception:
            raise ToolFailedError(
                "TRACE_HTTP", "Tempo trace request failed", retryable=True
            ) from None
        if not isinstance(raw, bytes) or len(raw) > self.max_bytes:
            raise ToolFailedError(
                "RESPONSE_TOO_LARGE", "Tempo trace response exceeded the read limit"
            )
        view = normalize_tempo_payload(
            raw,
            trace_id=arguments.trace_id,
            source_id=self.source_id,
            max_spans=self.max_spans,
        )
        observed = format_utc(datetime.now(UTC))
        return HandlerResult(view.model_dump(mode="json"), (_observation(view, observed),))


def _observation(view: TraceView, observed_at: str) -> Observation:
    payload = view.model_dump(mode="json")
    payload["source_id"] = view.source_ref
    return Observation(
        kind="observability.trace",
        source_type=_SOURCE_TYPE,
        source_system=_SOURCE_SYSTEM,
        source_locator={
            "source_id": view.source_ref,
            "capability": GET_TRACE,
            "capability_version": QUERY_VERSION,
            "trace_id": view.trace_id,
        },
        event_time=format_utc(view.start_time),
        time_basis="trace_start",
        correlation={
            "source_id": view.source_ref,
            "trace_id": view.trace_id,
            "service_name": view.root_service,
        },
        payload=payload,
        summary=(
            f"Trace {view.trace_id} on {view.root_service} {view.root_operation} is {view.status}."
        ),
        provenance={
            "synthetic": False,
            "untrusted_source": True,
            "untrusted_text": True,
            "source_id": view.source_ref,
            "capability": GET_TRACE,
            "capability_version": QUERY_VERSION,
        },
        coverage={
            "complete_for_record": not view.truncated,
            "truncated": view.truncated,
            "span_count": view.span_count,
            "row_count": view.span_count,
        },
        retrieval_time=observed_at,
    )


def _flatten(batches: list[Any], expected_trace_id: str) -> list[TraceSpanView]:
    spans: list[TraceSpanView] = []
    for batch in batches:
        if not isinstance(batch, dict):
            raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
        service = _service_name(batch.get("resource"))
        groups = batch.get(
            "scopeSpans", batch.get("scope_spans", batch.get("instrumentationLibrarySpans"))
        )
        if not isinstance(groups, list):
            raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
        for group in groups:
            if not isinstance(group, dict):
                raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
            raw_spans = group.get("spans")
            if not isinstance(raw_spans, list):
                raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
            for raw in raw_spans:
                spans.append(_span(raw, service, expected_trace_id))
    return spans


def _span(raw: Any, service: str, expected_trace_id: str) -> TraceSpanView:
    if not isinstance(raw, dict):
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    trace_id = _decode_id(raw.get("traceId", raw.get("trace_id")), 16)
    if trace_id != expected_trace_id:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    span_id = _decode_id(raw.get("spanId", raw.get("span_id")), 8)
    parent_raw = raw.get("parentSpanId", raw.get("parent_span_id"))
    parent = None
    if isinstance(parent_raw, str) and parent_raw not in {"", "0" * 16}:
        parent = _decode_id(parent_raw, 8)
    name = _bounded_name(raw.get("name"))
    start_ns = _nano(raw.get("startTimeUnixNano", raw.get("start_time_unix_nano")))
    end_ns = _nano(raw.get("endTimeUnixNano", raw.get("end_time_unix_nano")))
    if end_ns < start_ns:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    attributes = _attributes(raw.get("attributes"))
    span_service = attributes.pop("service.name", service)
    error_classification = attributes.get("error.classification")
    if error_classification is not None and _ERROR_CODE.fullmatch(error_classification) is None:
        attributes.pop("error.classification", None)
        error_classification = None
    try:
        return TraceSpanView(
            span_id=span_id,
            parent_span_id=parent,
            service_name=_bounded_name(span_service),
            span_name=name,
            start_time=_from_nano(start_ns),
            duration_ms=(end_ns - start_ns) // 1_000_000,
            status=_status(raw.get("status")),
            error_classification=error_classification,
            attributes=attributes,
        )
    except ValidationError:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed") from None


def _service_name(resource: Any) -> str:
    if not isinstance(resource, dict):
        return "unknown"
    for item in resource.get("attributes") or []:
        if isinstance(item, dict) and item.get("key") == "service.name":
            value = _plain_value(item.get("value"))
            if isinstance(value, str) and value:
                return _bounded_name(value)
    return "unknown"


def _attributes(raw: Any) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, list):
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    kept: dict[str, str] = {}
    for item in raw:
        if len(kept) >= _MAX_ATTRS:
            break
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str) or _ATTR_KEY.fullmatch(key) is None:
            continue
        value = _plain_value(item.get("value"))
        if value is None:
            continue
        if _SENSITIVE.search(key):
            kept[key] = _REDACTED
            continue
        if isinstance(value, bool):
            text = "true" if value else "false"
        elif isinstance(value, int):
            text = str(value)
        else:
            text = value
        kept[key] = text[:_MAX_ATTR_CHARS]
    return kept


def _plain_value(value: Any) -> str | bool | int | None:
    if not isinstance(value, dict) or len(value) != 1:
        return None
    kind, inner = next(iter(value.items()))
    if kind == "stringValue" and isinstance(inner, str):
        return inner
    if kind == "boolValue" and isinstance(inner, bool):
        return inner
    if kind == "intValue" and isinstance(inner, str) and inner.isdigit():
        return int(inner)
    if kind == "intValue" and isinstance(inner, int) and not isinstance(inner, bool):
        return inner
    return None


def _status(raw: Any) -> str:
    if raw is None:
        return "unset"
    if not isinstance(raw, dict):
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    code = raw.get("code", 0)
    if code in {0, "STATUS_CODE_UNSET", "UNSET"}:
        return "unset"
    if code in {1, "STATUS_CODE_OK", "OK"}:
        return "ok"
    if code in {2, "STATUS_CODE_ERROR", "ERROR"}:
        return "error"
    raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")


def _decode_id(value: Any, nbytes: int) -> str:
    if not isinstance(value, str) or not value:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    width = nbytes * 2
    if _HEX_ID.fullmatch(value) and len(value) == width:
        return value.lower()
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed") from None
    if len(raw) != nbytes:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    return raw.hex()


def _nano(value: Any) -> int:
    if isinstance(value, str) and value.isdigit():
        return int(value)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")


def _from_nano(value: int) -> datetime:
    seconds, rem = divmod(value, 1_000_000_000)
    return datetime.fromtimestamp(seconds, UTC) + timedelta(microseconds=rem // 1000)


def _bounded_name(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    return value.strip()[:_MAX_NAME]


def _root_span(spans: list[TraceSpanView]) -> TraceSpanView:
    roots = [item for item in spans if item.parent_span_id is None]
    if not roots:
        raise ToolFailedError("MALFORMED_RESULT", "Tempo trace response was malformed")
    return min(roots, key=lambda item: (item.start_time, item.span_id))
