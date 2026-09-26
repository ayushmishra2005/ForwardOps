"""Read-only customer PostgreSQL source.

The platform database and this source are separate connections. Tool arguments
choose a logical source id. The DSN, SQL text, and table names stay in this
module. Results are untrusted source data.
"""

import logging
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, ValidationError

from forwardops.domain.errors import ConfigError, ToolFailedError
from forwardops.domain.hashing import sha256_bytes
from forwardops.domain.investigation import DATABASE_POOL_SCENARIO, InvestigationScope
from forwardops.domain.time import MAX_WINDOW, format_utc, preceding_window
from forwardops.integrations.replay import HandlerResult, Observation
from forwardops.tools.contracts import (
    DatabaseErrorRecord,
    DatabaseErrorReport,
    DatabasePoolSnapshot,
    FailureCodeCount,
    GetDatabasePoolSnapshotInput,
    GetRecentDatabaseErrorsInput,
    GetServiceRequestSummaryInput,
    PoolSample,
    ServiceRequestSummary,
    WindowInput,
)

logger = logging.getLogger(__name__)

SERVICE_REQUEST_SUMMARY = "get_service_request_summary"
DATABASE_POOL_SNAPSHOT = "get_database_pool_snapshot"
RECENT_DATABASE_ERRORS = "get_recent_database_errors"
SERVICE_LOGS = "search_service_logs"
CUSTOMER_DB_TOOLS = frozenset(
    {SERVICE_REQUEST_SUMMARY, DATABASE_POOL_SNAPSHOT, RECENT_DATABASE_ERRORS}
)
POOL_PLAYBOOK_TOOLS = frozenset({*CUSTOMER_DB_TOOLS, SERVICE_LOGS, "get_trace"})
QUERY_VERSION = "v1"
_SOURCE_SYSTEM = "customer-postgres"
_SOURCE_TYPE = "customer_postgres"
_OUTCOMES = frozenset({"success", "failure"})
_IDENT = re.compile(r"^[a-z_]{1,63}$")
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|call|copy|"
    r"execute|prepare|listen|notify|vacuum|begin|commit|rollback|into|do)\b",
    re.IGNORECASE,
)
_FORBIDDEN_SNIPPETS = (
    "pg_sleep",
    "pg_read_file",
    "pg_ls_dir",
    "pg_stat_file",
    "lo_import",
    "lo_export",
    "dblink",
)


def _ident(name: str) -> str:
    if _IDENT.fullmatch(name) is None:
        raise ConfigError("invalid customer database identifier")
    return name


REQUESTS = _ident("customer_service_requests")
ERRORS = _ident("customer_database_errors")
POOLS = _ident("customer_pool_samples")


def _select(statement: str) -> str:
    text = statement.strip()
    if not text.lower().startswith("select"):
        raise UnsafeSqlError("registered query must be a single SELECT")
    if ";" in text or "--" in text or "/*" in text or "$" in text:
        raise UnsafeSqlError("registered query must be a single statement")
    if _FORBIDDEN.search(text) is not None:
        raise UnsafeSqlError("registered query contains a forbidden keyword")
    lowered = text.lower()
    if any(snippet in lowered for snippet in _FORBIDDEN_SNIPPETS):
        raise UnsafeSqlError("registered query contains a forbidden function")
    return text


class UnsafeSqlError(ValueError):
    """Engineer-authored SQL failed the read-only check."""


@dataclass(frozen=True)
class RegisteredQuery:
    capability: str
    version: str
    statement: str


def _registry() -> dict[str, RegisteredQuery]:
    statements = {
        "service_request_counts": f"""
            SELECT
              count(*) FILTER (WHERE occurred_at >= %s AND occurred_at < %s) AS incident_requests,
              count(*) FILTER (
                WHERE occurred_at >= %s AND occurred_at < %s AND outcome = 'failure'
              ) AS incident_failures,
              count(*) FILTER (WHERE occurred_at >= %s AND occurred_at < %s) AS baseline_requests,
              count(*) FILTER (
                WHERE occurred_at >= %s AND occurred_at < %s AND outcome = 'failure'
              ) AS baseline_failures,
              count(*) FILTER (
                WHERE occurred_at >= %s AND occurred_at < %s
                  AND outcome = 'failure' AND error_code IS NULL
              ) AS unlabeled_failures
            FROM {REQUESTS}
            WHERE service_ref = %s
              AND occurred_at >= %s
              AND occurred_at < %s
            """.strip(),
        "service_request_outcomes": f"""
            SELECT outcome
            FROM {REQUESTS}
            WHERE service_ref = %s
              AND occurred_at >= %s
              AND occurred_at < %s
            GROUP BY outcome
            ORDER BY outcome
            LIMIT 3
            """.strip(),
        "service_failure_codes": f"""
            SELECT error_code, count(*) AS failures
            FROM {REQUESTS}
            WHERE service_ref = %s
              AND occurred_at >= %s
              AND occurred_at < %s
              AND outcome = 'failure'
            GROUP BY error_code
            ORDER BY error_code
            LIMIT %s
            """.strip(),
        "database_errors": f"""
            SELECT occurred_at, request_id, error_code, message
            FROM {ERRORS}
            WHERE service_ref = %s
              AND occurred_at >= %s
              AND occurred_at < %s
              AND (CAST(%s AS text) IS NULL OR request_id = %s)
            ORDER BY occurred_at, request_id
            LIMIT %s
            """.strip(),
        "pool_samples": f"""
            SELECT
              observed_at, active_connections, max_connections, wait_duration_ms, database_reachable
            FROM {POOLS}
            WHERE service_ref = %s
              AND observed_at >= %s
              AND observed_at < %s
            ORDER BY observed_at
            LIMIT %s
            """.strip(),
    }
    registry: dict[str, RegisteredQuery] = {}
    for capability, statement in statements.items():
        registry[capability] = RegisteredQuery(capability, QUERY_VERSION, _select(statement))
    return registry


REGISTRY = _registry()


def assert_safe_select(statement: str) -> str:
    """Reject anything other than one read-only SELECT."""
    return _select(statement)


def statement_digest(statement: str) -> str:
    return sha256_bytes(statement.encode("utf-8"))


def count_params(service_ref: str, start: datetime, end: datetime) -> tuple[Any, ...]:
    baseline_start, _baseline_end = preceding_window(start, end)
    return (
        start,
        end,
        start,
        end,
        baseline_start,
        _baseline_end,
        baseline_start,
        _baseline_end,
        start,
        end,
        service_ref,
        baseline_start,
        end,
    )


def require_window(window: WindowInput) -> None:
    if window.start >= window.end or window.end - window.start > MAX_WINDOW:
        raise ToolFailedError("FORBIDDEN_RESOURCE", "window is outside the investigation scope")


def bounded_limit(requested: int, policy_max: int) -> int:
    if isinstance(requested, bool) or not isinstance(requested, int) or requested < 1:
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "row limit is outside policy")
    if (
        isinstance(policy_max, bool)
        or not isinstance(policy_max, int)
        or not 1 <= policy_max <= 100
    ):
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "row limit is outside policy")
    return min(requested, policy_max, 100)


@asynccontextmanager
async def read_only_session(conn: Any, timeout_ms: int) -> AsyncIterator[None]:
    """One read-only transaction with a local statement timeout."""
    if (
        isinstance(timeout_ms, bool)
        or not isinstance(timeout_ms, int)
        or not 1 <= timeout_ms <= 10_000
    ):
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "statement timeout is outside policy")
    async with conn.transaction():
        await conn.execute("SET TRANSACTION READ ONLY")
        await conn.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            (str(timeout_ms),),
        )
        cursor = await conn.execute("SHOW transaction_read_only")
        row = await cursor.fetchone()
        if _cell(row) != "on":
            raise ToolFailedError(
                "FORBIDDEN_RESOURCE",
                "customer database transaction is not read only",
            )
        yield


async def fetch_read_only(
    conn: Any,
    statement: str,
    params: tuple[Any, ...],
    *,
    timeout_ms: int,
    max_rows: int,
) -> list[dict[str, Any]]:
    try:
        assert_safe_select(statement)
    except UnsafeSqlError:
        raise ToolFailedError(
            "UNSUPPORTED_SCHEMA",
            "database statement is not a registered read",
        ) from None
    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or not 1 <= max_rows <= 100:
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "row limit is outside policy")
    try:
        async with read_only_session(conn, timeout_ms):
            cursor = await conn.execute(statement, params)
            fetched = await cursor.fetchmany(max_rows + 1)
    except ToolFailedError:
        raise
    except Exception as exc:
        raise _query_failure(exc) from None
    return [_mapping(row) for row in fetched]


async def run_registered(
    conn: Any,
    capability: str,
    params: tuple[Any, ...],
    *,
    timeout_ms: int,
    max_rows: int,
) -> list[dict[str, Any]]:
    query = REGISTRY.get(capability)
    if query is None or query.version != QUERY_VERSION:
        raise ToolFailedError("UNSUPPORTED_SCHEMA", "database capability is not registered")
    return await fetch_read_only(
        conn,
        query.statement,
        params,
        timeout_ms=timeout_ms,
        max_rows=max_rows,
    )


class CustomerDbHandlers:
    """Separate pool. The DSN is not a tool argument and is not part of repr."""

    def __init__(
        self,
        source_id: str,
        dsn: str,
        statement_timeout_ms: int,
        max_rows: int,
        *,
        synthetic: bool = True,
    ) -> None:
        if not isinstance(source_id, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", source_id):
            raise ConfigError("customer database source id must be a logical identifier")
        if (
            isinstance(statement_timeout_ms, bool)
            or not isinstance(statement_timeout_ms, int)
            or not 100 <= statement_timeout_ms <= 10_000
        ):
            raise ConfigError("customer database statement timeout is outside policy")
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or not 1 <= max_rows <= 100:
            raise ConfigError("customer database row limit is outside policy")
        self.source_id = source_id
        self._dsn = dsn
        self.statement_timeout_ms = statement_timeout_ms
        self.max_rows = max_rows
        self.synthetic = synthetic
        self._pool: AsyncConnectionPool | None = None

    def __repr__(self) -> str:
        return f"CustomerDbHandlers(source_id={self.source_id!r})"

    def serves(self, source_ref: str) -> bool:
        return source_ref == self.source_id

    async def close(self) -> None:
        pool = self._pool
        self._pool = None
        if pool is None:
            return
        await pool.close()

    async def get_service_request_summary(
        self,
        scope: InvestigationScope,
        arguments: GetServiceRequestSummaryInput,
    ) -> HandlerResult:
        return await self._call(
            SERVICE_REQUEST_SUMMARY,
            arguments.source_ref,
            lambda: self._summary(scope, arguments),
        )

    async def get_database_pool_snapshot(
        self,
        scope: InvestigationScope,
        arguments: GetDatabasePoolSnapshotInput,
    ) -> HandlerResult:
        return await self._call(
            DATABASE_POOL_SNAPSHOT,
            arguments.source_ref,
            lambda: self._pool_snapshot(scope, arguments),
        )

    async def get_recent_database_errors(
        self,
        scope: InvestigationScope,
        arguments: GetRecentDatabaseErrorsInput,
    ) -> HandlerResult:
        return await self._call(
            RECENT_DATABASE_ERRORS,
            arguments.source_ref,
            lambda: self._errors(scope, arguments),
        )

    async def _call(self, capability: str, source_ref: str, action: Any) -> HandlerResult:
        started = time.perf_counter()
        category = "failed"
        try:
            result = await action()
            category = "succeeded"
            return result
        except ToolFailedError as exc:
            category = exc.code.lower()
            raise
        finally:
            _log_read(
                source_id=source_ref if source_ref == self.source_id else self.source_id,
                capability=capability,
                category=category,
                started=started,
            )

    async def _summary(
        self,
        scope: InvestigationScope,
        arguments: GetServiceRequestSummaryInput,
    ) -> HandlerResult:
        self._check_scope(scope, arguments.service_ref, arguments.source_ref, arguments.window)
        limit = bounded_limit(arguments.limit, self.max_rows)
        start, end = arguments.window.start, arguments.window.end
        baseline_start, baseline_end = preceding_window(start, end)
        counts = await self._one(
            "service_request_counts",
            count_params(arguments.service_ref, start, end),
        )
        if _number(counts, "unlabeled_failures") != 0:
            raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")
        outcomes = await self._query(
            "service_request_outcomes",
            (arguments.service_ref, baseline_start, end),
            limit=3,
        )
        _checked_outcomes(outcomes)
        code_rows = await self._query(
            "service_failure_codes",
            (arguments.service_ref, start, end, limit + 1),
            limit=limit,
        )
        codes, truncated = _trim(code_rows, limit)
        failure_codes = [
            _parse(
                FailureCodeCount,
                {"error_code": row.get("error_code"), "failures": _number(row, "failures")},
            )
            for row in codes
        ]
        incident_failures = _number(counts, "incident_failures")
        if not truncated and sum(item.failures for item in failure_codes) != incident_failures:
            raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")
        summary = ServiceRequestSummary(
            service_ref=arguments.service_ref,
            source_ref=self.source_id,
            window=arguments.window,
            baseline_window=WindowInput(start=baseline_start, end=baseline_end),
            incident_requests=_number(counts, "incident_requests"),
            incident_failures=incident_failures,
            baseline_requests=_number(counts, "baseline_requests"),
            baseline_failures=_number(counts, "baseline_failures"),
            failure_codes=failure_codes,
            coverage_complete=not truncated,
            truncated=truncated,
            row_count=len(failure_codes),
        )
        digests = [
            statement_digest(REGISTRY[name].statement)
            for name in (
                "service_request_counts",
                "service_request_outcomes",
                "service_failure_codes",
            )
        ]
        observed = _now()
        return HandlerResult(
            summary.model_dump(mode="json"),
            (
                _summary_observation(
                    summary,
                    digests=digests,
                    observed_at=observed,
                    synthetic=self.synthetic,
                ),
            ),
        )

    async def _errors(
        self,
        scope: InvestigationScope,
        arguments: GetRecentDatabaseErrorsInput,
    ) -> HandlerResult:
        self._check_scope(scope, arguments.service_ref, arguments.source_ref, arguments.window)
        limit = bounded_limit(arguments.limit, self.max_rows)
        rows = await self._query(
            "database_errors",
            (
                arguments.service_ref,
                arguments.window.start,
                arguments.window.end,
                arguments.request_id,
                arguments.request_id,
                limit + 1,
            ),
            limit=limit,
        )
        kept, truncated = _trim(rows, limit)
        errors = [_error_record(row) for row in kept]
        report = DatabaseErrorReport(
            service_ref=arguments.service_ref,
            source_ref=self.source_id,
            window=arguments.window,
            errors=errors,
            coverage_complete=not truncated,
            truncated=truncated,
            row_count=len(errors),
        )
        digest = statement_digest(REGISTRY["database_errors"].statement)
        observed = _now()
        observations = tuple(
            _error_observation(
                report,
                error,
                index,
                digest=digest,
                observed_at=observed,
                synthetic=self.synthetic,
            )
            for index, error in enumerate(report.errors)
        )
        return HandlerResult(report.model_dump(mode="json"), observations)

    async def _pool_snapshot(
        self,
        scope: InvestigationScope,
        arguments: GetDatabasePoolSnapshotInput,
    ) -> HandlerResult:
        self._check_scope(scope, arguments.service_ref, arguments.source_ref, arguments.window)
        limit = bounded_limit(arguments.limit, self.max_rows)
        rows = await self._query(
            "pool_samples",
            (
                arguments.service_ref,
                arguments.window.start,
                arguments.window.end,
                limit + 1,
            ),
            limit=limit,
        )
        kept, truncated = _trim(rows, limit)
        samples = [_pool_sample(row) for row in kept]
        snapshot = DatabasePoolSnapshot(
            service_ref=arguments.service_ref,
            source_ref=self.source_id,
            window=arguments.window,
            samples=samples,
            coverage_complete=not truncated,
            truncated=truncated,
            row_count=len(samples),
        )
        digest = statement_digest(REGISTRY["pool_samples"].statement)
        observed = _now()
        observations = tuple(
            _pool_observation(
                snapshot,
                sample,
                index,
                digest=digest,
                observed_at=observed,
                synthetic=self.synthetic,
            )
            for index, sample in enumerate(snapshot.samples)
        )
        return HandlerResult(snapshot.model_dump(mode="json"), observations)

    def _check_scope(
        self,
        scope: InvestigationScope,
        service_ref: str,
        source_ref: str,
        window: WindowInput,
    ) -> None:
        if scope.scenario_id != DATABASE_POOL_SCENARIO:
            raise ToolFailedError("FORBIDDEN_RESOURCE", "tool is outside the selected playbook")
        if scope.database_source_ref != self.source_id or source_ref != self.source_id:
            raise ToolFailedError(
                "FORBIDDEN_RESOURCE",
                "database source is outside the investigation scope",
            )
        if service_ref != scope.service_ref:
            raise ToolFailedError(
                "FORBIDDEN_RESOURCE", "service is outside the investigation scope"
            )
        require_window(window)

    async def _one(self, capability: str, params: tuple[Any, ...]) -> dict[str, Any]:
        rows = await self._query(capability, params, limit=1)
        if len(rows) != 1:
            raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")
        return rows[0]

    async def _query(
        self,
        capability: str,
        params: tuple[Any, ...],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        pool = await self._open_pool()
        try:
            async with pool.connection() as conn:
                return await run_registered(
                    conn,
                    capability,
                    params,
                    timeout_ms=self.statement_timeout_ms,
                    max_rows=limit,
                )
        except ToolFailedError:
            raise
        except Exception as exc:
            raise _query_failure(exc) from None

    async def _open_pool(self) -> AsyncConnectionPool:
        if self._pool is not None:
            return self._pool
        timeout = self.statement_timeout_ms
        pool = AsyncConnectionPool(
            self._dsn,
            min_size=1,
            max_size=4,
            open=False,
            kwargs={
                "row_factory": dict_row,
                "connect_timeout": 3,
                "options": (
                    "-c timezone=UTC "
                    "-c default_transaction_read_only=on "
                    f"-c statement_timeout={timeout}"
                ),
            },
        )
        try:
            await pool.open(wait=True, timeout=3)
        except Exception:
            self._pool = None
            await pool.close()
            raise ToolFailedError(
                "SOURCE_UNAVAILABLE",
                "customer database source is unavailable",
                retryable=True,
            ) from None
        self._pool = pool
        return pool


def _summary_observation(
    summary: ServiceRequestSummary,
    *,
    digests: list[str],
    observed_at: str,
    synthetic: bool,
) -> Observation:
    payload = summary.model_dump(mode="json")
    payload["source_id"] = summary.source_ref
    return _customer_observation(
        kind="service.request_summary",
        capability=SERVICE_REQUEST_SUMMARY,
        source_id=summary.source_ref,
        payload=payload,
        summary_text=(
            f"{summary.incident_failures} of {summary.incident_requests} incident requests failed; "
            f"baseline {summary.baseline_failures} of {summary.baseline_requests}."
        ),
        event_time=None,
        time_basis="interval",
        correlation={"service_ref": summary.service_ref, "source_id": summary.source_ref},
        truncated=summary.truncated,
        row_count=summary.row_count,
        window_start=format_utc(summary.window.start),
        window_end=format_utc(summary.window.end),
        digests=digests,
        observed_at=observed_at,
        synthetic=synthetic,
        untrusted_text=False,
        locator_extra={"service_ref": summary.service_ref},
    )


def _error_observation(
    report: DatabaseErrorReport,
    error: DatabaseErrorRecord,
    index: int,
    *,
    digest: str,
    observed_at: str,
    synthetic: bool,
) -> Observation:
    payload = error.model_dump(mode="json")
    payload.update(
        {
            "service_ref": report.service_ref,
            "source_id": report.source_ref,
            "capability": RECENT_DATABASE_ERRORS,
            "capability_version": QUERY_VERSION,
            "truncated": report.truncated,
            "row_count": report.row_count,
            "window_start": format_utc(report.window.start),
            "window_end": format_utc(report.window.end),
        }
    )
    return _customer_observation(
        kind="customer.database_error",
        capability=RECENT_DATABASE_ERRORS,
        source_id=report.source_ref,
        payload=payload,
        summary_text=f"Database error {error.error_code} for request {error.request_id}.",
        event_time=format_utc(error.occurred_at),
        time_basis="source_event_time",
        correlation={
            "service_ref": report.service_ref,
            "source_id": report.source_ref,
            "request_id": error.request_id,
        },
        truncated=report.truncated,
        row_count=report.row_count,
        window_start=format_utc(report.window.start),
        window_end=format_utc(report.window.end),
        digests=[digest],
        observed_at=observed_at,
        synthetic=synthetic,
        untrusted_text=True,
        locator_extra={"request_id": error.request_id, "row_index": index},
    )


def _pool_observation(
    snapshot: DatabasePoolSnapshot,
    sample: PoolSample,
    index: int,
    *,
    digest: str,
    observed_at: str,
    synthetic: bool,
) -> Observation:
    payload = sample.model_dump(mode="json")
    payload.update(
        {
            "service_ref": snapshot.service_ref,
            "source_id": snapshot.source_ref,
            "capability": DATABASE_POOL_SNAPSHOT,
            "capability_version": QUERY_VERSION,
            "truncated": snapshot.truncated,
            "row_count": snapshot.row_count,
            "window_start": format_utc(snapshot.window.start),
            "window_end": format_utc(snapshot.window.end),
        }
    )
    reached = sample.active_connections == sample.max_connections
    return _customer_observation(
        kind="customer.pool_sample",
        capability=DATABASE_POOL_SNAPSHOT,
        source_id=snapshot.source_ref,
        payload=payload,
        summary_text=(
            f"Pool sample {sample.active_connections} of {sample.max_connections} connections, "
            f"wait {sample.wait_duration_ms} ms, database reachable {str(sample.database_reachable).lower()}, "
            f"at maximum {str(reached).lower()}."
        ),
        event_time=format_utc(sample.observed_at),
        time_basis="source_event_time",
        correlation={"service_ref": snapshot.service_ref, "source_id": snapshot.source_ref},
        truncated=snapshot.truncated,
        row_count=snapshot.row_count,
        window_start=format_utc(snapshot.window.start),
        window_end=format_utc(snapshot.window.end),
        digests=[digest],
        observed_at=observed_at,
        synthetic=synthetic,
        untrusted_text=False,
        locator_extra={"row_index": index},
    )


def _customer_observation(
    *,
    kind: str,
    capability: str,
    source_id: str,
    payload: dict[str, Any],
    summary_text: str,
    event_time: str | None,
    time_basis: str,
    correlation: dict[str, Any],
    truncated: bool,
    row_count: int,
    window_start: str,
    window_end: str,
    digests: list[str],
    observed_at: str,
    synthetic: bool,
    untrusted_text: bool,
    locator_extra: dict[str, Any],
) -> Observation:
    return Observation(
        kind=kind,
        source_type=_SOURCE_TYPE,
        source_system=_SOURCE_SYSTEM,
        source_locator={
            "source_id": source_id,
            "capability": capability,
            "capability_version": QUERY_VERSION,
            **locator_extra,
        },
        event_time=event_time,
        time_basis=time_basis,
        correlation=correlation,
        payload=payload,
        summary=summary_text,
        provenance={
            "synthetic": synthetic,
            "untrusted_source": True,
            "untrusted_text": untrusted_text,
            "source_id": source_id,
            "capability": capability,
            "capability_version": QUERY_VERSION,
            "statement_digests": digests,
        },
        coverage={
            "complete_for_record": not truncated,
            "truncated": truncated,
            "row_count": row_count,
            "window_start": window_start,
            "window_end": window_end,
        },
        retrieval_time=observed_at,
    )


def _error_record(row: dict[str, Any]) -> DatabaseErrorRecord:
    message = row.get("message")
    if not isinstance(message, str):
        raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")
    if len(message) > 2000:
        raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")
    return _parse(
        DatabaseErrorRecord,
        {
            "occurred_at": row.get("occurred_at"),
            "request_id": row.get("request_id"),
            "error_code": row.get("error_code"),
            "message": message[:500],
        },
    )


def _pool_sample(row: dict[str, Any]) -> PoolSample:
    return _parse(
        PoolSample,
        {
            "observed_at": row.get("observed_at"),
            "active_connections": row.get("active_connections"),
            "max_connections": row.get("max_connections"),
            "wait_duration_ms": row.get("wait_duration_ms"),
            "database_reachable": row.get("database_reachable"),
        },
    )


def _parse(model: type[BaseModel], payload: dict[str, Any]) -> Any:
    try:
        return model.model_validate(payload)
    except ValidationError:
        raise ToolFailedError(
            "MALFORMED_RESULT", "customer database result was malformed"
        ) from None


def _checked_outcomes(rows: list[dict[str, Any]]) -> None:
    if len(rows) > 2:
        raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")
    for row in rows:
        outcome = row.get("outcome")
        if not isinstance(outcome, str) or outcome not in _OUTCOMES:
            raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")


def _trim(rows: list[dict[str, Any]], limit: int) -> tuple[list[dict[str, Any]], bool]:
    if len(rows) > limit:
        return rows[:limit], True
    return rows, False


def _number(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if isinstance(value, Decimal) and value == value.to_integral_value():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")
    return value


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    raise ToolFailedError("MALFORMED_RESULT", "customer database result was malformed")


def _cell(row: Any) -> Any:
    if isinstance(row, dict):
        return next(iter(row.values()))
    if isinstance(row, tuple):
        return row[0]
    return None


def _now() -> str:
    return format_utc(datetime.now(UTC))


def _query_failure(exc: Exception) -> ToolFailedError:
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate == "57014":
        return ToolFailedError(
            "STATEMENT_TIMEOUT",
            "customer database statement timed out",
            retryable=True,
        )
    if sqlstate == "25006":
        return ToolFailedError("FORBIDDEN_RESOURCE", "customer database rejected a write")
    return ToolFailedError(
        "SOURCE_UNAVAILABLE",
        "customer database source is unavailable",
        retryable=True,
    )


def _log_read(*, source_id: str, capability: str, category: str, started: float) -> None:
    extra: dict[str, Any] = {
        "event": "customer_database_read",
        "source_id": source_id,
        "query_capability": capability,
        "result_category": category,
        "duration_ms": max(0, int((time.perf_counter() - started) * 1000)),
    }
    if category != "succeeded":
        extra["error_category"] = category
    logger.info("customer database read finished", extra=extra)
