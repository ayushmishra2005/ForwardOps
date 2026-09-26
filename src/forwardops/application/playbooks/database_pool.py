"""Deterministic database connection-pool investigation.

The predicate is computed from tool evidence. This playbook does not propose
a database change.
"""

from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4, uuid5

from forwardops.application.playbooks.withdrawals import ConclusionPlan
from forwardops.config import CustomerConfig
from forwardops.domain.errors import ToolFailedError
from forwardops.domain.evidence import EvidenceRef, FindingDraft
from forwardops.domain.investigation import DATABASE_POOL_SCENARIO, InvestigationScope
from forwardops.domain.time import format_utc, parse_utc
from forwardops.integrations.customer_db import (
    DATABASE_POOL_SNAPSHOT,
    RECENT_DATABASE_ERRORS,
    SERVICE_LOGS,
    SERVICE_REQUEST_SUMMARY,
)
from forwardops.tools.contracts import (
    DatabaseErrorRecord,
    DatabaseErrorReport,
    DatabasePoolSnapshot,
    LogRecord,
    LogSearchResult,
    PoolSample,
    ServiceRequestSummary,
    TraceView,
)
from forwardops.tools.gateway import ToolGateway

PLAYBOOK_VERSION = "database_pool_v1"
RULE_VERSION = "v1"
TIMEOUT_CODE = "db_acquisition_timeout"
INFERENCE_CLAIM = "Database connection pool exhaustion explains the observed application failures."
UNKNOWN_CLAIM = "Why connection usage increased is unknown."
POOL_LIMIT = 20


@dataclass(frozen=True)
class RequestLog:
    evidence_id: UUID
    record: LogRecord
    request_id: str


@dataclass(frozen=True)
class TraceHit:
    evidence_id: UUID
    view: TraceView
    request_id: str


@dataclass(frozen=True)
class PoolCollected:
    complete: bool
    gap: str | None
    summary: ServiceRequestSummary | None
    summary_evidence_id: UUID | None
    errors: tuple[DatabaseErrorRecord, ...]
    error_evidence_ids: tuple[UUID, ...]
    logs: tuple[RequestLog, ...]
    traces: tuple[TraceHit, ...]
    pool: DatabasePoolSnapshot | None
    pool_evidence_ids: tuple[UUID, ...]


async def investigate_database_pool(
    gateway: ToolGateway,
    scope: InvestigationScope,
    investigation_id: UUID,
    customer: CustomerConfig,
) -> ConclusionPlan:
    scenario = customer.database_scenario
    if (
        scenario is None
        or scope.scenario_id != DATABASE_POOL_SCENARIO
        or scope.service_ref != scenario.service_ref
        or scope.database_source_ref != scenario.database_source_ref
    ):
        return _incomplete(investigation_id, "unconfigured_database_scenario")
    try:
        collected = await _collect(gateway, scope)
    except ToolFailedError as exc:
        return _incomplete(investigation_id, exc.code.lower())
    return conclude_pool(collected, investigation_id, scenario.component_ref)


def conclude_pool(
    collected: PoolCollected,
    investigation_id: UUID,
    component: str,
) -> ConclusionPlan:
    """Build the application conclusion. There is no action proposal."""
    if not _ready(collected):
        return _incomplete(investigation_id, collected.gap or "missing_evidence", collected)
    judged = _judge(collected)
    if judged is None:
        return _not_established(investigation_id, collected)
    return _conclusion(collected, investigation_id, component, judged)


def pool_hypothesis_template(investigation_id: UUID) -> list[dict[str, Any]]:
    specs = (
        ("pool_exhaustion", "Database connection pool exhaustion is causing request failures."),
        ("database_unreachable", "The database is unreachable."),
        ("deployment_regression", "A recent deployment changed request behavior."),
        ("usage_increase", "The reason connection usage increased is known."),
    )
    return [
        {
            "hypothesis_id": str(uuid5(investigation_id, key)),
            "key": key,
            "claim": claim,
            "status": "open",
            "evidence_refs": [],
        }
        for key, claim in specs
    ]


async def _collect(gateway: ToolGateway, scope: InvestigationScope) -> PoolCollected:
    window = {"start": parse_utc(scope.interval_start), "end": parse_utc(scope.interval_end)}
    base = {
        "service_ref": scope.service_ref,
        "source_ref": scope.database_source_ref,
        "window": window,
    }
    try:
        summary_call = await gateway.call(
            SERVICE_REQUEST_SUMMARY,
            {**base, "limit": scope.sample_cap},
        )
    except ToolFailedError as exc:
        return _empty(exc.code.lower())
    summary = ServiceRequestSummary.model_validate(summary_call.output)
    if len(summary_call.evidence_ids) != 1:
        raise ToolFailedError("INVALID_OUTPUT", "request summary evidence did not match")
    try:
        error_call = await gateway.call(
            RECENT_DATABASE_ERRORS,
            {**base, "limit": scope.sample_cap},
        )
    except ToolFailedError as exc:
        return _empty(exc.code.lower(), summary, summary_call.evidence_ids[0])
    errors = list(DatabaseErrorReport.model_validate(error_call.output).errors)
    if len(error_call.evidence_ids) != len(errors):
        raise ToolFailedError("INVALID_OUTPUT", "database error evidence did not match")
    logs: list[RequestLog] = []
    for error in errors:
        try:
            log_call = await gateway.call(
                SERVICE_LOGS,
                {
                    "service_ref": scope.service_ref,
                    "window": window,
                    "request_id": error.request_id,
                    "limit": 20,
                },
            )
        except ToolFailedError as exc:
            return _empty(exc.code.lower(), summary, summary_call.evidence_ids[0])
        result = LogSearchResult.model_validate(log_call.output)
        if len(log_call.evidence_ids) != len(result.records):
            raise ToolFailedError("INVALID_OUTPUT", "service log evidence did not match")
        if len(result.records) != 1 or not result.coverage_complete:
            return _empty("log_correlation", summary, summary_call.evidence_ids[0])
        logs.append(RequestLog(log_call.evidence_ids[0], result.records[0], error.request_id))
    traces: list[TraceHit] = []
    for hit in logs:
        trace_id = hit.record.trace_id
        if not isinstance(trace_id, str) or not scope.trace_source_ref:
            return _empty("trace_correlation", summary, summary_call.evidence_ids[0])
        try:
            trace_call = await gateway.call(
                "get_trace",
                {"trace_source_ref": scope.trace_source_ref, "trace_id": trace_id},
            )
        except ToolFailedError as exc:
            return _empty(exc.code.lower(), summary, summary_call.evidence_ids[0])
        if len(trace_call.evidence_ids) != 1:
            raise ToolFailedError("INVALID_OUTPUT", "trace evidence did not match")
        view = TraceView.model_validate(trace_call.output)
        traces.append(TraceHit(trace_call.evidence_ids[0], view, hit.request_id))
    try:
        pool_call = await gateway.call(DATABASE_POOL_SNAPSHOT, {**base, "limit": POOL_LIMIT})
    except ToolFailedError as exc:
        return _empty(exc.code.lower(), summary, summary_call.evidence_ids[0])
    pool = DatabasePoolSnapshot.model_validate(pool_call.output)
    if len(pool_call.evidence_ids) != len(pool.samples):
        raise ToolFailedError("INVALID_OUTPUT", "pool snapshot evidence did not match")
    return PoolCollected(
        complete=True,
        gap=None,
        summary=summary,
        summary_evidence_id=summary_call.evidence_ids[0],
        errors=tuple(errors),
        error_evidence_ids=tuple(error_call.evidence_ids),
        logs=tuple(logs),
        traces=tuple(traces),
        pool=pool,
        pool_evidence_ids=tuple(pool_call.evidence_ids),
    )


@dataclass(frozen=True)
class _Judged:
    samples: tuple[PoolSample, ...]
    sample_ids: tuple[UUID, ...]
    at_max_index: int


def _ready(collected: PoolCollected) -> bool:
    return bool(
        collected.complete
        and collected.summary is not None
        and collected.summary_evidence_id is not None
        and collected.pool is not None
        and collected.summary.coverage_complete
        and not collected.summary.truncated
        and collected.pool.coverage_complete
        and not collected.pool.truncated
        and len(collected.errors) == len(collected.error_evidence_ids)
        and len(collected.logs) == len(collected.errors)
        and len(collected.traces) == len(collected.logs)
        and len(collected.pool.samples) == len(collected.pool_evidence_ids)
    )


def _judge(collected: PoolCollected) -> _Judged | None:
    summary = collected.summary
    pool = collected.pool
    if summary is None or pool is None:
        return None
    if summary.baseline_requests < 1 or summary.baseline_failures != 0:
        return None
    if summary.incident_requests < 1 or summary.incident_failures <= summary.baseline_failures:
        return None
    if len(summary.failure_codes) != 1:
        return None
    code = summary.failure_codes[0]
    if code.error_code != TIMEOUT_CODE or code.failures != summary.incident_failures:
        return None
    if not collected.errors or any(error.error_code != TIMEOUT_CODE for error in collected.errors):
        return None
    paired = sorted(
        zip(pool.samples, collected.pool_evidence_ids, strict=True),
        key=lambda item: (item[0].observed_at, item[1]),
    )
    samples = tuple(item[0] for item in paired)
    sample_ids = tuple(item[1] for item in paired)
    if len(samples) < 2 or any(not sample.database_reachable for sample in samples):
        return None
    waits = [sample.wait_duration_ms for sample in samples]
    if not all(later > earlier for earlier, later in zip(waits, waits[1:], strict=False)):
        return None
    at_max = [index for index, sample in enumerate(samples) if _at_maximum(sample)]
    if not at_max:
        return None
    by_request: dict[str, list[RequestLog]] = {}
    for hit in collected.logs:
        by_request.setdefault(hit.request_id, []).append(hit)
    for error in collected.errors:
        matches = [
            hit
            for hit in by_request.get(error.request_id, [])
            if hit.record.request_id == error.request_id
            and hit.record.error_code == error.error_code
        ]
        if len(matches) != 1:
            return None
    by_trace = {hit.view.trace_id: hit for hit in collected.traces}
    service = summary.service_ref
    for error, hit in zip(collected.errors, collected.logs, strict=True):
        trace_id = hit.record.trace_id
        traced = by_trace.get(trace_id or "")
        if (
            traced is None
            or traced.request_id != error.request_id
            or not _trace_supports(traced.view, error.request_id, service)
        ):
            return None
    return _Judged(samples, sample_ids, at_max[0])


def _trace_supports(view: TraceView, request_id: str, service: str) -> bool:
    if service not in view.service_names and view.root_service != service:
        return False
    return any(
        span.span_name == "db.pool.acquire"
        and span.status == "error"
        and span.error_classification == TIMEOUT_CODE
        and span.attributes.get("request.id") == request_id
        for span in view.spans
    )


def _at_maximum(sample: PoolSample) -> bool:
    return sample.max_connections > 0 and sample.active_connections == sample.max_connections


def _conclusion(
    collected: PoolCollected,
    investigation_id: UUID,
    component: str,
    judged: _Judged,
) -> ConclusionPlan:
    summary = collected.summary
    assert summary is not None and collected.summary_evidence_id is not None
    summary_id = collected.summary_evidence_id
    at_max = judged.samples[judged.at_max_index]
    at_max_id = judged.sample_ids[judged.at_max_index]
    first = judged.samples[0]
    last = judged.samples[-1]
    first_id = judged.sample_ids[0]
    last_id = judged.sample_ids[-1]
    findings = [
        _count_fact(summary, summary_id, component),
        _timeout_fact(summary_id, collected),
        _capacity_fact(at_max, at_max_id, component),
        _wait_fact(first, last, first_id, last_id, component),
        _reachable_fact(at_max_id, component),
        _correlation_fact(collected),
    ]
    supports = _inference_refs(summary_id, collected, at_max_id, first_id, last_id)
    inference_id = uuid4()
    scope_text = f"{summary.incident_failures} failed requests in the incident window"
    inference = FindingDraft(
        id=inference_id,
        classification="INFERENCE",
        claim=INFERENCE_CLAIM,
        component_ref=component,
        evidence_refs=supports,
        derivation={
            "rule": "database_connection_pool_exhaustion",
            "rule_version": RULE_VERSION,
            "cause": "database_connection_pool_exhaustion",
            "component": component,
            "scope": scope_text,
            "incident_failures": summary.incident_failures,
            "baseline_failures": summary.baseline_failures,
            "active_connections": at_max.active_connections,
            "max_connections": at_max.max_connections,
            "wait_start_ms": first.wait_duration_ms,
            "wait_end_ms": last.wait_duration_ms,
            "database_reachable": True,
            "timeout_error_code": TIMEOUT_CODE,
            "correlated_request_ids": [error.request_id for error in collected.errors],
            "correlated_trace_ids": [hit.view.trace_id for hit in collected.traces],
        },
        confidence="high",
        confidence_basis=[
            "Incident request failures increased from a baseline with no failures.",
            "Active connections reached the configured pool maximum.",
            "Pool wait duration increased during the incident window.",
            "The database remained reachable.",
            "Inspected requests emitted DB acquisition timeout errors with matching application logs.",
            "OpenTelemetry traces show the same timeout on the checkout service pool-acquisition span.",
        ],
        alternatives=["The evidence does not establish a cause beyond pool exhaustion."],
        limitations=[
            "Log correlation covers the returned database error rows.",
            "The evidence does not establish why connection usage increased.",
        ],
    )
    unknown = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim=UNKNOWN_CLAIM,
        component_ref=component,
        evidence_refs=[],
        confidence=None,
        limitations=["No evidence established the reason connection usage increased."],
    )
    findings.extend((inference, unknown))
    return ConclusionPlan(
        status="CONCLUDED",
        findings=tuple(findings),
        hypotheses=_final_hypotheses(investigation_id, supports),
        timeline=_timeline(collected, judged, summary_id),
        unknowns=(unknown.claim,),
        recommendations=({"summary": _guidance(component)},),
        confidence="high",
        confidence_basis=tuple(inference.confidence_basis),
        root_finding_id=inference_id,
        proposal=None,
    )


def _guidance(component: str) -> str:
    return (
        f"Inspect what increased {component} connection usage. "
        "This investigation records guidance only and does not change the database, "
        "the connection pool, or running sessions."
    )


def _count_fact(
    summary: ServiceRequestSummary,
    evidence_id: UUID,
    component: str,
) -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim=(
            f"The incident window has {summary.incident_failures} failed requests "
            f"out of {summary.incident_requests}, compared with {summary.baseline_failures} "
            f"failures out of {summary.baseline_requests} in the preceding window."
        ),
        component_ref=component,
        evidence_refs=[
            _ref(evidence_id, "/incident_failures"),
            _ref(evidence_id, "/incident_requests"),
            _ref(evidence_id, "/baseline_failures"),
            _ref(evidence_id, "/baseline_requests"),
        ],
    )


def _timeout_fact(summary_id: UUID, collected: PoolCollected) -> FindingDraft:
    refs = [_ref(summary_id, "/failure_codes/0/error_code")]
    refs.extend(_ref(evidence_id, "/error_code") for evidence_id in collected.error_evidence_ids)
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim="Requests emitted DB acquisition timeout errors.",
        component_ref=collected.summary.service_ref if collected.summary else None,
        evidence_refs=refs,
    )


def _capacity_fact(_sample: PoolSample, evidence_id: UUID, component: str) -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim="Active connections reached the configured pool maximum.",
        component_ref=component,
        evidence_refs=[
            _ref(evidence_id, "/active_connections"),
            _ref(evidence_id, "/max_connections"),
        ],
    )


def _wait_fact(
    first: PoolSample,
    last: PoolSample,
    first_id: UUID,
    last_id: UUID,
    component: str,
) -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim=(
            f"Pool wait duration increased from {first.wait_duration_ms} ms "
            f"to {last.wait_duration_ms} ms."
        ),
        component_ref=component,
        evidence_refs=[
            _ref(first_id, "/wait_duration_ms"),
            _ref(last_id, "/wait_duration_ms"),
        ],
    )


def _reachable_fact(evidence_id: UUID, component: str) -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim="The database remained reachable during the incident window.",
        component_ref=component,
        evidence_refs=[_ref(evidence_id, "/database_reachable")],
    )


def _correlation_fact(collected: PoolCollected) -> FindingDraft:
    refs: list[EvidenceRef] = []
    for error_id, hit, traced in zip(
        collected.error_evidence_ids, collected.logs, collected.traces, strict=True
    ):
        refs.append(_ref(error_id, "/request_id"))
        refs.append(_ref(hit.evidence_id, "/request_id"))
        refs.append(_ref(hit.evidence_id, "/error_code"))
        refs.append(_ref(traced.evidence_id, "/trace_id"))
        pointer = _timeout_pointer(traced.view)
        if pointer is not None:
            refs.append(_ref(traced.evidence_id, pointer))
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim=(
            "Application logs and traces for the inspected requests record "
            "the same DB acquisition timeout."
        ),
        component_ref=collected.summary.service_ref if collected.summary else None,
        evidence_refs=refs,
    )


def _inference_refs(
    summary_id: UUID,
    collected: PoolCollected,
    at_max_id: UUID,
    first_id: UUID,
    last_id: UUID,
) -> list[EvidenceRef]:
    refs = [
        _ref(summary_id, "/incident_failures"),
        _ref(summary_id, "/failure_codes/0/error_code"),
        _ref(at_max_id, "/active_connections"),
        _ref(at_max_id, "/max_connections"),
        _ref(at_max_id, "/database_reachable"),
        _ref(first_id, "/wait_duration_ms"),
        _ref(last_id, "/wait_duration_ms"),
    ]
    refs.extend(_ref(evidence_id, "/error_code") for evidence_id in collected.error_evidence_ids)
    refs.extend(_ref(hit.evidence_id, "/request_id") for hit in collected.logs)
    for traced in collected.traces:
        refs.append(_ref(traced.evidence_id, "/trace_id"))
        pointer = _timeout_pointer(traced.view)
        if pointer is not None:
            refs.append(_ref(traced.evidence_id, pointer))
    return refs


def _timeline(
    collected: PoolCollected,
    judged: _Judged,
    summary_id: UUID,
) -> list[dict[str, Any]]:
    summary = collected.summary
    assert summary is not None
    events = [
        {
            "event_time": format_utc(summary.window.start),
            "description": "Incident window started.",
            "evidence_ids": [str(summary_id)],
        }
    ]
    if collected.errors:
        events.append(
            {
                "event_time": format_utc(collected.errors[0].occurred_at),
                "description": "A request emitted a DB acquisition timeout.",
                "evidence_ids": [str(collected.error_evidence_ids[0])],
            }
        )
    at_max = judged.samples[judged.at_max_index]
    events.append(
        {
            "event_time": format_utc(at_max.observed_at),
            "description": "Active connections reached the configured pool maximum.",
            "evidence_ids": [str(judged.sample_ids[judged.at_max_index])],
        }
    )
    events.sort(key=lambda item: item["event_time"])
    return events


def _final_hypotheses(investigation_id: UUID, supports: list[EvidenceRef]) -> list[dict[str, Any]]:
    dumped = [ref.model_dump(mode="json") for ref in supports]
    rows = pool_hypothesis_template(investigation_id)
    for row in rows:
        if row["key"] == "pool_exhaustion":
            row["status"] = "supported"
            row["evidence_refs"] = dumped
        elif row["key"] == "database_unreachable":
            row["status"] = "unsupported"
        else:
            row["status"] = "unresolved"
    return rows


def _incomplete(
    investigation_id: UUID,
    gap: str,
    collected: PoolCollected | None = None,
) -> ConclusionPlan:
    findings: list[FindingDraft] = []
    if collected is not None and collected.summary is not None and collected.summary_evidence_id:
        findings.append(
            _count_fact(
                collected.summary, collected.summary_evidence_id, collected.summary.service_ref
            )
        )
    unknown = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="Service and database evidence is incomplete for this investigation.",
        component_ref=None,
        evidence_refs=[],
        confidence=None,
        limitations=[f"Collection stopped because {gap.replace('_', ' ')}."],
    )
    findings.append(unknown)
    rows = pool_hypothesis_template(investigation_id)
    for row in rows:
        row["status"] = "unresolved"
    return ConclusionPlan(
        status="INCONCLUSIVE",
        findings=tuple(findings),
        hypotheses=rows,
        timeline=[],
        unknowns=(unknown.claim,),
        recommendations=(
            {"summary": "Collect the missing service and database evidence before concluding."},
        ),
        confidence=None,
        confidence_basis=(),
        root_finding_id=None,
        proposal=None,
    )


def _not_established(investigation_id: UUID, _collected: PoolCollected) -> ConclusionPlan:
    unknown = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="Database connection pool exhaustion was not established from the collected evidence.",
        component_ref=None,
        evidence_refs=[],
        confidence=None,
        limitations=[
            "The pool, error, and request evidence did not satisfy the exhaustion predicate."
        ],
    )
    rows = pool_hypothesis_template(investigation_id)
    for row in rows:
        row["status"] = "unresolved"
    return ConclusionPlan(
        status="INCONCLUSIVE",
        findings=(unknown,),
        hypotheses=rows,
        timeline=[],
        unknowns=(unknown.claim,),
        recommendations=(
            {"summary": "Do not attribute the failures to pool exhaustion from this evidence."},
        ),
        confidence=None,
        confidence_basis=(),
        root_finding_id=None,
        proposal=None,
    )


def _timeout_pointer(view: TraceView) -> str | None:
    for index, span in enumerate(view.spans):
        if span.span_name == "db.pool.acquire" and span.error_classification == TIMEOUT_CODE:
            return f"/spans/{index}/error_classification"
    return None


def _empty(
    gap: str,
    summary: ServiceRequestSummary | None = None,
    summary_evidence_id: UUID | None = None,
) -> PoolCollected:
    return PoolCollected(
        complete=False,
        gap=gap,
        summary=summary,
        summary_evidence_id=summary_evidence_id,
        errors=(),
        error_evidence_ids=(),
        logs=(),
        traces=(),
        pool=None,
        pool_evidence_ids=(),
    )


def _ref(evidence_id: UUID, pointer: str) -> EvidenceRef:
    return EvidenceRef(evidence_id=evidence_id, json_pointer=pointer, relation="supports")
