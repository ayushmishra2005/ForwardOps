from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from forwardops.domain.errors import InvalidFindingError, LostLeaseError
from forwardops.storage.leases import Claim


@dataclass
class InvestigationRecord:
    tenant_id: str
    id: UUID
    requester_id: str
    question: str
    scope: dict[str, Any]
    status: str
    state_version: int
    config_digest: str
    playbook_version: str
    analysis_mode: str
    data_mode: str
    hypotheses: list[Any]
    timeline: list[Any]
    unknowns: list[Any]
    recommendations: list[Any]
    model_calls: list[Any]
    budget: dict[str, Any]
    confidence: str | None
    confidence_basis: list[Any]
    root_finding_id: UUID | None
    idempotency_key: str
    request_digest: str
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    next_attempt_at: datetime
    lease_owner: str | None
    lease_expires_at: datetime | None
    lease_epoch: int
    failure: dict[str, Any] | None


class Database:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    @asynccontextmanager
    async def transaction(self, tenant_id: str):
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT set_config('app.tenant_id', %s, true)",
                    (tenant_id,),
                )
                yield conn


def _json(value: Any) -> Jsonb:
    return Jsonb(value)


def _investigation(row: Mapping[str, Any]) -> InvestigationRecord:
    return InvestigationRecord(
        tenant_id=row["tenant_id"],
        id=row["id"],
        requester_id=row["requester_id"],
        question=row["question"],
        scope=row["scope"],
        status=row["status"],
        state_version=row["state_version"],
        config_digest=row["config_digest"],
        playbook_version=row["playbook_version"],
        analysis_mode=row["analysis_mode"],
        data_mode=row["data_mode"],
        hypotheses=row["hypotheses"],
        timeline=row["timeline"],
        unknowns=row["unknowns"],
        recommendations=row["recommendations"],
        model_calls=row["model_calls"],
        budget=row["budget"],
        confidence=row["confidence"],
        confidence_basis=row["confidence_basis"],
        root_finding_id=row["root_finding_id"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        completed_at=row["completed_at"],
        next_attempt_at=row["next_attempt_at"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        lease_epoch=row["lease_epoch"],
        failure=row["failure"],
    )


async def insert_investigation(conn: Any, record: InvestigationRecord) -> None:
    await conn.execute(
        """
        INSERT INTO investigations (
          tenant_id, id, requester_id, question, scope, status, state_version,
          config_digest, playbook_version, analysis_mode, data_mode, hypotheses,
          timeline, unknowns, recommendations, model_calls, budget, confidence,
          confidence_basis, root_finding_id, idempotency_key, request_digest,
          next_attempt_at, lease_epoch
        ) VALUES (
          %s, %s, %s, %s, %s, %s, 0,
          %s, %s, %s, %s, %s,
          '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, %s, NULL,
          '[]'::jsonb, NULL, %s, %s,
          clock_timestamp(), 0
        )
        """,
        (
            record.tenant_id,
            record.id,
            record.requester_id,
            record.question,
            _json(record.scope),
            record.status,
            record.config_digest,
            record.playbook_version,
            record.analysis_mode,
            record.data_mode,
            _json(record.hypotheses),
            _json(record.budget),
            record.idempotency_key,
            record.request_digest,
        ),
    )


async def get_investigation(
    conn: Any, tenant_id: str, investigation_id: UUID
) -> InvestigationRecord | None:
    cursor = await conn.execute(
        "SELECT * FROM investigations WHERE tenant_id = %s AND id = %s",
        (tenant_id, investigation_id),
    )
    row = await cursor.fetchone()
    return None if row is None else _investigation(row)


async def get_by_idempotency(
    conn: Any,
    tenant_id: str,
    requester_id: str,
    idempotency_key: str,
) -> InvestigationRecord | None:
    cursor = await conn.execute(
        """
        SELECT * FROM investigations
        WHERE tenant_id = %s AND requester_id = %s AND idempotency_key = %s
        """,
        (tenant_id, requester_id, idempotency_key),
    )
    row = await cursor.fetchone()
    return None if row is None else _investigation(row)


async def lease_matches(conn: Any, claim: Claim) -> bool:
    cursor = await conn.execute(
        """
        SELECT 1 FROM investigations
        WHERE tenant_id = %s AND id = %s AND lease_epoch = %s AND lease_owner = %s
        """,
        (claim.tenant_id, claim.investigation_id, claim.lease_epoch, claim.lease_owner),
    )
    return await cursor.fetchone() is not None


async def transition_status(
    conn: Any,
    claim: Claim,
    *,
    current: str,
    new: str,
    hypotheses: list[Any] | None = None,
) -> None:
    cursor = await conn.execute(
        """
        UPDATE investigations
        SET status = %s,
            state_version = state_version + 1,
            hypotheses = COALESCE(%s::jsonb, hypotheses),
            updated_at = clock_timestamp()
        WHERE tenant_id = %s AND id = %s AND lease_epoch = %s AND lease_owner = %s AND status = %s
        RETURNING id
        """,
        (
            new,
            None if hypotheses is None else _json(hypotheses),
            claim.tenant_id,
            claim.investigation_id,
            claim.lease_epoch,
            claim.lease_owner,
            current,
        ),
    )
    if await cursor.fetchone() is None:
        raise LostLeaseError("lost the investigation lease during a state transition")


async def mark_failed(conn: Any, claim: Claim, *, current: str, failure: dict[str, Any]) -> None:
    cursor = await conn.execute(
        """
        UPDATE investigations
        SET status = 'FAILED',
            state_version = state_version + 1,
            failure = %s,
            completed_at = clock_timestamp(),
            updated_at = clock_timestamp(),
            lease_owner = NULL,
            lease_expires_at = NULL
        WHERE tenant_id = %s AND id = %s AND lease_epoch = %s AND lease_owner = %s AND status = %s
        RETURNING id
        """,
        (
            _json(failure),
            claim.tenant_id,
            claim.investigation_id,
            claim.lease_epoch,
            claim.lease_owner,
            current,
        ),
    )
    if await cursor.fetchone() is None:
        raise LostLeaseError("lost the investigation lease while recording failure")


async def save_conclusion(
    conn: Any,
    claim: Claim,
    *,
    current: str,
    status: str,
    hypotheses: list[Any],
    timeline: list[Any],
    unknowns: list[Any],
    recommendations: list[Any],
    confidence: str | None,
    confidence_basis: list[Any],
    root_finding_id: UUID | None,
    budget: dict[str, Any],
) -> None:
    cursor = await conn.execute(
        """
        UPDATE investigations
        SET status = %s,
            state_version = state_version + 1,
            hypotheses = %s,
            timeline = %s,
            unknowns = %s,
            recommendations = %s,
            confidence = %s,
            confidence_basis = %s,
            root_finding_id = %s,
            budget = %s,
            completed_at = clock_timestamp(),
            updated_at = clock_timestamp(),
            lease_owner = NULL,
            lease_expires_at = NULL
        WHERE tenant_id = %s AND id = %s AND lease_epoch = %s AND lease_owner = %s AND status = %s
        RETURNING id
        """,
        (
            status,
            _json(hypotheses),
            _json(timeline),
            _json(unknowns),
            _json(recommendations),
            confidence,
            _json(confidence_basis),
            root_finding_id,
            _json(budget),
            claim.tenant_id,
            claim.investigation_id,
            claim.lease_epoch,
            claim.lease_owner,
            current,
        ),
    )
    if await cursor.fetchone() is None:
        raise LostLeaseError("lost the investigation lease while saving the conclusion")


async def insert_audit(
    conn: Any,
    *,
    tenant_id: str,
    audit_id: UUID,
    investigation_id: UUID | None,
    action_id: UUID | None,
    actor_id: str,
    actor_kind: str,
    event_type: str,
    request_id: str,
    correlation_id: str,
    trace_id: str | None,
    entity_type: str,
    entity_id: UUID | None,
    entity_version: int | None,
    details: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO audit_events (
          tenant_id, id, investigation_id, action_id, actor_id, actor_kind, event_type,
          request_id, correlation_id, trace_id, entity_type, entity_id, entity_version, details
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            tenant_id,
            audit_id,
            investigation_id,
            action_id,
            actor_id,
            actor_kind,
            event_type,
            request_id,
            correlation_id,
            trace_id,
            entity_type,
            entity_id,
            entity_version,
            _json(details),
        ),
    )


async def db_now(conn: Any) -> datetime:
    cursor = await conn.execute("SELECT clock_timestamp() AS now")
    row = await cursor.fetchone()
    return row["now"]


async def release_lease(conn: Any, claim: Claim) -> None:
    await conn.execute(
        """
        UPDATE investigations
        SET lease_owner = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
        WHERE tenant_id = %s AND id = %s AND lease_epoch = %s AND lease_owner = %s
        """,
        (claim.tenant_id, claim.investigation_id, claim.lease_epoch, claim.lease_owner),
    )


async def count_findings(conn: Any, tenant_id: str, investigation_id: UUID) -> int:
    cursor = await conn.execute(
        "SELECT count(*) AS count FROM findings WHERE tenant_id = %s AND investigation_id = %s",
        (tenant_id, investigation_id),
    )
    row = await cursor.fetchone()
    return int(row["count"])


async def list_findings(conn: Any, tenant_id: str, investigation_id: UUID) -> list[dict[str, Any]]:
    cursor = await conn.execute(
        """
        SELECT * FROM findings
        WHERE tenant_id = %s AND investigation_id = %s
        ORDER BY created_at, id
        """,
        (tenant_id, investigation_id),
    )
    return await cursor.fetchall()


async def insert_finding(
    conn: Any,
    *,
    tenant_id: str,
    finding_id: UUID,
    investigation_id: UUID,
    classification: str,
    claim: str,
    component_ref: str | None,
    evidence_refs: list[dict[str, Any]],
    derivation: dict[str, Any] | None,
    confidence: str | None,
    confidence_basis: list[str],
    alternatives: list[str],
    limitations: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO findings (
          tenant_id, id, investigation_id, classification, claim, component_ref,
          evidence_refs, derivation, confidence, confidence_basis, alternatives, limitations
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            tenant_id,
            finding_id,
            investigation_id,
            classification,
            claim,
            component_ref,
            _json(evidence_refs),
            None if derivation is None else _json(derivation),
            confidence,
            _json(confidence_basis),
            _json(alternatives),
            _json(limitations),
        ),
    )


async def count_succeeded_tools(conn: Any, tenant_id: str, investigation_id: UUID) -> int:
    cursor = await conn.execute(
        """
        SELECT count(*) AS count FROM tool_calls
        WHERE tenant_id = %s AND investigation_id = %s AND status = 'SUCCEEDED'
        """,
        (tenant_id, investigation_id),
    )
    row = await cursor.fetchone()
    return int(row["count"])


async def list_tool_calls(
    conn: Any, tenant_id: str, investigation_id: UUID
) -> list[dict[str, Any]]:
    cursor = await conn.execute(
        """
        SELECT * FROM tool_calls
        WHERE tenant_id = %s AND investigation_id = %s
        ORDER BY started_at, attempt
        """,
        (tenant_id, investigation_id),
    )
    return await cursor.fetchall()


async def succeeded_outputs(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
    tool_name: str,
) -> list[dict[str, Any]]:
    cursor = await conn.execute(
        """
        SELECT output FROM tool_calls
        WHERE tenant_id = %s AND investigation_id = %s AND tool_name = %s AND status = 'SUCCEEDED'
        ORDER BY started_at
        """,
        (tenant_id, investigation_id, tool_name),
    )
    rows = await cursor.fetchall()
    return [row["output"] for row in rows]


async def find_succeeded_call(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
    logical_call_id: UUID,
) -> dict[str, Any] | None:
    cursor = await conn.execute(
        """
        SELECT * FROM tool_calls
        WHERE tenant_id = %s AND investigation_id = %s AND logical_call_id = %s AND status = 'SUCCEEDED'
        ORDER BY attempt DESC
        LIMIT 1
        """,
        (tenant_id, investigation_id, logical_call_id),
    )
    return await cursor.fetchone()


async def next_attempt(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
    logical_call_id: UUID,
) -> int:
    cursor = await conn.execute(
        """
        SELECT COALESCE(MAX(attempt), 0) + 1 AS attempt
        FROM tool_calls
        WHERE tenant_id = %s AND investigation_id = %s AND logical_call_id = %s
        """,
        (tenant_id, investigation_id, logical_call_id),
    )
    row = await cursor.fetchone()
    return int(row["attempt"])


async def abandon_started(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
    logical_call_id: UUID,
) -> None:
    await conn.execute(
        """
        UPDATE tool_calls
        SET status = 'ABANDONED',
            finished_at = clock_timestamp(),
            error = %s
        WHERE tenant_id = %s AND investigation_id = %s AND logical_call_id = %s AND status = 'STARTED'
        """,
        (
            _json({"code": "ABANDONED", "message": "attempt did not finish"}),
            tenant_id,
            investigation_id,
            logical_call_id,
        ),
    )


async def insert_tool_call(
    conn: Any,
    *,
    tenant_id: str,
    tool_call_id: UUID,
    investigation_id: UUID,
    logical_call_id: UUID,
    attempt: int,
    tool_name: str,
    tool_version: str,
    arguments: dict[str, Any],
    arguments_digest: str,
    deadline_at: datetime,
    worker_epoch: int,
    source_id: str,
    trace_id: str,
    status: str = "STARTED",
    error: dict[str, Any] | None = None,
    retryable: bool = False,
) -> None:
    await conn.execute(
        """
        INSERT INTO tool_calls (
          tenant_id, id, investigation_id, logical_call_id, attempt, tool_name, tool_version,
          arguments, arguments_digest, status, error, deadline_at, worker_epoch, source_id,
          trace_id, retryable, schema_version, finished_at
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s,
          %s, %s, 1, CASE WHEN %s = 'STARTED' THEN NULL ELSE clock_timestamp() END
        )
        """,
        (
            tenant_id,
            tool_call_id,
            investigation_id,
            logical_call_id,
            attempt,
            tool_name,
            tool_version,
            _json(arguments),
            arguments_digest,
            status,
            None if error is None else _json(error),
            deadline_at,
            worker_epoch,
            source_id,
            trace_id,
            retryable,
            status,
        ),
    )


async def complete_tool_call(
    conn: Any,
    *,
    tenant_id: str,
    tool_call_id: UUID,
    output: dict[str, Any],
    output_digest: str,
) -> None:
    cursor = await conn.execute(
        """
        UPDATE tool_calls
        SET status = 'SUCCEEDED',
            output = %s,
            output_digest = %s,
            finished_at = clock_timestamp(),
            duration_ms = GREATEST(
              0,
              FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - started_at)) * 1000)
            )::int,
            retryable = false
        WHERE tenant_id = %s AND id = %s AND status = 'STARTED'
        RETURNING id
        """,
        (_json(output), output_digest, tenant_id, tool_call_id),
    )
    if await cursor.fetchone() is None:
        raise LostLeaseError("tool call was no longer open")


async def fail_tool_call(
    conn: Any,
    *,
    tenant_id: str,
    tool_call_id: UUID,
    error: dict[str, Any],
    retryable: bool,
) -> None:
    await conn.execute(
        """
        UPDATE tool_calls
        SET status = 'FAILED',
            error = %s,
            finished_at = clock_timestamp(),
            retryable = %s,
            duration_ms = GREATEST(
              0,
              FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - started_at)) * 1000)
            )::int
        WHERE tenant_id = %s AND id = %s AND status = 'STARTED'
        """,
        (_json(error), retryable, tenant_id, tool_call_id),
    )


async def insert_evidence(
    conn: Any,
    *,
    tenant_id: str,
    evidence_id: UUID,
    investigation_id: UUID,
    tool_call_id: UUID,
    kind: str,
    source_type: str,
    source_system: str,
    source_locator: dict[str, Any],
    event_time: datetime | None,
    observed_at: datetime,
    time_basis: str,
    correlation: dict[str, Any],
    payload: dict[str, Any],
    summary: str,
    provenance: dict[str, Any],
    coverage: dict[str, Any],
    payload_sha256: str,
    created_at: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO evidence (
          tenant_id, id, investigation_id, tool_call_id, kind, source_type, source_system,
          source_locator, event_time, observed_at, time_basis, correlation, payload, summary,
          provenance, coverage, schema_version, payload_sha256, redaction_version, created_at
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s, %s,
          %s, %s, 1, %s, 'v1', %s
        )
        """,
        (
            tenant_id,
            evidence_id,
            investigation_id,
            tool_call_id,
            kind,
            source_type,
            source_system,
            _json(source_locator),
            event_time,
            observed_at,
            time_basis,
            _json(correlation),
            _json(payload),
            summary,
            _json(provenance),
            _json(coverage),
            payload_sha256,
            created_at,
        ),
    )


async def evidence_ids_for_call(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
    tool_call_id: UUID,
) -> list[UUID]:
    cursor = await conn.execute(
        """
        SELECT id FROM evidence
        WHERE tenant_id = %s AND investigation_id = %s AND tool_call_id = %s
        ORDER BY created_at, id
        """,
        (tenant_id, investigation_id, tool_call_id),
    )
    rows = await cursor.fetchall()
    return [row["id"] for row in rows]


async def list_evidence(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
    *,
    evidence_id: UUID | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    if evidence_id is None:
        cursor = await conn.execute(
            """
            SELECT * FROM evidence
            WHERE tenant_id = %s AND investigation_id = %s
            ORDER BY created_at, id
            LIMIT %s OFFSET %s
            """,
            (tenant_id, investigation_id, limit, offset),
        )
    else:
        cursor = await conn.execute(
            """
            SELECT * FROM evidence
            WHERE tenant_id = %s AND investigation_id = %s AND id = %s
            ORDER BY created_at, id
            LIMIT %s OFFSET %s
            """,
            (tenant_id, investigation_id, evidence_id, limit, offset),
        )
    return await cursor.fetchall()


async def evidence_payloads(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
) -> dict[UUID, dict[str, Any]]:
    cursor = await conn.execute(
        """
        SELECT id, payload, payload_sha256 FROM evidence
        WHERE tenant_id = %s AND investigation_id = %s
        """,
        (tenant_id, investigation_id),
    )
    rows = await cursor.fetchall()
    return {
        row["id"]: {"payload": row["payload"], "payload_sha256": row["payload_sha256"]}
        for row in rows
    }


async def assert_evidence_present(
    conn: Any,
    tenant_id: str,
    investigation_id: UUID,
    evidence_ids: Sequence[UUID],
) -> None:
    if not evidence_ids:
        return
    cursor = await conn.execute(
        """
        SELECT id FROM evidence
        WHERE tenant_id = %s AND investigation_id = %s AND id = ANY(%s)
        """,
        (tenant_id, investigation_id, list(evidence_ids)),
    )
    found = {row["id"] for row in await cursor.fetchall()}
    missing = [str(item) for item in evidence_ids if item not in found]
    if missing:
        raise InvalidFindingError("evidence references are not in this investigation")


async def insert_action(
    conn: Any,
    *,
    tenant_id: str,
    action_id: UUID,
    investigation_id: UUID,
    requester_id: str,
    action_type: str,
    target_ref: str,
    parameters: dict[str, Any],
    reason: str,
    evidence_refs: list[dict[str, Any]],
    risk: str,
    proposal_digest: str,
    policy_version: str,
    config_digest: str,
    preconditions: dict[str, Any],
    expires_at: datetime,
    execution_key: UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO action_proposals (
          tenant_id, id, investigation_id, requester_id, action_type, target_ref, parameters,
          reason, evidence_refs, risk, status, proposal_digest, policy_version, config_digest,
          preconditions, expires_at, execution_enabled, execution_key, state_version
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s,
          %s, %s, %s, 'WAITING_FOR_APPROVAL', %s, %s, %s,
          %s, %s, FALSE, %s, 1
        )
        """,
        (
            tenant_id,
            action_id,
            investigation_id,
            requester_id,
            action_type,
            target_ref,
            _json(parameters),
            reason,
            _json(evidence_refs),
            risk,
            proposal_digest,
            policy_version,
            config_digest,
            _json(preconditions),
            expires_at,
            execution_key,
        ),
    )


async def list_actions(conn: Any, tenant_id: str, investigation_id: UUID) -> list[dict[str, Any]]:
    cursor = await conn.execute(
        """
        SELECT * FROM action_proposals
        WHERE tenant_id = %s AND investigation_id = %s
        ORDER BY created_at, id
        """,
        (tenant_id, investigation_id),
    )
    return await cursor.fetchall()


async def lock_action(conn: Any, tenant_id: str, action_id: UUID) -> dict[str, Any] | None:
    cursor = await conn.execute(
        "SELECT * FROM action_proposals WHERE tenant_id = %s AND id = %s FOR UPDATE",
        (tenant_id, action_id),
    )
    return await cursor.fetchone()


async def get_action(conn: Any, tenant_id: str, action_id: UUID) -> dict[str, Any] | None:
    cursor = await conn.execute(
        "SELECT * FROM action_proposals WHERE tenant_id = %s AND id = %s",
        (tenant_id, action_id),
    )
    return await cursor.fetchone()


async def approval_for_action(conn: Any, tenant_id: str, action_id: UUID) -> dict[str, Any] | None:
    cursor = await conn.execute(
        "SELECT * FROM approvals WHERE tenant_id = %s AND action_id = %s",
        (tenant_id, action_id),
    )
    return await cursor.fetchone()


async def approval_for_key(
    conn: Any,
    tenant_id: str,
    approver_id: str,
    idempotency_key: str,
) -> dict[str, Any] | None:
    cursor = await conn.execute(
        """
        SELECT * FROM approvals
        WHERE tenant_id = %s AND approver_id = %s AND idempotency_key = %s
        """,
        (tenant_id, approver_id, idempotency_key),
    )
    return await cursor.fetchone()


async def insert_approval(conn: Any, row: Mapping[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO approvals (
          tenant_id, id, investigation_id, action_id, decision, approver_id, proposal_digest,
          policy_version, reason, idempotency_key, request_digest, expires_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row["tenant_id"],
            row["id"],
            row["investigation_id"],
            row["action_id"],
            row["decision"],
            row["approver_id"],
            row["proposal_digest"],
            row["policy_version"],
            row["reason"],
            row["idempotency_key"],
            row["request_digest"],
            row["expires_at"],
        ),
    )


async def update_action_status(
    conn: Any,
    *,
    tenant_id: str,
    action_id: UUID,
    status: str,
    expected_version: int,
) -> None:
    cursor = await conn.execute(
        """
        UPDATE action_proposals
        SET status = %s,
            state_version = state_version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = %s AND id = %s AND state_version = %s
          AND execution_enabled = FALSE
          AND execution_result IS NULL
        RETURNING id
        """,
        (status, tenant_id, action_id, expected_version),
    )
    if await cursor.fetchone() is None:
        raise LostLeaseError("action decision lost a concurrent update")
