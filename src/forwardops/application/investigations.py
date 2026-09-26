from datetime import UTC, datetime
from uuid import uuid4

from psycopg.errors import UniqueViolation

from forwardops.config import Settings
from forwardops.domain.errors import IdempotencyConflictError, PermissionDeniedError
from forwardops.domain.hashing import sha256_canonical, to_canonical
from forwardops.domain.investigation import InvestigationScope
from forwardops.storage.postgres import (
    Database,
    InvestigationRecord,
    get_by_idempotency,
    get_investigation,
    insert_audit,
    insert_investigation,
)


async def create_investigation(
    database: Database,
    settings: Settings,
    *,
    principal_id: str,
    tenant_id: str,
    roles: set[str] | frozenset[str],
    question: str,
    idempotency_key: str,
    request_id: str,
) -> tuple[InvestigationRecord, bool]:
    if "investigator" not in roles:
        raise PermissionDeniedError("investigator role is required")
    if tenant_id != settings.customer.tenant_id:
        raise PermissionDeniedError("this deployment serves a different tenant")
    cleaned = question.strip()
    digest = sha256_canonical({"question": cleaned})
    customer = settings.customer
    scope = to_canonical(
        InvestigationScope(
            service_ref=customer.service_ref,
            vault_ref=customer.vault_ref,
            oracle_ref=customer.oracle_ref,
            cluster_ref=customer.cluster_ref,
            interval_start=customer.window.start,
            interval_end=customer.window.end,
            sample_cap=customer.sample_cap,
            updater_target=customer.updater_target,
            program_id=customer.program_id,
            oracle_program_id=customer.oracle_program_id,
            vault_address=customer.vault_address,
            oracle_address=customer.oracle_address,
        )
    )
    now = datetime.now(UTC)
    record = InvestigationRecord(
        tenant_id=tenant_id,
        id=uuid4(),
        requester_id=principal_id,
        question=cleaned,
        scope=scope,
        status="CREATED",
        state_version=0,
        config_digest=settings.config_digest,
        playbook_version=customer.playbook_version,
        analysis_mode=settings.analysis_mode,
        data_mode="replay",
        hypotheses=[],
        timeline=[],
        unknowns=[],
        recommendations=[],
        model_calls=[],
        budget={
            "max_tool_calls": settings.max_tool_calls,
            "deadline_seconds": settings.deadline_seconds,
            "tool_calls_used": 0,
            "max_model_calls": settings.max_model_calls,
            "max_analysis_rounds": settings.max_analysis_rounds,
            "token_budget": settings.token_budget,
            "model_calls_used": 0,
            "analysis_rounds_used": 0,
            "tokens_used": 0,
        },
        confidence=None,
        confidence_basis=[],
        root_finding_id=None,
        idempotency_key=idempotency_key,
        request_digest=digest,
        created_at=now,
        updated_at=now,
        completed_at=None,
        next_attempt_at=now,
        lease_owner=None,
        lease_expires_at=None,
        lease_epoch=0,
        failure=None,
    )
    try:
        return await _insert(database, record, request_id, digest)
    except UniqueViolation:
        return await _replay(database, record, digest)


async def _insert(
    database: Database,
    record: InvestigationRecord,
    request_id: str,
    digest: str,
) -> tuple[InvestigationRecord, bool]:
    async with database.transaction(record.tenant_id) as conn:
        existing = await get_by_idempotency(
            conn,
            record.tenant_id,
            record.requester_id,
            record.idempotency_key,
        )
        if existing is not None:
            _check_digest(existing, digest)
            return existing, True
        await insert_investigation(conn, record)
        await insert_audit(
            conn,
            tenant_id=record.tenant_id,
            audit_id=uuid4(),
            investigation_id=record.id,
            action_id=None,
            actor_id=record.requester_id,
            actor_kind="user",
            event_type="investigation.created",
            request_id=request_id,
            correlation_id=str(record.id),
            trace_id=None,
            entity_type="investigation",
            entity_id=record.id,
            entity_version=0,
            details={"status": "CREATED"},
        )
        stored = await get_investigation(conn, record.tenant_id, record.id)
    if stored is None:
        raise RuntimeError("investigation insert was not visible")
    return stored, False


async def _replay(
    database: Database,
    record: InvestigationRecord,
    digest: str,
) -> tuple[InvestigationRecord, bool]:
    async with database.transaction(record.tenant_id) as conn:
        existing = await get_by_idempotency(
            conn,
            record.tenant_id,
            record.requester_id,
            record.idempotency_key,
        )
    if existing is None:
        raise RuntimeError("idempotency conflict could not be reread")
    _check_digest(existing, digest)
    return existing, True


def _check_digest(existing: InvestigationRecord, digest: str) -> None:
    if existing.request_digest != digest:
        raise IdempotencyConflictError("idempotency key was reused with a different question")
