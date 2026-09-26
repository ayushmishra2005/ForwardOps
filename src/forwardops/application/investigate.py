import logging
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from forwardops.application.model_investigation import investigate_with_model
from forwardops.application.playbooks.database_pool import (
    investigate_database_pool,
    pool_hypothesis_template,
)
from forwardops.application.playbooks.withdrawals import ConclusionPlan, investigate_withdrawals
from forwardops.config import Settings, scenario_for_question
from forwardops.domain.actions import proposal_digest
from forwardops.domain.errors import InvalidTransitionError, LostLeaseError, SuspendedError
from forwardops.domain.evidence import FindingDraft, resolve_json_pointer, validate_finding
from forwardops.domain.investigation import (
    DATABASE_POOL_SCENARIO,
    TERMINAL_STATUSES,
    InvestigationScope,
    InvestigationStatus,
    assert_transition,
    hypothesis_template,
)
from forwardops.runtime import Runtime
from forwardops.storage.leases import Claim
from forwardops.storage.postgres import (
    Database,
    InvestigationRecord,
    assert_evidence_present,
    count_findings,
    count_succeeded_tools,
    db_now,
    evidence_payloads,
    get_investigation,
    insert_action,
    insert_audit,
    insert_finding,
    mark_failed,
    release_lease,
    require_live_lease,
    save_conclusion,
    transition_status,
)
from forwardops.tools.gateway import ToolContext, ToolGateway

logger = logging.getLogger(__name__)


async def run_claimed(
    database: Database,
    runtime: Runtime,
    claim: Claim,
    *,
    suspend_after_new_tool_calls: int | None,
    trace_id: str,
) -> None:
    try:
        await _advance(
            database,
            runtime,
            claim,
            suspend_after_new_tool_calls=suspend_after_new_tool_calls,
            trace_id=trace_id,
        )
    except SuspendedError:
        async with database.transaction(claim.tenant_id) as conn:
            await release_lease(conn, claim)
        logger.info(
            "investigation suspended",
            extra={"investigation_id": str(claim.investigation_id), "tenant_id": claim.tenant_id},
        )
    except LostLeaseError:
        logger.warning(
            "lost investigation lease",
            extra={"investigation_id": str(claim.investigation_id), "tenant_id": claim.tenant_id},
        )
    except Exception:
        logger.exception(
            "investigation failed",
            extra={"investigation_id": str(claim.investigation_id), "tenant_id": claim.tenant_id},
        )
        await _record_failure(database, claim, trace_id)


async def _advance(
    database: Database,
    runtime: Runtime,
    claim: Claim,
    *,
    suspend_after_new_tool_calls: int | None,
    trace_id: str,
) -> None:
    investigation, now = await _load(database, claim)
    if investigation.status in TERMINAL_STATUSES:
        async with database.transaction(claim.tenant_id) as conn:
            await release_lease(conn, claim)
        return
    customer = runtime.settings.customer
    if _past_deadline(investigation, runtime.settings, now):
        await _record_failure(database, claim, trace_id, "deadline exceeded")
        return
    if (
        investigation.tenant_id != customer.tenant_id
        or scenario_for_question(customer, investigation.question) is None
    ):
        await _persist(
            database,
            runtime.settings,
            claim,
            investigation,
            _unsupported_plan(investigation.id),
            trace_id,
        )
        return
    if investigation.status == InvestigationStatus.CREATED:
        await _transition(
            database,
            claim,
            investigation.status,
            InvestigationStatus.PLANNING,
            _opening_hypotheses(investigation),
            trace_id,
        )
        investigation, _now = await _load(database, claim)
    if investigation.status == InvestigationStatus.PLANNING:
        await _transition(
            database,
            claim,
            investigation.status,
            InvestigationStatus.COLLECTING_EVIDENCE,
            None,
            trace_id,
        )
        investigation, _now = await _load(database, claim)
    plan: ConclusionPlan | None = None
    if investigation.status == InvestigationStatus.COLLECTING_EVIDENCE:
        plan = await _playbook(
            database, runtime, claim, investigation, suspend_after_new_tool_calls, trace_id
        )
        await _transition(
            database,
            claim,
            investigation.status,
            InvestigationStatus.ANALYZING,
            None,
            trace_id,
        )
        investigation, _now = await _load(database, claim)
    if investigation.status == InvestigationStatus.ANALYZING:
        if plan is None:
            plan = await _playbook(database, runtime, claim, investigation, None, trace_id)
        await _persist(database, runtime.settings, claim, investigation, plan, trace_id)


async def _playbook(
    database: Database,
    runtime: Runtime,
    claim: Claim,
    investigation: InvestigationRecord,
    suspend_after_new_tool_calls: int | None,
    trace_id: str,
) -> ConclusionPlan:

    scope = InvestigationScope.model_validate(investigation.scope)
    expected = scenario_for_question(runtime.settings.customer, investigation.question)
    if expected is None or scope.scenario_id != expected:
        return _unsupported_plan(investigation.id)
    context = ToolContext(
        tenant_id=claim.tenant_id,
        principal_id=investigation.requester_id,
        investigation_id=investigation.id,
        scope=scope,
        request_id=trace_id,
        trace_id=trace_id,
        lease_epoch=claim.lease_epoch,
        lease_owner=claim.lease_owner,
        frozen_observed_at=_frozen_observed_at(runtime.settings.customer, scope),
        max_tool_calls=int(
            investigation.budget.get("max_tool_calls", runtime.settings.max_tool_calls)
        ),
        lease_seconds=runtime.settings.lease_seconds,
    )
    gateway = ToolGateway(
        database,
        runtime.handlers,
        context,
        suspend_after_new_tool_calls=suspend_after_new_tool_calls,
    )
    if scope.scenario_id == DATABASE_POOL_SCENARIO:
        if investigation.analysis_mode == "model":
            from forwardops.application.model_database import investigate_database_with_model

            return await investigate_database_with_model(
                database, gateway, runtime, investigation, scope
            )
        return await investigate_database_pool(
            gateway, scope, investigation.id, runtime.settings.customer
        )
    if investigation.analysis_mode == "model":
        return await investigate_with_model(database, gateway, runtime, investigation, scope)
    return await investigate_withdrawals(
        gateway, scope, investigation.id, runtime.settings.customer
    )


async def _persist(
    database: Database,
    settings: Settings,
    claim: Claim,
    investigation: InvestigationRecord,
    plan: ConclusionPlan,
    trace_id: str,
) -> None:
    assert_transition(InvestigationStatus(investigation.status), InvestigationStatus(plan.status))
    async with database.transaction(claim.tenant_id) as conn:
        await require_live_lease(conn, claim)
        fresh = await get_investigation(conn, claim.tenant_id, claim.investigation_id)
        if fresh is None:
            raise LostLeaseError("investigation lease does not match this worker")
        if await count_findings(conn, claim.tenant_id, claim.investigation_id):
            raise RuntimeError("findings already exist for this investigation")
        stored = await evidence_payloads(conn, claim.tenant_id, claim.investigation_id)
        payloads = {evidence_id: item["payload"] for evidence_id, item in stored.items()}
        for finding in plan.findings:
            validate_finding(finding, payloads)
            await assert_evidence_present(
                conn,
                claim.tenant_id,
                claim.investigation_id,
                [ref.evidence_id for ref in finding.evidence_refs],
            )
            await insert_finding(
                conn,
                tenant_id=claim.tenant_id,
                finding_id=finding.id,
                investigation_id=claim.investigation_id,
                classification=finding.classification,
                claim=finding.claim,
                component_ref=finding.component_ref,
                evidence_refs=[ref.model_dump(mode="json") for ref in finding.evidence_refs],
                derivation=finding.derivation,
                confidence=finding.confidence,
                confidence_basis=finding.confidence_basis,
                alternatives=finding.alternatives,
                limitations=finding.limitations,
            )
        action_id = None
        if plan.proposal is not None:
            action_id = uuid4()
            now = await db_now(conn)
            expires_at = now + timedelta(seconds=settings.customer.proposal_ttl_seconds)
            digest_evidence = _digest_evidence(plan.proposal.evidence_refs, stored)
            for ref in plan.proposal.evidence_refs:
                resolve_json_pointer(stored[ref.evidence_id]["payload"], ref.json_pointer)
            digest = proposal_digest(
                action_type=plan.proposal.action_type,
                target_ref=plan.proposal.target_ref,
                parameters=plan.proposal.parameters,
                evidence=digest_evidence,
                risk=plan.proposal.risk,
                preconditions=plan.proposal.preconditions,
                policy_version=plan.proposal.policy_version,
                config_digest=investigation.config_digest,
                expires_at=expires_at,
            )
            await insert_action(
                conn,
                tenant_id=claim.tenant_id,
                action_id=action_id,
                investigation_id=claim.investigation_id,
                requester_id=investigation.requester_id,
                action_type=plan.proposal.action_type,
                target_ref=plan.proposal.target_ref,
                parameters=plan.proposal.parameters,
                reason=plan.proposal.reason,
                evidence_refs=[ref.model_dump(mode="json") for ref in plan.proposal.evidence_refs],
                risk=plan.proposal.risk,
                proposal_digest=digest,
                policy_version=plan.proposal.policy_version,
                config_digest=investigation.config_digest,
                preconditions=plan.proposal.preconditions,
                expires_at=expires_at,
                execution_key=uuid4(),
            )
            await insert_audit(
                conn,
                tenant_id=claim.tenant_id,
                audit_id=uuid4(),
                investigation_id=claim.investigation_id,
                action_id=action_id,
                actor_id=claim.lease_owner,
                actor_kind="worker",
                event_type="action.proposed",
                request_id=trace_id,
                correlation_id=str(claim.investigation_id),
                trace_id=trace_id,
                entity_type="action",
                entity_id=action_id,
                entity_version=1,
                details={"action_type": plan.proposal.action_type, "execution_enabled": False},
            )
        used = await count_succeeded_tools(conn, claim.tenant_id, claim.investigation_id)
        budget = dict(fresh.budget)
        budget["tool_calls_used"] = used
        await save_conclusion(
            conn,
            claim,
            current=investigation.status,
            status=plan.status,
            hypotheses=plan.hypotheses,
            timeline=plan.timeline,
            unknowns=list(plan.unknowns),
            recommendations=list(plan.recommendations),
            confidence=plan.confidence,
            confidence_basis=list(plan.confidence_basis),
            root_finding_id=plan.root_finding_id,
            budget=budget,
            model_calls=list(fresh.model_calls or []),
        )
        await insert_audit(
            conn,
            tenant_id=claim.tenant_id,
            audit_id=uuid4(),
            investigation_id=claim.investigation_id,
            action_id=action_id,
            actor_id=claim.lease_owner,
            actor_kind="worker",
            event_type="investigation.state_changed",
            request_id=trace_id,
            correlation_id=str(claim.investigation_id),
            trace_id=trace_id,
            entity_type="investigation",
            entity_id=claim.investigation_id,
            entity_version=None,
            details={"from": investigation.status, "to": plan.status},
        )


def _digest_evidence(refs, stored: dict) -> list[dict[str, str]]:
    evidence: list[dict[str, str]] = []
    seen: set[str] = set()
    for ref in refs:
        evidence_id = str(ref.evidence_id)
        if evidence_id in seen:
            continue
        seen.add(evidence_id)
        evidence.append(
            {
                "evidence_id": evidence_id,
                "payload_sha256": stored[ref.evidence_id]["payload_sha256"],
            }
        )
    return evidence


def _unsupported_plan(investigation_id: UUID) -> ConclusionPlan:
    unknown = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="No playbook is registered for this question.",
        component_ref=None,
        evidence_refs=[],
        confidence=None,
        limitations=["The deployment only investigates its configured questions."],
    )
    rows = hypothesis_template(investigation_id)
    for row in rows:
        row["status"] = "unresolved"
    return ConclusionPlan(
        status="INCONCLUSIVE",
        findings=(unknown,),
        hypotheses=rows,
        timeline=[],
        unknowns=(unknown.claim,),
        recommendations=({"summary": "No automated playbook ran."},),
        confidence=None,
        confidence_basis=(),
        root_finding_id=None,
        proposal=None,
    )


async def _transition(
    database: Database,
    claim: Claim,
    current: str,
    new: InvestigationStatus,
    hypotheses: list | None,
    trace_id: str,
) -> None:
    assert_transition(InvestigationStatus(current), new)
    async with database.transaction(claim.tenant_id) as conn:
        await require_live_lease(conn, claim)
        await transition_status(conn, claim, current=current, new=new.value, hypotheses=hypotheses)
        await insert_audit(
            conn,
            tenant_id=claim.tenant_id,
            audit_id=uuid4(),
            investigation_id=claim.investigation_id,
            action_id=None,
            actor_id=claim.lease_owner,
            actor_kind="worker",
            event_type="investigation.state_changed",
            request_id=trace_id,
            correlation_id=str(claim.investigation_id),
            trace_id=trace_id,
            entity_type="investigation",
            entity_id=claim.investigation_id,
            entity_version=None,
            details={"from": current, "to": new.value},
        )


async def _load(database: Database, claim: Claim) -> tuple[InvestigationRecord, datetime]:
    async with database.transaction(claim.tenant_id) as conn:
        investigation = await get_investigation(conn, claim.tenant_id, claim.investigation_id)
        if (
            investigation is None
            or investigation.lease_epoch != claim.lease_epoch
            or investigation.lease_owner != claim.lease_owner
        ):
            raise LostLeaseError("investigation lease does not match this worker")
        return investigation, await db_now(conn)


def _opening_hypotheses(investigation: InvestigationRecord) -> list:
    if investigation.scope.get("scenario_id") == DATABASE_POOL_SCENARIO:
        return pool_hypothesis_template(investigation.id)
    return hypothesis_template(investigation.id)


def _frozen_observed_at(customer: Any, scope: InvestigationScope) -> str:
    scenario = customer.database_scenario
    if scope.scenario_id == DATABASE_POOL_SCENARIO and scenario is not None:
        return scenario.frozen_observed_at
    return customer.frozen_observed_at


def _past_deadline(investigation: InvestigationRecord, settings: Settings, now: datetime) -> bool:
    seconds = int(investigation.budget.get("deadline_seconds", settings.deadline_seconds))
    return now > investigation.created_at + timedelta(seconds=seconds)


async def _record_failure(
    database: Database,
    claim: Claim,
    trace_id: str,
    message: str = "investigation failed",
) -> None:
    try:
        async with database.transaction(claim.tenant_id) as conn:
            investigation = await get_investigation(conn, claim.tenant_id, claim.investigation_id)
            if investigation is None or investigation.status in TERMINAL_STATUSES:
                return
            if (
                investigation.lease_epoch != claim.lease_epoch
                or investigation.lease_owner != claim.lease_owner
            ):
                return
            assert_transition(InvestigationStatus(investigation.status), InvestigationStatus.FAILED)
            await mark_failed(
                conn,
                claim,
                current=investigation.status,
                failure={"code": "WORKER_ERROR", "message": message},
            )
            await insert_audit(
                conn,
                tenant_id=claim.tenant_id,
                audit_id=uuid4(),
                investigation_id=claim.investigation_id,
                action_id=None,
                actor_id=claim.lease_owner,
                actor_kind="worker",
                event_type="investigation.state_changed",
                request_id=trace_id,
                correlation_id=str(claim.investigation_id),
                trace_id=trace_id,
                entity_type="investigation",
                entity_id=claim.investigation_id,
                entity_version=None,
                details={"from": investigation.status, "to": "FAILED"},
            )
    except (LostLeaseError, InvalidTransitionError):
        return
