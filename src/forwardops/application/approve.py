from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from forwardops.domain.actions import (
    ActionStatus,
    DecisionKind,
    DecisionOutcome,
    evaluate_decision,
)
from forwardops.domain.errors import (
    ConflictError,
    IdempotencyConflictError,
    NotFoundError,
    PermissionDeniedError,
    ProposalExpiredError,
)
from forwardops.domain.hashing import sha256_canonical
from forwardops.storage.postgres import (
    Database,
    approval_for_action,
    approval_for_key,
    db_now,
    get_action,
    insert_approval,
    insert_audit,
    lock_action,
    update_action_status,
)


@dataclass(frozen=True)
class ActionDecision:
    action: dict[str, Any]
    approval: dict[str, Any]
    replayed: bool


async def decide_action(
    database: Database,
    *,
    tenant_id: str,
    principal_id: str,
    roles: set[str] | frozenset[str],
    action_id: Any,
    decision: DecisionKind,
    proposal_digest: str,
    reason: str,
    idempotency_key: str,
    request_id: str,
) -> ActionDecision:
    if "approver" not in roles:
        raise PermissionDeniedError("approver role is required")
    request_digest = sha256_canonical(
        {"decision": decision.value, "proposal_digest": proposal_digest, "reason": reason}
    )
    expired = False
    result: ActionDecision | None = None
    async with database.transaction(tenant_id) as conn:
        action = await lock_action(conn, tenant_id, action_id)
        if action is None:
            raise NotFoundError("action not found")
        if action["execution_enabled"]:
            raise ConflictError("execution is not enabled for this proposal")
        keyed = await approval_for_key(conn, tenant_id, principal_id, idempotency_key)
        if keyed is not None:
            same_request = (
                keyed["request_digest"] == request_digest and keyed["action_id"] == action["id"]
            )
            if not same_request:
                raise IdempotencyConflictError(
                    "idempotency key was reused with a different decision"
                )
            result = ActionDecision(action, keyed, True)
        else:
            existing = await approval_for_action(conn, tenant_id, action["id"])
            outcome = evaluate_decision(
                requester_id=action["requester_id"],
                approver_id=principal_id,
                status=ActionStatus(action["status"]),
                stored_digest=action["proposal_digest"],
                provided_digest=proposal_digest,
                expires_at=action["expires_at"],
                now=await db_now(conn),
                execution_enabled=bool(action["execution_enabled"]),
                existing_decision=None if existing is None else DecisionKind(existing["decision"]),
                requested=decision,
            )
            if outcome is DecisionOutcome.REPLAY:
                if existing is None:
                    raise ConflictError("approval replay is missing its decision row")
                result = ActionDecision(action, existing, True)
            elif outcome is DecisionOutcome.EXPIRE:
                await update_action_status(
                    conn,
                    tenant_id=tenant_id,
                    action_id=action["id"],
                    status=ActionStatus.EXPIRED.value,
                    expected_version=action["state_version"],
                )
                await insert_audit(
                    conn,
                    tenant_id=tenant_id,
                    audit_id=uuid4(),
                    investigation_id=action["investigation_id"],
                    action_id=action["id"],
                    actor_id=principal_id,
                    actor_kind="user",
                    event_type="action.expired",
                    request_id=request_id,
                    correlation_id=str(action["investigation_id"]),
                    trace_id=None,
                    entity_type="action",
                    entity_id=action["id"],
                    entity_version=action["state_version"] + 1,
                    details={"execution_enabled": False},
                )
                expired = True
            else:
                approval_id = uuid4()
                new_status = (
                    ActionStatus.APPROVED
                    if decision is DecisionKind.APPROVE
                    else ActionStatus.REJECTED
                )
                await insert_approval(
                    conn,
                    {
                        "tenant_id": tenant_id,
                        "id": approval_id,
                        "investigation_id": action["investigation_id"],
                        "action_id": action["id"],
                        "decision": decision.value,
                        "approver_id": principal_id,
                        "proposal_digest": proposal_digest,
                        "policy_version": action["policy_version"],
                        "reason": reason,
                        "idempotency_key": idempotency_key,
                        "request_digest": request_digest,
                        "expires_at": action["expires_at"],
                    },
                )
                await update_action_status(
                    conn,
                    tenant_id=tenant_id,
                    action_id=action["id"],
                    status=new_status.value,
                    expected_version=action["state_version"],
                )
                await insert_audit(
                    conn,
                    tenant_id=tenant_id,
                    audit_id=uuid4(),
                    investigation_id=action["investigation_id"],
                    action_id=action["id"],
                    actor_id=principal_id,
                    actor_kind="user",
                    event_type="action.approved"
                    if decision is DecisionKind.APPROVE
                    else "action.rejected",
                    request_id=request_id,
                    correlation_id=str(action["investigation_id"]),
                    trace_id=None,
                    entity_type="action",
                    entity_id=action["id"],
                    entity_version=action["state_version"] + 1,
                    details={"decision": decision.value, "execution_enabled": False},
                )
                updated = await get_action(conn, tenant_id, action["id"])
                approval = await approval_for_action(conn, tenant_id, action["id"])
                if updated is None or approval is None:
                    raise ConflictError("action decision was not stored")
                result = ActionDecision(updated, approval, False)
    if expired:
        raise ProposalExpiredError("proposal has expired")
    if result is None:
        raise ConflictError("action decision was not recorded")
    return result
