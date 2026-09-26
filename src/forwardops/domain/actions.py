from datetime import datetime
from enum import StrEnum
from typing import Any

from forwardops.domain.errors import (
    ConflictError,
    DigestMismatchError,
    SelfApprovalError,
)
from forwardops.domain.hashing import sha256_canonical
from forwardops.domain.time import require_aware


class ActionStatus(StrEnum):
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class DecisionKind(StrEnum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"


class DecisionOutcome(StrEnum):
    APPLY = "APPLY"
    REPLAY = "REPLAY"
    EXPIRE = "EXPIRE"


def proposal_digest(
    *,
    action_type: str,
    target_ref: str,
    parameters: dict[str, Any],
    evidence: list[dict[str, str]],
    risk: str,
    preconditions: dict[str, Any],
    policy_version: str,
    config_digest: str,
    expires_at: datetime,
) -> str:
    ordered = sorted(evidence, key=lambda item: item["evidence_id"])
    return sha256_canonical(
        {
            "action_type": action_type,
            "target_ref": target_ref,
            "parameters": parameters,
            "evidence": ordered,
            "risk": risk,
            "preconditions": preconditions,
            "policy_version": policy_version,
            "config_digest": config_digest,
            "expires_at": require_aware(expires_at),
        }
    )


def evaluate_decision(
    *,
    requester_id: str,
    approver_id: str,
    status: ActionStatus,
    stored_digest: str,
    provided_digest: str,
    expires_at: datetime,
    now: datetime,
    execution_enabled: bool,
    existing_decision: DecisionKind | None,
    requested: DecisionKind,
) -> DecisionOutcome:
    if approver_id == requester_id:
        raise SelfApprovalError("the requester cannot approve their own proposal")
    if execution_enabled:
        raise ConflictError("execution is not enabled for this proposal")
    if existing_decision is not None:
        expected = (
            ActionStatus.APPROVED
            if existing_decision is DecisionKind.APPROVE
            else ActionStatus.REJECTED
        )
        same_decision = (
            status is expected
            and requested is existing_decision
            and provided_digest == stored_digest
        )
        if same_decision:
            return DecisionOutcome.REPLAY
        raise ConflictError("the proposal already has a different decision")
    if provided_digest != stored_digest:
        raise DigestMismatchError("proposal digest does not match")
    if require_aware(now) >= require_aware(expires_at):
        return DecisionOutcome.EXPIRE
    if status is not ActionStatus.WAITING_FOR_APPROVAL:
        raise ConflictError(f"proposal is {status}")
    return DecisionOutcome.APPLY
