from datetime import UTC, datetime, timedelta

import pytest

from forwardops.domain.actions import (
    ActionStatus,
    DecisionKind,
    DecisionOutcome,
    evaluate_decision,
    proposal_digest,
)
from forwardops.domain.errors import ConflictError, DigestMismatchError, SelfApprovalError


def _now() -> datetime:
    return datetime(2026, 9, 26, 13, 0, tzinfo=UTC)


def _evaluate(**overrides):
    values = {
        "requester_id": "investigator-a",
        "approver_id": "approver-a",
        "status": ActionStatus.WAITING_FOR_APPROVAL,
        "stored_digest": "a" * 64,
        "provided_digest": "a" * 64,
        "expires_at": _now() + timedelta(hours=1),
        "now": _now(),
        "execution_enabled": False,
        "existing_decision": None,
        "requested": DecisionKind.APPROVE,
    }
    values.update(overrides)
    return evaluate_decision(**values)


def test_self_approval_is_rejected() -> None:
    with pytest.raises(SelfApprovalError):
        _evaluate(approver_id="investigator-a")


def test_digest_mismatch_is_rejected() -> None:
    with pytest.raises(DigestMismatchError):
        _evaluate(provided_digest="b" * 64)


def test_expired_proposal_is_not_applied() -> None:
    assert _evaluate(now=_now() + timedelta(hours=2)) is DecisionOutcome.EXPIRE
    boundary = _now() + timedelta(hours=1)
    assert _evaluate(expires_at=boundary, now=boundary) is DecisionOutcome.EXPIRE


def test_waiting_proposal_can_be_approved() -> None:
    assert _evaluate() is DecisionOutcome.APPLY


def test_identical_decision_replays() -> None:
    outcome = _evaluate(
        status=ActionStatus.APPROVED,
        existing_decision=DecisionKind.APPROVE,
        requested=DecisionKind.APPROVE,
    )
    assert outcome is DecisionOutcome.REPLAY


def test_conflicting_decision_is_rejected() -> None:
    with pytest.raises(ConflictError):
        _evaluate(
            status=ActionStatus.APPROVED,
            existing_decision=DecisionKind.APPROVE,
            requested=DecisionKind.REJECT,
        )


def test_execution_enabled_is_rejected() -> None:
    with pytest.raises(ConflictError, match="execution"):
        _evaluate(execution_enabled=True)


def test_digest_changes_when_parameters_change() -> None:
    expires_at = _now() + timedelta(hours=1)
    common = {
        "action_type": "restart_oracle_updater",
        "target_ref": "oracle-updater-a",
        "evidence": [{"evidence_id": "ev-1", "payload_sha256": "abc"}],
        "risk": "medium",
        "preconditions": {"publisher_cause_known": False},
        "policy_version": "v1",
        "config_digest": "cfg",
        "expires_at": expires_at,
    }
    first = proposal_digest(parameters={"workload_ref": "oracle-updater-a"}, **common)
    second = proposal_digest(parameters={"workload_ref": "other"}, **common)
    assert first != second
    assert len(first) == 64
