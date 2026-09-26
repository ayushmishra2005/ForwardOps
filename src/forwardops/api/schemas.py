from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CreateInvestigationBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1, max_length=512)


class DecisionBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    proposal_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str = Field(min_length=1, max_length=512)


class EvidenceView(BaseModel):
    id: UUID
    investigation_id: UUID
    source_type: str
    source_system: str
    event_time: Any
    retrieved_at: Any
    observed_at: Any
    correlation: dict[str, Any]
    payload: dict[str, Any]
    summary: str
    tool_call_id: UUID
    provenance: dict[str, Any]
    payload_digest: str
    kind: str


class FindingView(BaseModel):
    id: UUID
    classification: str
    claim: str
    component_ref: str | None
    evidence_refs: list[dict[str, Any]]
    derivation: dict[str, Any] | None
    confidence: str | None
    limitations: list[Any]


class ActionView(BaseModel):
    id: UUID
    investigation_id: UUID
    action_type: str
    target_ref: str
    parameters: dict[str, Any]
    reason: str
    evidence_refs: list[dict[str, Any]]
    risk: str
    status: str
    proposal_digest: str
    preconditions: dict[str, Any]
    expires_at: Any
    execution_enabled: bool
    execution_status: str
    execution_result: None
    decision: dict[str, Any] | None = None
    message: str | None = None


class InvestigationView(BaseModel):
    id: UUID
    status: str
    question: str
    analysis_mode: str
    data_mode: str
    scope: dict[str, Any]
    timeline: list[Any]
    evidence: list[EvidenceView]
    evidence_truncated: bool
    findings: list[FindingView]
    root_cause_hypothesis: dict[str, Any] | None
    confidence: str | None
    confidence_basis: list[Any]
    unknowns: list[Any]
    recommended_remediation: str | None
    pending_actions: list[ActionView]
    actions: list[ActionView]


def evidence_view(row: dict[str, Any]) -> EvidenceView:
    return EvidenceView(
        id=row["id"],
        investigation_id=row["investigation_id"],
        source_type=row["source_type"],
        source_system=row["source_system"],
        event_time=row["event_time"],
        retrieved_at=row["retrieved_at"],
        observed_at=row["observed_at"],
        correlation=row["correlation"],
        payload=row["payload"],
        summary=row["summary"],
        tool_call_id=row["tool_call_id"],
        provenance=row["provenance"],
        payload_digest=row["payload_sha256"],
        kind=row["kind"],
    )


def finding_view(row: dict[str, Any]) -> FindingView:
    return FindingView(
        id=row["id"],
        classification=row["classification"],
        claim=row["claim"],
        component_ref=row["component_ref"],
        evidence_refs=row["evidence_refs"],
        derivation=row["derivation"],
        confidence=row["confidence"],
        limitations=row["limitations"],
    )


def action_view(
    row: dict[str, Any], approval: dict[str, Any] | None = None, message: str | None = None
) -> ActionView:
    decision = None
    if approval is not None:
        decision = {
            "decision": approval["decision"],
            "approver_id": approval["approver_id"],
            "reason": approval["reason"],
            "decided_at": approval["decided_at"],
        }
    enabled = bool(row["execution_enabled"])
    return ActionView(
        id=row["id"],
        investigation_id=row["investigation_id"],
        action_type=row["action_type"],
        target_ref=row["target_ref"],
        parameters=row["parameters"],
        reason=row["reason"],
        evidence_refs=row["evidence_refs"],
        risk=row["risk"],
        status=row["status"],
        proposal_digest=row["proposal_digest"],
        preconditions=row["preconditions"],
        expires_at=row["expires_at"],
        execution_enabled=enabled,
        execution_status="NOT_ENABLED" if not enabled else "ENABLED",
        execution_result=None,
        decision=decision,
        message=message,
    )
