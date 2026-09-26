from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

_CLASSIFICATIONS = frozenset({"FACT", "INFERENCE", "UNKNOWN"})


class RequestScope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str
    vault_ref: str
    oracle_ref: str
    cluster_ref: str
    interval_start: str
    interval_end: str
    sample_cap: int = Field(ge=1, le=20)
    permitted_action_type: str
    permitted_target_ref: str


class HypothesisView(BaseModel):
    model_config = ConfigDict(extra="ignore")

    hypothesis_id: str
    key: str
    claim: str
    status: str


class EvidenceSummary(BaseModel):
    """Sanitized evidence returned to a model. Text fields stay untrusted data."""

    model_config = ConfigDict(extra="forbid")

    evidence_id: UUID
    kind: str
    summary: str
    untrusted: bool
    payload: dict[str, Any]


class ToolView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str
    parameters_schema: dict[str, Any]


class BudgetStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_tool_calls: int
    tool_calls_used: int
    max_model_calls: int
    model_calls_used: int
    max_analysis_rounds: int
    analysis_rounds_used: int
    token_budget: int
    tokens_used: int
    deadline_seconds: int


class ModelRequest(BaseModel):
    """What the application sends to a provider. Tool call ids are not included."""

    model_config = ConfigDict(extra="forbid")

    request_id: str
    investigation_id: UUID
    question: str
    hypotheses: list[HypothesisView]
    evidence: list[EvidenceSummary]
    allowed_tools: list[ToolView]
    budgets: BudgetStatus
    output_schema: dict[str, Any]
    scope: RequestScope
    collection_gaps: list[str]
    validation_notes: list[str]
    instructions: str


class ProposedEvidenceRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    evidence_id: UUID
    json_pointer: str
    relation: str

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        relation = value.get("relation")
        if isinstance(relation, str):
            value = {**value, "relation": relation.strip().lower()}
        return value


class ProposedFinding(BaseModel):
    model_config = ConfigDict(extra="ignore")

    classification: str
    claim: str = Field(min_length=1, max_length=2000)
    component_ref: str | None = None
    evidence_refs: list[ProposedEvidenceRef] = Field(default_factory=list)
    derivation: dict[str, Any] | None = None
    confidence: str | None = None
    confidence_basis: list[str] = Field(default_factory=list)
    alternatives: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        updated = dict(value)
        classification = updated.get("classification")
        if isinstance(classification, str):
            updated["classification"] = classification.strip().upper()
        confidence = updated.get("confidence")
        if isinstance(confidence, str):
            updated["confidence"] = confidence.strip().lower()
        return updated


class ProposedAction(BaseModel):
    """A suggestion. It cannot approve, enable, or execute remediation."""

    model_config = ConfigDict(extra="ignore")

    action_type: str = Field(min_length=1, max_length=64)
    target_ref: str | None = Field(default=None, max_length=64)


class ProposedAnalysis(BaseModel):
    model_config = ConfigDict(extra="ignore")

    findings: list[ProposedFinding]
    unknowns: list[str] = Field(default_factory=list)
    proposed_action: ProposedAction | None = None


class ProposedToolCall(BaseModel):
    """A tool the model wants called. ForwardOps assigns the tool call id."""

    model_config = ConfigDict(extra="ignore")

    name: str = Field(min_length=1, max_length=64)
    arguments: dict[str, Any] = Field(default_factory=dict)


class TokenUsage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


class ModelReply(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["tool_requests", "analysis"]
    tool_requests: list[ProposedToolCall] = Field(default_factory=list)
    analysis: ProposedAnalysis | None = None
    provider: str
    model: str
    finish_reason: str
    token_usage: TokenUsage | None = None

    @model_validator(mode="after")
    def _one_branch(self) -> "ModelReply":
        if self.kind == "tool_requests":
            if not self.tool_requests or self.analysis is not None:
                raise ValueError("a tool reply contains only tool requests")
        elif self.analysis is None or self.tool_requests:
            raise ValueError("an analysis reply contains only analysis")
        return self


def analysis_output_schema() -> dict[str, Any]:
    schema = ProposedAnalysis.model_json_schema()
    schema.pop("title", None)
    return schema


def classification_allowed(value: str) -> bool:
    return value in _CLASSIFICATIONS
