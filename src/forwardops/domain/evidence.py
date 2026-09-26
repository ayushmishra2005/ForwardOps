import re
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from forwardops.domain.errors import InvalidFindingError

_POINTER = re.compile(r"^/(?:[^/~]+|~0|~1)(?:/(?:[^/~]+|~0|~1))*$")
_CLASSIFICATIONS = frozenset({"FACT", "INFERENCE", "UNKNOWN"})
_RELATIONS = frozenset({"supports", "contradicts", "context"})


class EvidenceRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: UUID
    json_pointer: str
    relation: str

    @field_validator("relation")
    @classmethod
    def _relation(cls, value: str) -> str:
        if value not in _RELATIONS:
            raise ValueError("invalid evidence relation")
        return value

    @field_validator("json_pointer")
    @classmethod
    def _pointer(cls, value: str) -> str:
        if _POINTER.fullmatch(value) is None:
            raise ValueError("invalid JSON pointer")
        return value


class FindingDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    classification: str
    claim: str = Field(min_length=1, max_length=2000)
    component_ref: str | None = None
    evidence_refs: list[EvidenceRef]
    derivation: dict[str, Any] | None = None
    confidence: str | None = None
    confidence_basis: list[str] = Field(default_factory=list)
    alternatives: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)

    @field_validator("classification")
    @classmethod
    def _classification(cls, value: str) -> str:
        if value not in _CLASSIFICATIONS:
            raise ValueError("invalid finding classification")
        return value

    @field_validator("confidence")
    @classmethod
    def _confidence(cls, value: str | None) -> str | None:
        if value is not None and value not in {"low", "medium", "high"}:
            raise ValueError("invalid confidence")
        return value


def resolve_json_pointer(document: Any, pointer: str) -> Any:
    current = document
    for raw_part in pointer.lstrip("/").split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            index = int(part)
            if index >= len(current):
                raise InvalidFindingError(f"JSON pointer {pointer} is outside the payload")
            current = current[index]
            continue
        raise InvalidFindingError(f"JSON pointer {pointer} does not exist in the evidence payload")
    return current


def validate_finding(finding: FindingDraft, payloads: dict[UUID, dict[str, Any]]) -> None:
    known = set(payloads)
    supports = [ref for ref in finding.evidence_refs if ref.relation == "supports"]
    if finding.classification in {"FACT", "INFERENCE"} and not supports:
        raise InvalidFindingError(f"{finding.classification} findings require supporting evidence")
    if finding.classification == "INFERENCE" and not finding.derivation:
        raise InvalidFindingError("INFERENCE findings require a derivation")
    if finding.classification == "UNKNOWN":
        if not finding.limitations:
            raise InvalidFindingError("UNKNOWN findings require a limitation")
        if finding.confidence is not None:
            raise InvalidFindingError("UNKNOWN findings cannot carry a confidence")
    for ref in finding.evidence_refs:
        if ref.evidence_id not in known:
            raise InvalidFindingError(
                "finding cites evidence that is not part of the investigation"
            )
        resolve_json_pointer(payloads[ref.evidence_id], ref.json_pointer)


def evidence_digest(
    *,
    schema_version: int,
    source_type: str,
    source_system: str,
    source_locator: dict[str, Any],
    payload: dict[str, Any],
    provenance: dict[str, Any],
) -> str:
    from forwardops.domain.hashing import sha256_canonical

    return sha256_canonical(
        {
            "canonicalization": "forwardops-json-v1",
            "schema_version": schema_version,
            "source_type": source_type,
            "source_system": source_system,
            "source_locator": source_locator,
            "payload": payload,
            "provenance": provenance,
        }
    )
