from uuid import uuid4

import pytest

from forwardops.domain.errors import InvalidFindingError
from forwardops.domain.evidence import EvidenceRef, FindingDraft, validate_finding


def _fact(evidence_id, pointer: str = "/error_name") -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim="The vault program emitted a stale-oracle failure for transaction example.",
        component_ref="vault-a",
        evidence_refs=[
            EvidenceRef(evidence_id=evidence_id, json_pointer=pointer, relation="supports")
        ],
    )


def test_fact_without_evidence_is_rejected() -> None:
    finding = FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim="An uncited fact.",
        component_ref=None,
        evidence_refs=[],
    )
    with pytest.raises(InvalidFindingError, match="supporting evidence"):
        validate_finding(finding, {})


def test_fact_pointer_must_resolve() -> None:
    evidence_id = uuid4()
    finding = _fact(evidence_id, "/missing")
    with pytest.raises(InvalidFindingError, match="does not exist"):
        validate_finding(finding, {evidence_id: {"error_name": "StaleOracle"}})


def test_unknown_evidence_id_is_rejected() -> None:
    evidence_id = uuid4()
    finding = _fact(evidence_id)
    with pytest.raises(InvalidFindingError, match="not part of the investigation"):
        validate_finding(finding, {})


def test_inference_requires_derivation() -> None:
    evidence_id = uuid4()
    finding = FindingDraft(
        id=uuid4(),
        classification="INFERENCE",
        claim="Oracle freshness rejection explains the inspected withdrawal failures.",
        component_ref="oracle-a",
        evidence_refs=[
            EvidenceRef(evidence_id=evidence_id, json_pointer="/error_name", relation="supports")
        ],
        derivation=None,
        confidence="high",
    )
    with pytest.raises(InvalidFindingError, match="derivation"):
        validate_finding(finding, {evidence_id: {"error_name": "StaleOracle"}})


def test_unknown_requires_limitation_and_empty_confidence() -> None:
    missing_limitation = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="Why the oracle publisher stopped updating is unknown.",
        component_ref=None,
        evidence_refs=[],
        limitations=[],
    )
    with pytest.raises(InvalidFindingError, match="limitation"):
        validate_finding(missing_limitation, {})
    confident = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="Why the oracle publisher stopped updating is unknown.",
        component_ref=None,
        evidence_refs=[],
        confidence="high",
        limitations=["No publisher evidence was collected."],
    )
    with pytest.raises(InvalidFindingError, match="confidence"):
        validate_finding(confident, {})


def test_valid_fact_is_accepted() -> None:
    evidence_id = uuid4()
    validate_finding(_fact(evidence_id), {evidence_id: {"error_name": "StaleOracle"}})
