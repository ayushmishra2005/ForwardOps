from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from forwardops.domain.errors import InvalidTransitionError
from forwardops.domain.identifiers import require_decoded_length


class InvestigationStatus(StrEnum):
    CREATED = "CREATED"
    PLANNING = "PLANNING"
    COLLECTING_EVIDENCE = "COLLECTING_EVIDENCE"
    ANALYZING = "ANALYZING"
    CONCLUDED = "CONCLUDED"
    INCONCLUSIVE = "INCONCLUSIVE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = frozenset(
    {
        InvestigationStatus.CONCLUDED,
        InvestigationStatus.INCONCLUSIVE,
        InvestigationStatus.FAILED,
        InvestigationStatus.CANCELLED,
    }
)

ACTIVE_STATUSES = frozenset(
    {
        InvestigationStatus.CREATED,
        InvestigationStatus.PLANNING,
        InvestigationStatus.COLLECTING_EVIDENCE,
        InvestigationStatus.ANALYZING,
    }
)

_ALLOWED: dict[InvestigationStatus, frozenset[InvestigationStatus]] = {
    InvestigationStatus.CREATED: frozenset(
        {
            InvestigationStatus.PLANNING,
            InvestigationStatus.INCONCLUSIVE,
            InvestigationStatus.FAILED,
            InvestigationStatus.CANCELLED,
        }
    ),
    InvestigationStatus.PLANNING: frozenset(
        {
            InvestigationStatus.COLLECTING_EVIDENCE,
            InvestigationStatus.INCONCLUSIVE,
            InvestigationStatus.FAILED,
            InvestigationStatus.CANCELLED,
        }
    ),
    InvestigationStatus.COLLECTING_EVIDENCE: frozenset(
        {
            InvestigationStatus.ANALYZING,
            InvestigationStatus.INCONCLUSIVE,
            InvestigationStatus.FAILED,
            InvestigationStatus.CANCELLED,
        }
    ),
    InvestigationStatus.ANALYZING: frozenset(
        {
            InvestigationStatus.CONCLUDED,
            InvestigationStatus.INCONCLUSIVE,
            InvestigationStatus.FAILED,
            InvestigationStatus.CANCELLED,
        }
    ),
    InvestigationStatus.CONCLUDED: frozenset(),
    InvestigationStatus.INCONCLUSIVE: frozenset(),
    InvestigationStatus.FAILED: frozenset(),
    InvestigationStatus.CANCELLED: frozenset(),
}


def assert_transition(current: InvestigationStatus, new: InvestigationStatus) -> None:
    if new not in _ALLOWED[current]:
        raise InvalidTransitionError(f"cannot transition {current} to {new}")


class InvestigationScope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str = Field(min_length=1, max_length=64)
    vault_ref: str = Field(min_length=1, max_length=64)
    oracle_ref: str = Field(min_length=1, max_length=64)
    cluster_ref: str = Field(min_length=1, max_length=64)
    interval_start: str
    interval_end: str
    sample_cap: int = Field(ge=1, le=20)
    updater_target: str = Field(min_length=1, max_length=64)
    program_id: str
    oracle_program_id: str
    vault_address: str
    oracle_address: str

    def model_post_init(self, _context: object) -> None:
        require_decoded_length(self.program_id, 32)
        require_decoded_length(self.oracle_program_id, 32)
        require_decoded_length(self.vault_address, 32)
        require_decoded_length(self.oracle_address, 32)


def hypothesis_template(investigation_id: UUID) -> list[dict[str, object]]:
    from uuid import uuid5

    specs = (
        ("oracle_freshness", "Oracle data is older than the vault maximum age."),
        ("application_rejection", "The application is rejecting withdrawals before submission."),
        ("rpc_failure", "RPC failures are preventing withdrawals from landing."),
        ("deployment_regression", "A recent deployment changed withdrawal behavior."),
    )
    return [
        {
            "hypothesis_id": str(uuid5(investigation_id, key)),
            "key": key,
            "claim": claim,
            "status": "open",
            "evidence_refs": [],
        }
        for key, claim in specs
    ]
