import re
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from forwardops.domain.errors import InvalidTransitionError
from forwardops.domain.identifiers import require_decoded_length

STALE_ORACLE_SCENARIO = "stale_oracle_withdrawal_failure"
DATABASE_POOL_SCENARIO = "database_connection_pool_exhaustion"
_LOGICAL_ID = re.compile(r"^[a-z][a-z0-9-]{0,62}$")


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
    scenario_id: str = STALE_ORACLE_SCENARIO
    database_source_ref: str | None = None
    trace_source_ref: str | None = None

    @field_validator("scenario_id")
    @classmethod
    def _scenario(cls, value: str) -> str:
        if value not in {STALE_ORACLE_SCENARIO, DATABASE_POOL_SCENARIO}:
            raise ValueError("unknown investigation scenario")
        return value

    @field_validator("database_source_ref", "trace_source_ref")
    @classmethod
    def _source(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if _LOGICAL_ID.fullmatch(value) is None:
            raise ValueError("source must be a logical identifier")
        return value

    def model_post_init(self, _context: object) -> None:
        require_decoded_length(self.program_id, 32)
        require_decoded_length(self.oracle_program_id, 32)
        require_decoded_length(self.vault_address, 32)
        require_decoded_length(self.oracle_address, 32)
        if self.scenario_id == DATABASE_POOL_SCENARIO and self.database_source_ref is None:
            raise ValueError("database investigations require a logical database source")
        if self.scenario_id == DATABASE_POOL_SCENARIO and self.trace_source_ref is None:
            raise ValueError("database investigations require a logical trace source")
        if self.scenario_id != DATABASE_POOL_SCENARIO and self.database_source_ref is not None:
            raise ValueError("database source is only in scope for the database playbook")
        if self.scenario_id != DATABASE_POOL_SCENARIO and self.trace_source_ref is not None:
            raise ValueError("trace source is only in scope for the database playbook")


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
