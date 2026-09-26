from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from forwardops.domain.identifiers import require_decoded_length
from forwardops.domain.time import MAX_WINDOW, parse_utc, require_aware

_PRICE = r"^\d+(\.\d+)?$"


class WindowInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: datetime
    end: datetime

    @field_validator("start", "end")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return require_aware(value)

    @model_validator(mode="after")
    def _bounds(self) -> "WindowInput":
        if self.start >= self.end:
            raise ValueError("window start must be before end")
        if self.end - self.start > MAX_WINDOW:
            raise ValueError("window exceeds 1 hour")
        return self


class GetRecentWithdrawalFailuresInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str = Field(min_length=1, max_length=64)
    window: WindowInput
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=256)


class WithdrawalSample(BaseModel):
    model_config = ConfigDict(extra="forbid")

    withdrawal_id: str
    occurred_at: datetime
    signature: str
    vault_ref: str
    error_code: str
    request_id: str
    trace_id: str
    customer_ref: str

    @field_validator("signature")
    @classmethod
    def _signature(cls, value: str) -> str:
        require_decoded_length(value, 64)
        return value


class WithdrawalFailureSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str
    window: WindowInput
    baseline_window: WindowInput
    incident_attempts: int = Field(ge=0)
    incident_failures: int = Field(ge=0)
    baseline_attempts: int = Field(ge=0)
    baseline_failures: int = Field(ge=0)
    incident_attempt_ids: list[str]
    incident_failure_ids: list[str]
    coverage_complete: bool
    watermark: datetime
    sample_rule: str
    samples: list[WithdrawalSample]


class GetSolanaTransactionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signature: str
    cluster_ref: str = Field(min_length=1, max_length=64)

    @field_validator("signature")
    @classmethod
    def _signature(cls, value: str) -> str:
        require_decoded_length(value, 64)
        return value


class DecodedFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    oracle_ref: str
    oracle_address: str
    last_update: datetime
    execution_clock: datetime
    max_age_seconds: int = Field(ge=0)
    error_name: str
    freshness_comparison: str
    decoder_version: str
    withdrawal_id: str

    @field_validator("oracle_address")
    @classmethod
    def _address(cls, value: str) -> str:
        require_decoded_length(value, 32)
        return value

    @field_validator("last_update", "execution_clock")
    @classmethod
    def _times(cls, value: datetime) -> datetime:
        return require_aware(value)


class TransactionView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signature: str
    cluster_ref: str
    slot: int = Field(ge=0)
    block_time: datetime | None
    commitment: str
    program_ids: list[str]
    status: str
    instruction_errors: list[dict[str, Any]]
    decoded_failure: DecodedFailure | None
    relevant_logs: list[str]
    observed_at: datetime

    @field_validator("signature")
    @classmethod
    def _signature(cls, value: str) -> str:
        require_decoded_length(value, 64)
        return value

    @field_validator("program_ids")
    @classmethod
    def _programs(cls, value: list[str]) -> list[str]:
        for item in value:
            require_decoded_length(item, 32)
        return value


class GetSolanaAccountInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str
    cluster_ref: str = Field(min_length=1, max_length=64)

    @field_validator("address")
    @classmethod
    def _address(cls, value: str) -> str:
        require_decoded_length(value, 32)
        return value


class SolanaAccountView(BaseModel):
    """Current account snapshot. historical_state is always false."""

    model_config = ConfigDict(extra="forbid")

    address: str
    cluster_ref: str
    owner_program: str
    lamports: int = Field(ge=0)
    executable: bool
    data_encoding: Literal["base64"]
    data_length: int = Field(ge=0)
    context_slot: int = Field(ge=0)
    commitment: Literal["processed", "confirmed", "finalized"]
    observed_at: datetime
    snapshot_kind: Literal["current_account"] = "current_account"
    historical_state: Literal[False] = False

    @field_validator("address", "owner_program")
    @classmethod
    def _pubkey(cls, value: str) -> str:
        require_decoded_length(value, 32)
        return value

    @field_validator("observed_at")
    @classmethod
    def _observed(cls, value: datetime) -> datetime:
        return require_aware(value)


class GetVaultStateInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vault_ref: str = Field(min_length=1, max_length=64)


class VaultState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vault_ref: str
    address: str
    owner_program: str
    decoder_version: str
    oracle_ref: str
    oracle_address: str
    paused: bool
    max_age_seconds: int = Field(ge=0)
    freshness_comparison: str
    config_version: str
    context_slot: int = Field(ge=0)
    commitment: str
    observed_at: datetime
    snapshot_kind: str

    @field_validator("address", "owner_program", "oracle_address")
    @classmethod
    def _pubkey(cls, value: str) -> str:
        require_decoded_length(value, 32)
        return value


class GetOracleStateInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    oracle_ref: str = Field(min_length=1, max_length=64)


class OracleState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    oracle_ref: str
    address: str
    owner_program: str
    feed_id: str
    price_decimal: str = Field(pattern=_PRICE)
    scale: int = Field(ge=0, le=18)
    quote_unit: str
    status: str
    last_update: datetime
    timestamp_basis: str
    context_slot: int = Field(ge=0)
    commitment: str
    observed_at: datetime
    snapshot_kind: str

    @field_validator("address", "owner_program")
    @classmethod
    def _pubkey(cls, value: str) -> str:
        require_decoded_length(value, 32)
        return value


class SearchApplicationLogsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str = Field(min_length=1, max_length=64)
    window: WindowInput
    signature: str
    withdrawal_id: str | None = Field(default=None, max_length=64)
    trace_id: str | None = Field(default=None, max_length=64)
    limit: int = Field(default=20, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=256)

    @field_validator("signature")
    @classmethod
    def _signature(cls, value: str) -> str:
        require_decoded_length(value, 64)
        return value


class LogRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_time: datetime | None
    level: str
    event_name: str
    request_id: str | None = None
    trace_id: str | None = None
    signature: str | None = None
    withdrawal_id: str | None = None
    error_code: str | None = None
    fields: dict[str, str]
    redacted_message: str = Field(max_length=2000)


class LogSearchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str
    records: list[LogRecord]
    coverage_complete: bool


class SearchRunbooksInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str = Field(min_length=1, max_length=64)
    component_ref: str = Field(min_length=1, max_length=64)
    incident_kind: str = Field(min_length=1, max_length=64)
    query: str = Field(min_length=1, max_length=512)
    limit: int = Field(default=5, ge=1, le=5)


class RunbookMatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runbook_id: str
    version: str
    title: str
    digest: str
    section_id: str
    excerpt: str
    service_ref: str
    component_ref: str
    incident_kind: str
    owner: str
    review_status: str


class RunbookSearchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    matches: list[RunbookMatch]


class GetRecentDeploymentsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_ref: str = Field(min_length=1, max_length=64)
    window: WindowInput
    limit: int = Field(default=20, ge=1, le=100)


class DeploymentRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    deployment_id: str
    revision: str
    started_at: datetime
    completed_at: datetime | None
    status: str
    changed_components: list[str]
    source_ref: str


class DeploymentSearchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    deployments: list[DeploymentRecord]
    coverage_complete: bool


def coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        return parse_utc(value)
    return require_aware(value)
