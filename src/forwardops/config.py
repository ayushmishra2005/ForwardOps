import os
import re
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from forwardops.domain.errors import ConfigError
from forwardops.domain.hashing import sha256_canonical
from forwardops.domain.identifiers import require_decoded_length
from forwardops.domain.time import parse_utc


class WindowConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: str
    end: str

    @field_validator("start", "end")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        parse_utc(value)
        return value

    @model_validator(mode="after")
    def _order(self) -> "WindowConfig":
        if parse_utc(self.start) >= parse_utc(self.end):
            raise ValueError("window start must be before end")
        return self


class PermittedAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_type: Literal["restart_oracle_updater"]
    incident_kind: Literal["stale_oracle"]
    target_ref: str = Field(min_length=1, max_length=64)
    risk: Literal["medium"]


class CustomerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=200)
    data_mode: Literal["replay"]
    service_ref: str = Field(min_length=1, max_length=64)
    cluster_ref: str = Field(min_length=1, max_length=64)
    vault_ref: str = Field(min_length=1, max_length=64)
    oracle_ref: str = Field(min_length=1, max_length=64)
    updater_target: str = Field(min_length=1, max_length=64)
    program_id: str
    oracle_program_id: str
    vault_address: str
    oracle_address: str
    sample_cap: int = Field(ge=1, le=20)
    window: WindowConfig
    frozen_observed_at: str
    question: str = Field(min_length=1, max_length=512)
    playbook_version: str = Field(min_length=1, max_length=32)
    policy_version: str = Field(min_length=1, max_length=32)
    permitted_actions: list[PermittedAction] = Field(min_length=1)
    fixture_dir: str
    runbook_dir: str
    proposal_ttl_seconds: int = Field(ge=60, le=7 * 24 * 3600)

    @field_validator("frozen_observed_at")
    @classmethod
    def _frozen(cls, value: str) -> str:
        parse_utc(value)
        return value

    @model_validator(mode="after")
    def _identifiers(self) -> "CustomerConfig":
        require_decoded_length(self.program_id, 32)
        require_decoded_length(self.oracle_program_id, 32)
        require_decoded_length(self.vault_address, 32)
        require_decoded_length(self.oracle_address, 32)
        for action in self.permitted_actions:
            if action.target_ref != self.updater_target:
                raise ValueError("permitted action target must match updater_target")
        return self


class Identity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str = Field(min_length=8, max_length=128)
    principal_id: str = Field(min_length=1, max_length=64)
    tenant_id: str = Field(min_length=1, max_length=64)
    roles: tuple[str, ...]

    @field_validator("roles")
    @classmethod
    def _roles(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        allowed = {"investigator", "approver"}
        if not value or any(role not in allowed for role in value):
            raise ValueError("roles must be investigator and/or approver")
        return tuple(dict.fromkeys(value))


_CLUSTER_ID = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_SOLANA_COMMITMENTS = frozenset({"processed", "confirmed", "finalized"})


class DecoderBinding(BaseModel):
    """Names one decoder that is already installed in this build."""

    model_config = ConfigDict(extra="forbid")

    decoder_id: str = Field(min_length=1, max_length=64)
    version: str = Field(min_length=1, max_length=32)
    program_id: str

    @field_validator("decoder_id", "version")
    @classmethod
    def _token(cls, value: str) -> str:
        if re.fullmatch(r"[A-Za-z0-9._-]{1,64}", value) is None:
            raise ValueError("decoder id and version must be configuration tokens")
        return value

    @field_validator("program_id")
    @classmethod
    def _program(cls, value: str) -> str:
        require_decoded_length(value, 32)
        return value


class SolanaCluster(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cluster_id: str
    rpc_url: str = Field(repr=False)
    commitment: Literal["processed", "confirmed", "finalized"] = "finalized"

    @field_validator("cluster_id")
    @classmethod
    def _cluster(cls, value: str) -> str:
        if _CLUSTER_ID.fullmatch(value) is None:
            raise ValueError("Solana cluster id must be a logical identifier")
        return value

    @field_validator("rpc_url")
    @classmethod
    def _rpc_url(cls, value: str) -> str:
        stripped = value.strip()
        if stripped != value or any(character in value for character in "\r\n\t "):
            raise ValueError("Solana RPC URL must be an https endpoint")
        parts = urlsplit(stripped)
        if parts.scheme != "https" or not parts.hostname:
            raise ValueError("Solana RPC URL must be an https endpoint")
        return stripped


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    environment: Literal["development"]
    database_url: str
    migration_database_url: str
    migrations_dir: Path
    customer: CustomerConfig
    fixture_dir: Path
    runbook_dir: Path
    identities: tuple[Identity, ...]
    config_digest: str
    poll_seconds: float = 0.5
    lease_seconds: int = 60
    max_tool_calls: int = 12
    deadline_seconds: int = 120
    app_password: str = "forwardops_app"
    model_provider_name: Literal["deterministic", "openai"] = "deterministic"
    analysis_mode: Literal["deterministic", "model"] = "deterministic"
    openai_api_key: str | None = None
    openai_model: str = "gpt-4.1-mini"
    openai_base_url: str = "https://api.openai.com/v1"
    max_analysis_rounds: int = 16
    max_model_calls: int = 16
    token_budget: int = 120_000
    model_timeout_seconds: float = 30
    solana_clusters: tuple[SolanaCluster, ...] = ()
    solana_timeout_seconds: float = 8
    solana_decoders: tuple[DecoderBinding, ...] = ()


def load_customer(path: Path) -> CustomerConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ConfigError(f"{path} must contain a mapping")
    return CustomerConfig.model_validate(raw)


def load_identities(path: Path) -> tuple[Identity, ...]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not isinstance(raw.get("identities"), list):
        raise ConfigError(f"{path} must contain an identities list")
    identities = tuple(Identity.model_validate(item) for item in raw["identities"])
    tokens = [item.token for item in identities]
    if len(tokens) != len(set(tokens)):
        raise ConfigError("development identity tokens must be unique")
    return identities


def _require_roles(customer: CustomerConfig, identities: tuple[Identity, ...]) -> None:
    scoped = [item for item in identities if item.tenant_id == customer.tenant_id]
    if not any("investigator" in item.roles for item in scoped):
        raise ConfigError("customer tenant is missing an investigator identity")
    if not any("approver" in item.roles for item in scoped):
        raise ConfigError("customer tenant is missing an approver identity")


def configured_solana_clusters(
    *,
    cluster: str | None,
    rpc_url: str | None,
    commitment: str,
    customer_cluster_ref: str,
) -> tuple[SolanaCluster, ...]:
    cluster_id = (cluster or "").strip()
    url = (rpc_url or "").strip()
    if not cluster_id and not url:
        return ()
    if not cluster_id or not url:
        raise ConfigError(
            "FORWARDOPS_SOLANA_CLUSTER and FORWARDOPS_SOLANA_RPC_URL must both be set"
        )
    if cluster_id == customer_cluster_ref:
        raise ConfigError("FORWARDOPS_SOLANA_CLUSTER must not reuse the replay cluster_ref")
    if commitment not in _SOLANA_COMMITMENTS:
        raise ConfigError("FORWARDOPS_SOLANA_COMMITMENT must be processed, confirmed, or finalized")
    try:
        parsed = SolanaCluster.model_validate(
            {"cluster_id": cluster_id, "rpc_url": url, "commitment": commitment}
        )
    except ValidationError:
        raise ConfigError(
            "Solana configuration needs a logical cluster id and an https RPC URL"
        ) from None
    return (parsed,)


def solana_timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not 1 <= float(value) <= 30:
        raise ConfigError("FORWARDOPS_SOLANA_TIMEOUT_SECONDS must be between 1 and 30")
    return float(value)


def resolve_model_configuration(provider: str, api_key: str | None) -> tuple[str, str]:
    if provider == "deterministic":
        return "deterministic", "deterministic"
    if provider == "openai":
        if not api_key:
            raise ConfigError(
                "FORWARDOPS_OPENAI_API_KEY is required when FORWARDOPS_MODEL_PROVIDER=openai"
            )
        return "model", "openai"
    raise ConfigError("FORWARDOPS_MODEL_PROVIDER must be deterministic or openai")


def build_settings(
    *,
    environment: str,
    database_url: str,
    migration_database_url: str,
    migrations_dir: Path,
    customer_path: Path,
    identities_path: Path,
    poll_seconds: float = 0.5,
    lease_seconds: int = 60,
    app_password: str = "forwardops_app",
    model_provider_name: str = "deterministic",
    openai_api_key: str | None = None,
    openai_model: str = "gpt-4.1-mini",
    openai_base_url: str = "https://api.openai.com/v1",
    max_tool_calls: int = 12,
    max_model_calls: int = 16,
    max_analysis_rounds: int = 16,
    token_budget: int = 120_000,
    deadline_seconds: int = 120,
    model_timeout_seconds: float = 30,
    solana_cluster: str | None = None,
    solana_rpc_url: str | None = None,
    solana_commitment: str = "finalized",
    solana_timeout_seconds: float = 8,
    solana_decoders: tuple[DecoderBinding, ...] = (),
) -> Settings:
    if environment != "development":
        raise ConfigError("development authentication cannot start outside development")
    customer = load_customer(customer_path)
    if customer.playbook_version != "v1":
        raise ConfigError("this build only runs withdrawal playbook v1")
    identities = load_identities(identities_path)
    _require_roles(customer, identities)
    base = customer_path.parent
    fixture_dir = (base / customer.fixture_dir).resolve()
    runbook_dir = (base / customer.runbook_dir).resolve()
    if not fixture_dir.is_dir() or not runbook_dir.is_dir():
        raise ConfigError("fixture_dir and runbook_dir must exist")
    analysis_mode, provider_name = resolve_model_configuration(model_provider_name, openai_api_key)
    if provider_name == "openai" and not openai_base_url.startswith("https://"):
        raise ConfigError("model provider base URL must use https")
    solana_clusters = configured_solana_clusters(
        cluster=solana_cluster,
        rpc_url=solana_rpc_url,
        commitment=solana_commitment,
        customer_cluster_ref=customer.cluster_ref,
    )
    timeout = solana_timeout(solana_timeout_seconds)
    from forwardops.integrations.solana import (
        INSTALLED_ACCOUNT_DECODERS,
        require_installed_decoders,
    )

    require_installed_decoders(solana_decoders, INSTALLED_ACCOUNT_DECODERS)
    return Settings(
        environment="development",
        database_url=database_url,
        migration_database_url=migration_database_url,
        migrations_dir=migrations_dir,
        customer=customer,
        fixture_dir=fixture_dir,
        runbook_dir=runbook_dir,
        identities=identities,
        config_digest=sha256_canonical(customer.model_dump(mode="python")),
        poll_seconds=poll_seconds,
        lease_seconds=lease_seconds,
        max_tool_calls=max_tool_calls,
        deadline_seconds=deadline_seconds,
        app_password=app_password,
        model_provider_name="openai" if provider_name == "openai" else "deterministic",
        analysis_mode="model" if analysis_mode == "model" else "deterministic",
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_base_url=openai_base_url,
        max_analysis_rounds=max_analysis_rounds,
        max_model_calls=max_model_calls,
        token_budget=token_budget,
        model_timeout_seconds=model_timeout_seconds,
        solana_clusters=solana_clusters,
        solana_timeout_seconds=timeout,
        solana_decoders=solana_decoders,
    )


def load_settings() -> Settings:
    try:
        root = Path(os.environ.get("FORWARDOPS_ROOT", Path.cwd()))
        return build_settings(
            environment=os.environ.get("FORWARDOPS_ENVIRONMENT", "development"),
            database_url=os.environ["FORWARDOPS_DATABASE_URL"],
            migration_database_url=os.environ["FORWARDOPS_MIGRATION_DATABASE_URL"],
            migrations_dir=Path(os.environ.get("FORWARDOPS_MIGRATIONS_DIR", root / "migrations")),
            customer_path=Path(os.environ["FORWARDOPS_CONFIG"]),
            identities_path=Path(os.environ["FORWARDOPS_IDENTITIES"]),
            poll_seconds=float(os.environ.get("FORWARDOPS_POLL_SECONDS", "0.5")),
            lease_seconds=int(os.environ.get("FORWARDOPS_LEASE_SECONDS", "60")),
            app_password=os.environ.get("FORWARDOPS_APP_PASSWORD", "forwardops_app"),
            model_provider_name=os.environ.get("FORWARDOPS_MODEL_PROVIDER", "deterministic"),
            openai_api_key=os.environ.get("FORWARDOPS_OPENAI_API_KEY"),
            openai_model=os.environ.get("FORWARDOPS_OPENAI_MODEL", "gpt-4.1-mini"),
            openai_base_url=os.environ.get(
                "FORWARDOPS_OPENAI_BASE_URL", "https://api.openai.com/v1"
            ),
            max_tool_calls=int(os.environ.get("FORWARDOPS_MAX_TOOL_CALLS", "12")),
            max_model_calls=int(os.environ.get("FORWARDOPS_MAX_MODEL_CALLS", "16")),
            max_analysis_rounds=int(os.environ.get("FORWARDOPS_MAX_ANALYSIS_ROUNDS", "16")),
            token_budget=int(os.environ.get("FORWARDOPS_TOKEN_BUDGET", "120000")),
            deadline_seconds=int(os.environ.get("FORWARDOPS_DEADLINE_SECONDS", "120")),
            model_timeout_seconds=float(os.environ.get("FORWARDOPS_MODEL_TIMEOUT_SECONDS", "30")),
            solana_cluster=os.environ.get("FORWARDOPS_SOLANA_CLUSTER"),
            solana_rpc_url=os.environ.get("FORWARDOPS_SOLANA_RPC_URL"),
            solana_commitment=os.environ.get("FORWARDOPS_SOLANA_COMMITMENT", "finalized"),
            solana_timeout_seconds=float(os.environ.get("FORWARDOPS_SOLANA_TIMEOUT_SECONDS", "8")),
        )
    except KeyError as exc:
        raise ConfigError(f"missing environment variable {exc.args[0]}") from exc
