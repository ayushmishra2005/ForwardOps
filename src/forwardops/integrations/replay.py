"""Replay adapters and the pure withdrawal counter.

Source fixtures are labelled synthetic. Counts are derived from attempt rows.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from forwardops.domain.errors import ConfigError, ToolFailedError
from forwardops.domain.hashing import sha256_bytes
from forwardops.domain.investigation import InvestigationScope
from forwardops.domain.time import format_utc, parse_utc, preceding_window, require_aware
from forwardops.tools.contracts import (
    DeploymentRecord,
    DeploymentSearchResult,
    GetOracleStateInput,
    GetRecentDeploymentsInput,
    GetRecentWithdrawalFailuresInput,
    GetSolanaTransactionInput,
    GetVaultStateInput,
    LogRecord,
    LogSearchResult,
    OracleState,
    RunbookMatch,
    RunbookSearchResult,
    SearchApplicationLogsInput,
    SearchRunbooksInput,
    TransactionView,
    VaultState,
    WithdrawalFailureSummary,
    WithdrawalSample,
)
from forwardops.tools.registry import dump_model


@dataclass(frozen=True)
class Attempt:
    withdrawal_id: str
    occurred_at: datetime
    outcome: str
    signature: str | None
    vault_ref: str
    error_code: str | None
    request_id: str
    trace_id: str
    customer_ref: str


@dataclass(frozen=True)
class FixtureSource:
    withdrawals_label: str
    coverage_complete: bool
    watermark: datetime
    attempts: tuple[Attempt, ...]
    transactions: dict[str, dict[str, Any]]
    logs: tuple[dict[str, Any], ...]
    logs_complete: bool
    vault: dict[str, Any]
    oracle: dict[str, Any]
    deployments: tuple[dict[str, Any], ...]
    deployments_complete: bool

    @classmethod
    def load(cls, directory: Path) -> "FixtureSource":
        withdrawals = _read_json(directory / "withdrawals.json")
        transactions = _read_json(directory / "transactions.json")
        logs = _read_json(directory / "logs.json")
        vault = _read_json(directory / "vault.json")
        oracle = _read_json(directory / "oracle.json")
        deployments = _read_json(directory / "deployments.json")
        attempts = tuple(_attempt(item) for item in withdrawals["attempts"])
        return cls(
            withdrawals_label=str(withdrawals["label"]),
            coverage_complete=bool(withdrawals["coverage_complete"]),
            watermark=parse_utc(str(withdrawals["watermark"])),
            attempts=attempts,
            transactions=dict(transactions["transactions"]),
            logs=tuple(logs["records"]),
            logs_complete=bool(logs["coverage_complete"]),
            vault=vault,
            oracle=oracle,
            deployments=tuple(deployments["deployments"]),
            deployments_complete=bool(deployments["coverage_complete"]),
        )


def _only(model: type[BaseModel], payload: dict[str, Any]) -> dict[str, Any]:
    return {key: payload[key] for key in model.model_fields if key in payload}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"missing fixture {path.name}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("synthetic") is not True:
        raise ConfigError(f"{path.name} must be labelled synthetic")
    return payload


def _attempt(item: dict[str, Any]) -> Attempt:
    return Attempt(
        withdrawal_id=str(item["withdrawal_id"]),
        occurred_at=parse_utc(str(item["occurred_at"])),
        outcome=str(item["outcome"]),
        signature=item.get("signature"),
        vault_ref=str(item["vault_ref"]),
        error_code=item.get("error_code"),
        request_id=str(item["request_id"]),
        trace_id=str(item["trace_id"]),
        customer_ref=str(item["customer_ref"]),
    )


def _in_window(moment: datetime, start: datetime, end: datetime) -> bool:
    moment = require_aware(moment)
    return require_aware(start) <= moment < require_aware(end)


def summarize_attempts(
    attempts: tuple[Attempt, ...] | list[Attempt],
    *,
    start: datetime,
    end: datetime,
    sample_limit: int,
    service_ref: str,
    coverage_complete: bool,
    watermark: datetime,
) -> WithdrawalFailureSummary:
    baseline_start, baseline_end = preceding_window(start, end)
    incident = [row for row in attempts if _in_window(row.occurred_at, start, end)]
    baseline = [
        row for row in attempts if _in_window(row.occurred_at, baseline_start, baseline_end)
    ]
    incident_failures = [row for row in incident if row.outcome == "failure"]
    baseline_failures = [row for row in baseline if row.outcome == "failure"]
    sampled = [row for row in incident_failures if row.signature]
    sampled.sort(key=lambda row: (row.occurred_at, row.withdrawal_id))
    samples: list[WithdrawalSample] = []
    for row in sampled[:sample_limit]:
        if row.signature is None or row.error_code is None:
            continue
        samples.append(
            WithdrawalSample(
                withdrawal_id=row.withdrawal_id,
                occurred_at=row.occurred_at,
                signature=row.signature,
                vault_ref=row.vault_ref,
                error_code=row.error_code,
                request_id=row.request_id,
                trace_id=row.trace_id,
                customer_ref=row.customer_ref,
            )
        )
    return WithdrawalFailureSummary(
        service_ref=service_ref,
        window={"start": start, "end": end},
        baseline_window={"start": baseline_start, "end": baseline_end},
        incident_attempts=len(incident),
        incident_failures=len(incident_failures),
        baseline_attempts=len(baseline),
        baseline_failures=len(baseline_failures),
        incident_attempt_ids=sorted(row.withdrawal_id for row in incident),
        incident_failure_ids=sorted(row.withdrawal_id for row in incident_failures),
        coverage_complete=coverage_complete,
        watermark=watermark,
        sample_rule="earliest_failures_with_signature",
        samples=samples,
    )


def deployments_in_window(
    rows: tuple[dict[str, Any], ...] | list[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
    limit: int,
    coverage_complete: bool,
) -> DeploymentSearchResult:
    matched: list[DeploymentRecord] = []
    for row in rows:
        started = parse_utc(str(row["started_at"]))
        if _in_window(started, start, end):
            matched.append(DeploymentRecord.model_validate(row))
    matched.sort(key=lambda item: item.started_at)
    return DeploymentSearchResult(
        deployments=matched[:limit],
        coverage_complete=coverage_complete,
    )


@dataclass(frozen=True)
class RunbookDocument:
    runbook_id: str
    version: str
    title: str
    section_id: str
    service_ref: str
    component_ref: str
    incident_kind: str
    owner: str
    review_status: str
    body: str
    digest: str
    path: str


def load_runbooks(directory: Path) -> tuple[RunbookDocument, ...]:
    documents: list[RunbookDocument] = []
    for path in sorted(directory.glob("*.md")):
        documents.append(parse_runbook(path.read_text(encoding="utf-8"), path.name))
    if not documents:
        raise ConfigError(f"no runbooks found in {directory}")
    return tuple(documents)


def parse_runbook(text: str, name: str) -> RunbookDocument:
    if not text.startswith("---\n"):
        raise ConfigError(f"{name} is missing frontmatter")
    parts = text.split("---", 2)
    if len(parts) < 3:
        raise ConfigError(f"{name} frontmatter is not closed")
    import yaml

    raw = yaml.safe_load(parts[1])
    if not isinstance(raw, dict):
        raise ConfigError(f"{name} frontmatter must be a mapping")
    # Body text and unknown keys are not policy. Only these fields are metadata.
    required = (
        "id",
        "version",
        "title",
        "section_id",
        "service_ref",
        "component_ref",
        "incident_kind",
        "owner",
        "review_status",
    )
    missing = [key for key in required if key not in raw]
    if missing:
        raise ConfigError(f"{name} is missing {', '.join(missing)}")
    body = parts[2].strip()
    return RunbookDocument(
        runbook_id=str(raw["id"]),
        version=str(raw["version"]),
        title=str(raw["title"]),
        section_id=str(raw["section_id"]),
        service_ref=str(raw["service_ref"]),
        component_ref=str(raw["component_ref"]),
        incident_kind=str(raw["incident_kind"]),
        owner=str(raw["owner"]),
        review_status=str(raw["review_status"]),
        body=body,
        digest=sha256_bytes(text.encode("utf-8")),
        path=name,
    )


@dataclass(frozen=True)
class Observation:
    kind: str
    source_type: str
    source_system: str
    source_locator: dict[str, Any]
    event_time: str | None
    time_basis: str
    correlation: dict[str, Any]
    payload: dict[str, Any]
    summary: str
    provenance: dict[str, Any]
    coverage: dict[str, Any]


@dataclass(frozen=True)
class HandlerResult:
    output: dict[str, Any]
    observations: tuple[Observation, ...]


class ReplayHandlers:
    def __init__(self, source: FixtureSource, runbooks: tuple[RunbookDocument, ...]) -> None:
        self.source = source
        self.runbooks = runbooks

    async def get_recent_withdrawal_failures(
        self,
        scope: InvestigationScope,
        arguments: GetRecentWithdrawalFailuresInput,
    ) -> HandlerResult:
        if arguments.cursor:
            raise ToolFailedError("UNSUPPORTED_SCHEMA", "cursors are not used by the replay source")
        summary = summarize_attempts(
            self.source.attempts,
            start=arguments.window.start,
            end=arguments.window.end,
            sample_limit=arguments.limit,
            service_ref=arguments.service_ref,
            coverage_complete=self.source.coverage_complete,
            watermark=self.source.watermark,
        )
        observations = [_summary_observation(summary, self.source.withdrawals_label)]
        observations.extend(_sample_observation(sample) for sample in summary.samples)
        return HandlerResult(dump_model(summary), tuple(observations))

    async def get_solana_transaction(
        self,
        scope: InvestigationScope,
        arguments: GetSolanaTransactionInput,
    ) -> HandlerResult:
        raw = self.source.transactions.get(arguments.signature)
        if raw is None:
            raise ToolFailedError(
                "NOT_FOUND",
                "no synthetic transaction exists for this signature",
                retryable=False,
            )
        view = TransactionView.model_validate({**raw, "cluster_ref": arguments.cluster_ref})
        if scope.program_id not in view.program_ids:
            raise ToolFailedError("UNSUPPORTED_SCHEMA", "transaction program is outside scope")
        return HandlerResult(dump_model(view), (_transaction_observation(view, scope),))

    async def search_application_logs(
        self,
        scope: InvestigationScope,
        arguments: SearchApplicationLogsInput,
    ) -> HandlerResult:
        if arguments.cursor:
            raise ToolFailedError("UNSUPPORTED_SCHEMA", "cursors are not used by the replay source")
        matched: list[LogRecord] = []
        for raw in self.source.logs:
            record = LogRecord.model_validate(raw)
            if not _log_matches(record, arguments):
                continue
            matched.append(record)
        matched.sort(key=lambda item: (item.event_time or arguments.window.start, item.event_name))
        result = LogSearchResult(
            service_ref=arguments.service_ref,
            records=matched[: arguments.limit],
            coverage_complete=self.source.logs_complete,
        )
        observations = tuple(
            _log_observation(record, index, arguments.service_ref)
            for index, record in enumerate(result.records)
        )
        return HandlerResult(dump_model(result), observations)

    async def get_vault_state(
        self,
        scope: InvestigationScope,
        arguments: GetVaultStateInput,
    ) -> HandlerResult:
        if self.source.vault.get("vault_ref") != arguments.vault_ref:
            raise ToolFailedError("NOT_FOUND", "vault is not in the synthetic source")
        if self.source.vault.get("address") != scope.vault_address:
            raise ToolFailedError("UNSUPPORTED_SCHEMA", "vault address does not match scope")
        state = VaultState.model_validate(_only(VaultState, self.source.vault))
        return HandlerResult(dump_model(state), (_vault_observation(state),))

    async def get_oracle_state(
        self,
        scope: InvestigationScope,
        arguments: GetOracleStateInput,
    ) -> HandlerResult:
        if self.source.oracle.get("oracle_ref") != arguments.oracle_ref:
            raise ToolFailedError("NOT_FOUND", "oracle is not in the synthetic source")
        if self.source.oracle.get("address") != scope.oracle_address:
            raise ToolFailedError("UNSUPPORTED_SCHEMA", "oracle address does not match scope")
        state = OracleState.model_validate(_only(OracleState, self.source.oracle))
        return HandlerResult(dump_model(state), (_oracle_observation(state),))

    async def search_runbooks(
        self,
        scope: InvestigationScope,
        arguments: SearchRunbooksInput,
    ) -> HandlerResult:
        # Runbook body and the free-text query are evidence, not instructions.
        _ = (scope, arguments.query)
        matches = [
            document
            for document in self.runbooks
            if document.service_ref == arguments.service_ref
            and document.component_ref == arguments.component_ref
            and document.incident_kind == arguments.incident_kind
        ]
        selected = matches[: arguments.limit]
        result = RunbookSearchResult(
            matches=[
                RunbookMatch(
                    runbook_id=item.runbook_id,
                    version=item.version,
                    title=item.title,
                    digest=item.digest,
                    section_id=item.section_id,
                    excerpt=item.body,
                    service_ref=item.service_ref,
                    component_ref=item.component_ref,
                    incident_kind=item.incident_kind,
                    owner=item.owner,
                    review_status=item.review_status,
                )
                for item in selected
            ]
        )
        return HandlerResult(
            dump_model(result),
            tuple(_runbook_observation(item) for item in result.matches),
        )

    async def get_recent_deployments(
        self,
        scope: InvestigationScope,
        arguments: GetRecentDeploymentsInput,
    ) -> HandlerResult:
        del scope
        result = deployments_in_window(
            self.source.deployments,
            start=arguments.window.start,
            end=arguments.window.end,
            limit=arguments.limit,
            coverage_complete=self.source.deployments_complete,
        )
        observations = tuple(_deployment_observation(item) for item in result.deployments)
        return HandlerResult(dump_model(result), observations)


def _log_matches(record: LogRecord, arguments: SearchApplicationLogsInput) -> bool:
    if record.event_time is not None and not _in_window(
        record.event_time,
        arguments.window.start,
        arguments.window.end,
    ):
        return False
    if arguments.signature and record.signature != arguments.signature:
        return False
    if arguments.withdrawal_id and record.withdrawal_id != arguments.withdrawal_id:
        return False
    if arguments.trace_id and record.trace_id != arguments.trace_id:
        return False
    return True


def _summary_observation(summary: WithdrawalFailureSummary, label: str) -> Observation:
    payload = {
        "incident_attempts": summary.incident_attempts,
        "incident_failures": summary.incident_failures,
        "baseline_attempts": summary.baseline_attempts,
        "baseline_failures": summary.baseline_failures,
        "incident_attempt_ids": summary.incident_attempt_ids,
        "incident_failure_ids": summary.incident_failure_ids,
        "window_start": format_utc(summary.window.start),
        "window_end": format_utc(summary.window.end),
        "baseline_window_start": format_utc(summary.baseline_window.start),
        "baseline_window_end": format_utc(summary.baseline_window.end),
        "sample_rule": summary.sample_rule,
        "sample_size": len(summary.samples),
        "coverage_complete": summary.coverage_complete,
        "watermark": format_utc(summary.watermark),
    }
    return Observation(
        kind="withdrawal.failure_summary",
        source_type="application_db",
        source_system="synthetic-withdrawals",
        source_locator={"label": label, "service_ref": summary.service_ref},
        event_time=None,
        time_basis="interval",
        correlation={"service_ref": summary.service_ref},
        payload=payload,
        summary=(
            f"{summary.incident_failures} of {summary.incident_attempts} incident attempts failed; "
            f"baseline {summary.baseline_failures} of {summary.baseline_attempts}."
        ),
        provenance={"synthetic": True, "aggregate": True},
        coverage={"complete_for_record": summary.coverage_complete, "truncated": False},
    )


def _sample_observation(sample: WithdrawalSample) -> Observation:
    payload = {
        "withdrawal_id": sample.withdrawal_id,
        "occurred_at": format_utc(sample.occurred_at),
        "signature": sample.signature,
        "vault_ref": sample.vault_ref,
        "error_code": sample.error_code,
        "request_id": sample.request_id,
        "trace_id": sample.trace_id,
        "customer_ref": sample.customer_ref,
        "outcome": "failure",
    }
    return Observation(
        kind="withdrawal.attempt",
        source_type="application_db",
        source_system="synthetic-withdrawals",
        source_locator={"withdrawal_id": sample.withdrawal_id},
        event_time=format_utc(sample.occurred_at),
        time_basis="source_event_time",
        correlation={
            "withdrawal_id": sample.withdrawal_id,
            "signature": sample.signature,
            "request_id": sample.request_id,
            "trace_id": sample.trace_id,
            "vault_ref": sample.vault_ref,
        },
        payload=payload,
        summary=f"Failed withdrawal {sample.withdrawal_id} was selected for inspection.",
        provenance={"synthetic": True},
        coverage={"complete_for_record": True, "truncated": False},
    )


def _transaction_observation(view: TransactionView, scope: InvestigationScope) -> Observation:
    failure = view.decoded_failure
    if failure is None:
        raise ToolFailedError("INVALID_OUTPUT", "transaction has no decoded failure")
    payload = {
        "signature": view.signature,
        "slot": view.slot,
        "block_time": format_utc(view.block_time) if view.block_time else None,
        "commitment": view.commitment,
        "error_name": failure.error_name,
        "execution_clock": format_utc(failure.execution_clock),
        "last_update": format_utc(failure.last_update),
        "max_age_seconds": failure.max_age_seconds,
        "oracle_ref": failure.oracle_ref,
        "oracle_address": failure.oracle_address,
        "freshness_comparison": failure.freshness_comparison,
        "decoder_version": failure.decoder_version,
        "withdrawal_id": failure.withdrawal_id,
        "observed_at": format_utc(view.observed_at),
    }
    return Observation(
        kind="solana.program_failure",
        source_type="blockchain",
        source_system="solana-fixture",
        source_locator={"cluster": scope.cluster_ref, "signature": view.signature},
        event_time=format_utc(failure.execution_clock),
        time_basis="program_execution_clock",
        correlation={
            "signature": view.signature,
            "withdrawal_id": failure.withdrawal_id,
            "vault_ref": scope.vault_ref,
        },
        payload=payload,
        summary=f"Vault program emitted {failure.error_name} for transaction {view.signature}.",
        provenance={
            "synthetic": True,
            "cluster": scope.cluster_ref,
            "slot": view.slot,
            "commitment": view.commitment,
            "decoder_version": failure.decoder_version,
            "historical_state": "execution_log",
            "snapshot_kind": "execution_log",
        },
        coverage={"complete_for_record": True, "truncated": False},
    )


def _log_observation(record: LogRecord, index: int, service_ref: str) -> Observation:
    payload = record.model_dump(mode="json")
    event_time = format_utc(record.event_time) if record.event_time else None
    return Observation(
        kind="application.log",
        source_type="logs",
        source_system="synthetic-logs",
        source_locator={
            "service_ref": service_ref,
            "record_index": index,
            "event_name": record.event_name,
        },
        event_time=event_time,
        time_basis="application_event_time",
        correlation={
            "withdrawal_id": record.withdrawal_id,
            "signature": record.signature,
            "request_id": record.request_id,
            "trace_id": record.trace_id,
        },
        payload=payload,
        summary=f"Application log {record.event_name} for withdrawal {record.withdrawal_id}.",
        provenance={"synthetic": True, "untrusted_text": True},
        coverage={"complete_for_record": True, "truncated": False},
    )


def _vault_observation(state: VaultState) -> Observation:
    payload = state.model_dump(mode="json")
    return Observation(
        kind="vault.state",
        source_type="blockchain",
        source_system="solana-fixture",
        source_locator={"vault_ref": state.vault_ref, "address": state.address},
        event_time=None,
        time_basis="current_snapshot",
        correlation={"vault_ref": state.vault_ref, "oracle_ref": state.oracle_ref},
        payload=payload,
        summary=f"Current vault {state.vault_ref} binds oracle {state.oracle_ref}.",
        provenance={
            "synthetic": True,
            "snapshot_kind": state.snapshot_kind,
            "context_slot": state.context_slot,
            "commitment": state.commitment,
            "decoder_version": state.decoder_version,
        },
        coverage={"complete_for_record": True, "truncated": False},
    )


def _oracle_observation(state: OracleState) -> Observation:
    payload = state.model_dump(mode="json")
    return Observation(
        kind="oracle.state",
        source_type="blockchain",
        source_system="solana-fixture",
        source_locator={"oracle_ref": state.oracle_ref, "address": state.address},
        event_time=format_utc(state.last_update),
        time_basis="oracle_publish_time",
        correlation={"oracle_ref": state.oracle_ref, "feed_id": state.feed_id},
        payload=payload,
        summary=f"Current oracle {state.oracle_ref} last updated at {format_utc(state.last_update)}.",
        provenance={
            "synthetic": True,
            "snapshot_kind": state.snapshot_kind,
            "context_slot": state.context_slot,
            "commitment": state.commitment,
        },
        coverage={"complete_for_record": True, "truncated": False},
    )


def _runbook_observation(match: RunbookMatch) -> Observation:
    payload = {
        "runbook_id": match.runbook_id,
        "version": match.version,
        "title": match.title,
        "section_id": match.section_id,
        "digest": match.digest,
        "excerpt": match.excerpt,
        "service_ref": match.service_ref,
        "component_ref": match.component_ref,
        "incident_kind": match.incident_kind,
    }
    return Observation(
        kind="runbook.excerpt",
        source_type="runbook",
        source_system="runbook-files",
        source_locator={"runbook_id": match.runbook_id, "version": match.version},
        event_time=None,
        time_basis="document",
        correlation={"service_ref": match.service_ref, "component_ref": match.component_ref},
        payload=payload,
        summary=f"Runbook {match.runbook_id} {match.version}.",
        provenance={"synthetic": True, "untrusted_text": True, "digest": match.digest},
        coverage={"complete_for_record": True, "truncated": False},
    )


def _deployment_observation(record: DeploymentRecord) -> Observation:
    payload = record.model_dump(mode="json")
    return Observation(
        kind="deployment.record",
        source_type="deployments",
        source_system="synthetic-deployments",
        source_locator={"deployment_id": record.deployment_id},
        event_time=format_utc(record.started_at),
        time_basis="deployment_start",
        correlation={"deployment_id": record.deployment_id},
        payload=payload,
        summary=f"Deployment {record.deployment_id} revision {record.revision}.",
        provenance={"synthetic": True},
        coverage={"complete_for_record": True, "truncated": False},
    )
