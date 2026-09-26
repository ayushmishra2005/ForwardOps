from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from forwardops.config import CustomerConfig, PermittedAction
from forwardops.domain.errors import ToolFailedError
from forwardops.domain.evidence import EvidenceRef, FindingDraft
from forwardops.domain.freshness import FreshnessComparison, compare_oracle_age
from forwardops.domain.investigation import InvestigationScope, hypothesis_template
from forwardops.domain.time import format_utc, parse_utc
from forwardops.tools.contracts import (
    LogRecord,
    LogSearchResult,
    OracleState,
    RunbookMatch,
    RunbookSearchResult,
    TransactionView,
    VaultState,
    WithdrawalFailureSummary,
    WithdrawalSample,
)
from forwardops.tools.gateway import ToolGateway, ToolSuccess

PLAYBOOK_VERSION = "v1"
_STALE_CAUSE = "stale_oracle"
_INJECTION_MARK = "Ignore previous instructions"


@dataclass(frozen=True)
class LogHit:
    evidence_id: UUID
    record: LogRecord


@dataclass(frozen=True)
class SampleBundle:
    sample: WithdrawalSample
    transaction: TransactionView
    transaction_evidence_id: UUID
    logs: tuple[LogHit, ...]


@dataclass(frozen=True)
class Collected:
    complete: bool
    gap: str | None
    summary: WithdrawalFailureSummary | None
    summary_evidence_id: UUID | None
    samples: tuple[SampleBundle, ...]
    vault: VaultState | None
    vault_evidence_id: UUID | None
    oracle: OracleState | None
    oracle_evidence_id: UUID | None


@dataclass(frozen=True)
class ComparedSample:
    bundle: SampleBundle
    comparison: FreshnessComparison


@dataclass(frozen=True)
class StaleAssessment:
    collected: Collected
    comparisons: tuple[ComparedSample, ...]


@dataclass(frozen=True)
class ProposalDraft:
    action_type: str
    target_ref: str
    parameters: dict[str, Any]
    reason: str
    evidence_refs: tuple[EvidenceRef, ...]
    risk: str
    preconditions: dict[str, Any]
    policy_version: str


@dataclass(frozen=True)
class ConclusionPlan:
    status: str
    findings: tuple[FindingDraft, ...]
    hypotheses: list[dict[str, Any]]
    timeline: list[dict[str, Any]]
    unknowns: tuple[str, ...]
    recommendations: tuple[dict[str, Any], ...]
    confidence: str | None
    confidence_basis: tuple[str, ...]
    root_finding_id: UUID | None
    proposal: ProposalDraft | None


async def investigate_withdrawals(
    gateway: ToolGateway,
    scope: InvestigationScope,
    investigation_id: UUID,
    customer: CustomerConfig,
) -> ConclusionPlan:
    collected = await _collect(gateway, scope)
    assessed = assess(collected, scope, investigation_id)
    if isinstance(assessed, ConclusionPlan):
        return assessed
    runbook_call = await gateway.call(
        "search_runbooks",
        {
            "service_ref": scope.service_ref,
            "component_ref": scope.oracle_ref,
            "incident_kind": _STALE_CAUSE,
            "query": "stale oracle",
            "limit": 5,
        },
    )
    runbook = _selected_runbook(runbook_call)
    action = permitted_action(customer, _STALE_CAUSE) if runbook is not None else None
    if runbook is not None and runbook.review_status != "approved":
        action = None
    return build_stale_plan(
        assessed,
        scope,
        investigation_id,
        runbook,
        None if runbook is None else runbook_call.evidence_ids[0],
        action,
        customer.policy_version,
    )


def permitted_action(customer: CustomerConfig, incident_kind: str) -> PermittedAction | None:
    matches = [item for item in customer.permitted_actions if item.incident_kind == incident_kind]
    if len(matches) != 1:
        return None
    return matches[0]


def assess(
    collected: Collected,
    scope: InvestigationScope,
    investigation_id: UUID,
) -> StaleAssessment | ConclusionPlan:
    if (
        not collected.complete
        or collected.summary is None
        or collected.vault is None
        or collected.oracle is None
    ):
        return _incomplete_plan(collected, investigation_id, collected.gap or "missing_evidence")
    comparisons: list[ComparedSample] = []
    failures = []
    for bundle in collected.samples:
        failure = bundle.transaction.decoded_failure
        if failure is None:
            return _incomplete_plan(collected, investigation_id, "missing_decoded_failure")
        comparison = compare_oracle_age(
            failure.execution_clock,
            failure.last_update,
            failure.max_age_seconds,
        )
        if comparison is None:
            return _incomplete_plan(collected, investigation_id, "incomparable_oracle_age")
        comparisons.append(ComparedSample(bundle, comparison))
        failures.append(failure)
    if not comparisons:
        return _incomplete_plan(collected, investigation_id, "no_sampled_transactions")
    if not _binding_ok(collected.vault, collected.oracle, failures, scope):
        return _incomplete_plan(collected, investigation_id, "vault_oracle_binding")
    if not all(item.comparison.stale for item in comparisons):
        return _not_stale_plan(collected, investigation_id, tuple(comparisons))
    return StaleAssessment(collected, tuple(comparisons))


def build_stale_plan(
    assessment: StaleAssessment,
    scope: InvestigationScope,
    investigation_id: UUID,
    runbook: RunbookMatch | None,
    runbook_evidence_id: UUID | None,
    action: PermittedAction | None,
    policy_version: str,
) -> ConclusionPlan:
    summary = assessment.collected.summary
    vault = assessment.collected.vault
    oracle = assessment.collected.oracle
    assert summary is not None and vault is not None and oracle is not None
    assert assessment.collected.summary_evidence_id is not None
    assert assessment.collected.vault_evidence_id is not None
    findings: list[FindingDraft] = [
        _aggregate_fact(summary, assessment.collected.summary_evidence_id),
    ]
    supports: list[EvidenceRef] = []
    for item in assessment.comparisons:
        failure = item.bundle.transaction.decoded_failure
        assert failure is not None
        findings.append(_transaction_fact(item.bundle))
        supports.extend(_transaction_supports(item.bundle.transaction_evidence_id))
        chain_logs = _chain_logs(item.bundle)
        for hit in chain_logs:
            findings.append(_log_fact(hit))
            supports.append(
                EvidenceRef(
                    evidence_id=hit.evidence_id, json_pointer="/error_code", relation="supports"
                )
            )
    supports.append(
        EvidenceRef(
            evidence_id=assessment.collected.vault_evidence_id,
            json_pointer="/oracle_ref",
            relation="supports",
        )
    )
    if assessment.collected.oracle_evidence_id is not None:
        supports.append(
            EvidenceRef(
                evidence_id=assessment.collected.oracle_evidence_id,
                json_pointer="/snapshot_kind",
                relation="context",
            )
        )
    expressions = [item.comparison.expression for item in assessment.comparisons]
    scope_text = f"{len(assessment.comparisons)} sampled failed withdrawals"
    inference_id = uuid4()
    inference_refs = tuple(supports)
    inference = FindingDraft(
        id=inference_id,
        classification="INFERENCE",
        claim="Oracle freshness rejection explains the inspected withdrawal failures.",
        component_ref=scope.oracle_ref,
        evidence_refs=list(inference_refs),
        derivation={
            "rule": "oracle_age_greater_than_max",
            "rule_version": PLAYBOOK_VERSION,
            "cause": _STALE_CAUSE,
            "component": scope.oracle_ref,
            "scope": scope_text,
            "comparisons": [
                {
                    "withdrawal_id": item.bundle.sample.withdrawal_id,
                    "age_seconds": item.comparison.age_seconds,
                    "max_age_seconds": item.comparison.max_age_seconds,
                    "expression": item.comparison.expression,
                    "stale": item.comparison.stale,
                }
                for item in assessment.comparisons
            ],
        },
        confidence="high" if summary.coverage_complete else "medium",
        confidence_basis=[
            "Transaction-time oracle fields were present for the inspected sample.",
            "The vault comparison is age greater than the configured maximum.",
            "Calculated " + ", ".join(expressions) + ".",
        ],
        alternatives=[
            "Application admission failure was not established for the inspected sample.",
            "Deployment history was not queried, so a deployment regression remains unresolved.",
        ],
        limitations=[
            "The reason the oracle publisher stopped updating was not identified.",
            "Failures outside the inspected sample were not individually verified.",
        ],
    )
    findings.append(inference)
    publisher = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="Why the oracle publisher stopped updating is unknown.",
        component_ref=scope.oracle_ref,
        evidence_refs=[],
        derivation=None,
        confidence=None,
        limitations=["No publisher or updater health evidence was collected."],
    )
    unsampled = summary.incident_failures - len(summary.samples)
    impact = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="The customer impact beyond the inspected sample is unknown.",
        component_ref=scope.vault_ref,
        evidence_refs=[],
        derivation=None,
        confidence=None,
        limitations=[
            f"{unsampled} of {summary.incident_failures} failures in the incident window were not inspected."
        ],
    )
    findings.extend((publisher, impact))
    proposal = None
    if runbook is not None and action is not None and runbook_evidence_id is not None:
        proposal = _proposal(
            assessment,
            scope,
            runbook,
            runbook_evidence_id,
            action,
            policy_version,
            inference_refs,
        )
    recommendation = (
        remediation_text(runbook.runbook_id, runbook.version, action.target_ref)
        if runbook is not None and action is not None
        else "Inspect the oracle publisher manually. No approved runbook action was available, so no restart was proposed."
    )
    hypotheses = _hypotheses(investigation_id, "supported", list(inference_refs))
    return ConclusionPlan(
        status="CONCLUDED",
        findings=tuple(findings),
        hypotheses=hypotheses,
        timeline=_timeline(assessment),
        unknowns=(publisher.claim, impact.claim),
        recommendations=(
            {"summary": recommendation, "runbook_id": runbook.runbook_id if runbook else None},
        ),
        confidence="high" if summary.coverage_complete else "medium",
        confidence_basis=tuple(inference.confidence_basis),
        root_finding_id=inference_id,
        proposal=proposal,
    )


def remediation_text(runbook_id: str, version: str, target: str) -> str:
    return (
        f"Follow runbook {runbook_id} {version}: verify oracle updater {target} and its RPC dependency, "
        "restart that mapped updater only after human approval, verify a fresh oracle publication, "
        "and assess withdrawal retry separately. "
        "Do not disable freshness checks, invent a price, or bypass authorization."
    )


async def _collect(gateway: ToolGateway, scope: InvestigationScope) -> Collected:
    window = {"start": parse_utc(scope.interval_start), "end": parse_utc(scope.interval_end)}
    try:
        summary_call = await gateway.call(
            "get_recent_withdrawal_failures",
            {"service_ref": scope.service_ref, "window": window, "limit": scope.sample_cap},
        )
    except ToolFailedError as exc:
        return Collected(False, exc.code, None, None, (), None, None, None, None)
    summary = WithdrawalFailureSummary.model_validate(summary_call.output)
    if len(summary_call.evidence_ids) != 1 + len(summary.samples):
        raise ToolFailedError("INVALID_OUTPUT", "withdrawal evidence did not match the sample")
    bundles: list[SampleBundle] = []
    for sample in summary.samples:
        try:
            transaction_call = await gateway.call(
                "get_solana_transaction",
                {"signature": sample.signature, "cluster_ref": scope.cluster_ref},
            )
        except ToolFailedError as exc:
            if exc.code == "NOT_FOUND":
                return Collected(
                    False,
                    "missing_transaction",
                    summary,
                    summary_call.evidence_ids[0],
                    tuple(bundles),
                    None,
                    None,
                    None,
                    None,
                )
            raise
        transaction = TransactionView.model_validate(transaction_call.output)
        log_call = await gateway.call(
            "search_application_logs",
            {
                "service_ref": scope.service_ref,
                "window": window,
                "signature": sample.signature,
                "withdrawal_id": sample.withdrawal_id,
                "limit": 20,
            },
        )
        logs = LogSearchResult.model_validate(log_call.output)
        if len(log_call.evidence_ids) != len(logs.records):
            raise ToolFailedError("INVALID_OUTPUT", "log evidence did not match the records")
        bundles.append(
            SampleBundle(
                sample=sample,
                transaction=transaction,
                transaction_evidence_id=transaction_call.evidence_ids[0],
                logs=tuple(
                    LogHit(evidence_id, record)
                    for evidence_id, record in zip(log_call.evidence_ids, logs.records, strict=True)
                ),
            )
        )
    vault_call = await gateway.call("get_vault_state", {"vault_ref": scope.vault_ref})
    vault = VaultState.model_validate(vault_call.output)
    if vault.oracle_ref != scope.oracle_ref:
        return Collected(
            False,
            "vault_oracle_binding",
            summary,
            summary_call.evidence_ids[0],
            tuple(bundles),
            vault,
            vault_call.evidence_ids[0],
            None,
            None,
        )
    oracle_call = await gateway.call("get_oracle_state", {"oracle_ref": vault.oracle_ref})
    oracle = OracleState.model_validate(oracle_call.output)
    return Collected(
        True,
        None,
        summary,
        summary_call.evidence_ids[0],
        tuple(bundles),
        vault,
        vault_call.evidence_ids[0],
        oracle,
        oracle_call.evidence_ids[0],
    )


def _selected_runbook(call: ToolSuccess) -> RunbookMatch | None:
    result = RunbookSearchResult.model_validate(call.output)
    if not result.matches or not call.evidence_ids:
        return None
    return result.matches[0]


def _binding_ok(
    vault: VaultState,
    oracle: OracleState,
    failures: list[Any],
    scope: InvestigationScope,
) -> bool:
    if vault.paused or vault.snapshot_kind != "current" or oracle.snapshot_kind != "current":
        return False
    if vault.owner_program != scope.program_id or vault.address != scope.vault_address:
        return False
    if oracle.address != scope.oracle_address or oracle.oracle_ref != scope.oracle_ref:
        return False
    if vault.oracle_ref != scope.oracle_ref or vault.oracle_address != oracle.address:
        return False
    if vault.freshness_comparison != "age_greater_than_max":
        return False
    for failure in failures:
        if failure.oracle_ref != vault.oracle_ref or failure.oracle_address != vault.oracle_address:
            return False
        if failure.max_age_seconds != vault.max_age_seconds:
            return False
        if (
            failure.freshness_comparison != vault.freshness_comparison
            or failure.error_name != "StaleOracle"
        ):
            return False
        if failure.decoder_version != vault.decoder_version:
            return False
    return True


def _proposal(
    assessment: StaleAssessment,
    scope: InvestigationScope,
    runbook: RunbookMatch,
    runbook_evidence_id: UUID,
    action: PermittedAction,
    policy_version: str,
    supports: tuple[EvidenceRef, ...],
) -> ProposalDraft:
    expressions = ", ".join(item.comparison.expression for item in assessment.comparisons)
    reason = (
        f"Sampled withdrawal failures exceeded the vault freshness maximum ({expressions}). "
        f"Runbook {runbook.runbook_id} {runbook.version} permits a conditional restart of {action.target_ref} "
        "after a separate approval. The publisher outage cause is unknown. "
        "Approval does not execute the restart."
    )
    if _INJECTION_MARK in reason:
        raise ToolFailedError("INVALID_OUTPUT", "action reason incorporated untrusted instructions")
    refs = list(supports)
    refs.append(
        EvidenceRef(evidence_id=runbook_evidence_id, json_pointer="/runbook_id", relation="context")
    )
    parameters = {
        "workload_ref": action.target_ref,
        "service_ref": scope.service_ref,
        "oracle_ref": scope.oracle_ref,
    }
    return ProposalDraft(
        action_type=action.action_type,
        target_ref=action.target_ref,
        parameters=parameters,
        reason=reason,
        evidence_refs=tuple(refs),
        risk=action.risk,
        preconditions={
            "runbook_id": runbook.runbook_id,
            "runbook_version": runbook.version,
            "publisher_cause_known": False,
            "freshness_checks_remain_enabled": True,
            "price_write_forbidden": True,
        },
        policy_version=policy_version,
    )


def _aggregate_fact(summary: WithdrawalFailureSummary, evidence_id: UUID) -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim=(
            f"The incident window contains {summary.incident_failures} failed withdrawals "
            f"out of {summary.incident_attempts} attempts, compared with {summary.baseline_failures} "
            f"failures out of {summary.baseline_attempts} attempts in the preceding window."
        ),
        component_ref=summary.service_ref,
        evidence_refs=[
            EvidenceRef(
                evidence_id=evidence_id, json_pointer="/incident_failures", relation="supports"
            ),
            EvidenceRef(
                evidence_id=evidence_id, json_pointer="/incident_attempts", relation="supports"
            ),
            EvidenceRef(
                evidence_id=evidence_id, json_pointer="/baseline_failures", relation="supports"
            ),
            EvidenceRef(
                evidence_id=evidence_id, json_pointer="/baseline_attempts", relation="supports"
            ),
        ],
        confidence=None,
    )


def _transaction_fact(bundle: SampleBundle) -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim=(
            "The vault program emitted a stale-oracle failure for transaction "
            f"{bundle.transaction.signature}."
        ),
        component_ref=bundle.sample.vault_ref,
        evidence_refs=list(_transaction_supports(bundle.transaction_evidence_id)),
        confidence=None,
    )


def _transaction_supports(evidence_id: UUID) -> tuple[EvidenceRef, ...]:
    return tuple(
        EvidenceRef(evidence_id=evidence_id, json_pointer=pointer, relation="supports")
        for pointer in ("/error_name", "/execution_clock", "/last_update", "/max_age_seconds")
    )


def _log_fact(hit: LogHit) -> FindingDraft:
    return FindingDraft(
        id=uuid4(),
        classification="FACT",
        claim=(
            f"Application logs for withdrawal {hit.record.withdrawal_id} "
            f"record error {hit.record.error_code}."
        ),
        component_ref=hit.record.withdrawal_id,
        evidence_refs=[
            EvidenceRef(
                evidence_id=hit.evidence_id, json_pointer="/error_code", relation="supports"
            )
        ],
        confidence=None,
    )


def _chain_logs(bundle: SampleBundle) -> tuple[LogHit, ...]:
    return tuple(
        hit
        for hit in bundle.logs
        if hit.record.event_name == "withdrawal_chain_result"
        and hit.record.error_code == "StaleOracle"
    )


def _timeline(assessment: StaleAssessment) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    updates = {
        item.bundle.transaction.decoded_failure.last_update
        for item in assessment.comparisons
        if item.bundle.transaction.decoded_failure is not None
    }
    if len(updates) == 1:
        moment = next(iter(updates))
        events.append(
            {
                "event_id": str(uuid4()),
                "event_time": format_utc(moment),
                "description": "Oracle last update recorded by the vault program.",
                "evidence_ids": [
                    str(item.bundle.transaction_evidence_id) for item in assessment.comparisons
                ],
            }
        )
    for item in assessment.comparisons:
        failure = item.bundle.transaction.decoded_failure
        assert failure is not None
        events.append(
            {
                "event_id": str(uuid4()),
                "event_time": format_utc(failure.execution_clock),
                "observed_at": format_utc(item.bundle.transaction.observed_at),
                "description": f"Withdrawal {item.bundle.sample.withdrawal_id} was rejected by the vault program.",
                "evidence_ids": [str(item.bundle.transaction_evidence_id)],
            }
        )
    events.sort(key=lambda item: (item["event_time"], item["evidence_ids"][0]))
    return events


def _hypotheses(
    investigation_id: UUID,
    oracle_status: str,
    oracle_refs: list[EvidenceRef],
) -> list[dict[str, Any]]:
    rows = hypothesis_template(investigation_id)
    dumped = [ref.model_dump(mode="json") for ref in oracle_refs]
    for row in rows:
        if row["key"] == "oracle_freshness":
            row["status"] = oracle_status
            row["evidence_refs"] = dumped
        else:
            row["status"] = "unresolved"
    return rows


def _incomplete_plan(collected: Collected, investigation_id: UUID, gap: str) -> ConclusionPlan:
    findings: list[FindingDraft] = []
    if collected.summary is not None and collected.summary_evidence_id is not None:
        findings.append(_aggregate_fact(collected.summary, collected.summary_evidence_id))
    unknown = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="Transaction-time oracle freshness inputs are missing for the sampled withdrawals.",
        component_ref=None,
        evidence_refs=[],
        confidence=None,
        limitations=[f"Collection stopped because {gap.replace('_', ' ')}."],
    )
    findings.append(unknown)
    return ConclusionPlan(
        status="INCONCLUSIVE",
        findings=tuple(findings),
        hypotheses=_hypotheses(investigation_id, "unresolved", []),
        timeline=[],
        unknowns=(unknown.claim,),
        recommendations=(
            {"summary": "Collect the missing transaction-time oracle evidence before concluding."},
        ),
        confidence=None,
        confidence_basis=(),
        root_finding_id=None,
        proposal=None,
    )


def _not_stale_plan(
    collected: Collected,
    investigation_id: UUID,
    comparisons: tuple[ComparedSample, ...],
) -> ConclusionPlan:
    summary = collected.summary
    findings: list[FindingDraft] = []
    if summary is not None and collected.summary_evidence_id is not None:
        findings.append(_aggregate_fact(summary, collected.summary_evidence_id))
    expressions = ", ".join(item.comparison.expression for item in comparisons)
    unknown = FindingDraft(
        id=uuid4(),
        classification="UNKNOWN",
        claim="The inspected samples are not explained by an oracle age above the configured maximum.",
        component_ref=None,
        evidence_refs=[],
        confidence=None,
        limitations=[f"Calculated {expressions}."],
    )
    findings.append(unknown)
    return ConclusionPlan(
        status="INCONCLUSIVE",
        findings=tuple(findings),
        hypotheses=_hypotheses(investigation_id, "unresolved", []),
        timeline=[],
        unknowns=(unknown.claim,),
        recommendations=(
            {"summary": "Do not treat oracle staleness as the cause of this sample."},
        ),
        confidence=None,
        confidence_basis=(),
        root_finding_id=None,
        proposal=None,
    )
