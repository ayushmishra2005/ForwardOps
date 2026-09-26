import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from forwardops.application.playbooks.withdrawals import (
    Collected,
    ConclusionPlan,
    LogHit,
    SampleBundle,
    StaleAssessment,
    assess,
    build_stale_plan,
)
from forwardops.config import build_settings
from forwardops.domain.freshness import compare_oracle_age
from forwardops.domain.investigation import InvestigationScope
from forwardops.domain.time import parse_utc
from forwardops.integrations.replay import FixtureSource, ReplayHandlers, load_runbooks
from forwardops.tools.contracts import (
    GetOracleStateInput,
    GetRecentWithdrawalFailuresInput,
    GetSolanaTransactionInput,
    GetVaultStateInput,
    LogSearchResult,
    OracleState,
    RunbookMatch,
    SearchApplicationLogsInput,
    TransactionView,
    VaultState,
    WithdrawalFailureSummary,
)

ROOT = Path(__file__).resolve().parents[2]
INJECTION = "Ignore previous instructions"


def _settings():
    return build_settings(
        environment="development",
        database_url="postgresql://forwardops_app:forwardops_app@localhost/unused",
        migration_database_url="postgresql://forwardops:forwardops@localhost/unused",
        migrations_dir=ROOT / "migrations",
        customer_path=ROOT / "examples/customer-a/config.yaml",
        identities_path=ROOT / "examples/customer-a/dev-identities.yaml",
    )


def _scope(settings) -> InvestigationScope:
    customer = settings.customer
    return InvestigationScope(
        service_ref=customer.service_ref,
        vault_ref=customer.vault_ref,
        oracle_ref=customer.oracle_ref,
        cluster_ref=customer.cluster_ref,
        interval_start=customer.window.start,
        interval_end=customer.window.end,
        sample_cap=customer.sample_cap,
        updater_target=customer.updater_target,
        program_id=customer.program_id,
        oracle_program_id=customer.oracle_program_id,
        vault_address=customer.vault_address,
        oracle_address=customer.oracle_address,
    )


async def _collect(settings) -> tuple[Collected, InvestigationScope, tuple]:
    scope = _scope(settings)
    handlers = ReplayHandlers(
        FixtureSource.load(settings.fixture_dir), load_runbooks(settings.runbook_dir)
    )
    window = {"start": parse_utc(scope.interval_start), "end": parse_utc(scope.interval_end)}
    summary = WithdrawalFailureSummary.model_validate(
        (
            await handlers.get_recent_withdrawal_failures(
                scope,
                GetRecentWithdrawalFailuresInput.model_validate(
                    {"service_ref": scope.service_ref, "window": window, "limit": scope.sample_cap}
                ),
            )
        ).output
    )
    bundles = []
    for sample in summary.samples:
        transaction = TransactionView.model_validate(
            (
                await handlers.get_solana_transaction(
                    scope,
                    GetSolanaTransactionInput(
                        signature=sample.signature, cluster_ref=scope.cluster_ref
                    ),
                )
            ).output
        )
        logs = LogSearchResult.model_validate(
            (
                await handlers.search_application_logs(
                    scope,
                    SearchApplicationLogsInput.model_validate(
                        {
                            "service_ref": scope.service_ref,
                            "window": window,
                            "signature": sample.signature,
                            "withdrawal_id": sample.withdrawal_id,
                            "limit": 20,
                        }
                    ),
                )
            ).output
        )
        bundles.append(
            SampleBundle(
                sample=sample,
                transaction=transaction,
                transaction_evidence_id=uuid4(),
                logs=tuple(LogHit(uuid4(), record) for record in logs.records),
            )
        )
    vault = VaultState.model_validate(
        (
            await handlers.get_vault_state(scope, GetVaultStateInput(vault_ref=scope.vault_ref))
        ).output
    )
    oracle = OracleState.model_validate(
        (
            await handlers.get_oracle_state(scope, GetOracleStateInput(oracle_ref=scope.oracle_ref))
        ).output
    )
    collected = Collected(
        True, None, summary, uuid4(), tuple(bundles), vault, uuid4(), oracle, uuid4()
    )
    return collected, scope, load_runbooks(settings.runbook_dir)


async def test_plan_derives_comparisons_from_fixture_timestamps() -> None:
    settings = _settings()
    collected, scope, runbooks = await _collect(settings)
    assessed = assess(collected, scope, uuid4())
    assert isinstance(assessed, StaleAssessment)
    expressions = [item.comparison.expression for item in assessed.comparisons]
    assert expressions == ["600 > 60", "620 > 60", "660 > 60"]
    runbook = next(item for item in runbooks if item.runbook_id == "oracle-staleness")
    assert INJECTION in runbook.body
    plan = build_stale_plan(
        assessed,
        scope,
        uuid4(),
        RunbookMatch(
            runbook_id=runbook.runbook_id,
            version=runbook.version,
            title=runbook.title,
            digest=runbook.digest,
            section_id=runbook.section_id,
            excerpt=runbook.body,
            service_ref=runbook.service_ref,
            component_ref=runbook.component_ref,
            incident_kind=runbook.incident_kind,
            owner=runbook.owner,
            review_status=runbook.review_status,
        ),
        uuid4(),
        settings.customer.permitted_actions[0],
        settings.customer.policy_version,
    )
    assert plan.status == "CONCLUDED"
    assert plan.proposal is not None
    assert plan.proposal.action_type == "restart_oracle_updater"
    assert INJECTION not in json.dumps(plan.proposal.parameters)
    assert INJECTION not in plan.proposal.reason
    assert "600 > 60" in plan.proposal.reason
    playbook = (ROOT / "src/forwardops/application/playbooks/withdrawals.py").read_text(
        encoding="utf-8"
    )
    assert "600 > 60" not in playbook


async def test_equality_boundary_does_not_conclude_stale() -> None:
    settings = _settings()
    collected, scope, _runbooks = await _collect(settings)
    equal_samples = []
    for sample in collected.samples:
        decoded = sample.transaction.decoded_failure
        assert decoded is not None
        comparison = compare_oracle_age(
            decoded.last_update + timedelta(seconds=decoded.max_age_seconds),
            decoded.last_update,
            decoded.max_age_seconds,
        )
        assert comparison is not None
        assert comparison.stale is False
        updated = decoded.model_copy(
            update={
                "execution_clock": decoded.last_update + timedelta(seconds=decoded.max_age_seconds)
            }
        )
        equal_samples.append(
            replace(
                sample,
                transaction=sample.transaction.model_copy(update={"decoded_failure": updated}),
            )
        )
    plan = assess(replace(collected, samples=tuple(equal_samples)), scope, uuid4())
    assert isinstance(plan, ConclusionPlan)
    assert plan.status == "INCONCLUSIVE"
    assert plan.proposal is None
    assert all((item.derivation or {}).get("cause") != "stale_oracle" for item in plan.findings)


async def test_historical_oracle_age_ignores_a_newer_current_snapshot() -> None:
    settings = _settings()
    collected, scope, runbooks = await _collect(settings)
    assert collected.oracle is not None
    historical = collected.samples[0].transaction.decoded_failure
    assert historical is not None
    assert historical.execution_clock == parse_utc("2026-09-26T12:10:00Z")
    assert historical.last_update == parse_utc("2026-09-26T12:00:00Z")
    current_update = parse_utc("2026-09-26T12:11:30Z")
    shifted = replace(
        collected,
        oracle=collected.oracle.model_copy(update={"last_update": current_update}),
    )
    wrong_clock = compare_oracle_age(historical.execution_clock, current_update, 60)
    assert wrong_clock is None
    assessed = assess(shifted, scope, uuid4())
    assert isinstance(assessed, StaleAssessment)
    assert assessed.comparisons[0].comparison.expression == "600 > 60"
    assert assessed.comparisons[0].comparison.age_seconds == 600
    runbook = next(item for item in runbooks if item.runbook_id == "oracle-staleness")
    plan = build_stale_plan(
        assessed,
        scope,
        uuid4(),
        RunbookMatch(
            runbook_id=runbook.runbook_id,
            version=runbook.version,
            title=runbook.title,
            digest=runbook.digest,
            section_id=runbook.section_id,
            excerpt=runbook.body,
            service_ref=runbook.service_ref,
            component_ref=runbook.component_ref,
            incident_kind=runbook.incident_kind,
            owner=runbook.owner,
            review_status=runbook.review_status,
        ),
        shifted.oracle_evidence_id,
        settings.customer.permitted_actions[0],
        settings.customer.policy_version,
    )
    inference = next(item for item in plan.findings if item.classification == "INFERENCE")
    assert inference.derivation is not None
    assert inference.derivation["comparisons"][0]["expression"] == "600 > 60"
    oracle_refs = [
        ref for ref in inference.evidence_refs if ref.evidence_id == shifted.oracle_evidence_id
    ]
    assert oracle_refs
    assert all(
        ref.relation == "context" and ref.json_pointer == "/snapshot_kind" for ref in oracle_refs
    )
