import json
from datetime import UTC, datetime
from pathlib import Path

from forwardops.domain.time import parse_utc
from forwardops.integrations.replay import Attempt, deployments_in_window, summarize_attempts

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "examples" / "customer-a" / "fixtures"


def _attempt(
    withdrawal_id: str, minute: int, outcome: str, signature: str | None = None
) -> Attempt:
    return Attempt(
        withdrawal_id=withdrawal_id,
        occurred_at=datetime(2026, 9, 26, 12, minute, tzinfo=UTC),
        outcome=outcome,
        signature=signature,
        vault_ref="vault-a",
        error_code="StaleOracle" if outcome == "failure" else None,
        request_id=f"req-{withdrawal_id}",
        trace_id=f"trace-{withdrawal_id}",
        customer_ref=f"cust-{withdrawal_id}",
    )


def test_counts_come_from_rows_not_a_constant() -> None:
    signature = (
        "32DAMcoUj19vMkceHxeiaZXaXvMK1rcf2hWMUWCULAR3FMCSXWKsYVBfY4qZuuFEwar68FUUsDz98254qCXW17o3"
    )
    rows = [
        _attempt("old", 0, "failure", signature),
        _attempt("in-1", 6, "failure", signature),
        _attempt("in-2", 7, "success"),
        _attempt("after", 20, "failure", signature),
    ]
    summary = summarize_attempts(
        rows,
        start=datetime(2026, 9, 26, 12, 5, tzinfo=UTC),
        end=datetime(2026, 9, 26, 12, 12, tzinfo=UTC),
        sample_limit=10,
        service_ref="withdrawal-service",
        coverage_complete=True,
        watermark=datetime(2026, 9, 26, 12, 12, tzinfo=UTC),
    )
    assert summary.incident_attempts == 2
    assert summary.incident_failures == 1
    assert [sample.withdrawal_id for sample in summary.samples] == ["in-1"]
    assert summary.baseline_window.start == datetime(2026, 9, 26, 11, 58, tzinfo=UTC)


def test_fixture_windows_have_the_canonical_counts() -> None:
    payload = json.loads((FIXTURES / "withdrawals.json").read_text(encoding="utf-8"))
    attempts = [
        Attempt(
            withdrawal_id=item["withdrawal_id"],
            occurred_at=parse_utc(item["occurred_at"]),
            outcome=item["outcome"],
            signature=item["signature"],
            vault_ref=item["vault_ref"],
            error_code=item["error_code"],
            request_id=item["request_id"],
            trace_id=item["trace_id"],
            customer_ref=item["customer_ref"],
        )
        for item in payload["attempts"]
    ]
    summary = summarize_attempts(
        attempts,
        start=parse_utc("2026-09-26T12:05:00Z"),
        end=parse_utc("2026-09-26T12:12:00Z"),
        sample_limit=3,
        service_ref="withdrawal-service",
        coverage_complete=True,
        watermark=parse_utc(payload["watermark"]),
    )
    assert summary.incident_attempts == 10
    assert summary.incident_failures == 8
    assert summary.baseline_attempts == 100
    assert summary.baseline_failures == 0
    assert [sample.withdrawal_id for sample in summary.samples] == ["w1", "w2", "w3"]


def test_deployment_filter_uses_the_window() -> None:
    payload = json.loads((FIXTURES / "deployments.json").read_text(encoding="utf-8"))
    incident = deployments_in_window(
        payload["deployments"],
        start=parse_utc("2026-09-26T12:05:00Z"),
        end=parse_utc("2026-09-26T12:12:00Z"),
        limit=20,
        coverage_complete=True,
    )
    assert incident.deployments == []
    wider = deployments_in_window(
        payload["deployments"],
        start=parse_utc("2026-09-25T15:00:00Z"),
        end=parse_utc("2026-09-25T16:00:00Z"),
        limit=20,
        coverage_complete=True,
    )
    assert [item.deployment_id for item in wider.deployments] == ["dep-2026-09-25-1"]


def test_transaction_fixtures_do_not_store_a_stale_flag() -> None:
    text = (FIXTURES / "transactions.json").read_text(encoding="utf-8")
    assert '"stale"' not in text
