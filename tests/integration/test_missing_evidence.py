import json
import shutil
from pathlib import Path

from forwardops.api.app import create_app
from forwardops.worker import process_one


async def test_missing_transaction_evidence_is_inconclusive(
    client, app, settings, pool, tmp_path: Path
) -> None:
    del client, app
    fixture_dir = tmp_path / "fixtures"
    shutil.copytree(settings.fixture_dir, fixture_dir)
    transactions = json.loads((fixture_dir / "transactions.json").read_text(encoding="utf-8"))
    transactions["transactions"] = {}
    (fixture_dir / "transactions.json").write_text(json.dumps(transactions), encoding="utf-8")
    isolated = settings.model_copy(update={"fixture_dir": fixture_dir})
    application = create_app(isolated, pool=pool)
    async with application.router.lifespan_context(application):
        import httpx

        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://forwardops") as http:
            created = await http.post(
                "/investigations",
                headers={
                    "Authorization": "Bearer dev-investigator",
                    "Idempotency-Key": "missing-tx",
                },
                json={"question": "Why are vault withdrawals failing?"},
            )
            assert created.status_code == 202
            assert await process_one(pool, application.state.runtime)
            body = await http.get(
                f"/investigations/{created.json()['id']}",
                headers={"Authorization": "Bearer dev-investigator"},
            )
    payload = body.json()
    assert payload["status"] == "INCONCLUSIVE"
    assert payload["root_cause_hypothesis"] is None
    assert payload["confidence"] is None
    assert payload["pending_actions"] == []
    assert any(item["classification"] == "UNKNOWN" for item in payload["findings"])
    assert all(
        (item.get("derivation") or {}).get("cause") != "stale_oracle"
        for item in payload["findings"]
    )
