"""Live path: checkout-api → OTLP → Tempo → typed get_trace → evidence.

Skipped unless FORWARDOPS_TEMPO_SMOKE=1. Normal CI does not start Tempo.
"""

import asyncio
import json
import os
import time
import urllib.error
import urllib.request

import httpx
import pytest
from evals.database import ROOT

from forwardops.api.app import create_app
from forwardops.config import build_settings
from forwardops.domain.errors import ToolFailedError
from forwardops.domain.investigation import InvestigationScope
from forwardops.storage.leases import claim_next
from forwardops.storage.postgres import Database
from forwardops.tools.gateway import ToolContext, ToolGateway

pytestmark = pytest.mark.skipif(
    os.environ.get("FORWARDOPS_TEMPO_SMOKE") != "1",
    reason="FORWARDOPS_TEMPO_SMOKE is not set",
)


async def test_live_tempo_trace_is_persisted(database, pool) -> None:
    checkout = os.environ.get("FORWARDOPS_CHECKOUT_URL", "http://127.0.0.1:8081")
    tempo_url = os.environ.get("FORWARDOPS_TEMPO_URL", "http://127.0.0.1:3200")
    request = urllib.request.Request(
        f"{checkout.rstrip('/')}/checkout",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        response = urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        payload = json.loads(response.read().decode("utf-8"))
        trace_id = response.headers.get("X-Trace-Id") or payload["trace_id"]
    assert len(trace_id) == 32
    settings = build_settings(
        environment="development",
        database_url=database["app"],
        migration_database_url=database["admin"],
        migrations_dir=ROOT / "migrations",
        customer_path=ROOT / "examples/customer-a/config.yaml",
        identities_path=ROOT / "examples/customer-a/dev-identities.yaml",
        tempo_source_id="tempo-local",
        tempo_url=tempo_url,
    )
    application = create_app(settings, pool=pool)
    async with application.router.lifespan_context(application):
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://forwardops") as client:
            created = await client.post(
                "/investigations",
                headers={
                    "Authorization": "Bearer dev-investigator",
                    "Idempotency-Key": f"tempo-smoke-{trace_id[:8]}",
                },
                json={"question": "Why are checkout-api requests failing?"},
            )
        assert created.status_code == 202, created.text
        claim = await claim_next(pool, "tempo-smoke", 60)
        assert claim is not None
        scope = InvestigationScope.model_validate(created.json()["resolved_scope"])
        gateway = ToolGateway(
            Database(pool),
            application.state.runtime.handlers,
            ToolContext(
                tenant_id=claim.tenant_id,
                principal_id="investigator-a",
                investigation_id=claim.investigation_id,
                scope=scope,
                request_id="tempo-smoke",
                trace_id="tempo-smoke",
                lease_epoch=claim.lease_epoch,
                lease_owner=claim.lease_owner,
                frozen_observed_at="2026-09-26T14:10:00Z",
                max_tool_calls=12,
                lease_seconds=60,
            ),
        )
        deadline = time.monotonic() + 20
        result = None
        while time.monotonic() < deadline:
            try:
                result = await gateway.call(
                    "get_trace",
                    {"trace_source_ref": "tempo-local", "trace_id": trace_id},
                )
                break
            except ToolFailedError as exc:
                if exc.code != "NOT_FOUND":
                    raise
                await asyncio.sleep(0.5)
        assert result is not None
        assert result.output["trace_id"] == trace_id
        assert result.output["root_service"] == "checkout-api"
        names = {span["span_name"] for span in result.output["spans"]}
        assert "db.pool.acquire" in names
        assert "SELECT" not in json.dumps(result.output)
        async with pool.connection() as conn:
            async with conn.transaction():
                await conn.execute("SELECT set_config('app.tenant_id', 'customer-a', true)")
                cursor = await conn.execute(
                    """
                    SELECT source_type, source_locator, provenance, payload
                    FROM evidence
                    WHERE investigation_id = %s AND kind = 'observability.trace'
                    """,
                    (claim.investigation_id,),
                )
                stored = await cursor.fetchone()
        assert stored is not None
        assert stored["source_type"] == "observability"
        assert stored["source_locator"]["source_id"] == "tempo-local"
        assert stored["source_locator"]["trace_id"] == trace_id
        assert stored["provenance"]["untrusted_text"] is True
        assert "3200" not in json.dumps(stored["source_locator"])
