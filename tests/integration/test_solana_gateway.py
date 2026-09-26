import json
import os
from urllib.parse import urlsplit

import pytest
from tests.unit.test_solana_adapter import (
    CLOCK,
    SECRET_URL,
    SIGNATURE,
    TOKEN,
    _clock,
    account_result,
    rpc_body,
    transaction_result,
)

from forwardops.domain.errors import ToolFailedError
from forwardops.domain.investigation import InvestigationScope
from forwardops.domain.time import parse_utc
from forwardops.integrations.routing import RoutingHandlers
from forwardops.integrations.solana import (
    ENABLED_RPC_METHODS,
    SolanaEndpoint,
    SolanaHandlers,
    execute_read,
    post_rpc,
)
from forwardops.storage.leases import claim_next
from forwardops.storage.postgres import Database, get_investigation, list_evidence
from forwardops.tools.contracts import SolanaAccountView, TransactionView
from forwardops.tools.gateway import ToolContext, ToolGateway

QUESTION = "Why are vault withdrawals failing?"


async def _scoped_gateway(
    client,
    app,
    *,
    cluster: str,
    endpoint: SolanaEndpoint,
    transport,
    idempotency_key: str,
):
    created = await client.post(
        "/investigations",
        headers={
            "Authorization": "Bearer dev-investigator",
            "Idempotency-Key": idempotency_key,
        },
        json={"question": QUESTION},
    )
    assert created.status_code == 202, created.text
    pool = app.state.pool
    claim = await claim_next(pool, f"solana-{cluster}", 60)
    assert claim is not None
    database = Database(pool)
    async with database.transaction(claim.tenant_id) as conn:
        await conn.execute(
            """
            UPDATE investigations
            SET scope = jsonb_set(scope, '{cluster_ref}', %s::jsonb)
            WHERE tenant_id = %s AND id = %s
            """,
            (json.dumps(cluster), claim.tenant_id, claim.investigation_id),
        )
        record = await get_investigation(conn, claim.tenant_id, claim.investigation_id)
    assert record is not None
    scope = InvestigationScope.model_validate(record.scope)
    assert scope.cluster_ref == cluster
    routing = RoutingHandlers(
        app.state.runtime.handlers.replay,
        SolanaHandlers((endpoint,), transport=transport, clock=_clock),
    )
    context = ToolContext(
        tenant_id=claim.tenant_id,
        principal_id=record.requester_id,
        investigation_id=claim.investigation_id,
        scope=scope,
        request_id=f"solana-{cluster}",
        trace_id=f"solana-{cluster}",
        lease_epoch=claim.lease_epoch,
        lease_owner=claim.lease_owner,
        frozen_observed_at="2026-09-26T12:12:00Z",
        max_tool_calls=12,
        lease_seconds=60,
    )
    return database, claim, ToolGateway(database, routing, context)


async def test_gateway_persists_mocked_solana_reads(client, app) -> None:
    seen: list[str] = []

    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
        del url, headers, timeout
        payload = json.loads(body)
        seen.append(payload["method"])
        if payload["method"] == "getTransaction":
            return rpc_body(transaction_result())
        if payload["method"] == "getAccountInfo":
            return rpc_body(account_result())
        raise AssertionError(payload["method"])

    database, claim, gateway = await _scoped_gateway(
        client,
        app,
        cluster="mainnet-beta",
        endpoint=SolanaEndpoint("mainnet-beta", SECRET_URL, "finalized", 8),
        transport=transport,
        idempotency_key="solana-mock",
    )
    transaction = await gateway.call(
        "get_solana_transaction",
        {"signature": SIGNATURE, "cluster_ref": "mainnet-beta"},
    )
    account = await gateway.call(
        "get_solana_account",
        {"address": TOKEN, "cluster_ref": "mainnet-beta"},
    )
    with pytest.raises(ToolFailedError) as exc:
        await gateway.call(
            "get_solana_transaction",
            {"signature": SIGNATURE, "cluster_ref": "devnet"},
        )
    assert exc.value.code == "FORBIDDEN_RESOURCE"
    assert seen == ["getTransaction", "getAccountInfo"]

    view = TransactionView.model_validate(transaction.output)
    assert view.decoded_failure is None
    assert view.cluster_ref == "mainnet-beta"
    account_view = SolanaAccountView.model_validate(account.output)
    assert account_view.historical_state is False
    assert account_view.snapshot_kind == "current_account"

    async with database.transaction(claim.tenant_id) as conn:
        evidence = await list_evidence(conn, claim.tenant_id, claim.investigation_id, limit=10)
        cursor = await conn.execute(
            """
            SELECT tool_name, source_id, status
            FROM tool_calls
            WHERE tenant_id = %s AND investigation_id = %s AND status = 'SUCCEEDED'
            ORDER BY started_at
            """,
            (claim.tenant_id, claim.investigation_id),
        )
        tools = await cursor.fetchall()
    assert [row["source_id"] for row in tools] == ["solana-rpc", "solana-rpc"]
    kinds = {row["kind"]: row for row in evidence}
    assert set(kinds) == {"solana.transaction", "solana.account"}
    stored_tx = kinds["solana.transaction"]
    stored_account = kinds["solana.account"]
    assert stored_tx["source_system"] == "solana-rpc"
    assert stored_tx["provenance"]["synthetic"] is False
    assert stored_tx["provenance"]["cluster"] == "mainnet-beta"
    assert stored_tx["provenance"]["commitment"] == "finalized"
    assert stored_tx["provenance"]["slot"] == 123456
    assert stored_tx["provenance"]["current_account_state"] is False
    assert parse_utc(stored_tx["payload"]["observed_at"]) == CLOCK
    assert stored_tx["observed_at"].astimezone(CLOCK.tzinfo) == CLOCK
    assert stored_account["payload"]["historical_state"] is False
    assert stored_account["payload"]["snapshot_kind"] == "current_account"
    assert stored_account["time_basis"] == "current_snapshot"
    assert stored_account["event_time"] is None
    rendered = json.dumps({"evidence": evidence, "output": transaction.output}, default=str)
    assert "supersecret" not in rendered
    assert "secret-token" not in rendered
    import base64

    assert base64.b64encode(b"forwardops-account-bytes").decode() not in rendered


@pytest.mark.skipif(
    not (
        os.environ.get("FORWARDOPS_SOLANA_RPC_URL")
        and os.environ.get("FORWARDOPS_SOLANA_CLUSTER")
        and os.environ.get("FORWARDOPS_SOLANA_SIGNATURE")
    ),
    reason=(
        "FORWARDOPS_SOLANA_RPC_URL, FORWARDOPS_SOLANA_CLUSTER, "
        "and FORWARDOPS_SOLANA_SIGNATURE are not configured"
    ),
)
async def test_live_solana_transaction_smoke(client, app) -> None:
    cluster = os.environ["FORWARDOPS_SOLANA_CLUSTER"].strip()
    rpc_url = os.environ["FORWARDOPS_SOLANA_RPC_URL"].strip()
    signature = os.environ["FORWARDOPS_SOLANA_SIGNATURE"].strip()
    commitment = os.environ.get("FORWARDOPS_SOLANA_COMMITMENT", "finalized").strip()
    timeout = float(os.environ.get("FORWARDOPS_SOLANA_TIMEOUT_SECONDS", "20"))
    sent: list[str] = []

    def transport(url: str, body: bytes, headers: dict[str, str], timeout_seconds: float) -> bytes:
        sent.append(json.loads(body)["method"])
        return post_rpc(url, body, headers, timeout_seconds)

    database, claim, gateway = await _scoped_gateway(
        client,
        app,
        cluster=cluster,
        endpoint=SolanaEndpoint(cluster, rpc_url, commitment, timeout),
        transport=transport,
        idempotency_key="solana-live",
    )
    result = await gateway.call(
        "get_solana_transaction",
        {"signature": signature, "cluster_ref": cluster},
    )
    view = TransactionView.model_validate(result.output)
    assert view.signature == signature
    assert view.cluster_ref == cluster
    assert view.slot >= 0
    assert view.commitment == commitment
    assert view.status in {"success", "failed"}
    assert view.decoded_failure is None
    assert view.program_ids
    async with database.transaction(claim.tenant_id) as conn:
        evidence = await list_evidence(conn, claim.tenant_id, claim.investigation_id, limit=10)
        cursor = await conn.execute(
            """
            SELECT source_id, status
            FROM tool_calls
            WHERE tenant_id = %s AND investigation_id = %s AND tool_name = 'get_solana_transaction'
            """,
            (claim.tenant_id, claim.investigation_id),
        )
        tool = await cursor.fetchone()
    assert tool["status"] == "SUCCEEDED"
    assert tool["source_id"] == "solana-rpc"
    assert len(evidence) == 1
    row = evidence[0]
    assert row["kind"] == "solana.transaction"
    assert row["source_system"] == "solana-rpc"
    assert row["provenance"]["synthetic"] is False
    assert row["provenance"]["cluster"] == cluster
    assert row["provenance"]["commitment"] == commitment
    assert row["provenance"]["slot"] == view.slot
    assert row["provenance"]["current_account_state"] is False
    assert parse_utc(row["payload"]["observed_at"]) == CLOCK
    assert row["observed_at"].astimezone(CLOCK.tzinfo) == CLOCK
    assert "retrieved_at" in row["provenance"]
    stored = json.dumps({"evidence": evidence, "output": result.output}, default=str)
    parts = urlsplit(rpc_url)
    for fragment in (parts.username, parts.password, parts.query):
        if fragment:
            assert fragment not in stored
    for segment in parts.path.split("/"):
        if len(segment) >= 12:
            assert segment not in stored
    assert sent == ["getTransaction"]
    assert ENABLED_RPC_METHODS == frozenset({"getTransaction", "getAccountInfo"})
    with pytest.raises(ToolFailedError) as exc:
        execute_read(
            SolanaEndpoint(cluster, rpc_url, commitment, timeout),
            "sendTransaction",
            [],
            transport=transport,
        )
    assert exc.value.code == "FORBIDDEN_METHOD"
    assert sent == ["getTransaction"]
