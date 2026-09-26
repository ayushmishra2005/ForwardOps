from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from forwardops.domain.errors import LostLeaseError
from forwardops.domain.investigation import InvestigationScope
from forwardops.integrations.replay import Observation
from forwardops.storage.leases import claim_next
from forwardops.storage.postgres import (
    Database,
    get_investigation,
    insert_tool_call,
    require_live_lease,
    save_conclusion,
)
from forwardops.tools.gateway import ToolContext, ToolGateway

QUESTION = "Why are vault withdrawals failing?"


async def test_stale_worker_cannot_publish_or_conclude(client, app) -> None:
    created = await client.post(
        "/investigations",
        headers={"Authorization": "Bearer dev-investigator", "Idempotency-Key": "stale-lease"},
        json={"question": QUESTION},
    )
    assert created.status_code == 202
    pool = app.state.pool
    claim_a = await claim_next(pool, "worker-a", 60)
    assert claim_a is not None
    database = Database(pool)
    async with database.transaction(claim_a.tenant_id) as conn:
        record = await get_investigation(conn, claim_a.tenant_id, claim_a.investigation_id)
        await require_live_lease(conn, claim_a)
        tool_call_id = uuid4()
        await insert_tool_call(
            conn,
            tenant_id=claim_a.tenant_id,
            tool_call_id=tool_call_id,
            investigation_id=claim_a.investigation_id,
            logical_call_id=uuid4(),
            attempt=1,
            tool_name="get_vault_state",
            tool_version="v1",
            arguments={"vault_ref": "vault-a"},
            arguments_digest="stale-worker",
            deadline_at=datetime.now(UTC) + timedelta(seconds=8),
            worker_epoch=claim_a.lease_epoch,
            source_id="test",
            trace_id="stale-worker",
        )
    assert record is not None
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.tenant_id', %s, true)",
                (claim_a.tenant_id,),
            )
            await conn.execute(
                """
                UPDATE investigations
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE tenant_id = %s AND id = %s
                """,
                (claim_a.tenant_id, claim_a.investigation_id),
            )
    claim_b = await claim_next(pool, "worker-b", 60)
    assert claim_b is not None
    assert claim_b.investigation_id == claim_a.investigation_id
    assert claim_b.lease_epoch == claim_a.lease_epoch + 1
    assert claim_b.lease_owner == "worker-b"

    context = ToolContext(
        tenant_id=claim_a.tenant_id,
        principal_id=record.requester_id,
        investigation_id=claim_a.investigation_id,
        scope=InvestigationScope.model_validate(record.scope),
        request_id="stale-worker",
        trace_id="stale-worker",
        lease_epoch=claim_a.lease_epoch,
        lease_owner=claim_a.lease_owner,
        frozen_observed_at="2026-09-26T12:12:00Z",
        max_tool_calls=12,
        lease_seconds=60,
    )
    gateway = ToolGateway(database, app.state.runtime.handlers, context)
    with pytest.raises(LostLeaseError):
        await gateway.publish_result(
            tool_call_id,
            {"probe": True},
            (
                Observation(
                    kind="test.probe",
                    source_type="test",
                    source_system="test",
                    source_locator={"probe": "stale-worker"},
                    event_time=None,
                    time_basis="test",
                    correlation={},
                    payload={"probe": True},
                    summary="Stale worker probe.",
                    provenance={"synthetic": True},
                    coverage={"complete_for_record": True, "truncated": False},
                ),
            ),
        )
    with pytest.raises(LostLeaseError):
        async with database.transaction(claim_a.tenant_id) as conn:
            await save_conclusion(
                conn,
                claim_a,
                current="CREATED",
                status="CONCLUDED",
                hypotheses=[],
                timeline=[],
                unknowns=[],
                recommendations=[],
                confidence=None,
                confidence_basis=[],
                root_finding_id=None,
                budget={"tool_calls_used": 0},
            )

    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.tenant_id', %s, true)",
                (claim_a.tenant_id,),
            )
            evidence = await conn.execute(
                "SELECT count(*) AS count FROM evidence WHERE investigation_id = %s",
                (claim_a.investigation_id,),
            )
            evidence_row = await evidence.fetchone()
            tool_call = await conn.execute(
                "SELECT status FROM tool_calls WHERE tenant_id = %s AND id = %s",
                (claim_a.tenant_id, tool_call_id),
            )
            tool_row = await tool_call.fetchone()
            investigation = await conn.execute(
                """
                SELECT status, lease_owner, lease_epoch
                FROM investigations
                WHERE tenant_id = %s AND id = %s
                """,
                (claim_a.tenant_id, claim_a.investigation_id),
            )
            investigation_row = await investigation.fetchone()
    assert evidence_row["count"] == 0
    assert tool_row["status"] == "STARTED"
    assert investigation_row["status"] == "CREATED"
    assert investigation_row["lease_owner"] == "worker-b"
    assert investigation_row["lease_epoch"] == claim_b.lease_epoch
