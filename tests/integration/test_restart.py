from uuid import UUID

from forwardops.worker import process_one

QUESTION = "Why are vault withdrawals failing?"


async def test_worker_reuses_completed_tool_calls_after_suspension(client, app) -> None:
    created = await client.post(
        "/investigations",
        headers={"Authorization": "Bearer dev-investigator", "Idempotency-Key": "restart"},
        json={"question": QUESTION},
    )
    assert created.status_code == 202
    investigation_id = created.json()["id"]
    assert await process_one(app.state.pool, app.state.runtime, suspend_after_new_tool_calls=2)
    paused = await client.get(
        f"/investigations/{investigation_id}",
        headers={"Authorization": "Bearer dev-investigator"},
    )
    assert paused.json()["status"] == "COLLECTING_EVIDENCE"
    assert await process_one(app.state.pool, app.state.runtime)
    finished = await client.get(
        f"/investigations/{investigation_id}",
        headers={"Authorization": "Bearer dev-investigator"},
    )
    assert finished.json()["status"] == "CONCLUDED"
    async with app.state.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.tenant_id', 'customer-a', true)")
            cursor = await conn.execute(
                """
                SELECT tool_name, status, attempt, logical_call_id
                FROM tool_calls
                WHERE investigation_id = %s AND status = 'SUCCEEDED'
                """,
                (UUID(investigation_id),),
            )
            rows = await cursor.fetchall()
    assert len(rows) == 10
    assert len({row["logical_call_id"] for row in rows}) == 10
    assert all(row["attempt"] == 1 for row in rows)


async def test_two_workers_cannot_claim_the_same_investigation(client, app) -> None:
    from forwardops.storage.leases import claim_next

    created = await client.post(
        "/investigations",
        headers={"Authorization": "Bearer dev-investigator", "Idempotency-Key": "lease"},
        json={"question": QUESTION},
    )
    assert created.status_code == 202
    import asyncio

    first, second = await asyncio.gather(
        claim_next(app.state.pool, "worker-a", 30),
        claim_next(app.state.pool, "worker-b", 30),
    )
    claims = [item for item in (first, second) if item is not None]
    assert len(claims) == 1
