from uuid import UUID

QUESTION = "Why are vault withdrawals failing?"


async def test_other_tenant_cannot_read_investigation(client, app) -> None:
    created = await client.post(
        "/investigations",
        headers={"Authorization": "Bearer dev-investigator", "Idempotency-Key": "tenant-a"},
        json={"question": QUESTION},
    )
    assert created.status_code == 202
    investigation_id = created.json()["id"]
    hidden = await client.get(
        f"/investigations/{investigation_id}",
        headers={"Authorization": "Bearer dev-investigator-b"},
    )
    hidden_evidence = await client.get(
        f"/investigations/{investigation_id}/evidence",
        headers={"Authorization": "Bearer dev-investigator-b"},
    )
    assert hidden.status_code == 404
    assert hidden_evidence.status_code == 404
    denied = await client.post(
        "/investigations",
        headers={"Authorization": "Bearer dev-investigator-b", "Idempotency-Key": "tenant-b"},
        json={"question": QUESTION},
    )
    assert denied.status_code == 403

    async with app.state.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.tenant_id', 'customer-b', true)")
            cursor = await conn.execute("SELECT count(*) AS count FROM investigations")
            row = await cursor.fetchone()
    assert row["count"] == 0

    async with app.state.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.tenant_id', 'customer-a', true)")
            cursor = await conn.execute(
                "SELECT id FROM investigations WHERE id = %s",
                (UUID(investigation_id),),
            )
            visible = await cursor.fetchone()
    assert visible is not None
