from evals.scenario import assert_stale_oracle, run_stale_oracle


async def test_stale_oracle_withdrawal_failure(client, app) -> None:
    result = await run_stale_oracle(client, app)
    assert_stale_oracle(result)
    execute = await client.post(
        f"/actions/{result['approved']['id']}/execute",
        headers={"Authorization": "Bearer dev-approver"},
    )
    assert execute.status_code == 404
