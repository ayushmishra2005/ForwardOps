from evals.scenario import run_stale_oracle


async def test_approval_rules_and_replay(client, app) -> None:
    result = await run_stale_oracle(client, app)
    action = result["investigation"]["pending_actions"][0]
    # The scenario already approved this action. A second distinct request replays it.
    replay = await client.post(
        f"/actions/{action['id']}/approve",
        headers={"Authorization": "Bearer dev-approver", "Idempotency-Key": "approve-again"},
        json={
            "proposal_digest": action["proposal_digest"],
            "reason": "A later identical approval is the stored decision.",
        },
    )
    assert replay.status_code == 200
    assert replay.json()["status"] == "APPROVED"
    assert replay.json()["execution_enabled"] is False

    reject = await client.post(
        f"/actions/{action['id']}/reject",
        headers={"Authorization": "Bearer dev-approver", "Idempotency-Key": "reject-after"},
        json={"proposal_digest": action["proposal_digest"], "reason": "Too late to reject."},
    )
    assert reject.status_code == 409

    mismatch = await client.post(
        f"/actions/{action['id']}/approve",
        headers={"Authorization": "Bearer dev-approver", "Idempotency-Key": "bad-digest"},
        json={"proposal_digest": "0" * 64, "reason": "Wrong digest."},
    )
    # The proposal is already approved, so a different digest conflicts.
    assert mismatch.status_code == 409


async def test_self_approval_and_digest_mismatch_before_decision(client, app) -> None:
    from forwardops.worker import process_one

    created = await client.post(
        "/investigations",
        headers={"Authorization": "Bearer dev-investigator", "Idempotency-Key": "self-approval"},
        json={"question": "Why are vault withdrawals failing?"},
    )
    assert created.status_code == 202
    assert await process_one(app.state.pool, app.state.runtime)
    body = await client.get(
        f"/investigations/{created.json()['id']}",
        headers={"Authorization": "Bearer dev-investigator"},
    )
    action = body.json()["pending_actions"][0]
    self_approval = await client.post(
        f"/actions/{action['id']}/approve",
        headers={"Authorization": "Bearer dev-requester-approver", "Idempotency-Key": "self"},
        json={"proposal_digest": action["proposal_digest"], "reason": "I requested this."},
    )
    assert self_approval.status_code == 403
    mismatch = await client.post(
        f"/actions/{action['id']}/approve",
        headers={"Authorization": "Bearer dev-approver", "Idempotency-Key": "mismatch"},
        json={"proposal_digest": "f" * 64, "reason": "Digest does not match."},
    )
    assert mismatch.status_code == 409
    still_pending = await client.get(
        f"/actions/{action['id']}",
        headers={"Authorization": "Bearer dev-approver"},
    )
    assert still_pending.json()["status"] == "WAITING_FOR_APPROVAL"
    assert still_pending.json()["decision"] is None
