QUESTION = "Why are vault withdrawals failing?"


def _headers(token: str, key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Idempotency-Key": key}


async def test_duplicate_post_returns_the_same_investigation(client) -> None:
    first = await client.post(
        "/investigations",
        headers=_headers("dev-investigator", "dup-1"),
        json={"question": QUESTION},
    )
    second = await client.post(
        "/investigations",
        headers=_headers("dev-investigator", "dup-1"),
        json={"question": QUESTION},
    )
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    assert first.headers["location"].endswith(first.json()["id"])


async def test_same_key_with_a_different_question_conflicts(client) -> None:
    await client.post(
        "/investigations",
        headers=_headers("dev-investigator", "dup-2"),
        json={"question": QUESTION},
    )
    conflict = await client.post(
        "/investigations",
        headers=_headers("dev-investigator", "dup-2"),
        json={"question": "Why are vault withdrawals failing now?"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "conflict"
