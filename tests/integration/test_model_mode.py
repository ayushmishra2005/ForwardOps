import json
import os
from uuid import uuid4

import pytest
from evals.model_eval import (
    assert_model_assisted,
    assert_prompt_injection,
    live_provider,
    live_setting_updates,
    run_model_investigation,
)
from evals.model_script import EXECUTE_PHRASE, StaleOracleModel
from evals.scenario import QUESTION, _headers, _rows, run_stale_oracle

from forwardops.domain.errors import ModelError
from forwardops.models.contracts import (
    ModelReply,
    ProposedAnalysis,
    ProposedEvidenceRef,
    ProposedFinding,
    ProposedToolCall,
)
from forwardops.runtime import Runtime
from forwardops.worker import process_one

_METADATA_KEYS = {
    "request_id",
    "provider",
    "model",
    "duration_ms",
    "token_usage",
    "finish_reason",
    "error_category",
    "reply_kind",
    "requested_tools",
    "application_outcome",
    "unsupported_claims",
}


class _Guard:
    provider_name = "guard"
    model_name = "none"

    def __init__(self) -> None:
        self.called = False

    async def complete(self, request) -> ModelReply:
        del request
        self.called = True
        raise ModelError("model_unavailable", "deterministic mode called the model")


class _Fixed:
    provider_name = "scripted"
    model_name = "fixed"

    def __init__(self, reply: ModelReply | Exception) -> None:
        self.reply = reply
        self.calls = 0

    async def complete(self, request) -> ModelReply:
        del request
        self.calls += 1
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply


def _tool(name: str, arguments: dict) -> ModelReply:
    return ModelReply(
        kind="tool_requests",
        tool_requests=[ProposedToolCall(name=name, arguments=arguments)],
        provider="scripted",
        model="fixed",
        finish_reason="tool_calls",
    )


def _analysis(evidence_id) -> ModelReply:
    return ModelReply(
        kind="analysis",
        analysis=ProposedAnalysis(
            findings=[
                ProposedFinding(
                    classification="FACT",
                    claim="This citation is not from the current investigation.",
                    evidence_refs=[
                        ProposedEvidenceRef(
                            evidence_id=evidence_id,
                            json_pointer="/incident_failures",
                            relation="supports",
                        )
                    ],
                )
            ]
        ),
        provider="scripted",
        model="fixed",
        finish_reason="stop",
    )


async def test_model_stale_oracle_uses_application_freshness(client, app) -> None:
    result = await run_model_investigation(
        client,
        app,
        StaleOracleModel(),
        idempotency_key="model-stale-oracle",
    )
    assert_model_assisted(result)
    rendered = json.dumps(result["model_calls"])
    assert "1 > 60" not in rendered
    assert EXECUTE_PHRASE not in rendered
    assert result["model_calls"]
    assert all(set(call) <= _METADATA_KEYS for call in result["model_calls"])
    assert sum(call["token_usage"]["total_tokens"] for call in result["model_calls"]) > 0


async def test_invalid_model_tool_name(client, app) -> None:
    provider = _Fixed(_tool("run_shell", {"command": "restart_oracle_updater"}))
    result = await run_model_investigation(
        client,
        app,
        provider,
        idempotency_key="invalid-tool",
        max_model_calls=1,
        max_analysis_rounds=1,
    )
    assert result["investigation"]["status"] == "INCONCLUSIVE"
    assert result["tool_calls"] == []
    assert result["model_calls"][0]["requested_tools"] == ["run_shell"]
    assert provider.calls == 1


async def test_tool_arguments_outside_investigation_scope(client, app) -> None:
    provider = _Fixed(_tool("get_vault_state", {"vault_ref": "vault-other"}))
    result = await run_model_investigation(
        client,
        app,
        provider,
        idempotency_key="outside-scope",
        max_model_calls=1,
        max_analysis_rounds=1,
    )
    assert result["investigation"]["status"] == "INCONCLUSIVE"
    assert result["tool_calls"][0]["status"] == "FAILED"
    assert result["tool_calls"][0]["error"]["code"] == "FORBIDDEN_RESOURCE"
    assert result["tool_calls"][0]["arguments"]["vault_ref"] == "vault-other"


async def test_fabricated_evidence_id_is_not_persisted(client, app) -> None:
    fabricated = uuid4()
    result = await run_model_investigation(
        client,
        app,
        _Fixed(_analysis(fabricated)),
        idempotency_key="fabricated-evidence",
        max_model_calls=1,
        max_analysis_rounds=1,
    )
    rendered = json.dumps(result["investigation"]["findings"])
    assert str(fabricated) not in rendered
    assert result["investigation"]["status"] == "INCONCLUSIVE"
    assert result["model_calls"][0]["application_outcome"] == "analysis_rejected"
    assert result["model_calls"][0]["unsupported_claims"] >= 1


async def test_cross_investigation_evidence_reference(client, app) -> None:
    first = await run_stale_oracle(client, app)
    foreign = first["evidence"][0]["id"]
    result = await run_model_investigation(
        client,
        app,
        _Fixed(_analysis(foreign)),
        idempotency_key="cross-investigation",
        max_model_calls=1,
        max_analysis_rounds=1,
    )
    assert result["investigation"]["status"] == "INCONCLUSIVE"
    assert foreign not in json.dumps(result["investigation"]["findings"])
    assert foreign not in {item["id"] for item in result["evidence"]}
    async with app.state.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.tenant_id', 'customer-b', true)")
            cursor = await conn.execute("SELECT id FROM evidence WHERE id = %s", (foreign,))
            hidden = await cursor.fetchone()
    assert hidden is None


async def test_malformed_structured_output(client, app) -> None:
    provider = _Fixed(ModelError("malformed_output", "structured output was malformed"))
    result = await run_model_investigation(
        client,
        app,
        provider,
        idempotency_key="malformed-output",
        max_model_calls=1,
        max_analysis_rounds=1,
    )
    assert provider.calls == 1
    assert result["investigation"]["status"] == "INCONCLUSIVE"
    assert result["model_calls"][0]["error_category"] == "malformed_output"
    assert result["investigation"]["root_cause_hypothesis"] is None


async def test_model_unavailable_is_inconclusive(client, app) -> None:
    provider = _Fixed(ModelError("model_unavailable", "model provider was unavailable"))
    result = await run_model_investigation(
        client,
        app,
        provider,
        idempotency_key="model-down",
        max_model_calls=4,
        max_analysis_rounds=4,
    )
    assert provider.calls == 1
    assert result["investigation"]["status"] == "INCONCLUSIVE"
    assert "unavailable" in " ".join(result["investigation"]["unknowns"])
    assert result["model_calls"][0]["error_category"] == "model_unavailable"


async def test_model_tool_budget_is_inconclusive(client, app) -> None:
    result = await run_model_investigation(
        client,
        app,
        StaleOracleModel(),
        idempotency_key="tool-budget",
        max_tool_calls=1,
        max_model_calls=4,
        max_analysis_rounds=4,
    )
    assert result["investigation"]["status"] == "INCONCLUSIVE"
    assert "tool call budget" in " ".join(result["investigation"]["unknowns"])
    succeeded = [row for row in result["tool_calls"] if row["status"] == "SUCCEEDED"]
    assert len(succeeded) == 1
    assert any(
        row["status"] == "FAILED" and row["error"]["code"] == "BUDGET_EXCEEDED"
        for row in result["tool_calls"]
    )


async def test_prompt_injection_cannot_execute(client, app) -> None:
    provider = StaleOracleModel(inject=True)
    result = await run_model_investigation(
        client,
        app,
        provider,
        idempotency_key="prompt-injection",
    )
    assert_prompt_injection(result, provider)


async def test_deterministic_mode_does_not_call_the_model(client, app) -> None:
    guard = _Guard()
    app.state.runtime = Runtime(
        settings=app.state.settings,
        handlers=app.state.runtime.handlers,
        model_provider=guard,
    )
    created = await client.post(
        "/investigations",
        headers=_headers("dev-investigator", "deterministic-fallback"),
        json={"question": QUESTION},
    )
    assert created.status_code == 202
    assert created.json()["analysis_mode"] == "deterministic"
    assert await process_one(app.state.pool, app.state.runtime)
    completed = await client.get(
        f"/investigations/{created.json()['id']}",
        headers=_headers("dev-investigator"),
    )
    assert completed.status_code == 200
    body = completed.json()
    assert body["status"] == "CONCLUDED"
    assert body["analysis_mode"] == "deterministic"
    assert body["root_cause_hypothesis"]["cause"] == "stale_oracle"
    assert guard.called is False
    rows = await _rows(app.state.pool, created.json()["id"])
    assert rows["model_calls"] == []


@pytest.mark.skipif(
    not os.environ.get("FORWARDOPS_OPENAI_API_KEY"),
    reason="FORWARDOPS_OPENAI_API_KEY is not configured",
)
async def test_live_model_stale_oracle(client, app) -> None:
    result = await run_model_investigation(
        client,
        app,
        live_provider(),
        idempotency_key="live-model",
        **live_setting_updates(),
    )
    assert_model_assisted(result)
