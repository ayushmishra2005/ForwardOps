import io
import json
import logging
from datetime import UTC, datetime
from http.client import HTTPMessage
from urllib import error as urllib_error

import pytest
from pydantic import ValidationError
from tests.unit.test_plan import _scope, _settings

from forwardops.config import (
    DecoderBinding,
    build_settings,
    configured_solana_clusters,
)
from forwardops.domain.errors import ConfigError, ToolFailedError
from forwardops.domain.time import parse_utc
from forwardops.integrations.replay import FixtureSource, ReplayHandlers, load_runbooks
from forwardops.integrations.routing import RoutingHandlers
from forwardops.integrations.solana import (
    ENABLED_RPC_METHODS,
    SolanaEndpoint,
    SolanaHandlers,
    bind_rpc_log,
    execute_read,
    read_account,
    read_transaction,
    redact_rpc_url,
    require_installed_decoders,
    select_account_decoder,
)
from forwardops.logging import _JsonFormatter
from forwardops.tools.contracts import (
    GetSolanaAccountInput,
    GetSolanaTransactionInput,
    SolanaAccountView,
    TransactionView,
)

SIGNATURE = (
    "32DAMcoUj19vMkceHxeiaZXaXvMK1rcf2hWMUWCULAR3FMCSXWKsYVBfY4qZuuFEwar68FUUsDz98254qCXW17o3"
)
FEE_PAYER = "7WQTGkNUH6UiLT77pSvMGtGLAzK59va9z3zchkP9FzjR"
SYSTEM = "11111111111111111111111111111111"
TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
CLOCK = datetime(2026, 9, 26, 15, 0, tzinfo=UTC)
SECRET_URL = "https://user:supersecret@rpc.example/secret-token?api-key=supersecret"
ACCOUNT_BYTES = b"forwardops-account-bytes"


def _endpoint(url: str = "https://rpc.example") -> SolanaEndpoint:
    return SolanaEndpoint("mainnet-beta", url, "finalized", 8)


def _clock() -> datetime:
    return CLOCK


def rpc_body(result: object) -> bytes:
    return json.dumps({"jsonrpc": "2.0", "result": result, "id": "1"}).encode()


def transaction_result(
    *,
    block_time: int | None = 1_758_900_000,
    version: object = 0,
    err: object = None,
    logs: list[str] | None = None,
) -> dict:
    return {
        "slot": 123456,
        "blockTime": block_time,
        "version": version,
        "transaction": {
            "signatures": [SIGNATURE],
            "message": {
                "accountKeys": [FEE_PAYER],
                "instructions": [{"programIdIndex": 1, "accounts": [0], "data": ""}],
            },
        },
        "meta": {
            "err": err,
            "logMessages": logs
            if logs is not None
            else [
                "Program 11111111111111111111111111111111 invoke [1]",
                "Program log: unique-marker-not-for-logs",
            ],
            "loadedAddresses": {"writable": [], "readonly": [SYSTEM]},
        },
    }


def account_result() -> dict:
    import base64

    return {
        "context": {"slot": 99},
        "value": {
            "lamports": 2_039_280,
            "owner": TOKEN,
            "executable": False,
            "rentEpoch": 0,
            "data": [base64.b64encode(ACCOUNT_BYTES).decode(), "base64"],
        },
    }


def _returning(payload: bytes):
    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
        del url, body, headers, timeout
        return payload

    return transport


def test_get_transaction_normalization() -> None:
    result = read_transaction(
        _endpoint(),
        SIGNATURE,
        transport=_returning(rpc_body(transaction_result())),
        clock=_clock,
    )
    view = TransactionView.model_validate(result.output)
    assert view.signature == SIGNATURE
    assert view.cluster_ref == "mainnet-beta"
    assert view.slot == 123456
    assert view.block_time == datetime.fromtimestamp(1_758_900_000, UTC)
    assert view.commitment == "finalized"
    assert view.program_ids == [SYSTEM]
    assert view.status == "success"
    assert view.instruction_errors == []
    assert view.decoded_failure is None
    assert any("unique-marker-not-for-logs" in line for line in view.relevant_logs)
    observation = result.observations[0]
    assert observation.kind == "solana.transaction"
    assert observation.source_system == "solana-rpc"
    assert observation.provenance["synthetic"] is False
    assert observation.provenance["cluster"] == "mainnet-beta"
    assert observation.provenance["current_account_state"] is False
    failed = read_transaction(
        _endpoint(),
        SIGNATURE,
        transport=_returning(
            rpc_body(transaction_result(err={"InstructionError": [0, {"Custom": 42}]}))
        ),
        clock=_clock,
    )
    failed_view = TransactionView.model_validate(failed.output)
    assert failed_view.status == "failed"
    assert failed_view.instruction_errors == [{"index": 0, "error": {"Custom": 42}}]


def test_transaction_not_found_is_not_proof_of_absence() -> None:
    with pytest.raises(ToolFailedError) as exc:
        read_transaction(
            _endpoint(),
            SIGNATURE,
            transport=_returning(rpc_body(None)),
            clock=_clock,
        )
    assert exc.value.code == "NOT_FOUND"
    assert exc.value.retryable is False
    assert "does not establish that the transaction never existed" in str(exc.value)


def test_null_block_time() -> None:
    result = read_transaction(
        _endpoint(),
        SIGNATURE,
        transport=_returning(rpc_body(transaction_result(block_time=None))),
        clock=_clock,
    )
    view = TransactionView.model_validate(result.output)
    assert view.block_time is None
    assert result.observations[0].event_time is None
    assert result.observations[0].time_basis == "unknown_block_time"
    assert result.observations[0].payload["block_time"] is None
    assert parse_utc(result.observations[0].payload["observed_at"]) == CLOCK


def test_malformed_rpc_result() -> None:
    payloads = (
        b"",
        b"not-json",
        b"[]",
        b'{"jsonrpc":"2.0","id":"1"}',
        rpc_body({"slot": "nope"}),
    )
    for payload in payloads:
        with pytest.raises(ToolFailedError) as exc:
            read_transaction(_endpoint(), SIGNATURE, transport=_returning(payload), clock=_clock)
        assert exc.value.code == "MALFORMED_RESPONSE"


def test_unsupported_transaction_version() -> None:
    with pytest.raises(ToolFailedError) as exc:
        read_transaction(
            _endpoint(),
            SIGNATURE,
            transport=_returning(rpc_body(transaction_result(version=1))),
            clock=_clock,
        )
    assert exc.value.code == "UNSUPPORTED_VERSION"
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "error": {"code": -32015, "message": "Transaction version (1) is not supported"},
            "id": "1",
        }
    ).encode()
    with pytest.raises(ToolFailedError) as exc:
        read_transaction(_endpoint(), SIGNATURE, transport=_returning(body), clock=_clock)
    assert exc.value.code == "UNSUPPORTED_VERSION"


def test_rpc_timeout() -> None:
    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
        del url, body, headers, timeout
        raise TimeoutError("https://user:supersecret@rpc.example")

    with pytest.raises(ToolFailedError) as exc:
        read_transaction(_endpoint(SECRET_URL), SIGNATURE, transport=transport, clock=_clock)
    assert exc.value.code == "RPC_TIMEOUT"
    assert exc.value.retryable is True
    assert "supersecret" not in str(exc.value)


def test_rate_limit() -> None:
    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
        del url, body, headers, timeout
        raise urllib_error.HTTPError(
            SECRET_URL,
            429,
            "Too Many Requests",
            HTTPMessage(),
            io.BytesIO(b"supersecret"),
        )

    with pytest.raises(ToolFailedError) as exc:
        read_transaction(_endpoint(SECRET_URL), SIGNATURE, transport=transport, clock=_clock)
    assert exc.value.code == "RATE_LIMITED"
    assert exc.value.retryable is True
    assert "supersecret" not in str(exc.value)
    limited = json.dumps(
        {"jsonrpc": "2.0", "error": {"code": -32005, "message": "rate limit"}, "id": "1"}
    ).encode()
    with pytest.raises(ToolFailedError) as exc:
        read_transaction(_endpoint(), SIGNATURE, transport=_returning(limited), clock=_clock)
    assert exc.value.code == "RATE_LIMITED"


def test_arbitrary_rpc_method_is_rejected() -> None:
    assert ENABLED_RPC_METHODS == frozenset({"getTransaction", "getAccountInfo"})
    called = False

    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
        del url, body, headers, timeout
        nonlocal called
        called = True
        return b""

    for method in ("sendTransaction", "simulateTransaction", "requestAirdrop", "getBalance"):
        with pytest.raises(ToolFailedError) as exc:
            execute_read(_endpoint(), method, [], transport=transport)
        assert exc.value.code == "FORBIDDEN_METHOD"
    assert called is False


def test_arbitrary_rpc_url_is_rejected() -> None:
    called = False

    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
        del url, body, headers, timeout
        nonlocal called
        called = True
        return b""

    with pytest.raises(ToolFailedError) as exc:
        execute_read(
            _endpoint(),
            "getTransaction",
            [SIGNATURE],
            transport=transport,
            url="https://attacker.example",
        )
    assert exc.value.code == "FORBIDDEN_URL"
    with pytest.raises(ToolFailedError) as exc:
        execute_read(
            SolanaEndpoint("mainnet-beta", "http://rpc.example", "finalized", 8),
            "getTransaction",
            [SIGNATURE],
            transport=transport,
        )
    assert exc.value.code == "FORBIDDEN_URL"
    assert called is False
    with pytest.raises(ValidationError):
        GetSolanaTransactionInput.model_validate(
            {
                "signature": SIGNATURE,
                "cluster_ref": "mainnet-beta",
                "rpc_url": "https://attacker.example",
            }
        )
    with pytest.raises(ValidationError):
        GetSolanaAccountInput.model_validate(
            {"address": TOKEN, "cluster_ref": "mainnet-beta", "rpc_url": "https://attacker.example"}
        )


def test_account_normalization_is_current_state() -> None:
    import base64

    result = read_account(
        _endpoint(),
        TOKEN,
        transport=_returning(rpc_body(account_result())),
        clock=_clock,
    )
    view = SolanaAccountView.model_validate(result.output)
    assert view.address == TOKEN
    assert view.owner_program == TOKEN
    assert view.lamports == 2_039_280
    assert view.executable is False
    assert view.data_encoding == "base64"
    assert view.data_length == len(ACCOUNT_BYTES)
    assert view.context_slot == 99
    assert view.observed_at == CLOCK
    assert view.snapshot_kind == "current_account"
    assert view.historical_state is False
    observation = result.observations[0]
    assert observation.kind == "solana.account"
    assert observation.event_time is None
    assert observation.time_basis == "current_snapshot"
    assert observation.payload["historical_state"] is False
    assert observation.payload["decoded"] is None
    assert observation.provenance["historical_state"] is False
    assert observation.provenance["snapshot_kind"] == "current_account"
    rendered = json.dumps(observation.payload)
    assert base64.b64encode(ACCOUNT_BYTES).decode() not in rendered


def test_decoder_must_be_installed_and_configured() -> None:
    class Marker:
        decoder_id = "marker"
        version = "v1"
        program_id = TOKEN

        def decode(self, data: bytes) -> dict:
            return {"byte_length": len(data), "historical_state": True}

    binding = DecoderBinding(decoder_id="marker", version="v1", program_id=TOKEN)
    with pytest.raises(ConfigError, match="not installed"):
        require_installed_decoders((binding,), ())
    with pytest.raises(ConfigError, match="not installed"):
        build_settings(
            environment="development",
            database_url="postgresql://forwardops_app:forwardops_app@localhost/unused",
            migration_database_url="postgresql://forwardops:forwardops@localhost/unused",
            migrations_dir=_settings().migrations_dir,
            customer_path=_settings().migrations_dir.parent / "examples/customer-a/config.yaml",
            identities_path=_settings().migrations_dir.parent
            / "examples/customer-a/dev-identities.yaml",
            solana_decoders=(binding,),
        )
    assert select_account_decoder(SYSTEM, (binding,), (Marker(),)) is None
    result = read_account(
        _endpoint(),
        TOKEN,
        transport=_returning(rpc_body(account_result())),
        clock=_clock,
        account_bindings=(binding,),
        installed_account_decoders=(Marker(),),
    )
    payload = result.observations[0].payload
    assert payload["historical_state"] is False
    assert payload["decoded"]["fields"] == {"byte_length": len(ACCOUNT_BYTES)}
    with pytest.raises(ValidationError):
        GetSolanaAccountInput.model_validate(
            {"address": TOKEN, "cluster_ref": "mainnet-beta", "decoder": "marker"}
        )


def test_logs_redact_credentials_and_skip_payloads(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="forwardops.integrations.solana")
    with bind_rpc_log(investigation_id="inv-1", request_id="req-1"):
        read_transaction(
            _endpoint(SECRET_URL),
            SIGNATURE,
            transport=_returning(rpc_body(transaction_result())),
            clock=_clock,
        )
    rendered = _JsonFormatter().format(caplog.records[-1])
    assert "supersecret" not in rendered
    assert "api-key" not in rendered
    assert "secret-token" not in rendered
    assert "unique-marker-not-for-logs" not in rendered
    payload = json.loads(rendered)
    assert payload["cluster"] == "mainnet-beta"
    assert payload["rpc_operation"] == "getTransaction"
    assert payload["result_category"] == "succeeded"
    assert payload["investigation_id"] == "inv-1"
    assert payload["request_id"] == "req-1"
    assert isinstance(payload["duration_ms"], int)
    assert "supersecret" not in redact_rpc_url(SECRET_URL)
    assert redact_rpc_url(SECRET_URL) == "https://rpc.example/"


async def test_replay_cluster_does_not_call_solana() -> None:
    called = False

    def transport(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
        del url, body, headers, timeout
        nonlocal called
        called = True
        raise AssertionError("replay must not call Solana")

    settings = _settings()
    scope = _scope(settings)
    routing = RoutingHandlers(
        ReplayHandlers(
            FixtureSource.load(settings.fixture_dir), load_runbooks(settings.runbook_dir)
        ),
        SolanaHandlers((_endpoint(),), transport=transport),
    )
    result = await routing.get_solana_transaction(
        scope,
        GetSolanaTransactionInput(signature=SIGNATURE, cluster_ref="fixture"),
    )
    assert called is False
    assert result.output["decoded_failure"]["error_name"] == "StaleOracle"
    assert result.observations[0].kind == "solana.program_failure"
    assert result.observations[0].provenance["synthetic"] is True
    with pytest.raises(ToolFailedError) as exc:
        await routing.get_solana_account(
            scope,
            GetSolanaAccountInput(address=TOKEN, cluster_ref="fixture"),
        )
    assert exc.value.code == "FORBIDDEN_RESOURCE"
    assert called is False


def test_solana_configuration_uses_logical_clusters() -> None:
    mainnet = configured_solana_clusters(
        cluster="mainnet-beta",
        rpc_url="https://rpc.example",
        commitment="finalized",
        customer_cluster_ref="fixture",
    )
    devnet = configured_solana_clusters(
        cluster="devnet",
        rpc_url="https://rpc.example/devnet",
        commitment="confirmed",
        customer_cluster_ref="fixture",
    )
    custom = configured_solana_clusters(
        cluster="provider-mainnet",
        rpc_url=SECRET_URL,
        commitment="finalized",
        customer_cluster_ref="fixture",
    )
    assert [item.cluster_id for item in mainnet] == ["mainnet-beta"]
    assert devnet[0].commitment == "confirmed"
    assert custom[0].cluster_id == "provider-mainnet"
    assert "supersecret" not in repr(custom[0])
    settings = build_settings(
        environment="development",
        database_url="postgresql://forwardops_app:forwardops_app@localhost/unused",
        migration_database_url="postgresql://forwardops:forwardops@localhost/unused",
        migrations_dir=_settings().migrations_dir,
        customer_path=_settings().migrations_dir.parent / "examples/customer-a/config.yaml",
        identities_path=_settings().migrations_dir.parent
        / "examples/customer-a/dev-identities.yaml",
        solana_cluster="mainnet-beta",
        solana_rpc_url=SECRET_URL,
    )
    assert settings.solana_clusters[0].cluster_id == "mainnet-beta"
    assert "supersecret" not in repr(settings)
    with pytest.raises(ConfigError, match="both be set"):
        configured_solana_clusters(
            cluster="mainnet-beta",
            rpc_url=None,
            commitment="finalized",
            customer_cluster_ref="fixture",
        )
    with pytest.raises(ConfigError, match="replay cluster_ref"):
        configured_solana_clusters(
            cluster="fixture",
            rpc_url="https://rpc.example",
            commitment="finalized",
            customer_cluster_ref="fixture",
        )
    with pytest.raises(ConfigError, match="https"):
        configured_solana_clusters(
            cluster="https://evil.example",
            rpc_url="https://rpc.example",
            commitment="finalized",
            customer_cluster_ref="fixture",
        )
    with pytest.raises(ConfigError, match="https"):
        configured_solana_clusters(
            cluster="mainnet-beta",
            rpc_url="http://rpc.example",
            commitment="finalized",
            customer_cluster_ref="fixture",
        )
