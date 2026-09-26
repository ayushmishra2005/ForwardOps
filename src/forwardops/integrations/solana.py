"""Read-only Solana JSON-RPC adapter.

The endpoint, commitment, and any decoder come from ForwardOps configuration.
Tool arguments cannot select a URL, a JSON-RPC method, or a decoder.
"""

import asyncio
import base64
import json
import logging
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

from pydantic import ValidationError

from forwardops.config import DecoderBinding
from forwardops.domain.errors import ConfigError, ToolFailedError
from forwardops.domain.investigation import InvestigationScope
from forwardops.domain.time import format_utc
from forwardops.integrations.replay import HandlerResult, Observation
from forwardops.tools.contracts import (
    GetSolanaAccountInput,
    GetSolanaTransactionInput,
    SolanaAccountView,
    TransactionView,
)
from forwardops.tools.registry import dump_model

logger = logging.getLogger(__name__)

ENABLED_RPC_METHODS = frozenset({"getTransaction", "getAccountInfo"})
_MAX_RESPONSE_BYTES = 1_048_576
_MAX_LOG_LINES = 20
_MAX_LOG_CHARS = 180
_MAX_ERROR_CHARS = 2_000
_MAX_DECODED_CHARS = 8_192
_COMMITMENTS = frozenset({"processed", "confirmed", "finalized"})
_SAFE_METHOD = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,63}$")

_MALFORMED = "Solana RPC response was malformed"
_UNSUPPORTED = "The transaction version is not supported"
_FORBIDDEN_METHOD = "only getTransaction and getAccountInfo are enabled"
_FORBIDDEN_URL = "the Solana RPC endpoint is configured by ForwardOps and is not a tool argument"
_NOT_FOUND_TX = (
    "Solana RPC returned no transaction for this signature at the configured commitment. "
    "This does not establish that the transaction never existed."
)
_NOT_FOUND_ACCOUNT = (
    "Solana RPC returned no account for this address at the configured commitment. "
    "This does not establish that the account never existed, and it is not historical state."
)

Transport = Callable[[str, bytes, dict[str, str], float], bytes]
Clock = Callable[[], datetime]


class AccountDecoder(Protocol):
    """Trusted account decoder installed with ForwardOps, not supplied by a model."""

    decoder_id: str
    version: str
    program_id: str

    def decode(self, data: bytes) -> dict[str, Any]: ...


class TransactionDecoder(Protocol):
    """Trusted program decoder installed with ForwardOps, not supplied by a model."""

    decoder_id: str
    version: str
    program_id: str

    def decode(self, transaction: dict[str, Any]) -> dict[str, Any]: ...


# This build does not install a vault, oracle, or other program decoder.
INSTALLED_ACCOUNT_DECODERS: tuple[AccountDecoder, ...] = ()
INSTALLED_TRANSACTION_DECODERS: tuple[TransactionDecoder, ...] = ()


@dataclass(frozen=True)
class RpcLogContext:
    investigation_id: str
    request_id: str


_rpc_log_context: ContextVar[RpcLogContext | None] = ContextVar(
    "forwardops_solana_rpc_log", default=None
)


@contextmanager
def bind_rpc_log(*, investigation_id: str, request_id: str) -> Iterator[None]:
    token = _rpc_log_context.set(RpcLogContext(investigation_id, request_id))
    try:
        yield
    finally:
        _rpc_log_context.reset(token)


class RpcTransportError(Exception):
    def __init__(self, code: str, *, retryable: bool) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True)
class SolanaEndpoint:
    cluster_id: str
    rpc_url: str
    commitment: str
    timeout_seconds: float

    def __repr__(self) -> str:
        return f"SolanaEndpoint(cluster_id={self.cluster_id!r}, commitment={self.commitment!r})"


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str):
        return None


_OPENER = urllib.request.build_opener(_RejectRedirect)


def redact_rpc_url(url: str) -> str:
    """Return scheme and host only. Credentials, path tokens, and queries are removed."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "redacted"
    host = parts.hostname
    if parts.scheme not in {"https", "http"} or not host:
        return "redacted"
    netloc = f"{host}:{parts.port}" if parts.port else host
    return urlunsplit((parts.scheme, netloc, "/", "", ""))


def classify_http_status(status: int) -> tuple[str, bool]:
    if status == 429:
        return "RATE_LIMITED", True
    if status == 408:
        return "RPC_TIMEOUT", True
    if status >= 500:
        return "RPC_HTTP", True
    return "RPC_HTTP", False


def require_installed_decoders(
    bindings: tuple[DecoderBinding, ...], installed: tuple[Any, ...]
) -> None:
    programs = [item.program_id for item in bindings]
    if len(programs) != len(set(programs)):
        raise ConfigError("duplicate Solana decoder for one program")
    for binding in bindings:
        if not any(_binding_matches(binding, decoder) for decoder in installed):
            raise ConfigError(
                f"Solana decoder {binding.decoder_id} {binding.version} is not installed"
            )


def select_account_decoder(
    owner_program: str,
    bindings: tuple[DecoderBinding, ...],
    installed: tuple[Any, ...],
) -> AccountDecoder | None:
    return _select_decoder(bindings, installed, matched=lambda program: program == owner_program)


def select_transaction_decoder(
    program_ids: list[str],
    bindings: tuple[DecoderBinding, ...],
    installed: tuple[Any, ...],
) -> TransactionDecoder | None:
    return _select_decoder(bindings, installed, matched=lambda program: program in program_ids)


def post_rpc(url: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
    _require_https(url)
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            raw = response.read(_MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        code, retryable = classify_http_status(exc.code if isinstance(exc.code, int) else 0)
        raise RpcTransportError(code, retryable=retryable) from None
    except TimeoutError:
        raise RpcTransportError("RPC_TIMEOUT", retryable=True) from None
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            raise RpcTransportError("RPC_TIMEOUT", retryable=True) from None
        raise RpcTransportError("RPC_HTTP", retryable=True) from None
    except OSError:
        raise RpcTransportError("RPC_HTTP", retryable=True) from None
    if len(raw) > _MAX_RESPONSE_BYTES:
        raise ToolFailedError("RESPONSE_TOO_LARGE", "Solana RPC response exceeded the read limit")
    return raw


def execute_read(
    endpoint: SolanaEndpoint,
    method: str,
    params: list[Any],
    *,
    transport: Transport,
    url: str | None = None,
    log: bool = True,
) -> Any:
    started = time.perf_counter()
    category = "rpc_error"
    try:
        if url is not None:
            category = "forbidden_url"
            raise ToolFailedError("FORBIDDEN_URL", _FORBIDDEN_URL)
        if method not in ENABLED_RPC_METHODS:
            category = "forbidden_method"
            raise ToolFailedError("FORBIDDEN_METHOD", _FORBIDDEN_METHOD)
        _require_https(endpoint.rpc_url)
        if endpoint.commitment not in _COMMITMENTS:
            category = "forbidden_url"
            raise ToolFailedError("FORBIDDEN_URL", _FORBIDDEN_URL)
        body = json.dumps({"jsonrpc": "2.0", "id": "1", "method": method, "params": params}).encode(
            "utf-8"
        )
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        try:
            raw = transport(endpoint.rpc_url, body, headers, endpoint.timeout_seconds)
        except RpcTransportError as exc:
            raise _transport_failure(exc) from None
        except urllib.error.HTTPError as exc:
            code, retryable = classify_http_status(exc.code if isinstance(exc.code, int) else 0)
            raise ToolFailedError(code, _transport_message(code), retryable=retryable) from None
        except TimeoutError:
            raise ToolFailedError("RPC_TIMEOUT", "Solana RPC timed out", retryable=True) from None
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, TimeoutError):
                raise ToolFailedError(
                    "RPC_TIMEOUT", "Solana RPC timed out", retryable=True
                ) from None
            raise ToolFailedError("RPC_HTTP", "Solana RPC request failed", retryable=True) from None
        except OSError:
            raise ToolFailedError("RPC_HTTP", "Solana RPC request failed", retryable=True) from None
        if not isinstance(raw, bytes) or len(raw) > _MAX_RESPONSE_BYTES:
            raise ToolFailedError(
                "RESPONSE_TOO_LARGE", "Solana RPC response exceeded the read limit"
            )
        parsed = _parse_rpc_result(raw)
        category = "succeeded"
        return parsed
    except ToolFailedError as exc:
        category = exc.code.lower()
        raise
    finally:
        if log:
            _log_rpc(
                cluster=endpoint.cluster_id,
                operation=_safe_operation(method),
                category=category,
                started=started,
            )


def read_transaction(
    endpoint: SolanaEndpoint,
    signature: str,
    *,
    transport: Transport,
    clock: Clock,
    transaction_bindings: tuple[DecoderBinding, ...] = (),
    installed_transaction_decoders: tuple[Any, ...] = (),
) -> HandlerResult:
    started = time.perf_counter()
    category = "rpc_error"
    try:
        result = execute_read(
            endpoint,
            "getTransaction",
            [
                signature,
                {
                    "encoding": "json",
                    "commitment": endpoint.commitment,
                    "maxSupportedTransactionVersion": 0,
                },
            ],
            transport=transport,
            log=False,
        )
        if result is None:
            category = "not_found"
            raise ToolFailedError("NOT_FOUND", _NOT_FOUND_TX)
        observed_at = _aware(clock())
        view, observation, _ = _normalize_transaction(
            result,
            signature=signature,
            endpoint=endpoint,
            observed_at=observed_at,
            transaction_bindings=transaction_bindings,
            installed_transaction_decoders=installed_transaction_decoders,
        )
        category = "succeeded"
        return HandlerResult(dump_model(view), (observation,))
    except ToolFailedError as exc:
        category = exc.code.lower()
        raise
    finally:
        _log_rpc(
            cluster=endpoint.cluster_id,
            operation="getTransaction",
            category=category,
            started=started,
        )


def read_account(
    endpoint: SolanaEndpoint,
    address: str,
    *,
    transport: Transport,
    clock: Clock,
    account_bindings: tuple[DecoderBinding, ...] = (),
    installed_account_decoders: tuple[Any, ...] = (),
) -> HandlerResult:
    started = time.perf_counter()
    category = "rpc_error"
    try:
        result = execute_read(
            endpoint,
            "getAccountInfo",
            [address, {"encoding": "base64", "commitment": endpoint.commitment}],
            transport=transport,
            log=False,
        )
        if result is None:
            category = "not_found"
            raise ToolFailedError("NOT_FOUND", _NOT_FOUND_ACCOUNT)
        observed_at = _aware(clock())
        view, observation = _normalize_account(
            result,
            address=address,
            endpoint=endpoint,
            observed_at=observed_at,
            account_bindings=account_bindings,
            installed_account_decoders=installed_account_decoders,
        )
        category = "succeeded"
        return HandlerResult(dump_model(view), (observation,))
    except ToolFailedError as exc:
        category = exc.code.lower()
        raise
    finally:
        _log_rpc(
            cluster=endpoint.cluster_id,
            operation="getAccountInfo",
            category=category,
            started=started,
        )


class SolanaHandlers:
    """Configured clusters only. Replay clusters are not served here."""

    def __init__(
        self,
        endpoints: tuple[SolanaEndpoint, ...],
        *,
        transport: Transport | None = None,
        clock: Clock | None = None,
        account_decoder_bindings: tuple[DecoderBinding, ...] = (),
        installed_account_decoders: tuple[Any, ...] = INSTALLED_ACCOUNT_DECODERS,
        transaction_decoder_bindings: tuple[DecoderBinding, ...] = (),
        installed_transaction_decoders: tuple[Any, ...] = INSTALLED_TRANSACTION_DECODERS,
    ) -> None:
        if not endpoints:
            raise ConfigError("Solana RPC configuration is empty")
        require_installed_decoders(account_decoder_bindings, installed_account_decoders)
        require_installed_decoders(transaction_decoder_bindings, installed_transaction_decoders)
        self.endpoints = {item.cluster_id: item for item in endpoints}
        self.transport = transport or post_rpc
        self.clock = clock or _now
        self.account_decoder_bindings = account_decoder_bindings
        self.installed_account_decoders = installed_account_decoders
        self.transaction_decoder_bindings = transaction_decoder_bindings
        self.installed_transaction_decoders = installed_transaction_decoders

    def serves(self, cluster_ref: str) -> bool:
        return cluster_ref in self.endpoints

    async def get_solana_transaction(
        self,
        scope: InvestigationScope,
        arguments: GetSolanaTransactionInput,
    ) -> HandlerResult:
        endpoint = self._endpoint(scope, arguments.cluster_ref)
        return await asyncio.to_thread(
            read_transaction,
            endpoint,
            arguments.signature,
            transport=self.transport,
            clock=self.clock,
            transaction_bindings=self.transaction_decoder_bindings,
            installed_transaction_decoders=self.installed_transaction_decoders,
        )

    async def get_solana_account(
        self,
        scope: InvestigationScope,
        arguments: GetSolanaAccountInput,
    ) -> HandlerResult:
        endpoint = self._endpoint(scope, arguments.cluster_ref)
        return await asyncio.to_thread(
            read_account,
            endpoint,
            arguments.address,
            transport=self.transport,
            clock=self.clock,
            account_bindings=self.account_decoder_bindings,
            installed_account_decoders=self.installed_account_decoders,
        )

    def _endpoint(self, scope: InvestigationScope, cluster_ref: str) -> SolanaEndpoint:
        if cluster_ref != scope.cluster_ref or cluster_ref not in self.endpoints:
            raise ToolFailedError(
                "FORBIDDEN_RESOURCE",
                "no Solana RPC source is configured for this cluster",
            )
        return self.endpoints[cluster_ref]


def _normalize_transaction(
    result: Any,
    *,
    signature: str,
    endpoint: SolanaEndpoint,
    observed_at: datetime,
    transaction_bindings: tuple[DecoderBinding, ...],
    installed_transaction_decoders: tuple[Any, ...],
) -> tuple[TransactionView, Observation, bool]:
    if not isinstance(result, dict):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    _require_supported_version(result)
    slot = _non_negative_int(result.get("slot"))
    block_time = _block_time(result.get("blockTime")) if "blockTime" in result else None
    transaction = result.get("transaction")
    meta = result.get("meta")
    if not isinstance(transaction, dict) or not isinstance(meta, dict):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    signatures = transaction.get("signatures")
    if isinstance(signatures, list) and signatures and signatures[0] != signature:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    message = transaction.get("message")
    if not isinstance(message, dict):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    program_ids = _program_ids(message, meta)
    logs, truncated = _logs(meta.get("logMessages"))
    try:
        view = TransactionView(
            signature=signature,
            cluster_ref=endpoint.cluster_id,
            slot=slot,
            block_time=block_time,
            commitment=endpoint.commitment,
            program_ids=program_ids,
            status="failed" if meta.get("err") is not None else "success",
            instruction_errors=_instruction_errors(meta.get("err")),
            decoded_failure=None,
            relevant_logs=logs,
            observed_at=observed_at,
        )
    except ValidationError:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED) from None
    decoded = _transaction_decoded(
        result, program_ids, transaction_bindings, installed_transaction_decoders
    )
    payload = dump_model(view)
    payload["decoded"] = decoded
    payload["current_account_state"] = False
    observation = Observation(
        kind="solana.transaction",
        source_type="blockchain",
        source_system="solana-rpc",
        source_locator={
            "cluster": endpoint.cluster_id,
            "signature": signature,
            "commitment": endpoint.commitment,
        },
        event_time=format_utc(block_time) if block_time else None,
        time_basis="block_time" if block_time else "unknown_block_time",
        correlation={"signature": signature, "cluster": endpoint.cluster_id},
        payload=payload,
        summary=(
            f"Solana transaction {signature} on {endpoint.cluster_id} "
            f"at slot {slot} is {view.status}."
        ),
        provenance={
            "synthetic": False,
            "source": "solana-rpc",
            "cluster": endpoint.cluster_id,
            "slot": slot,
            "commitment": endpoint.commitment,
            "retrieved_at": format_utc(observed_at),
            "chain_record": True,
            "current_account_state": False,
            "decoder": None
            if decoded is None
            else {"decoder_id": decoded["decoder_id"], "version": decoded["version"]},
        },
        coverage={"complete_for_record": not truncated, "truncated": truncated},
        retrieval_time=format_utc(observed_at),
    )
    return view, observation, truncated


def _normalize_account(
    result: Any,
    *,
    address: str,
    endpoint: SolanaEndpoint,
    observed_at: datetime,
    account_bindings: tuple[DecoderBinding, ...],
    installed_account_decoders: tuple[Any, ...],
) -> tuple[SolanaAccountView, Observation]:
    if not isinstance(result, dict):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    if "value" not in result or result["value"] is None:
        raise ToolFailedError("NOT_FOUND", _NOT_FOUND_ACCOUNT)
    value = result["value"]
    context = result.get("context")
    if not isinstance(value, dict) or not isinstance(context, dict):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    if not isinstance(value.get("executable"), bool):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    data_length, raw = _account_data(value.get("data"))
    owner = value.get("owner")
    if not isinstance(owner, str):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    try:
        decoder = select_account_decoder(owner, account_bindings, installed_account_decoders)
    except ConfigError:
        raise ToolFailedError(
            "INVALID_OUTPUT", "configured account decoder is not installed"
        ) from None
    decoded = _account_decoded(decoder, raw) if decoder is not None else None
    try:
        view = SolanaAccountView(
            address=address,
            cluster_ref=endpoint.cluster_id,
            owner_program=owner,
            lamports=_non_negative_int(value.get("lamports")),
            executable=value["executable"],
            data_encoding="base64",
            data_length=data_length,
            context_slot=_non_negative_int(context.get("slot")),
            commitment=_commitment(endpoint.commitment),
            observed_at=observed_at,
        )
    except (ValidationError, ToolFailedError):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED) from None
    payload = dump_model(view)
    payload["decoded"] = decoded
    decoder_meta = (
        None if decoder is None else {"decoder_id": decoder.decoder_id, "version": decoder.version}
    )
    observation = Observation(
        kind="solana.account",
        source_type="blockchain",
        source_system="solana-rpc",
        source_locator={
            "cluster": endpoint.cluster_id,
            "address": address,
            "commitment": endpoint.commitment,
        },
        event_time=None,
        time_basis="current_snapshot",
        correlation={"address": address, "cluster": endpoint.cluster_id, "owner_program": owner},
        payload=payload,
        summary=(
            f"Current account {address} on {endpoint.cluster_id} at slot {view.context_slot}. "
            "This snapshot is not historical state."
        ),
        provenance={
            "synthetic": False,
            "source": "solana-rpc",
            "cluster": endpoint.cluster_id,
            "commitment": endpoint.commitment,
            "context_slot": view.context_slot,
            "retrieved_at": format_utc(observed_at),
            "snapshot_kind": "current_account",
            "historical_state": False,
            "account_data_retained": False,
            "decoder": decoder_meta,
        },
        coverage={"complete_for_record": True, "truncated": False},
        retrieval_time=format_utc(observed_at),
    )
    return view, observation


def _transaction_decoded(
    result: dict[str, Any],
    program_ids: list[str],
    bindings: tuple[DecoderBinding, ...],
    installed: tuple[Any, ...],
) -> dict[str, Any] | None:
    try:
        decoder = select_transaction_decoder(program_ids, bindings, installed)
    except ConfigError:
        raise ToolFailedError(
            "INVALID_OUTPUT", "configured transaction decoder is not installed"
        ) from None
    if decoder is None:
        return None
    try:
        decoded = decoder.decode(result)
    except Exception:
        raise ToolFailedError("INVALID_OUTPUT", "configured transaction decoder failed") from None
    return {
        "decoder_id": decoder.decoder_id,
        "version": decoder.version,
        "fields": _accept_decoded(decoded),
    }


def _account_decoded(decoder: AccountDecoder, data: bytes) -> dict[str, Any]:
    try:
        decoded = decoder.decode(data)
    except Exception:
        raise ToolFailedError("INVALID_OUTPUT", "configured account decoder failed") from None
    return {
        "decoder_id": decoder.decoder_id,
        "version": decoder.version,
        "fields": _accept_decoded(decoded),
    }


def _accept_decoded(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ToolFailedError("INVALID_OUTPUT", "configured decoder output was rejected")
    cleaned = {
        key: item
        for key, item in value.items()
        if key not in {"historical_state", "snapshot_kind", "decoded_failure"}
    }
    try:
        encoded = json.dumps(cleaned)
    except TypeError:
        raise ToolFailedError("INVALID_OUTPUT", "configured decoder output was rejected") from None
    if len(encoded) > _MAX_DECODED_CHARS:
        raise ToolFailedError("INVALID_OUTPUT", "configured decoder output was rejected")
    return cleaned


def _select_decoder(
    bindings: tuple[DecoderBinding, ...],
    installed: tuple[Any, ...],
    *,
    matched: Callable[[str], bool],
) -> Any | None:
    for binding in bindings:
        if not matched(binding.program_id):
            continue
        for decoder in installed:
            if _binding_matches(binding, decoder):
                return decoder
        raise ConfigError(f"Solana decoder {binding.decoder_id} {binding.version} is not installed")
    return None


def _binding_matches(binding: DecoderBinding, decoder: Any) -> bool:
    return (
        getattr(decoder, "decoder_id", None) == binding.decoder_id
        and getattr(decoder, "version", None) == binding.version
        and getattr(decoder, "program_id", None) == binding.program_id
    )


def _parse_rpc_result(raw: bytes) -> Any:
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED) from None
    if not isinstance(parsed, dict):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    error = parsed.get("error")
    if error is not None:
        raise _rpc_error(error)
    if "result" not in parsed:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    return parsed["result"]


def _rpc_error(error: Any) -> ToolFailedError:
    if not isinstance(error, dict):
        return ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    code = error.get("code")
    message = error.get("message")
    text = message.lower() if isinstance(message, str) else ""
    if code == -32015 or ("version" in text and "not supported" in text):
        return ToolFailedError("UNSUPPORTED_VERSION", _UNSUPPORTED)
    if code in {-32005, 429} or "too many requests" in text or "rate limit" in text:
        return ToolFailedError("RATE_LIMITED", "Solana RPC rate limited the read", retryable=True)
    if isinstance(code, int) and not isinstance(code, bool):
        return ToolFailedError("RPC_ERROR", f"Solana RPC returned error {code}")
    return ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)


def _require_supported_version(result: dict[str, Any]) -> None:
    if "version" not in result:
        return
    version = result["version"]
    if version in {"legacy", 0, "0"}:
        return
    if isinstance(version, bool) or version is None or isinstance(version, dict | list):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    if isinstance(version, int | str):
        raise ToolFailedError("UNSUPPORTED_VERSION", _UNSUPPORTED)
    raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)


def _program_ids(message: dict[str, Any], meta: dict[str, Any]) -> list[str]:
    keys = [_account_key(item) for item in _account_key_list(message)]
    loaded = meta.get("loadedAddresses") or {}
    if loaded is None:
        loaded = {}
    if not isinstance(loaded, dict):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    for name in ("writable", "readonly"):
        extra = loaded.get(name) or []
        if not isinstance(extra, list):
            raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
        keys.extend(_account_key(item) for item in extra)
    instructions = message.get("instructions")
    if not isinstance(instructions, list):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    found: list[str] = []
    for instruction in _iter_instructions(instructions, meta):
        program_id = instruction.get("programId")
        if isinstance(program_id, str):
            found.append(program_id)
            continue
        index = instruction.get("programIdIndex")
        if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < len(keys):
            raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
        found.append(keys[index])
    unique: list[str] = []
    for item in found:
        if item not in unique:
            unique.append(item)
    return unique


def _account_key_list(message: dict[str, Any]) -> list[Any]:
    keys = message.get("accountKeys")
    if not isinstance(keys, list):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    return keys


def _account_key(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and isinstance(value.get("pubkey"), str):
        return value["pubkey"]
    raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)


def _iter_instructions(instructions: list[Any], meta: dict[str, Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for item in instructions:
        if not isinstance(item, dict):
            raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
        found.append(item)
    inner = meta.get("innerInstructions") or []
    if inner is None:
        return found
    if not isinstance(inner, list):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    for group in inner:
        if not isinstance(group, dict):
            raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
        nested = group.get("instructions") or []
        if not isinstance(nested, list):
            raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
        for item in nested:
            if not isinstance(item, dict):
                raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
            found.append(item)
    return found


def _logs(value: Any) -> tuple[list[str], bool]:
    if value is None:
        return [], False
    if not isinstance(value, list):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    lines = [item.strip() for item in value if isinstance(item, str) and item.strip()]
    truncated = len(lines) > _MAX_LOG_LINES
    capped = [item[:_MAX_LOG_CHARS] for item in lines[:_MAX_LOG_LINES]]
    return capped, truncated


def _instruction_errors(err: Any) -> list[dict[str, Any]]:
    if err is None:
        return []
    if isinstance(err, str):
        parsed: list[dict[str, Any]] = [{"error": err[:200]}]
    elif isinstance(err, dict):
        instruction = err.get("InstructionError")
        if (
            isinstance(instruction, list)
            and len(instruction) == 2
            and isinstance(instruction[0], int)
            and not isinstance(instruction[0], bool)
        ):
            detail = instruction[1]
            item: dict[str, Any] = {"index": instruction[0]}
            if isinstance(detail, dict):
                item["error"] = detail
            elif isinstance(detail, str | int) and not isinstance(detail, bool):
                item["error"] = detail
            else:
                item["error"] = "unparsed"
            parsed = [item]
        else:
            parsed = [err]
    else:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    try:
        encoded = json.dumps(parsed)
    except TypeError:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED) from None
    if len(encoded) > _MAX_ERROR_CHARS:
        return [{"error": "truncated"}]
    return parsed


def _account_data(value: Any) -> tuple[int, bytes]:
    if (
        not isinstance(value, list)
        or len(value) != 2
        or not isinstance(value[0], str)
        or value[1] != "base64"
    ):
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    padded = value[0] + ("=" * (-len(value[0]) % 4))
    try:
        raw = base64.b64decode(padded, validate=True)
    except Exception:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED) from None
    return len(raw), raw


def _block_time(value: Any) -> datetime | None:
    if value is None:
        return None
    number = _non_negative_int(value)
    if number > 4_102_444_800:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    return datetime.fromtimestamp(number, UTC)


def _non_negative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    return value


def _commitment(value: str) -> Any:
    if value not in _COMMITMENTS:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    return value


def _require_https(url: str) -> None:
    if any(character in url for character in "\r\n\t "):
        raise ToolFailedError("FORBIDDEN_URL", _FORBIDDEN_URL)
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        raise ToolFailedError("FORBIDDEN_URL", _FORBIDDEN_URL)


def _transport_failure(exc: RpcTransportError) -> ToolFailedError:
    return ToolFailedError(exc.code, _transport_message(exc.code), retryable=exc.retryable)


def _transport_message(code: str) -> str:
    if code == "RATE_LIMITED":
        return "Solana RPC rate limited the read"
    if code == "RPC_TIMEOUT":
        return "Solana RPC timed out"
    if code == "RESPONSE_TOO_LARGE":
        return "Solana RPC response exceeded the read limit"
    return "Solana RPC request failed"


def _safe_operation(method: str) -> str:
    if method in ENABLED_RPC_METHODS or _SAFE_METHOD.fullmatch(method):
        return method
    return "rejected"


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ToolFailedError("MALFORMED_RESPONSE", _MALFORMED)
    return value.astimezone(UTC)


def _now() -> datetime:
    return datetime.now(UTC)


def _log_rpc(*, cluster: str, operation: str, category: str, started: float) -> None:
    context = _rpc_log_context.get()
    extra: dict[str, Any] = {
        "event": "solana_rpc",
        "cluster": cluster,
        "rpc_operation": operation,
        "duration_ms": max(0, int((time.perf_counter() - started) * 1000)),
        "result_category": category,
    }
    if context is not None:
        extra["investigation_id"] = context.investigation_id
        extra["request_id"] = context.request_id
    if category != "succeeded":
        extra["error_category"] = category
    logger.info("solana rpc finished", extra=extra)
