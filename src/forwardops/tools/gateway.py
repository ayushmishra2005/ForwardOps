import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4, uuid5

from pydantic import BaseModel, ValidationError

from forwardops.domain.errors import SuspendedError, ToolFailedError
from forwardops.domain.evidence import evidence_digest
from forwardops.domain.hashing import sha256_canonical, to_canonical
from forwardops.domain.investigation import DATABASE_POOL_SCENARIO, InvestigationScope
from forwardops.domain.time import parse_utc
from forwardops.integrations.customer_db import POOL_PLAYBOOK_TOOLS
from forwardops.integrations.replay import HandlerResult, ReplayHandlers
from forwardops.integrations.routing import RoutingHandlers
from forwardops.integrations.solana import bind_rpc_log
from forwardops.storage.leases import Claim
from forwardops.storage.postgres import (
    Database,
    abandon_started,
    complete_tool_call,
    count_succeeded_tools,
    db_now,
    evidence_ids_for_call,
    fail_tool_call,
    find_succeeded_call,
    insert_evidence,
    insert_tool_call,
    next_attempt,
    require_live_lease,
    succeeded_outputs,
)
from forwardops.tools.registry import ToolDefinition, tool_definitions

logger = logging.getLogger(__name__)
_MAX_BYTES = 65_536
_LOGICAL_ID = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_SECRET_KEYS = frozenset(
    {
        "connection_string",
        "database_url",
        "dsn",
        "host",
        "hostname",
        "password",
        "query",
        "secret",
        "sql",
        "statement",
        "url",
    }
)
_ORACLE_PLAYBOOK_TOOLS = frozenset(
    {
        "get_oracle_state",
        "get_recent_withdrawal_failures",
        "get_solana_account",
        "get_solana_transaction",
        "get_vault_state",
        "search_application_logs",
        "search_runbooks",
    }
)


@dataclass(frozen=True)
class ToolContext:
    tenant_id: str
    principal_id: str
    investigation_id: UUID
    scope: InvestigationScope
    request_id: str
    trace_id: str
    lease_epoch: int
    lease_owner: str
    frozen_observed_at: str
    max_tool_calls: int
    lease_seconds: int

    @property
    def claim(self) -> Claim:
        return Claim(self.tenant_id, self.investigation_id, self.lease_epoch, self.lease_owner)


@dataclass(frozen=True)
class ToolSuccess:
    tool_name: str
    tool_call_id: UUID
    output: dict[str, Any]
    evidence_ids: list[UUID]
    reused: bool


def enforce_tool_scope(parsed: BaseModel, scope: InvestigationScope) -> None:
    service_ref = getattr(parsed, "service_ref", None)
    vault_ref = getattr(parsed, "vault_ref", None)
    oracle_ref = getattr(parsed, "oracle_ref", None)
    cluster_ref = getattr(parsed, "cluster_ref", None)
    window = getattr(parsed, "window", None)
    if service_ref is not None and service_ref != scope.service_ref:
        raise ToolFailedError("FORBIDDEN_RESOURCE", "service is outside the investigation scope")
    if vault_ref is not None and vault_ref != scope.vault_ref:
        raise ToolFailedError("FORBIDDEN_RESOURCE", "vault is outside the investigation scope")
    if oracle_ref is not None and oracle_ref != scope.oracle_ref:
        raise ToolFailedError("FORBIDDEN_RESOURCE", "oracle is outside the investigation scope")
    if cluster_ref is not None and cluster_ref != scope.cluster_ref:
        raise ToolFailedError("FORBIDDEN_RESOURCE", "cluster is outside the investigation scope")
    source_ref = getattr(parsed, "source_ref", None)
    if source_ref is not None and source_ref != scope.database_source_ref:
        raise ToolFailedError(
            "FORBIDDEN_RESOURCE",
            "database source is outside the investigation scope",
        )
    if window is not None:
        start = parse_utc(scope.interval_start)
        end = parse_utc(scope.interval_end)
        if window.start != start or window.end != end:
            raise ToolFailedError("FORBIDDEN_RESOURCE", "window is outside the investigation scope")


def enforce_playbook_tool(name: str, scope: InvestigationScope) -> None:
    """Database tools are available only on the database playbook."""
    pool = scope.scenario_id == DATABASE_POOL_SCENARIO
    if pool and name in _ORACLE_PLAYBOOK_TOOLS:
        raise ToolFailedError("FORBIDDEN_RESOURCE", "tool is outside the selected playbook")
    if not pool and name in POOL_PLAYBOOK_TOOLS:
        raise ToolFailedError("FORBIDDEN_RESOURCE", "tool is outside the selected playbook")


class ToolGateway:
    """The only path from the playbook to a tool handler."""

    def __init__(
        self,
        database: Database,
        handlers: ReplayHandlers | RoutingHandlers,
        context: ToolContext,
        *,
        suspend_after_new_tool_calls: int | None = None,
    ) -> None:
        self.database = database
        self.handlers = handlers
        self.context = context
        self.suspend_after_new_tool_calls = suspend_after_new_tool_calls
        self.new_calls = 0
        self._definitions = tool_definitions()

    async def call(self, name: str, arguments: dict[str, Any]) -> ToolSuccess:
        definition = self._definitions.get(name)
        if definition is None:
            raise ToolFailedError("UNKNOWN_TOOL", f"tool {name} is not registered")
        try:
            parsed = definition.input_model.model_validate(arguments)
        except ValidationError as exc:
            await self._reject(
                definition, arguments, "UNSUPPORTED_SCHEMA", "invalid tool arguments", False
            )
            raise ToolFailedError("UNSUPPORTED_SCHEMA", "invalid tool arguments") from exc
        canonical_args = parsed.model_dump(mode="json")
        logical_id = uuid5(
            self.context.investigation_id,
            sha256_canonical({"tool": name, "arguments": canonical_args}),
        )
        try:
            self._enforce_scope(definition.name, parsed)
        except ToolFailedError as exc:
            await self._reject(
                definition, canonical_args, exc.code, str(exc), exc.retryable, logical_id
            )
            raise

        started: tuple[UUID, int] | None = None
        async with self.database.transaction(self.context.tenant_id) as conn:
            await require_live_lease(
                conn, self.context.claim, renew_seconds=self.context.lease_seconds
            )
            existing = await find_succeeded_call(
                conn,
                self.context.tenant_id,
                self.context.investigation_id,
                logical_id,
            )
            if existing is not None:
                evidence_ids = await evidence_ids_for_call(
                    conn,
                    self.context.tenant_id,
                    self.context.investigation_id,
                    existing["id"],
                )
                return ToolSuccess(name, existing["id"], existing["output"], evidence_ids, True)
            if await count_succeeded_tools(
                conn, self.context.tenant_id, self.context.investigation_id
            ) >= (self.context.max_tool_calls):
                rejection = ToolFailedError("BUDGET_EXCEEDED", "tool call budget is exhausted")
            else:
                rejection = None
                try:
                    await self._enforce_prerequisites(conn, name, parsed)
                except ToolFailedError as exc:
                    rejection = exc
            if rejection is not None:
                await self._insert_terminal(
                    conn,
                    definition,
                    canonical_args,
                    logical_id,
                    rejection.code,
                    str(rejection),
                    rejection.retryable,
                )
            else:
                await abandon_started(
                    conn,
                    self.context.tenant_id,
                    self.context.investigation_id,
                    logical_id,
                )
                attempt = await next_attempt(
                    conn,
                    self.context.tenant_id,
                    self.context.investigation_id,
                    logical_id,
                )
                tool_call_id = uuid4()
                await insert_tool_call(
                    conn,
                    tenant_id=self.context.tenant_id,
                    tool_call_id=tool_call_id,
                    investigation_id=self.context.investigation_id,
                    logical_call_id=logical_id,
                    attempt=attempt,
                    tool_name=definition.name,
                    tool_version=definition.version,
                    arguments=canonical_args,
                    arguments_digest=sha256_canonical(canonical_args),
                    deadline_at=datetime.now(UTC) + timedelta(seconds=8),
                    worker_epoch=self.context.lease_epoch,
                    source_id=self._source_id(definition, parsed),
                    trace_id=self.context.trace_id,
                )
                started = (tool_call_id, attempt)
        if rejection is not None:
            raise rejection
        assert started is not None
        tool_call_id = started[0]
        try:
            handled = await self._invoke(definition, parsed)
            output_model = definition.output_model.model_validate(handled.output)
            output = output_model.model_dump(mode="json")
        except ToolFailedError as exc:
            await self._fail_started(tool_call_id, exc)
            raise
        except (ValidationError, TypeError, ValueError) as exc:
            failed = ToolFailedError("INVALID_OUTPUT", "tool output failed validation")
            await self._fail_started(tool_call_id, failed)
            raise failed from exc

        evidence_ids = await self.publish_result(tool_call_id, output, tuple(handled.observations))
        self.new_calls += 1
        logger.info(
            "tool completed",
            extra={
                "investigation_id": str(self.context.investigation_id),
                "tool_name": name,
                "event": "tool_completed",
                "tenant_id": self.context.tenant_id,
            },
        )
        if (
            self.suspend_after_new_tool_calls is not None
            and self.new_calls >= self.suspend_after_new_tool_calls
        ):
            raise SuspendedError("worker suspended after a committed tool call")
        return ToolSuccess(name, tool_call_id, output, evidence_ids, False)

    async def publish_result(
        self,
        tool_call_id: UUID,
        output: dict[str, Any],
        observations: tuple[Any, ...],
    ) -> list[UUID]:
        """Persist tool evidence only while this worker still owns a live lease."""
        evidence_ids: list[UUID] = []
        async with self.database.transaction(self.context.tenant_id) as conn:
            await require_live_lease(
                conn, self.context.claim, renew_seconds=self.context.lease_seconds
            )
            base = await db_now(conn)
            frozen_observed_at = parse_utc(self.context.frozen_observed_at)
            for index, observation in enumerate(observations):
                evidence_id = uuid4()
                self._check_size(observation.payload)
                digest = evidence_digest(
                    schema_version=1,
                    source_type=observation.source_type,
                    source_system=observation.source_system,
                    source_locator=observation.source_locator,
                    payload=observation.payload,
                    provenance=observation.provenance,
                )
                await insert_evidence(
                    conn,
                    tenant_id=self.context.tenant_id,
                    evidence_id=evidence_id,
                    investigation_id=self.context.investigation_id,
                    tool_call_id=tool_call_id,
                    kind=observation.kind,
                    source_type=observation.source_type,
                    source_system=observation.source_system,
                    source_locator=observation.source_locator,
                    event_time=parse_utc(observation.event_time)
                    if observation.event_time
                    else None,
                    observed_at=parse_utc(observation.retrieval_time)
                    if observation.retrieval_time
                    else frozen_observed_at,
                    time_basis=observation.time_basis,
                    correlation=observation.correlation,
                    payload=observation.payload,
                    summary=observation.summary,
                    provenance=observation.provenance,
                    coverage=observation.coverage,
                    payload_sha256=digest,
                    created_at=base + timedelta(microseconds=index),
                )
                evidence_ids.append(evidence_id)
            await complete_tool_call(
                conn,
                tenant_id=self.context.tenant_id,
                tool_call_id=tool_call_id,
                output=output,
                output_digest=sha256_canonical(output),
            )
        return evidence_ids

    async def _invoke(self, definition: ToolDefinition, parsed: BaseModel) -> HandlerResult:
        method = getattr(self.handlers, definition.name)
        with bind_rpc_log(
            investigation_id=str(self.context.investigation_id),
            request_id=self.context.request_id,
        ):
            return await method(self.context.scope, parsed)

    def _source_id(self, definition: ToolDefinition, parsed: BaseModel) -> str:
        resolver = getattr(self.handlers, "source_id_for", None)
        if resolver is not None:
            chosen = resolver(definition.name, parsed)
            if isinstance(chosen, str) and chosen:
                return chosen
        return definition.source_id

    def _serves_live_solana(self, parsed: BaseModel) -> bool:
        cluster = getattr(parsed, "cluster_ref", None)
        serves = getattr(self.handlers, "serves_solana_cluster", None)
        if not isinstance(cluster, str) or serves is None:
            return False
        return bool(serves(cluster))

    def _enforce_scope(self, name: str, parsed: BaseModel) -> None:
        enforce_playbook_tool(name, self.context.scope)
        enforce_tool_scope(parsed, self.context.scope)

    async def _enforce_prerequisites(self, conn: Any, name: str, parsed: BaseModel) -> None:
        tenant_id = self.context.tenant_id
        investigation_id = self.context.investigation_id
        if name == "get_solana_transaction":
            if self._serves_live_solana(parsed):
                return
            summaries = await succeeded_outputs(
                conn, tenant_id, investigation_id, "get_recent_withdrawal_failures"
            )
            if not summaries:
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "withdrawal failures must be collected first"
                )
            allowed = {
                sample["signature"] for summary in summaries for sample in summary["samples"]
            }
            if parsed.signature not in allowed:
                raise ToolFailedError(
                    "FORBIDDEN_RESOURCE", "signature is not in the scoped failure sample"
                )
        elif name == "get_solana_account":
            if not self._serves_live_solana(parsed):
                raise ToolFailedError(
                    "FORBIDDEN_RESOURCE",
                    "account reads require a configured Solana cluster in the investigation scope",
                )
        elif name == "search_application_logs":
            transactions = await succeeded_outputs(
                conn, tenant_id, investigation_id, "get_solana_transaction"
            )
            signatures = {item["signature"] for item in transactions}
            if parsed.signature not in signatures:
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "the transaction must be read before its logs"
                )
        elif name == "get_vault_state":
            if not await succeeded_outputs(
                conn, tenant_id, investigation_id, "get_solana_transaction"
            ):
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "a transaction must be read before vault state"
                )
        elif name == "get_oracle_state":
            vaults = await succeeded_outputs(conn, tenant_id, investigation_id, "get_vault_state")
            if not vaults or vaults[-1]["oracle_ref"] != parsed.oracle_ref:
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "oracle reads require the vault binding"
                )
        elif name == "search_runbooks":
            if not await succeeded_outputs(conn, tenant_id, investigation_id, "get_oracle_state"):
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "runbook search follows oracle collection"
                )
        elif name == "get_recent_database_errors":
            if not await succeeded_outputs(
                conn, tenant_id, investigation_id, "get_service_request_summary"
            ):
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "request summary must be collected first"
                )
        elif name == "get_database_pool_snapshot":
            if not await succeeded_outputs(
                conn, tenant_id, investigation_id, "get_recent_database_errors"
            ):
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "database errors must be collected first"
                )
        elif name == "search_service_logs":
            reports = await succeeded_outputs(
                conn, tenant_id, investigation_id, "get_recent_database_errors"
            )
            if not reports:
                raise ToolFailedError(
                    "PREREQUISITE_FAILED", "database errors must be collected first"
                )
            allowed = {error["request_id"] for report in reports for error in report["errors"]}
            if parsed.request_id not in allowed:
                raise ToolFailedError(
                    "FORBIDDEN_RESOURCE", "request id is not in the scoped database errors"
                )

    async def _reject(
        self,
        definition: ToolDefinition,
        arguments: dict[str, Any],
        code: str,
        message: str,
        retryable: bool,
        logical_id: UUID | None = None,
    ) -> None:
        if logical_id is None:
            logical_id = uuid5(self.context.investigation_id, f"{definition.name}:{uuid4()}")
        async with self.database.transaction(self.context.tenant_id) as conn:
            await require_live_lease(
                conn, self.context.claim, renew_seconds=self.context.lease_seconds
            )
            await self._insert_terminal(
                conn, definition, arguments, logical_id, code, message, retryable
            )

    async def _insert_terminal(
        self,
        conn: Any,
        definition: ToolDefinition,
        arguments: dict[str, Any],
        logical_id: UUID,
        code: str,
        message: str,
        retryable: bool,
    ) -> None:
        safe_arguments = _safe_arguments(arguments)
        attempt = await next_attempt(
            conn, self.context.tenant_id, self.context.investigation_id, logical_id
        )
        await insert_tool_call(
            conn,
            tenant_id=self.context.tenant_id,
            tool_call_id=uuid4(),
            investigation_id=self.context.investigation_id,
            logical_call_id=logical_id,
            attempt=attempt,
            tool_name=definition.name,
            tool_version=definition.version,
            arguments=safe_arguments,
            arguments_digest=sha256_canonical(safe_arguments),
            deadline_at=datetime.now(UTC) + timedelta(seconds=8),
            worker_epoch=self.context.lease_epoch,
            source_id=definition.source_id,
            trace_id=self.context.trace_id,
            status="FAILED",
            error={"code": code, "message": message},
            retryable=retryable,
        )

    async def _fail_started(self, tool_call_id: UUID, exc: ToolFailedError) -> None:
        async with self.database.transaction(self.context.tenant_id) as conn:
            await require_live_lease(
                conn, self.context.claim, renew_seconds=self.context.lease_seconds
            )
            await fail_tool_call(
                conn,
                tenant_id=self.context.tenant_id,
                tool_call_id=tool_call_id,
                error={"code": exc.code, "message": str(exc)},
                retryable=exc.retryable,
            )

    def _check_size(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        if len(encoded) > _MAX_BYTES:
            raise ToolFailedError("INVALID_OUTPUT", "tool output exceeds the size limit")


def _safe_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    try:
        canonical = to_canonical(arguments)
    except TypeError:
        return {"unparsed": True}
    if not isinstance(canonical, dict):
        return {"unparsed": True}
    redacted = _redact(canonical)
    if not isinstance(redacted, dict):
        return {"unparsed": True}
    return redacted


def _redact(value: Any, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {str(item_key): _redact(item, str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, key) for item in value]
    if isinstance(value, str) and _sensitive(key, value):
        return "redacted"
    return value


def _sensitive(key: str | None, value: str) -> bool:
    if key is not None and key.lower() in _SECRET_KEYS:
        return True
    if key in {"source_ref", "service_ref"} and _LOGICAL_ID.fullmatch(value) is None:
        return True
    lowered = value.lower()
    return "://" in lowered or "password=" in lowered or lowered.startswith("postgres")
