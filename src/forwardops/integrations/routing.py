"""Send configured sources to their adapters. Unconfigured calls stay on replay."""

from typing import Any

from pydantic import BaseModel

from forwardops.domain.errors import ToolFailedError
from forwardops.domain.investigation import InvestigationScope
from forwardops.integrations.customer_db import CUSTOMER_DB_TOOLS, CustomerDbHandlers
from forwardops.integrations.replay import HandlerResult, ReplayHandlers
from forwardops.integrations.solana import SolanaHandlers
from forwardops.tools.contracts import (
    GetDatabasePoolSnapshotInput,
    GetRecentDatabaseErrorsInput,
    GetServiceRequestSummaryInput,
    GetSolanaAccountInput,
    GetSolanaTransactionInput,
)


class RoutingHandlers:
    def __init__(
        self,
        replay: ReplayHandlers,
        solana: SolanaHandlers | None,
        customer_db: CustomerDbHandlers | None = None,
    ) -> None:
        self.replay = replay
        self.solana = solana
        self.customer_db = customer_db

    def serves_solana_cluster(self, cluster_ref: str) -> bool:
        return self.solana is not None and self.solana.serves(cluster_ref)

    def source_id_for(self, name: str, parsed: BaseModel) -> str | None:
        cluster = getattr(parsed, "cluster_ref", None)
        if (
            name in {"get_solana_transaction", "get_solana_account"}
            and isinstance(cluster, str)
            and self.serves_solana_cluster(cluster)
        ):
            return "solana-rpc"
        if name in CUSTOMER_DB_TOOLS:
            source = getattr(parsed, "source_ref", None)
            if isinstance(source, str) and source:
                return source
        return None

    async def get_solana_transaction(
        self,
        scope: InvestigationScope,
        arguments: GetSolanaTransactionInput,
    ) -> HandlerResult:
        if self.solana is not None and self.solana.serves(arguments.cluster_ref):
            return await self.solana.get_solana_transaction(scope, arguments)
        return await self.replay.get_solana_transaction(scope, arguments)

    async def get_solana_account(
        self,
        scope: InvestigationScope,
        arguments: GetSolanaAccountInput,
    ) -> HandlerResult:
        if self.solana is None or not self.solana.serves(arguments.cluster_ref):
            raise ToolFailedError(
                "FORBIDDEN_RESOURCE",
                "no Solana RPC source is configured for this cluster",
            )
        return await self.solana.get_solana_account(scope, arguments)

    async def get_service_request_summary(
        self,
        scope: InvestigationScope,
        arguments: GetServiceRequestSummaryInput,
    ) -> HandlerResult:
        return await self._customer_db().get_service_request_summary(scope, arguments)

    async def get_database_pool_snapshot(
        self,
        scope: InvestigationScope,
        arguments: GetDatabasePoolSnapshotInput,
    ) -> HandlerResult:
        return await self._customer_db().get_database_pool_snapshot(scope, arguments)

    async def get_recent_database_errors(
        self,
        scope: InvestigationScope,
        arguments: GetRecentDatabaseErrorsInput,
    ) -> HandlerResult:
        return await self._customer_db().get_recent_database_errors(scope, arguments)

    def _customer_db(self) -> CustomerDbHandlers:
        if self.customer_db is None:
            raise ToolFailedError(
                "SOURCE_UNAVAILABLE",
                "customer database source is unavailable",
                retryable=True,
            )
        return self.customer_db

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.replay, name)
