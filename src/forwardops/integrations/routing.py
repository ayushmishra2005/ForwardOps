"""Send configured Solana clusters to the RPC adapter. Every other call stays on replay."""

from typing import Any

from pydantic import BaseModel

from forwardops.domain.errors import ToolFailedError
from forwardops.domain.investigation import InvestigationScope
from forwardops.integrations.replay import HandlerResult, ReplayHandlers
from forwardops.integrations.solana import SolanaHandlers
from forwardops.tools.contracts import GetSolanaAccountInput, GetSolanaTransactionInput


class RoutingHandlers:
    def __init__(self, replay: ReplayHandlers, solana: SolanaHandlers | None) -> None:
        self.replay = replay
        self.solana = solana

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

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.replay, name)
