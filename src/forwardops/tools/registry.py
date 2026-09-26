from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from forwardops.tools.contracts import (
    DeploymentSearchResult,
    GetOracleStateInput,
    GetRecentDeploymentsInput,
    GetRecentWithdrawalFailuresInput,
    GetSolanaTransactionInput,
    GetVaultStateInput,
    LogSearchResult,
    OracleState,
    RunbookSearchResult,
    SearchApplicationLogsInput,
    SearchRunbooksInput,
    TransactionView,
    VaultState,
    WithdrawalFailureSummary,
)


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    version: str
    input_model: type[BaseModel]
    output_model: type[BaseModel]
    source_id: str


def tool_definitions() -> dict[str, ToolDefinition]:
    rows = (
        ToolDefinition(
            "get_recent_withdrawal_failures",
            "v1",
            GetRecentWithdrawalFailuresInput,
            WithdrawalFailureSummary,
            "synthetic-withdrawals",
        ),
        ToolDefinition(
            "get_solana_transaction",
            "v1",
            GetSolanaTransactionInput,
            TransactionView,
            "solana-fixture",
        ),
        ToolDefinition(
            "search_application_logs",
            "v1",
            SearchApplicationLogsInput,
            LogSearchResult,
            "synthetic-logs",
        ),
        ToolDefinition("get_vault_state", "v1", GetVaultStateInput, VaultState, "solana-fixture"),
        ToolDefinition(
            "get_oracle_state", "v1", GetOracleStateInput, OracleState, "solana-fixture"
        ),
        ToolDefinition(
            "search_runbooks",
            "v1",
            SearchRunbooksInput,
            RunbookSearchResult,
            "runbook-files",
        ),
        ToolDefinition(
            "get_recent_deployments",
            "v1",
            GetRecentDeploymentsInput,
            DeploymentSearchResult,
            "synthetic-deployments",
        ),
    )
    return {item.name: item for item in rows}


def dump_model(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")
