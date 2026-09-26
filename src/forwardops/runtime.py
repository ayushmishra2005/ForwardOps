from dataclasses import dataclass

from forwardops.config import Settings
from forwardops.integrations.customer_db import CustomerDbHandlers
from forwardops.integrations.replay import FixtureSource, ReplayHandlers, load_runbooks
from forwardops.integrations.routing import RoutingHandlers
from forwardops.integrations.solana import (
    INSTALLED_ACCOUNT_DECODERS,
    INSTALLED_TRANSACTION_DECODERS,
    SolanaEndpoint,
    SolanaHandlers,
)
from forwardops.models.openai import OpenAIModelProvider
from forwardops.models.provider import ModelProvider


@dataclass(frozen=True)
class Runtime:
    settings: Settings
    handlers: RoutingHandlers
    model_provider: ModelProvider | None = None


def build_runtime(settings: Settings) -> Runtime:
    source = FixtureSource.load(settings.fixture_dir)
    runbooks = load_runbooks(settings.runbook_dir)
    provider = None
    if settings.analysis_mode == "model" and settings.openai_api_key:
        provider = OpenAIModelProvider(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            base_url=settings.openai_base_url,
            timeout_seconds=settings.model_timeout_seconds,
        )
    solana = None
    if settings.solana_clusters:
        solana = SolanaHandlers(
            tuple(
                SolanaEndpoint(
                    cluster_id=item.cluster_id,
                    rpc_url=item.rpc_url,
                    commitment=item.commitment,
                    timeout_seconds=settings.solana_timeout_seconds,
                )
                for item in settings.solana_clusters
            ),
            account_decoder_bindings=settings.solana_decoders,
            installed_account_decoders=INSTALLED_ACCOUNT_DECODERS,
            installed_transaction_decoders=INSTALLED_TRANSACTION_DECODERS,
        )
    customer_db = None
    configured = settings.customer_database
    if configured is not None:
        customer_db = CustomerDbHandlers(
            configured.source_id,
            configured.dsn,
            configured.statement_timeout_ms,
            configured.max_rows,
            synthetic=configured.synthetic,
        )
    return Runtime(
        settings=settings,
        handlers=RoutingHandlers(ReplayHandlers(source, runbooks), solana, customer_db),
        model_provider=provider,
    )
