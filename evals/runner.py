"""Run the offline deterministic evaluations against PostgreSQL.

The stale-oracle scenario remains the baseline. The database pool scenario
uses a separate customer PostgreSQL source. A scripted prompt-injection case
always runs. The live model case runs only when a provider key is configured.
"""

import asyncio
import os
import sys

import httpx

from evals.customer_source import SOURCE_ID, prepare_customer_source, seed_customer_source
from evals.database import ROOT, prepare_database, truncate
from evals.database_pool import assert_database_pool, run_database_pool
from evals.metrics import POOL_METRIC_KEYS, format_metrics, score_database_pool, score_investigation
from evals.model_eval import (
    assert_model_assisted,
    assert_prompt_injection,
    live_provider,
    live_setting_updates,
    run_model_investigation,
)
from evals.model_script import StaleOracleModel
from evals.scenario import assert_stale_oracle, run_stale_oracle
from evals.tempo_mock import TempoMock
from forwardops.api.app import create_app
from forwardops.config import build_settings
from forwardops.storage.leases import open_pool


async def _main() -> int:
    admin_dsn, application_dsn = prepare_database("forwardops_eval")
    truncate(admin_dsn)
    customer_admin, customer_readonly = prepare_customer_source()
    seed_customer_source(customer_admin)
    tempo = TempoMock()
    tempo_url = tempo.start()
    try:
        return await _evaluate(admin_dsn, application_dsn, customer_readonly, tempo_url)
    finally:
        tempo.close()


async def _evaluate(
    admin_dsn: str,
    application_dsn: str,
    customer_readonly: str,
    tempo_url: str,
) -> int:
    settings = build_settings(
        environment="development",
        database_url=application_dsn,
        migration_database_url=admin_dsn,
        migrations_dir=ROOT / "migrations",
        customer_path=ROOT / "examples/customer-a/config.yaml",
        identities_path=ROOT / "examples/customer-a/dev-identities.yaml",
        customer_db_source_id=SOURCE_ID,
        customer_db_url=customer_readonly,
        tempo_source_id="tempo-local",
        tempo_url=tempo_url,
    )
    pool = await open_pool(application_dsn)
    app = create_app(settings, pool=pool)
    try:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://forwardops"
            ) as client:
                result = await run_stale_oracle(client, app)
                assert_stale_oracle(result)
                print("stale_oracle_withdrawal_failure: pass")
                print("status=CONCLUDED cause=stale_oracle execution=not_enabled")
                print(format_metrics("deterministic", score_investigation(result)))
                pool_result = await run_database_pool(client, app)
                assert_database_pool(pool_result)
                print("database_connection_pool_exhaustion: pass")
                print("status=CONCLUDED cause=database_connection_pool_exhaustion actions=none")
                print(
                    format_metrics(
                        "database_pool",
                        score_database_pool(pool_result),
                        keys=POOL_METRIC_KEYS,
                    )
                )
                injection_provider = StaleOracleModel(inject=True)
                injection = await run_model_investigation(
                    client,
                    app,
                    injection_provider,
                    idempotency_key="prompt-injection",
                )
                assert_prompt_injection(injection, injection_provider)
                print("prompt_injection: pass")
                print(format_metrics("prompt_injection", score_investigation(injection)))
                if not os.environ.get("FORWARDOPS_OPENAI_API_KEY"):
                    print("model_assisted: skip (FORWARDOPS_OPENAI_API_KEY is not configured)")
                else:
                    live = await run_model_investigation(
                        client,
                        app,
                        live_provider(),
                        idempotency_key="live-model",
                        **live_setting_updates(),
                    )
                    assert_model_assisted(live)
                    print("model_assisted: pass")
                    print(format_metrics("model_assisted", score_investigation(live)))
    finally:
        await pool.close()
    return 0


def main() -> None:
    try:
        code = asyncio.run(_main())
    except Exception as exc:
        print(f"evaluation: fail ({type(exc).__name__}: {exc})", file=sys.stderr)
        raise SystemExit(1) from exc
    raise SystemExit(code)


if __name__ == "__main__":
    main()
