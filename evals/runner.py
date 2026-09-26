"""Run the offline stale-oracle evaluation against PostgreSQL."""

import asyncio
import sys

import httpx

from evals.database import ROOT, prepare_database, truncate
from evals.scenario import assert_stale_oracle, run_stale_oracle
from forwardops.api.app import create_app
from forwardops.config import build_settings
from forwardops.storage.leases import open_pool


async def _main() -> int:
    admin_dsn, application_dsn = prepare_database("forwardops_eval")
    truncate(admin_dsn)
    settings = build_settings(
        environment="development",
        database_url=application_dsn,
        migration_database_url=admin_dsn,
        migrations_dir=ROOT / "migrations",
        customer_path=ROOT / "examples/customer-a/config.yaml",
        identities_path=ROOT / "examples/customer-a/dev-identities.yaml",
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
    finally:
        await pool.close()
    print("stale_oracle_withdrawal_failure: pass")
    print("status=CONCLUDED cause=stale_oracle execution=not_enabled")
    return 0


def main() -> None:
    try:
        code = asyncio.run(_main())
    except Exception as exc:
        print(
            f"stale_oracle_withdrawal_failure: fail ({type(exc).__name__}: {exc})", file=sys.stderr
        )
        raise SystemExit(1) from exc
    raise SystemExit(code)


if __name__ == "__main__":
    main()
