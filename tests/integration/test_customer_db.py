import json
from datetime import UTC, datetime

import psycopg
import pytest
from evals.customer_source import (
    SERVICE,
    SOURCE_ID,
    prepare_customer_source,
    seed_customer_source,
)
from evals.database import ROOT
from evals.database_pool import assert_database_pool, run_database_pool
from evals.metrics import score_database_pool
from psycopg import sql
from psycopg.rows import dict_row

from forwardops.api.app import create_app
from forwardops.config import build_settings
from forwardops.domain.errors import ToolFailedError
from forwardops.domain.investigation import DATABASE_POOL_SCENARIO, InvestigationScope
from forwardops.domain.time import parse_utc
from forwardops.integrations.customer_db import (
    REGISTRY,
    REQUESTS,
    CustomerDbHandlers,
    count_params,
    fetch_read_only,
    read_only_session,
    run_registered,
)
from forwardops.storage.leases import claim_next
from forwardops.storage.postgres import Database
from forwardops.tools.contracts import (
    GetRecentDatabaseErrorsInput,
    GetServiceRequestSummaryInput,
    WindowInput,
)
from forwardops.tools.gateway import ToolContext, ToolGateway

WINDOW_START = parse_utc("2026-09-26T14:00:00Z")
WINDOW_END = parse_utc("2026-09-26T14:10:00Z")
SECRET = "super-secret-db-password"


@pytest.fixture
def customer_db():
    admin, readonly = prepare_customer_source()
    seed_customer_source(admin)
    return {"admin": admin, "readonly": readonly}


@pytest.fixture
def source_settings(database, customer_db):
    return build_settings(
        environment="development",
        database_url=database["app"],
        migration_database_url=database["admin"],
        migrations_dir=ROOT / "migrations",
        customer_path=ROOT / "examples/customer-a/config.yaml",
        identities_path=ROOT / "examples/customer-a/dev-identities.yaml",
        customer_db_source_id=SOURCE_ID,
        customer_db_url=customer_db["readonly"],
    )


@pytest.fixture
async def source_app(source_settings, pool):
    application = create_app(source_settings, pool=pool)
    async with application.router.lifespan_context(application):
        yield application


def _scope(settings) -> InvestigationScope:
    customer = settings.customer
    scenario = customer.database_scenario
    assert scenario is not None
    return InvestigationScope(
        service_ref=scenario.service_ref,
        vault_ref=customer.vault_ref,
        oracle_ref=customer.oracle_ref,
        cluster_ref=customer.cluster_ref,
        interval_start=scenario.window.start,
        interval_end=scenario.window.end,
        sample_cap=scenario.sample_cap,
        updater_target=customer.updater_target,
        program_id=customer.program_id,
        oracle_program_id=customer.oracle_program_id,
        vault_address=customer.vault_address,
        oracle_address=customer.oracle_address,
        scenario_id=DATABASE_POOL_SCENARIO,
        database_source_ref=scenario.database_source_ref,
    )


def _summary_input(scope: InvestigationScope) -> GetServiceRequestSummaryInput:
    return GetServiceRequestSummaryInput(
        service_ref=scope.service_ref,
        source_ref=scope.database_source_ref or SOURCE_ID,
        window=WindowInput(
            start=parse_utc(scope.interval_start), end=parse_utc(scope.interval_end)
        ),
        limit=20,
    )


async def test_predefined_query_parameter_binding_and_window(customer_db, source_settings) -> None:
    scope = _scope(source_settings)
    handlers = CustomerDbHandlers(SOURCE_ID, customer_db["readonly"], 2000, 100)
    try:
        result = await handlers.get_service_request_summary(scope, _summary_input(scope))
    finally:
        await handlers.close()
    assert result.output["incident_failures"] == 24
    assert result.output["baseline_requests"] == 80
    assert result.output["baseline_failures"] == 0
    assert result.output["failure_codes"] == [
        {"error_code": "db_acquisition_timeout", "failures": 24}
    ]
    narrow = GetServiceRequestSummaryInput(
        service_ref=scope.service_ref,
        source_ref=SOURCE_ID,
        window=WindowInput(
            start=WINDOW_START,
            end=parse_utc("2026-09-26T14:00:30Z"),
        ),
        limit=20,
    )
    handlers = CustomerDbHandlers(SOURCE_ID, customer_db["readonly"], 2000, 100)
    try:
        missed = await handlers.get_service_request_summary(scope, narrow)
    finally:
        await handlers.close()
    assert missed.output["incident_failures"] == 0
    statement = REGISTRY["service_request_counts"].statement
    assert "%s" in statement
    assert "checkout-api' OR '1'='1" not in statement
    async with await psycopg.AsyncConnection.connect(
        customer_db["readonly"], row_factory=dict_row
    ) as conn:
        bound = await run_registered(
            conn,
            "service_request_counts",
            count_params("checkout-api' OR '1'='1", WINDOW_START, WINDOW_END),
            timeout_ms=2000,
            max_rows=1,
        )
        with pytest.raises(ToolFailedError) as rejected:
            await run_registered(conn, "execute_sql", ("SELECT 1",), timeout_ms=2000, max_rows=1)
        with pytest.raises(ToolFailedError) as inserted:
            await fetch_read_only(
                conn,
                "INSERT INTO customer_service_requests (service_ref) VALUES ('x')",
                (),
                timeout_ms=2000,
                max_rows=1,
            )
    assert bound[0]["incident_requests"] == 0
    assert rejected.value.code == "UNSUPPORTED_SCHEMA"
    assert inserted.value.code == "UNSUPPORTED_SCHEMA"
    with psycopg.connect(customer_db["admin"]) as admin:
        remaining = admin.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(REQUESTS))
        ).fetchone()
    assert remaining is not None
    assert remaining[0] > 0


async def test_row_limit_and_truncation_metadata(customer_db, source_settings) -> None:
    scope = _scope(source_settings)
    handlers = CustomerDbHandlers(SOURCE_ID, customer_db["readonly"], 2000, max_rows=2)
    try:
        result = await handlers.get_recent_database_errors(
            scope,
            GetRecentDatabaseErrorsInput(
                service_ref=scope.service_ref,
                source_ref=SOURCE_ID,
                window=WindowInput(start=WINDOW_START, end=WINDOW_END),
                limit=20,
            ),
        )
    finally:
        await handlers.close()
    assert result.output["row_count"] == 2
    assert result.output["truncated"] is True
    assert result.output["coverage_complete"] is False
    assert len(result.output["errors"]) == 2
    assert result.observations[0].coverage["truncated"] is True
    assert result.observations[0].coverage["row_count"] == 2
    assert result.observations[0].coverage["window_start"] == "2026-09-26T14:00:00Z"


async def test_statement_timeout(customer_db, source_settings) -> None:
    scope = _scope(source_settings)
    handlers = CustomerDbHandlers(SOURCE_ID, customer_db["readonly"], 300, 100)
    locker = await psycopg.AsyncConnection.connect(customer_db["admin"])
    try:
        await locker.execute(
            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(sql.Identifier(REQUESTS))
        )
        with pytest.raises(ToolFailedError) as exc:
            await handlers.get_service_request_summary(scope, _summary_input(scope))
        assert exc.value.code == "STATEMENT_TIMEOUT"
        assert SECRET not in str(exc.value)
    finally:
        await locker.rollback()
        await locker.close()
        await handlers.close()


async def test_source_unavailable_hides_the_dsn(source_settings) -> None:
    scope = _scope(source_settings)
    handlers = CustomerDbHandlers(
        SOURCE_ID,
        f"postgresql://forwardops_customer_ro:{SECRET}@127.0.0.1:1/forwardops_customer_source",
        2000,
        100,
    )
    try:
        with pytest.raises(ToolFailedError) as exc:
            await handlers.get_service_request_summary(scope, _summary_input(scope))
    finally:
        await handlers.close()
    assert exc.value.code == "SOURCE_UNAVAILABLE"
    assert SECRET not in str(exc.value)
    assert "127.0.0.1" not in str(exc.value)


async def test_malformed_result(customer_db, source_settings) -> None:
    with psycopg.connect(customer_db["admin"], autocommit=True) as admin:
        admin.execute(
            sql.SQL(
                "INSERT INTO {} (service_ref, request_id, occurred_at, outcome, error_code) "
                "VALUES (%s, %s, %s, %s, %s)"
            ).format(sql.Identifier(REQUESTS)),
            (
                SERVICE,
                "req-bad",
                datetime(2026, 9, 26, 14, 3, tzinfo=UTC),
                "exploded",
                None,
            ),
        )
    scope = _scope(source_settings)
    handlers = CustomerDbHandlers(SOURCE_ID, customer_db["readonly"], 2000, 100)
    try:
        with pytest.raises(ToolFailedError) as exc:
            await handlers.get_service_request_summary(scope, _summary_input(scope))
    finally:
        await handlers.close()
    assert exc.value.code == "MALFORMED_RESULT"
    assert "exploded" not in str(exc.value)


async def test_read_only_transaction_rejects_writes(customer_db) -> None:
    async with await psycopg.AsyncConnection.connect(customer_db["admin"]) as admin:
        with pytest.raises(psycopg.Error) as exc:
            async with read_only_session(admin, 2000):
                await admin.execute(
                    sql.SQL(
                        "INSERT INTO {} (service_ref, request_id, occurred_at, outcome) "
                        "VALUES ('checkout-api', 'req-write', now(), 'failure')"
                    ).format(sql.Identifier(REQUESTS))
                )
    assert exc.value.sqlstate == "25006"
    async with await psycopg.AsyncConnection.connect(customer_db["readonly"]) as reader:
        with pytest.raises(psycopg.Error) as role_error:
            await reader.execute(
                sql.SQL(
                    "UPDATE {} SET outcome = 'failure' WHERE service_ref = 'checkout-api'"
                ).format(sql.Identifier(REQUESTS))
            )
    assert role_error.value.sqlstate in {"25006", "42501"}


async def test_source_outside_scope_and_sql_tool(client, app) -> None:
    created = await client.post(
        "/investigations",
        headers={
            "Authorization": "Bearer dev-investigator",
            "Idempotency-Key": "pool-scope",
        },
        json={"question": "Why are checkout-api requests failing?"},
    )
    assert created.status_code == 202, created.text
    assert created.json()["resolved_scope"]["database_source_ref"] == SOURCE_ID
    claim = await claim_next(app.state.pool, "pool-scope", 60)
    assert claim is not None
    scope = InvestigationScope.model_validate(created.json()["resolved_scope"])
    context = ToolContext(
        tenant_id=claim.tenant_id,
        principal_id="investigator-a",
        investigation_id=claim.investigation_id,
        scope=scope,
        request_id="pool-scope",
        trace_id="pool-scope",
        lease_epoch=claim.lease_epoch,
        lease_owner=claim.lease_owner,
        frozen_observed_at="2026-09-26T14:10:00Z",
        max_tool_calls=12,
        lease_seconds=60,
    )
    gateway = ToolGateway(Database(app.state.pool), app.state.runtime.handlers, context)
    with pytest.raises(ToolFailedError) as source_error:
        await gateway.call(
            "get_service_request_summary",
            {
                "service_ref": "checkout-api",
                "source_ref": "customer-db-b",
                "window": {"start": "2026-09-26T14:00:00Z", "end": "2026-09-26T14:10:00Z"},
                "limit": 3,
            },
        )
    assert source_error.value.code == "FORBIDDEN_RESOURCE"
    with pytest.raises(ToolFailedError) as sql_error:
        await gateway.call("execute_sql", {"sql": "DROP TABLE evidence"})
    assert sql_error.value.code == "UNKNOWN_TOOL"
    with pytest.raises(ToolFailedError) as dsn_error:
        await gateway.call(
            "get_service_request_summary",
            {
                "service_ref": "checkout-api",
                "source_ref": f"postgresql://user:{SECRET}@db.internal/app",
                "window": {"start": "2026-09-26T14:00:00Z", "end": "2026-09-26T14:10:00Z"},
                "sql": "SELECT * FROM customer_service_requests",
            },
        )
    assert dsn_error.value.code == "UNSUPPORTED_SCHEMA"
    async with app.state.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT set_config('app.tenant_id', 'customer-a', true)")
            cursor = await conn.execute(
                "SELECT arguments::text AS arguments FROM tool_calls WHERE investigation_id = %s",
                (claim.investigation_id,),
            )
            stored = await cursor.fetchall()
    blob = json.dumps(stored, default=str)
    assert SECRET not in blob
    assert "customer_service_requests" not in blob


async def test_database_pool_scenario(source_app) -> None:
    import httpx

    transport = httpx.ASGITransport(app=source_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://forwardops") as client:
        result = await run_database_pool(client, source_app)
    assert_database_pool(result)
    metrics = score_database_pool(result)
    assert metrics == {
        "correct_tool_selection": True,
        "evidence_recall": True,
        "citation_validity": True,
        "correct_affected_component": True,
        "correct_root_cause": True,
        "unsupported_claims": 0,
        "unknown_preservation": True,
        "action_safety": True,
        "tool_count": 6,
    }
