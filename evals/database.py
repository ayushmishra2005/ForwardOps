import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
from psycopg import sql

from forwardops.storage.migrate import APP_ROLE, apply_migrations

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"
APP_PASSWORD = "forwardops_app"


def admin_dsn() -> str:
    return os.environ.get(
        "TEST_DATABASE_ADMIN_URL",
        "postgresql://forwardops:forwardops@localhost:5432/postgres",
    )


def with_database(dsn: str, database: str) -> str:
    parsed = urlparse(dsn)
    return urlunparse(parsed._replace(path=f"/{database}"))


def app_dsn(admin: str, database: str) -> str:
    parsed = urlparse(admin)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    return f"postgresql://{APP_ROLE}:{APP_PASSWORD}@{host}:{port}/{database}"


def prepare_database(database: str) -> tuple[str, str]:
    """Create a database, migrate it, and return (admin_dsn, app_dsn)."""
    maintenance = admin_dsn()
    try:
        connection = psycopg.connect(maintenance, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        raise RuntimeError(
            "PostgreSQL is not reachable. Start it with "
            "`docker compose -f deploy/compose.yaml up -d postgres` "
            f"or set TEST_DATABASE_ADMIN_URL. ({exc})"
        ) from exc
    with connection:
        exists = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (database,),
        ).fetchone()
        if exists is None:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    admin_for_db = with_database(maintenance, database)
    apply_migrations(admin_for_db, MIGRATIONS, APP_PASSWORD)
    return admin_for_db, app_dsn(maintenance, database)


def truncate(admin_for_db: str) -> None:
    with psycopg.connect(admin_for_db, autocommit=True) as connection:
        connection.execute(
            """
            TRUNCATE TABLE
              audit_events,
              approvals,
              action_proposals,
              findings,
              evidence,
              tool_calls,
              investigations
            CASCADE
            """
        )
