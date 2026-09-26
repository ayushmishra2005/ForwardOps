import hashlib
from pathlib import Path

import psycopg
from psycopg import sql

from forwardops.domain.errors import ConfigError
from forwardops.domain.hashing import sha256_bytes

APP_ROLE = "forwardops_app"
_LOCK_KEY = 84261001


def ensure_app_role(dsn: str, password: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s",
            (APP_ROLE,),
        ).fetchone()
        statement = (
            "ALTER ROLE {} WITH LOGIN PASSWORD {} NOSUPERUSER NOBYPASSRLS"
            if exists
            else "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
        )
        conn.execute(sql.SQL(statement).format(sql.Identifier(APP_ROLE), sql.Literal(password)))
        database = conn.execute("SELECT current_database()").fetchone()
        if database is None:
            raise ConfigError("could not read the current database name")
        conn.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database[0]),
                sql.Identifier(APP_ROLE),
            )
        )


def apply_migrations(dsn: str, directory: Path, app_password: str) -> None:
    if not directory.is_dir():
        raise ConfigError(f"migrations directory does not exist: {directory}")
    ensure_app_role(dsn, app_password)
    with psycopg.connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))
        conn.commit()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                  version text PRIMARY KEY,
                  checksum text NOT NULL,
                  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
                )
                """
            )
            conn.commit()
            for path in sorted(directory.glob("*.sql")):
                _apply_file(conn, path)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))
            conn.commit()


def _apply_file(conn: psycopg.Connection, path: Path) -> None:
    version = path.name
    checksum = sha256_bytes(path.read_bytes())
    existing = conn.execute(
        "SELECT checksum FROM schema_migrations WHERE version = %s",
        (version,),
    ).fetchone()
    if existing is not None:
        if existing[0] != checksum:
            raise ConfigError(f"checksum mismatch for migration {version}")
        return
    with conn.transaction():
        for statement in _split_sql(path.read_text(encoding="utf-8")):
            conn.execute(statement)
        conn.execute(
            "INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)",
            (version, checksum),
        )


def _split_sql(script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    index = 0
    dollar: str | None = None
    while index < len(script):
        if dollar is not None:
            if script.startswith(dollar, index):
                current.append(dollar)
                index += len(dollar)
                dollar = None
                continue
            current.append(script[index])
            index += 1
            continue
        if script.startswith("$$", index):
            dollar = "$$"
            current.append(dollar)
            index += 2
            continue
        if script[index] == ";":
            statement = "".join(current).strip()
            if _meaningful(statement):
                statements.append(statement)
            current = []
            index += 1
            continue
        current.append(script[index])
        index += 1
    tail = "".join(current).strip()
    if _meaningful(tail):
        statements.append(tail)
    if dollar is not None:
        raise ConfigError("migration has an unterminated dollar quote")
    return statements


def _meaningful(statement: str) -> bool:
    for line in statement.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return True
    return False


def file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
