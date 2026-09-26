from dataclasses import dataclass
from uuid import UUID

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


@dataclass(frozen=True)
class Claim:
    tenant_id: str
    investigation_id: UUID
    lease_epoch: int
    lease_owner: str


async def open_pool(database_url: str) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        database_url,
        min_size=1,
        max_size=5,
        open=False,
        kwargs={
            "row_factory": dict_row,
            "options": "-c timezone=UTC -c statement_timeout=10000",
        },
    )
    await pool.open()
    return pool


async def claim_next(pool: AsyncConnectionPool, owner: str, lease_seconds: int) -> Claim | None:
    async with pool.connection() as conn:
        async with conn.transaction():
            cursor = await conn.execute(
                "SELECT tenant_id, id, lease_epoch FROM claim_investigation(%s, %s)",
                (owner, lease_seconds),
            )
            row = await cursor.fetchone()
    if row is None or row["id"] is None:
        return None
    return Claim(
        tenant_id=row["tenant_id"],
        investigation_id=row["id"],
        lease_epoch=row["lease_epoch"],
        lease_owner=owner,
    )
