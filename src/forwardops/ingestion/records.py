"""Read normalized Solana rows written by the Rust backfill.

This is not an investigation tool. The model cannot call it, and it does not
choose an RPC endpoint.
"""

from typing import Any

import psycopg
from psycopg.rows import dict_row


async def list_source_records(
    connection: psycopg.AsyncConnection[Any],
    *,
    source_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    async with connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """
            SELECT source_id, cluster_id, signature, slot, outcome, decoder_version,
                   commitment, gap_reason, payload_sha256
            FROM source_records
            WHERE source_id = %s
            ORDER BY slot, signature
            LIMIT %s
            """,
            (source_id, limit),
        )
        return list(await cursor.fetchall())
