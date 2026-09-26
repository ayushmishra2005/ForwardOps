import psycopg

from forwardops.ingestion.records import list_source_records

SOURCE = "solana-ingest-read"


async def test_python_can_read_rows_written_for_the_backfill(database, pool):
    with psycopg.connect(database["admin"]) as connection:
        connection.execute("DELETE FROM source_records WHERE source_id = %s", (SOURCE,))
        connection.execute(
            """
            INSERT INTO source_records (
              source_id, cluster_id, signature, record_identity, decoder_version,
              address, slot, block_time, commitment, outcome, program_ids,
              instruction_errors, log_messages, logs_truncated, payload_sha256, gap_reason
            ) VALUES (
              %s, 'mainnet-beta', %s, 'transaction', 'generic-v1',
              '11111111111111111111111111111111', 20, NULL, 'finalized', 'stored',
              '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, false, %s, NULL
            )
            """,
            (SOURCE, "sig-read-1", "ab" * 32),
        )
        connection.commit()

    async with pool.connection() as connection:
        rows = await list_source_records(connection, source_id=SOURCE)

    assert [row["signature"] for row in rows] == ["sig-read-1"]
    assert rows[0]["outcome"] == "stored"
    assert rows[0]["decoder_version"] == "generic-v1"
    assert rows[0]["gap_reason"] is None
