use sha2::{Digest, Sha256};
use tokio_postgres::types::Json;
use tokio_postgres::NoTls;

use crate::checkpoint::high_water_slot;
use crate::config::validate_signature;
use crate::normalize::NormalizedRecord;
use crate::rpc::SignatureInfo;
use crate::{IngestError, DECODER_VERSION, RECORD_IDENTITY};

#[derive(Clone)]
pub struct Window {
    pub source_id: String,
    pub cluster_id: String,
    pub address: String,
    pub commitment: String,
    pub from_slot: i64,
    pub to_slot: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub signature_before: Option<String>,
    pub enumeration_complete: bool,
    pub high_water_slot: Option<i64>,
    pub status: String,
    pub stopped_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingSignature {
    pub signature: String,
    pub slot: i64,
    pub block_time_unix: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub stored: i64,
    pub gaps: i64,
    pub pending: i64,
    pub records: i64,
}

pub struct Store {
    client: tokio_postgres::Client,
}

impl Store {
    pub async fn connect(database_url: &str) -> Result<Self, IngestError> {
        let (client, connection) =
            tokio_postgres::connect(database_url, NoTls)
                .await
                .map_err(|error| {
                    IngestError::Storage(format!("postgres connection failed: {error}"))
                })?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres connection closed: {error}");
            }
        });
        Ok(Self { client })
    }

    pub async fn try_lock(&self, window: &Window) -> Result<bool, IngestError> {
        let locked = self
            .client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&lock_id(window)])
            .await
            .map_err(db)?;
        Ok(locked.get(0))
    }

    pub async fn unlock(&self, window: &Window) -> Result<(), IngestError> {
        self.client
            .execute("SELECT pg_advisory_unlock($1)", &[&lock_id(window)])
            .await
            .map_err(db)?;
        Ok(())
    }

    pub async fn prepare(&self, window: &Window) -> Result<Checkpoint, IngestError> {
        self.client
            .execute(
                "INSERT INTO ingestion_checkpoints
                   (source_id, address, commitment, from_slot, to_slot, status)
                 VALUES ($1, $2, $3, $4, $5, 'in_progress')
                 ON CONFLICT DO NOTHING",
                &[
                    &window.source_id,
                    &window.address,
                    &window.commitment,
                    &window.from_slot,
                    &window.to_slot,
                ],
            )
            .await
            .map_err(db)?;
        self.checkpoint(window).await
    }

    pub async fn checkpoint(&self, window: &Window) -> Result<Checkpoint, IngestError> {
        let row = self
            .client
            .query_one(
                "SELECT signature_before, enumeration_complete, high_water_slot, status, stopped_reason
                 FROM ingestion_checkpoints
                 WHERE source_id = $1 AND address = $2 AND commitment = $3 AND from_slot = $4 AND to_slot = $5",
                &[
                    &window.source_id,
                    &window.address,
                    &window.commitment,
                    &window.from_slot,
                    &window.to_slot,
                ],
            )
            .await
            .map_err(db)?;
        Ok(read_checkpoint(&row))
    }

    pub async fn counts(&self, window: &Window) -> Result<Counts, IngestError> {
        let row = self
            .client
            .query_one(
                "SELECT
                   (count(*) FILTER (WHERE fetch_status = 'stored'))::bigint,
                   (count(*) FILTER (WHERE fetch_status = 'gap'))::bigint,
                   (count(*) FILTER (WHERE fetch_status = 'pending'))::bigint
                 FROM ingestion_signatures
                 WHERE source_id = $1 AND address = $2 AND commitment = $3 AND from_slot = $4 AND to_slot = $5",
                &[
                    &window.source_id,
                    &window.address,
                    &window.commitment,
                    &window.from_slot,
                    &window.to_slot,
                ],
            )
            .await
            .map_err(db)?;
        let records = self
            .client
            .query_one(
                "SELECT count(*)::bigint
                 FROM source_records AS records
                 WHERE records.source_id = $1 AND records.address = $2 AND records.commitment = $3
                   AND EXISTS (
                     SELECT 1 FROM ingestion_signatures AS signatures
                     WHERE signatures.source_id = records.source_id
                       AND signatures.address = records.address
                       AND signatures.commitment = records.commitment
                       AND signatures.signature = records.signature
                       AND signatures.from_slot = $4 AND signatures.to_slot = $5
                   )",
                &[
                    &window.source_id,
                    &window.address,
                    &window.commitment,
                    &window.from_slot,
                    &window.to_slot,
                ],
            )
            .await
            .map_err(db)?;
        Ok(Counts {
            stored: row.get(0),
            gaps: row.get(1),
            pending: row.get(2),
            records: records.get(0),
        })
    }

    pub async fn commit_page(
        &mut self,
        window: &Window,
        signatures: &[SignatureInfo],
        signature_before: Option<&str>,
        enumeration_complete: bool,
        stopped_reason: Option<&str>,
    ) -> Result<Checkpoint, IngestError> {
        let transaction = self.client.transaction().await.map_err(db)?;
        for signature in signatures {
            validate_signature(&signature.signature)
                .map_err(|_| IngestError::Rpc("signature page is malformed".into()))?;
            transaction
                .execute(
                    "INSERT INTO ingestion_signatures
                       (source_id, address, commitment, from_slot, to_slot, signature, slot, block_time_unix, fetch_status)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
                     ON CONFLICT DO NOTHING",
                    &[
                        &window.source_id,
                        &window.address,
                        &window.commitment,
                        &window.from_slot,
                        &window.to_slot,
                        &signature.signature,
                        &signature.slot,
                        &signature.block_time,
                    ],
                )
                .await
                .map_err(db)?;
        }
        let checkpoint = write_progress(
            &transaction,
            window,
            Some(signature_before.map(str::to_owned)),
            Some(enumeration_complete),
            false,
            stopped_reason,
            false,
        )
        .await?;
        transaction.commit().await.map_err(db)?;
        Ok(checkpoint)
    }

    pub async fn pending(
        &self,
        window: &Window,
        limit: i64,
    ) -> Result<Vec<PendingSignature>, IngestError> {
        let rows = self
            .client
            .query(
                "SELECT signature, slot, block_time_unix
                 FROM ingestion_signatures
                 WHERE source_id = $1 AND address = $2 AND commitment = $3
                   AND from_slot = $4 AND to_slot = $5 AND fetch_status = 'pending'
                 ORDER BY slot, signature
                 LIMIT $6",
                &[
                    &window.source_id,
                    &window.address,
                    &window.commitment,
                    &window.from_slot,
                    &window.to_slot,
                    &limit,
                ],
            )
            .await
            .map_err(db)?;
        Ok(rows
            .iter()
            .map(|row| PendingSignature {
                signature: row.get(0),
                slot: row.get(1),
                block_time_unix: row.get(2),
            })
            .collect())
    }

    pub async fn commit_records(
        &mut self,
        window: &Window,
        records: &[NormalizedRecord],
    ) -> Result<Checkpoint, IngestError> {
        let transaction = self.client.transaction().await.map_err(db)?;
        for record in records {
            let programs = Json(&record.program_ids);
            let errors = Json(&record.instruction_errors);
            let logs = Json(&record.log_messages);
            let outcome = record.outcome;
            transaction
                .execute(
                    "INSERT INTO source_records (
                       source_id, cluster_id, signature, record_identity, decoder_version, address, slot,
                       block_time, commitment, outcome, program_ids, instruction_errors, log_messages,
                       logs_truncated, payload_sha256, gap_reason
                     ) VALUES (
                       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                     )
                     ON CONFLICT (source_id, cluster_id, signature, record_identity, decoder_version) DO NOTHING",
                    &[
                        &window.source_id,
                        &window.cluster_id,
                        &record.signature,
                        &RECORD_IDENTITY,
                        &DECODER_VERSION,
                        &window.address,
                        &record.slot,
                        &record.block_time,
                        &window.commitment,
                        &outcome,
                        &programs,
                        &errors,
                        &logs,
                        &record.logs_truncated,
                        &record.payload_sha256,
                        &record.gap_reason,
                    ],
                )
                .await
                .map_err(db)?;
            let fetch_status = if record.outcome == "stored" {
                "stored"
            } else {
                "gap"
            };
            transaction
                .execute(
                    "UPDATE ingestion_signatures
                     SET fetch_status = $7
                     WHERE source_id = $1 AND address = $2 AND commitment = $3
                       AND from_slot = $4 AND to_slot = $5 AND signature = $6
                       AND fetch_status = 'pending'",
                    &[
                        &window.source_id,
                        &window.address,
                        &window.commitment,
                        &window.from_slot,
                        &window.to_slot,
                        &record.signature,
                        &fetch_status,
                    ],
                )
                .await
                .map_err(db)?;
        }
        let checkpoint =
            write_progress(&transaction, window, None, None, false, None, true).await?;
        transaction.commit().await.map_err(db)?;
        Ok(checkpoint)
    }

    pub async fn note(
        &mut self,
        window: &Window,
        reason: &str,
        blocked: bool,
    ) -> Result<Checkpoint, IngestError> {
        let transaction = self.client.transaction().await.map_err(db)?;
        let checkpoint = write_progress(
            &transaction,
            window,
            None,
            None,
            blocked,
            Some(reason),
            false,
        )
        .await?;
        transaction.commit().await.map_err(db)?;
        Ok(checkpoint)
    }
}

fn lock_id(window: &Window) -> i64 {
    let material = format!(
        "{}|{}|{}|{}|{}",
        window.source_id, window.address, window.commitment, window.from_slot, window.to_slot
    );
    let digest = Sha256::digest(material.as_bytes());
    i64::from_le_bytes(digest[..8].try_into().expect("sha256 prefix"))
}

fn db(error: tokio_postgres::Error) -> IngestError {
    IngestError::Storage(error.to_string())
}

fn read_checkpoint(row: &tokio_postgres::Row) -> Checkpoint {
    Checkpoint {
        signature_before: row.get(0),
        enumeration_complete: row.get(1),
        high_water_slot: row.get(2),
        status: row.get(3),
        stopped_reason: row.get(4),
    }
}

async fn write_progress(
    transaction: &tokio_postgres::Transaction<'_>,
    window: &Window,
    cursor: Option<Option<String>>,
    enumeration_complete: Option<bool>,
    force_blocked: bool,
    stopped_reason: Option<&str>,
    keep_reason: bool,
) -> Result<Checkpoint, IngestError> {
    let row = transaction
        .query_one(
            "SELECT signature_before, enumeration_complete, high_water_slot, status, stopped_reason
             FROM ingestion_checkpoints
             WHERE source_id = $1 AND address = $2 AND commitment = $3 AND from_slot = $4 AND to_slot = $5
             FOR UPDATE",
            &[
                &window.source_id,
                &window.address,
                &window.commitment,
                &window.from_slot,
                &window.to_slot,
            ],
        )
        .await
        .map_err(db)?;
    let current = read_checkpoint(&row);
    let signature_before = cursor.unwrap_or(current.signature_before);
    let enumeration_complete =
        current.enumeration_complete || enumeration_complete.unwrap_or(false);
    let pending = transaction
        .query_one(
            "SELECT min(slot)
             FROM ingestion_signatures
             WHERE source_id = $1 AND address = $2 AND commitment = $3
               AND from_slot = $4 AND to_slot = $5 AND fetch_status = 'pending'",
            &[
                &window.source_id,
                &window.address,
                &window.commitment,
                &window.from_slot,
                &window.to_slot,
            ],
        )
        .await
        .map_err(db)?;
    let earliest_pending: Option<i64> = pending.get(0);
    let high_water = high_water_slot(
        window.from_slot,
        window.to_slot,
        enumeration_complete,
        earliest_pending,
    );
    let status = if force_blocked {
        "blocked"
    } else if enumeration_complete && earliest_pending.is_none() {
        "complete"
    } else {
        "in_progress"
    };
    let reason = if status == "complete" {
        None
    } else if keep_reason {
        stopped_reason.map(str::to_owned).or(current.stopped_reason)
    } else {
        stopped_reason.map(str::to_owned)
    };
    transaction
        .execute(
            "UPDATE ingestion_checkpoints
             SET signature_before = $6, enumeration_complete = $7, high_water_slot = $8,
                 status = $9, stopped_reason = $10, updated_at = clock_timestamp()
             WHERE source_id = $1 AND address = $2 AND commitment = $3 AND from_slot = $4 AND to_slot = $5",
            &[
                &window.source_id,
                &window.address,
                &window.commitment,
                &window.from_slot,
                &window.to_slot,
                &signature_before,
                &enumeration_complete,
                &high_water,
                &status,
                &reason,
            ],
        )
        .await
        .map_err(db)?;
    Ok(Checkpoint {
        signature_before,
        enumeration_complete,
        high_water_slot: high_water,
        status: status.to_owned(),
        stopped_reason: reason,
    })
}
