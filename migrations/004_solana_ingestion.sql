-- Bounded Solana backfill state. These tables are not investigation evidence.
-- The Python tool gateway does not read them while it is answering an investigation.

CREATE TABLE source_records (
  source_id text NOT NULL,
  cluster_id text NOT NULL,
  signature text NOT NULL,
  record_identity text NOT NULL,
  decoder_version text NOT NULL,
  address text NOT NULL,
  slot bigint NOT NULL CHECK (slot >= 0),
  block_time timestamptz,
  commitment text NOT NULL,
  outcome text NOT NULL,
  program_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  instruction_errors jsonb NOT NULL DEFAULT '[]'::jsonb,
  log_messages jsonb NOT NULL DEFAULT '[]'::jsonb,
  logs_truncated boolean NOT NULL DEFAULT false,
  payload_sha256 text,
  gap_reason text,
  ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (source_id, cluster_id, signature, record_identity, decoder_version),
  CHECK (outcome IN ('stored', 'not_found', 'malformed', 'unsupported_version')),
  CHECK (commitment IN ('processed', 'confirmed', 'finalized')),
  CHECK (
    (outcome = 'stored' AND payload_sha256 IS NOT NULL AND gap_reason IS NULL)
    OR (outcome <> 'stored' AND gap_reason IS NOT NULL)
  )
);

CREATE INDEX source_records_slot_idx
  ON source_records (source_id, slot, signature);

CREATE TABLE ingestion_signatures (
  source_id text NOT NULL,
  address text NOT NULL,
  commitment text NOT NULL,
  from_slot bigint NOT NULL,
  to_slot bigint NOT NULL,
  signature text NOT NULL,
  slot bigint NOT NULL CHECK (slot >= 0),
  block_time_unix bigint,
  fetch_status text NOT NULL,
  PRIMARY KEY (source_id, address, commitment, from_slot, to_slot, signature),
  CHECK (from_slot <= to_slot),
  CHECK (fetch_status IN ('pending', 'stored', 'gap')),
  CHECK (commitment IN ('processed', 'confirmed', 'finalized'))
);

CREATE INDEX ingestion_signatures_pending_idx
  ON ingestion_signatures (source_id, address, commitment, from_slot, to_slot, slot)
  WHERE fetch_status = 'pending';

CREATE TABLE ingestion_checkpoints (
  source_id text NOT NULL,
  address text NOT NULL,
  commitment text NOT NULL,
  from_slot bigint NOT NULL,
  to_slot bigint NOT NULL,
  signature_before text,
  enumeration_complete boolean NOT NULL DEFAULT false,
  high_water_slot bigint,
  status text NOT NULL,
  stopped_reason text,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (source_id, address, commitment, from_slot, to_slot),
  CHECK (from_slot <= to_slot),
  CHECK (status IN ('in_progress', 'complete', 'blocked')),
  CHECK (commitment IN ('processed', 'confirmed', 'finalized')),
  CHECK (high_water_slot IS NULL OR high_water_slot >= from_slot - 1)
);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'forwardops_app') THEN
    GRANT SELECT ON source_records, ingestion_signatures, ingestion_checkpoints TO forwardops_app;
  END IF;
END
$$;
