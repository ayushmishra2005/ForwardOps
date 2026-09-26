-- Allow investigations whose evidence comes from a customer PostgreSQL source.
-- Replay investigations remain valid. This does not grant the customer source
-- access to the ForwardOps platform schema.

DO $$
DECLARE
  constraint_name text;
BEGIN
  SELECT con.conname
  INTO constraint_name
  FROM pg_constraint AS con
  WHERE con.conrelid = 'public.investigations'::regclass
    AND con.contype = 'c'
    AND pg_get_constraintdef(con.oid) ILIKE '%data_mode%'
    AND pg_get_constraintdef(con.oid) NOT ILIKE '%analysis_mode%';
  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE investigations DROP CONSTRAINT %I', constraint_name);
  END IF;
END
$$;

ALTER TABLE investigations
  ADD CONSTRAINT investigations_data_mode_check
  CHECK (data_mode IN ('replay', 'customer_postgres'));
