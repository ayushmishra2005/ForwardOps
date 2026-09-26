-- Allow model-assisted investigations. Deterministic mode remains valid.

DO $$
DECLARE
  constraint_name text;
BEGIN
  SELECT con.conname
  INTO constraint_name
  FROM pg_constraint AS con
  WHERE con.conrelid = 'public.investigations'::regclass
    AND con.contype = 'c'
    AND pg_get_constraintdef(con.oid) ILIKE '%analysis_mode%';
  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE investigations DROP CONSTRAINT %I', constraint_name);
  END IF;
END
$$;

ALTER TABLE investigations
  ADD CONSTRAINT investigations_analysis_mode_check
  CHECK (analysis_mode IN ('deterministic', 'model'));
