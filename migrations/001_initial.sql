-- Phase-1 investigation schema. Runtime roles cannot execute remediation.

CREATE TABLE investigations (
  tenant_id text NOT NULL,
  id uuid NOT NULL,
  requester_id text NOT NULL,
  question text NOT NULL CHECK (char_length(question) BETWEEN 1 AND 2000),
  scope jsonb NOT NULL,
  status text NOT NULL,
  state_version bigint NOT NULL DEFAULT 0 CHECK (state_version >= 0),
  config_digest text NOT NULL,
  playbook_version text NOT NULL,
  analysis_mode text NOT NULL,
  data_mode text NOT NULL,
  hypotheses jsonb NOT NULL DEFAULT '[]'::jsonb,
  timeline jsonb NOT NULL DEFAULT '[]'::jsonb,
  unknowns jsonb NOT NULL DEFAULT '[]'::jsonb,
  recommendations jsonb NOT NULL DEFAULT '[]'::jsonb,
  model_calls jsonb NOT NULL DEFAULT '[]'::jsonb,
  budget jsonb NOT NULL DEFAULT '{}'::jsonb,
  confidence text,
  confidence_basis jsonb NOT NULL DEFAULT '[]'::jsonb,
  root_finding_id uuid,
  idempotency_key text NOT NULL,
  request_digest text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  lease_owner text,
  lease_expires_at timestamptz,
  lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
  failure jsonb,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, requester_id, idempotency_key),
  CHECK (status IN (
    'CREATED', 'PLANNING', 'COLLECTING_EVIDENCE', 'ANALYZING',
    'CONCLUDED', 'INCONCLUSIVE', 'FAILED', 'CANCELLED'
  )),
  CHECK (analysis_mode = 'deterministic'),
  CHECK (data_mode = 'replay'),
  CHECK (confidence IS NULL OR confidence IN ('low', 'medium', 'high'))
);

CREATE INDEX investigations_claimable_idx
  ON investigations (next_attempt_at, lease_expires_at, created_at)
  WHERE status IN ('CREATED', 'PLANNING', 'COLLECTING_EVIDENCE', 'ANALYZING');

CREATE INDEX investigations_tenant_created_idx
  ON investigations (tenant_id, created_at);

CREATE TABLE tool_calls (
  tenant_id text NOT NULL,
  id uuid NOT NULL,
  investigation_id uuid NOT NULL,
  logical_call_id uuid NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  tool_name text NOT NULL,
  tool_version text NOT NULL,
  arguments jsonb NOT NULL,
  arguments_digest text NOT NULL,
  status text NOT NULL,
  output jsonb,
  output_digest text,
  error jsonb,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  deadline_at timestamptz NOT NULL,
  worker_epoch bigint NOT NULL,
  source_id text NOT NULL,
  trace_id text NOT NULL,
  duration_ms integer,
  retryable boolean NOT NULL,
  schema_version integer NOT NULL,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, investigation_id, id),
  UNIQUE (tenant_id, investigation_id, logical_call_id, attempt),
  FOREIGN KEY (tenant_id, investigation_id) REFERENCES investigations (tenant_id, id),
  CHECK (status IN ('STARTED', 'SUCCEEDED', 'FAILED', 'ABANDONED'))
);

CREATE INDEX tool_calls_investigation_idx
  ON tool_calls (tenant_id, investigation_id, started_at);

CREATE TABLE evidence (
  tenant_id text NOT NULL,
  id uuid NOT NULL,
  investigation_id uuid NOT NULL,
  tool_call_id uuid NOT NULL,
  kind text NOT NULL,
  source_type text NOT NULL,
  source_system text NOT NULL,
  source_locator jsonb NOT NULL,
  event_time timestamptz,
  observed_at timestamptz NOT NULL,
  retrieved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  time_basis text NOT NULL,
  correlation jsonb NOT NULL,
  payload jsonb NOT NULL,
  summary text NOT NULL CHECK (char_length(summary) BETWEEN 1 AND 1000),
  provenance jsonb NOT NULL,
  coverage jsonb NOT NULL,
  schema_version integer NOT NULL,
  payload_sha256 text NOT NULL,
  redaction_version text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, investigation_id, id),
  FOREIGN KEY (tenant_id, investigation_id, tool_call_id)
    REFERENCES tool_calls (tenant_id, investigation_id, id)
);

CREATE INDEX evidence_investigation_idx
  ON evidence (tenant_id, investigation_id, event_time);

CREATE TABLE findings (
  tenant_id text NOT NULL,
  id uuid NOT NULL,
  investigation_id uuid NOT NULL,
  classification text NOT NULL,
  claim text NOT NULL CHECK (char_length(claim) BETWEEN 1 AND 2000),
  component_ref text,
  evidence_refs jsonb NOT NULL,
  derivation jsonb,
  confidence text,
  confidence_basis jsonb NOT NULL DEFAULT '[]'::jsonb,
  alternatives jsonb NOT NULL DEFAULT '[]'::jsonb,
  limitations jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, investigation_id, id),
  FOREIGN KEY (tenant_id, investigation_id) REFERENCES investigations (tenant_id, id),
  CHECK (classification IN ('FACT', 'INFERENCE', 'UNKNOWN')),
  CHECK (confidence IS NULL OR confidence IN ('low', 'medium', 'high'))
);

CREATE INDEX findings_investigation_idx
  ON findings (tenant_id, investigation_id);

ALTER TABLE investigations
  ADD CONSTRAINT investigations_root_finding_fk
  FOREIGN KEY (tenant_id, id, root_finding_id)
  REFERENCES findings (tenant_id, investigation_id, id);

CREATE TABLE action_proposals (
  tenant_id text NOT NULL,
  id uuid NOT NULL,
  investigation_id uuid NOT NULL,
  requester_id text NOT NULL,
  action_type text NOT NULL,
  target_ref text NOT NULL,
  parameters jsonb NOT NULL,
  reason text NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 2000),
  evidence_refs jsonb NOT NULL,
  risk text NOT NULL,
  status text NOT NULL,
  proposal_digest text NOT NULL,
  policy_version text NOT NULL,
  config_digest text NOT NULL,
  preconditions jsonb NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  execution_enabled boolean NOT NULL DEFAULT false CHECK (execution_enabled = false),
  execution_key uuid NOT NULL UNIQUE,
  execution_result jsonb,
  execution_started_at timestamptz,
  execution_finished_at timestamptz,
  state_version bigint NOT NULL DEFAULT 1 CHECK (state_version >= 0),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, investigation_id, id),
  FOREIGN KEY (tenant_id, investigation_id) REFERENCES investigations (tenant_id, id),
  CHECK (risk IN ('low', 'medium', 'high')),
  CHECK (status IN (
    'WAITING_FOR_APPROVAL', 'APPROVED', 'REJECTED', 'EXPIRED', 'CANCELLED'
  )),
  CONSTRAINT action_proposals_no_execution CHECK (
    execution_result IS NULL
    AND execution_started_at IS NULL
    AND execution_finished_at IS NULL
  )
);

CREATE INDEX action_proposals_status_idx
  ON action_proposals (tenant_id, status, created_at);

CREATE TABLE approvals (
  tenant_id text NOT NULL,
  id uuid NOT NULL,
  investigation_id uuid NOT NULL,
  action_id uuid NOT NULL,
  decision text NOT NULL,
  approver_id text NOT NULL,
  proposal_digest text NOT NULL,
  policy_version text NOT NULL,
  reason text NOT NULL CHECK (char_length(reason) BETWEEN 1 AND 2000),
  idempotency_key text NOT NULL,
  request_digest text NOT NULL,
  decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, action_id),
  UNIQUE (tenant_id, approver_id, idempotency_key),
  FOREIGN KEY (tenant_id, investigation_id, action_id)
    REFERENCES action_proposals (tenant_id, investigation_id, id),
  CHECK (decision IN ('APPROVE', 'REJECT'))
);

CREATE TABLE audit_events (
  tenant_id text NOT NULL,
  id uuid NOT NULL,
  investigation_id uuid,
  action_id uuid,
  actor_id text NOT NULL,
  actor_kind text NOT NULL,
  event_type text NOT NULL,
  occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  request_id text NOT NULL,
  correlation_id text NOT NULL,
  trace_id text,
  entity_type text NOT NULL,
  entity_id uuid,
  entity_version bigint,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, investigation_id) REFERENCES investigations (tenant_id, id),
  FOREIGN KEY (tenant_id, investigation_id, action_id)
    REFERENCES action_proposals (tenant_id, investigation_id, id),
  CHECK (actor_kind IN ('user', 'worker'))
);

CREATE INDEX audit_events_time_idx ON audit_events (tenant_id, occurred_at);
CREATE INDEX audit_events_investigation_idx ON audit_events (tenant_id, investigation_id);

ALTER TABLE investigations ENABLE ROW LEVEL SECURITY;
ALTER TABLE investigations FORCE ROW LEVEL SECURITY;
ALTER TABLE tool_calls ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_calls FORCE ROW LEVEL SECURITY;
ALTER TABLE evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE evidence FORCE ROW LEVEL SECURITY;
ALTER TABLE findings ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings FORCE ROW LEVEL SECURITY;
ALTER TABLE action_proposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE action_proposals FORCE ROW LEVEL SECURITY;
ALTER TABLE approvals ENABLE ROW LEVEL SECURITY;
ALTER TABLE approvals FORCE ROW LEVEL SECURITY;
ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_events FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON investigations
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON tool_calls
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON evidence
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON findings
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON action_proposals
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON approvals
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_isolation ON audit_events
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

GRANT USAGE ON SCHEMA public TO forwardops_app;
GRANT SELECT, INSERT, UPDATE ON investigations, tool_calls, action_proposals TO forwardops_app;
GRANT SELECT, INSERT ON evidence, findings, approvals, audit_events TO forwardops_app;

CREATE FUNCTION claim_investigation(p_owner text, p_lease_seconds integer)
RETURNS TABLE (tenant_id text, id uuid, lease_epoch bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
  claimed_tenant text;
  claimed_id uuid;
BEGIN
  IF p_owner IS NULL OR length(p_owner) < 1 OR p_lease_seconds < 1 OR p_lease_seconds > 3600 THEN
    RAISE EXCEPTION 'invalid claim arguments';
  END IF;

  SELECT investigation.tenant_id, investigation.id
  INTO claimed_tenant, claimed_id
  FROM public.investigations AS investigation
  WHERE investigation.status IN ('CREATED', 'PLANNING', 'COLLECTING_EVIDENCE', 'ANALYZING')
    AND investigation.next_attempt_at <= clock_timestamp()
    AND (
      investigation.lease_expires_at IS NULL
      OR investigation.lease_expires_at <= clock_timestamp()
    )
  ORDER BY investigation.next_attempt_at, investigation.created_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  RETURN QUERY
  UPDATE public.investigations AS investigation
  SET lease_owner = p_owner,
      lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds),
      lease_epoch = investigation.lease_epoch + 1,
      updated_at = clock_timestamp()
  WHERE investigation.tenant_id = claimed_tenant
    AND investigation.id = claimed_id
  RETURNING investigation.tenant_id, investigation.id, investigation.lease_epoch;
END;
$$;

REVOKE ALL ON FUNCTION claim_investigation(text, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION claim_investigation(text, integer) TO forwardops_app;
