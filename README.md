# ForwardOps

ForwardOps is a customer-hosted service that investigates an operational question, keeps the evidence, and records a human decision before any remediation. This repository is the first offline slice: a fictional vault whose withdrawals fail because its oracle value is stale.

The slice is not a production deployment. Authentication is a development token file, source data is synthetic, and approving an action does not run it.

## What runs today

One Python package serves two processes:

- **API** (`forwardops-api`) accepts an investigation, reads its result, and records approval or rejection.
- **Worker** (`forwardops-worker`) claims the investigation from PostgreSQL, runs the withdrawal playbook, and writes the result back.

PostgreSQL stores investigations, tool calls, evidence, findings, action proposals, approvals, and audit events. The worker claims a row with `FOR UPDATE SKIP LOCKED` and a lease, so the API does not investigate inside the request.

The playbook is deterministic. It calls typed tools through one gateway:

1. Recent withdrawal failures
2. The three sampled failed transactions
3. Correlated application logs
4. Current vault state and its oracle binding
5. Current oracle state
6. The stale-oracle runbook

`get_recent_deployments` is registered and is not used for this incident. Freshness is calculated in application code as oracle age strictly greater than the configured maximum. Equality is fresh. The canonical sample produces `600 > 60`, `620 > 60`, and `660 > 60` from fixture timestamps, not from a stored conclusion.

Findings are `FACT`, `INFERENCE`, or `UNKNOWN`. Facts and inferences cite evidence. The publisher outage remains unknown. The only proposal is `restart_oracle_updater`, and it stays pending until a different person approves or rejects it. `execution_enabled` is false. There is no executor.

## Stale-oracle demo

Customer A is synthetic. In the frozen window `2026-09-26T12:05:00Z` to `2026-09-26T12:12:00Z` there are 10 withdrawal attempts and 8 failures. The previous window has 100 attempts and 0 failures. Three failed withdrawals, at 12:10:00, 12:10:20, and 12:11:00 UTC, carry program logs. Those logs say the oracle was last updated at 12:00:00 UTC and the maximum age is 60 seconds. The current oracle snapshot is still that old update. It does not by itself prove the historical failure; the program execution clocks do.

## Local quickstart

Docker Compose runs PostgreSQL, the API, and the worker from one image:

```bash
docker compose -f deploy/compose.yaml up --build
```

If port 5432 is already in use, publish Postgres elsewhere:

```bash
FORWARDOPS_POSTGRES_PORT=5433 docker compose -f deploy/compose.yaml up --build
```

The API listens on `http://localhost:8000`. Development tokens live in `examples/customer-a/dev-identities.yaml`.

```bash
curl -sS -D - -X POST http://localhost:8000/investigations \
  -H 'Authorization: Bearer dev-investigator' \
  -H 'Idempotency-Key: demo-1' \
  -H 'Content-Type: application/json' \
  -d '{"question":"Why are vault withdrawals failing?"}'
```

The response is `202 Accepted` with status `CREATED`. Poll until the worker finishes:

```bash
curl -sS http://localhost:8000/investigations/<id> \
  -H 'Authorization: Bearer dev-investigator'
```

A concluded investigation looks like this:

```json
{
  "status": "CONCLUDED",
  "analysis_mode": "deterministic",
  "data_mode": "replay",
  "root_cause_hypothesis": {
    "component": "oracle-a",
    "cause": "stale_oracle",
    "scope": "3 sampled failed withdrawals"
  },
  "confidence": "high",
  "unknowns": [
    "Why the oracle publisher stopped updating is unknown."
  ],
  "recommended_remediation": "Follow runbook oracle-staleness v1: verify oracle updater oracle-updater-a and its RPC dependency, restart that mapped updater only after human approval, verify a fresh oracle publication, and assess withdrawal retry separately. Do not disable freshness checks, invent a price, or bypass authorization.",
  "pending_actions": [
    {
      "action_type": "restart_oracle_updater",
      "status": "WAITING_FOR_APPROVAL",
      "execution_enabled": false,
      "execution_status": "NOT_ENABLED"
    }
  ]
}
```

The real payload also includes the timeline, evidence, findings, and confidence basis. Findings cite evidence ids. `GET /investigations/<id>/evidence` returns the stored records.

Approve with a different identity. Approval records the decision and does not restart anything:

```bash
curl -sS -X POST http://localhost:8000/actions/<action-id>/approve \
  -H 'Authorization: Bearer dev-approver' \
  -H 'Idempotency-Key: approve-1' \
  -H 'Content-Type: application/json' \
  -d '{"proposal_digest":"<digest-from-the-action>","reason":"Evidence supports a conditional restart."}'
```

The response status is `APPROVED`, `execution_status` is `NOT_ENABLED`, and the message says no remediation was executed. Reject with `POST /actions/<id>/reject`. The requester cannot approve their own proposal.

## Safety boundary

- Source fixtures and the runbook are labeled synthetic. Log text and runbook prose are evidence. They cannot change tool choice, disable freshness checks, or invent a price.
- Tools accept only typed arguments and configured resource ids. There is no SQL console, HTTP client, or shell tool.
- The database role used by the API cannot update evidence, findings, approvals, or audit rows.
- Action rows cannot store an execution result. `execution_enabled` must stay false.
- There is no route that restarts an updater.
- `FORWARDOPS_ENVIRONMENT=production` refuses to start, because this build only has development tokens.

## Tests

Unit tests do not need Postgres. Integration tests and the evaluation do. They create `forwardops_test` and `forwardops_eval` using an admin URL:

```bash
uv sync --all-extras
export TEST_DATABASE_ADMIN_URL=postgresql://forwardops:forwardops@localhost:5432/postgres
uv run pytest
uv run python -m evals.runner
```

With Compose Postgres published on another port:

```bash
export TEST_DATABASE_ADMIN_URL=postgresql://forwardops:forwardops@localhost:5433/postgres
```

`uv run ruff check src tests evals` and `uv run ruff format --check src tests evals` match CI. No LLM key and no network call are required for the investigation.

## Later work

Not in this repository yet:

- Live Solana RPC, application logs, and withdrawal queries
- A model provider participating inside the same tool gateway
- Production OIDC and deployment hardening
- An executor process with its own identity
- Bounded Rust ingestion
- Kubernetes manifests
- Semantic retrieval

Those stay future work until they have the same evidence, approval, and test boundaries as this slice.
