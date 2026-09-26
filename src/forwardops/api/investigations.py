from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse

from forwardops.api.auth import (
    Principal,
    authenticate,
    request_id_from_header,
    require_any_role,
    require_idempotency_key,
)
from forwardops.api.schemas import (
    CreateInvestigationBody,
    EvidenceView,
    InvestigationView,
    action_view,
    evidence_view,
    finding_view,
)
from forwardops.application.investigations import create_investigation
from forwardops.domain.investigation import InvestigationScope
from forwardops.storage.postgres import (
    Database,
    approval_for_action,
    get_investigation,
    list_actions,
    list_evidence,
    list_findings,
)

router = APIRouter()
_EVIDENCE_LIMIT = 50


def _principal(request: Request, authorization: str | None) -> Principal:
    return authenticate(request.app.state.settings, authorization)


@router.post("/investigations", status_code=202)
async def post_investigation(
    body: CreateInvestigationBody,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    x_request_id: Annotated[str | None, Header(alias="X-Request-ID")] = None,
) -> JSONResponse:
    principal = _principal(request, authorization)
    require_any_role(principal, "investigator")
    if idempotency_key is None:
        from forwardops.api.auth import InvalidRequestError

        raise InvalidRequestError("Idempotency-Key is required")
    key = require_idempotency_key(idempotency_key)
    database = Database(request.app.state.pool)
    record, _replayed = await create_investigation(
        database,
        request.app.state.settings,
        principal_id=principal.principal_id,
        tenant_id=principal.tenant_id,
        roles=principal.roles,
        question=body.question,
        idempotency_key=key,
        request_id=request_id_from_header(x_request_id),
    )
    scope = InvestigationScope.model_validate(record.scope)
    payload = {
        "id": str(record.id),
        "status": record.status,
        "question": record.question,
        "analysis_mode": record.analysis_mode,
        "data_mode": record.data_mode,
        "resolved_scope": scope.model_dump(mode="json"),
    }
    return JSONResponse(
        status_code=202,
        content=payload,
        headers={"Location": f"/investigations/{record.id}"},
    )


@router.get("/investigations/{investigation_id}", response_model=InvestigationView)
async def get_investigation_view(
    investigation_id: UUID,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> InvestigationView:
    principal = _principal(request, authorization)
    require_any_role(principal, "investigator", "approver")
    database = Database(request.app.state.pool)
    async with database.transaction(principal.tenant_id) as conn:
        record = await get_investigation(conn, principal.tenant_id, investigation_id)
        if record is None:
            from forwardops.domain.errors import NotFoundError

            raise NotFoundError("investigation not found")
        findings = await list_findings(conn, principal.tenant_id, investigation_id)
        evidence = await list_evidence(
            conn,
            principal.tenant_id,
            investigation_id,
            limit=_EVIDENCE_LIMIT + 1,
        )
        actions = await list_actions(conn, principal.tenant_id, investigation_id)
        approvals = []
        for action in actions:
            approvals.append(await approval_for_action(conn, principal.tenant_id, action["id"]))
    truncated = len(evidence) > _EVIDENCE_LIMIT
    evidence = evidence[:_EVIDENCE_LIMIT]
    action_views = [
        action_view(action, approval) for action, approval in zip(actions, approvals, strict=True)
    ]
    root = next((row for row in findings if row["id"] == record.root_finding_id), None)
    hypothesis = None
    if root is not None and root["classification"] == "INFERENCE":
        derivation = root["derivation"] or {}
        if derivation.get("cause") == "stale_oracle":
            hypothesis = {
                "component": root["component_ref"],
                "cause": "stale_oracle",
                "claim": root["claim"],
                "scope": derivation.get("scope"),
                "finding_id": root["id"],
                "evidence_ids": [
                    ref["evidence_id"]
                    for ref in root["evidence_refs"]
                    if ref.get("relation") == "supports"
                ],
            }
    recommendation = None
    if record.recommendations:
        recommendation = record.recommendations[0].get("summary")
    return InvestigationView(
        id=record.id,
        status=record.status,
        question=record.question,
        analysis_mode=record.analysis_mode,
        data_mode=record.data_mode,
        scope=record.scope,
        timeline=record.timeline,
        evidence=[evidence_view(row) for row in evidence],
        evidence_truncated=truncated,
        findings=[finding_view(row) for row in findings],
        root_cause_hypothesis=hypothesis,
        confidence=record.confidence,
        confidence_basis=record.confidence_basis,
        unknowns=record.unknowns,
        recommended_remediation=recommendation,
        pending_actions=[item for item in action_views if item.status == "WAITING_FOR_APPROVAL"],
        actions=action_views,
    )


@router.get("/investigations/{investigation_id}/evidence")
async def get_evidence(
    investigation_id: UUID,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    evidence_id: Annotated[UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, object]:
    principal = _principal(request, authorization)
    require_any_role(principal, "investigator", "approver")
    database = Database(request.app.state.pool)
    async with database.transaction(principal.tenant_id) as conn:
        record = await get_investigation(conn, principal.tenant_id, investigation_id)
        if record is None:
            from forwardops.domain.errors import NotFoundError

            raise NotFoundError("investigation not found")
        rows = await list_evidence(
            conn,
            principal.tenant_id,
            investigation_id,
            evidence_id=evidence_id,
            limit=limit + 1,
            offset=offset,
        )
    truncated = len(rows) > limit
    items: list[EvidenceView] = [evidence_view(row) for row in rows[:limit]]
    return {
        "items": [item.model_dump(mode="json") for item in items],
        "limit": limit,
        "offset": offset,
        "truncated": truncated,
    }
