from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Request

from forwardops.api.auth import (
    Principal,
    authenticate,
    request_id_from_header,
    require_any_role,
    require_idempotency_key,
)
from forwardops.api.schemas import ActionView, DecisionBody, action_view
from forwardops.application.approve import decide_action
from forwardops.domain.actions import DecisionKind
from forwardops.domain.errors import NotFoundError
from forwardops.storage.postgres import Database, approval_for_action, get_action

router = APIRouter()


def _principal(request: Request, authorization: str | None) -> Principal:
    return authenticate(request.app.state.settings, authorization)


@router.get("/actions/{action_id}", response_model=ActionView)
async def get_action_view(
    action_id: UUID,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> ActionView:
    principal = _principal(request, authorization)
    require_any_role(principal, "investigator", "approver")
    database = Database(request.app.state.pool)
    async with database.transaction(principal.tenant_id) as conn:
        action = await get_action(conn, principal.tenant_id, action_id)
        if action is None:
            raise NotFoundError("action not found")
        approval = await approval_for_action(conn, principal.tenant_id, action_id)
    return action_view(action, approval)


@router.post("/actions/{action_id}/approve", response_model=ActionView)
async def approve_action(
    action_id: UUID,
    body: DecisionBody,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    x_request_id: Annotated[str | None, Header(alias="X-Request-ID")] = None,
) -> ActionView:
    return await _decide(
        action_id, body, request, authorization, idempotency_key, x_request_id, DecisionKind.APPROVE
    )


@router.post("/actions/{action_id}/reject", response_model=ActionView)
async def reject_action(
    action_id: UUID,
    body: DecisionBody,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    x_request_id: Annotated[str | None, Header(alias="X-Request-ID")] = None,
) -> ActionView:
    return await _decide(
        action_id, body, request, authorization, idempotency_key, x_request_id, DecisionKind.REJECT
    )


async def _decide(
    action_id: UUID,
    body: DecisionBody,
    request: Request,
    authorization: str | None,
    idempotency_key: str | None,
    x_request_id: str | None,
    decision: DecisionKind,
) -> ActionView:
    principal = _principal(request, authorization)
    require_any_role(principal, "approver")
    if idempotency_key is None:
        from forwardops.api.auth import InvalidRequestError

        raise InvalidRequestError("Idempotency-Key is required")
    result = await decide_action(
        Database(request.app.state.pool),
        tenant_id=principal.tenant_id,
        principal_id=principal.principal_id,
        roles=principal.roles,
        action_id=action_id,
        decision=decision,
        proposal_digest=body.proposal_digest,
        reason=body.reason,
        idempotency_key=require_idempotency_key(idempotency_key),
        request_id=request_id_from_header(x_request_id),
    )
    message = (
        "Approval was recorded. No remediation was executed."
        if decision is DecisionKind.APPROVE
        else "Rejection was recorded. No remediation was executed."
    )
    return action_view(result.action, result.approval, message)
