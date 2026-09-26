import hmac
import re
from dataclasses import dataclass

from forwardops.config import Settings
from forwardops.domain.errors import PermissionDeniedError

_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class AuthenticationError(Exception):
    pass


class InvalidRequestError(Exception):
    pass


@dataclass(frozen=True)
class Principal:
    principal_id: str
    tenant_id: str
    roles: frozenset[str]


def authenticate(settings: Settings, authorization: str | None) -> Principal:
    if authorization is None or not authorization.startswith("Bearer "):
        raise AuthenticationError
    token = authorization.removeprefix("Bearer ").strip()
    for identity in settings.identities:
        if _equals(identity.token, token):
            return Principal(identity.principal_id, identity.tenant_id, frozenset(identity.roles))
    raise AuthenticationError


def require_idempotency_key(value: str) -> str:
    if _IDEMPOTENCY_KEY.fullmatch(value) is None:
        raise InvalidRequestError("Idempotency-Key is invalid")
    return value


def request_id_from_header(value: str | None) -> str:
    from uuid import uuid4

    if value and _REQUEST_ID.fullmatch(value):
        return value
    return str(uuid4())


def require_any_role(principal: Principal, *roles: str) -> None:
    if not any(role in principal.roles for role in roles):
        raise PermissionDeniedError(f"requires role: {', '.join(roles)}")


def _equals(left: str, right: str) -> bool:
    left_bytes = left.encode()
    right_bytes = right.encode()
    if len(left_bytes) != len(right_bytes):
        return False
    return hmac.compare_digest(left_bytes, right_bytes)
