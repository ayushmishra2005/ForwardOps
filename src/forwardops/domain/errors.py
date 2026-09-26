class ForwardOpsError(Exception):
    """Expected application failure."""


class NotFoundError(ForwardOpsError):
    pass


class PermissionDeniedError(ForwardOpsError):
    pass


class ConflictError(ForwardOpsError):
    pass


class IdempotencyConflictError(ConflictError):
    pass


class SelfApprovalError(PermissionDeniedError):
    pass


class DigestMismatchError(ConflictError):
    pass


class ProposalExpiredError(ConflictError):
    pass


class InvalidTransitionError(ConflictError):
    pass


class InvalidFindingError(ForwardOpsError):
    pass


class LostLeaseError(ForwardOpsError):
    pass


class SuspendedError(ForwardOpsError):
    """The worker released its lease before the investigation reached a terminal state."""


class ConfigError(ForwardOpsError):
    pass


class ToolFailedError(ForwardOpsError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class ModelError(ForwardOpsError):
    """A provider failed or returned output the application will not use."""

    def __init__(self, category: str, message: str) -> None:
        super().__init__(message)
        self.category = category
