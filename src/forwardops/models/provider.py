from typing import Protocol

from forwardops.models.contracts import ModelReply, ModelRequest


class ModelProvider(Protocol):
    """A model that proposes tool calls or analysis and never executes either."""

    provider_name: str
    model_name: str

    async def complete(self, request: ModelRequest) -> ModelReply:
        """Return one provider-neutral reply for this investigation state."""
