"""Provider-neutral model boundary.

Provider SDK objects stay inside the adapter that implements ModelProvider.
"""

from forwardops.models.contracts import ModelReply, ModelRequest
from forwardops.models.openai import OpenAIModelProvider

__all__ = ["ModelReply", "ModelRequest", "OpenAIModelProvider"]
