from dataclasses import dataclass

from forwardops.config import Settings
from forwardops.integrations.replay import FixtureSource, ReplayHandlers, load_runbooks
from forwardops.models.openai import OpenAIModelProvider
from forwardops.models.provider import ModelProvider


@dataclass(frozen=True)
class Runtime:
    settings: Settings
    handlers: ReplayHandlers
    model_provider: ModelProvider | None = None


def build_runtime(settings: Settings) -> Runtime:
    source = FixtureSource.load(settings.fixture_dir)
    runbooks = load_runbooks(settings.runbook_dir)
    provider = None
    if settings.analysis_mode == "model" and settings.openai_api_key:
        provider = OpenAIModelProvider(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            base_url=settings.openai_base_url,
            timeout_seconds=settings.model_timeout_seconds,
        )
    return Runtime(
        settings=settings,
        handlers=ReplayHandlers(source, runbooks),
        model_provider=provider,
    )
