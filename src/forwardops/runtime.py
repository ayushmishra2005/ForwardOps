from dataclasses import dataclass

from forwardops.config import Settings
from forwardops.integrations.replay import FixtureSource, ReplayHandlers, load_runbooks


@dataclass(frozen=True)
class Runtime:
    settings: Settings
    handlers: ReplayHandlers


def build_runtime(settings: Settings) -> Runtime:
    source = FixtureSource.load(settings.fixture_dir)
    runbooks = load_runbooks(settings.runbook_dir)
    return Runtime(settings=settings, handlers=ReplayHandlers(source, runbooks))
