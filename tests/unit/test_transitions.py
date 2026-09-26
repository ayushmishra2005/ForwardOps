import pytest

from forwardops.domain.errors import InvalidTransitionError
from forwardops.domain.investigation import InvestigationStatus, assert_transition


def test_happy_path_transitions() -> None:
    assert_transition(InvestigationStatus.CREATED, InvestigationStatus.PLANNING)
    assert_transition(InvestigationStatus.PLANNING, InvestigationStatus.COLLECTING_EVIDENCE)
    assert_transition(InvestigationStatus.COLLECTING_EVIDENCE, InvestigationStatus.ANALYZING)
    assert_transition(InvestigationStatus.ANALYZING, InvestigationStatus.CONCLUDED)


def test_failure_and_inconclusive_are_reachable() -> None:
    assert_transition(InvestigationStatus.COLLECTING_EVIDENCE, InvestigationStatus.INCONCLUSIVE)
    assert_transition(InvestigationStatus.ANALYZING, InvestigationStatus.FAILED)
    assert_transition(InvestigationStatus.CREATED, InvestigationStatus.INCONCLUSIVE)


def test_skipping_to_concluded_is_rejected() -> None:
    with pytest.raises(InvalidTransitionError):
        assert_transition(InvestigationStatus.CREATED, InvestigationStatus.CONCLUDED)


def test_terminal_state_does_not_resume() -> None:
    with pytest.raises(InvalidTransitionError):
        assert_transition(InvestigationStatus.CONCLUDED, InvestigationStatus.ANALYZING)
    with pytest.raises(InvalidTransitionError):
        assert_transition(InvestigationStatus.FAILED, InvestigationStatus.PLANNING)
