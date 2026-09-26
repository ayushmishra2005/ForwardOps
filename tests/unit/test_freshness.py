from datetime import UTC, datetime, timedelta

from forwardops.domain.freshness import age_seconds, compare_oracle_age


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 26, hour, minute, second, tzinfo=UTC)


def test_age_above_maximum_is_stale() -> None:
    comparison = compare_oracle_age(_at(12, 10), _at(12, 0), 60)
    assert comparison is not None
    assert comparison.age_seconds == 600
    assert comparison.stale is True
    assert comparison.expression == "600 > 60"


def test_equality_boundary_is_fresh() -> None:
    last_update = _at(12, 0)
    comparison = compare_oracle_age(last_update + timedelta(seconds=60), last_update, 60)
    assert comparison is not None
    assert comparison.age_seconds == 60
    assert comparison.stale is False
    assert comparison.expression == "60 > 60"


def test_age_below_maximum_is_fresh() -> None:
    comparison = compare_oracle_age(_at(12, 0, 59), _at(12, 0), 60)
    assert comparison is not None
    assert comparison.stale is False


def test_future_update_is_unknown() -> None:
    assert age_seconds(_at(12, 0), _at(12, 10)) is None
    assert compare_oracle_age(_at(12, 0), _at(12, 10), 60) is None


def test_naive_timestamps_are_unknown() -> None:
    naive = datetime(2026, 9, 26, 12, 10)
    assert age_seconds(naive, _at(12, 0)) is None


def test_negative_maximum_is_unknown() -> None:
    assert compare_oracle_age(_at(12, 10), _at(12, 0), -1) is None
