from dataclasses import dataclass
from datetime import datetime

from forwardops.domain.time import require_aware


@dataclass(frozen=True)
class FreshnessComparison:
    age_seconds: int
    max_age_seconds: int
    stale: bool

    @property
    def expression(self) -> str:
        return f"{self.age_seconds} > {self.max_age_seconds}"


def age_seconds(execution_clock: datetime, last_update: datetime) -> int | None:
    """Return the age in whole seconds, or None when the inputs cannot be compared."""
    try:
        execution = require_aware(execution_clock)
        updated = require_aware(last_update)
    except ValueError:
        return None
    delta = (execution - updated).total_seconds()
    if delta < 0 or delta != int(delta):
        return None
    return int(delta)


def compare_oracle_age(
    execution_clock: datetime,
    last_update: datetime,
    max_age_seconds: int,
) -> FreshnessComparison | None:
    """Equality is fresh. Only age strictly greater than the maximum is stale."""
    if max_age_seconds < 0:
        return None
    age = age_seconds(execution_clock, last_update)
    if age is None:
        return None
    return FreshnessComparison(
        age_seconds=age,
        max_age_seconds=max_age_seconds,
        stale=age > max_age_seconds,
    )
