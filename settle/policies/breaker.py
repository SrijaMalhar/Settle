"""A circuit breaker per PSP that only counts the PSP's own failures.

Closed: calls flow. After `threshold` PSP-side failures in a row it opens for `cooldown`
seconds. When that time is over, the next attempt is the half-open trial: a success closes
the breaker, a failure opens it again. A wrong PIN or a customer's bank being down never
extends the streak (Settle tells them apart from the NPCI code before counting).

The open state lives in the HealthStore, so with Redis a breaker opened by any shard protects
every shard (a key that expires by itself); with the local store it protects this process.
"""

from __future__ import annotations

from enum import StrEnum

from settle.policies.routing import blame
from settle.ports import HealthStore


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, psp: str, shared: HealthStore, threshold: int = 5, cooldown: float = 30.0) -> None:
        self.psp, self.shared, self.threshold, self.cooldown = psp, shared, threshold, cooldown
        self.state = BreakerState.CLOSED
        self.streak = 0
        self.trial_in_flight = False

    def available(self, now: float) -> bool:
        """May a new attempt go to this PSP right now?"""
        if self.shared.breaker_open(self.psp, now):
            self.state = BreakerState.OPEN
            return False
        if self.state is BreakerState.OPEN:  # the open period is over: allow one trial
            self.state = BreakerState.HALF_OPEN
            self.trial_in_flight = False
        if self.state is BreakerState.HALF_OPEN:
            return not self.trial_in_flight
        return True

    def on_attempt(self, now: float) -> None:
        """An attempt was sent here. In half-open state it is the trial."""
        if self.state is BreakerState.HALF_OPEN:
            self.trial_in_flight = True

    def on_success(self, now: float) -> None:
        self.state = BreakerState.CLOSED
        self.streak = 0
        self.trial_in_flight = False

    def on_failure(self, code: str | None, now: float) -> None:
        if blame(code) != "psp":
            return  # the customer's or their bank's doing
        if self.state is BreakerState.HALF_OPEN:
            self._open(now)
            return
        self.streak += 1
        if self.streak >= self.threshold:
            self._open(now)

    def _open(self, now: float) -> None:
        self.state = BreakerState.OPEN
        self.streak = 0
        self.trial_in_flight = False
        self.shared.open_breaker(self.psp, self.cooldown, now)
