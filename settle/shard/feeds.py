"""Observers that teach the router, the breakers and the check planner from stored events.

They are added to the engine as subscribers, so learning never touches decide() or the engine.
"""

from __future__ import annotations

from settle.core.messages import AttemptMoved, Event
from settle.policies.breaker import CircuitBreaker
from settle.policies.checks import CheckPlanner
from settle.policies.routing import Router

# Signals that say the PSP itself is in trouble (if the code agrees): they extend the breaker's streak.
TROUBLE = {"read_timeout", "deemed", "not_delivered", "checks_exhausted", "declined", "failure"}
FINE = {"accepted", "success"}


class HealthFeed:
    """Router scores and circuit breakers, from every attempt transition."""

    def __init__(self, router: Router, breakers: dict[str, CircuitBreaker]) -> None:
        self.router, self.breakers = router, breakers

    def on_event(self, payment_id: str, event: Event) -> None:
        if not isinstance(event, AttemptMoved):
            return
        if event.outcome is not None:
            self.router.record(event.psp, event.outcome, event.code, event.at)
        breaker = self.breakers.get(event.psp)
        if breaker is None:
            return
        if event.signal in FINE:
            breaker.on_success(event.at)
        elif event.signal in TROUBLE:
            code = event.code or ("TIMEOUT" if event.signal in ("read_timeout", "deemed") else None)
            breaker.on_failure(code, event.at)


class PlannerFeed:
    """Per-PSP resolution times for the percentile planner."""

    def __init__(self, planner: CheckPlanner) -> None:
        self.planner = planner

    def on_event(self, payment_id: str, event: Event) -> None:
        if isinstance(event, AttemptMoved) and event.took is not None:
            self.planner.observe(event.psp, event.took)
