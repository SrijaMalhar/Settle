"""The five interfaces the core and engine depend on. Any class with these methods fits.

Settle depends on these ports, never on Flask, PostgreSQL, Redis or a bank directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from settle.core.messages import Event


@dataclass(frozen=True)
class PspReply:
    """What a PSP call came back with, already turned into a signal for the state machine.

    initiate: accepted, declined, success, failure, deemed, read_timeout, not_delivered
    check_status: success, failure, pending, transport_error
    refund: confirmed, unconfirmed
    """

    signal: str
    code: str | None = None


class PspGateway(Protocol):  # one adapter per bank: Adapter pattern
    name: str

    def initiate(self, attempt_id: str, order_id: str, amount: int) -> PspReply: ...

    def check_status(self, attempt_id: str) -> PspReply: ...

    def refund(self, refund_id: str, attempt_id: str, amount: int) -> PspReply: ...


class EventStore(Protocol):  # Repository: PostgreSQL in production, a dict in tests
    def load(self, payment_id: str) -> list[Event]: ...

    def append(self, payment_id: str, events: list[Event], *, expected: int, open: bool) -> None:
        """All or nothing. `expected` is how many events the caller has already seen; if the
        store holds a different number, someone else wrote, and the append fails."""
        ...

    def open_payments(self, shard: int) -> list[str]: ...

    def payment_of(self, order_id: str) -> str | None: ...


class HealthStore(Protocol):  # Redis shared by all shards; a local window if Redis is down
    def record(self, psp: str, value: float, now: float) -> None: ...

    def window(self, psp: str, now: float) -> tuple[float, int]: ...

    def open_breaker(self, psp: str, seconds: float, now: float) -> None: ...

    def breaker_open(self, psp: str, now: float) -> bool: ...


class Clock(Protocol):  # SystemClock in production, FakeClock in the simulator
    def now(self) -> float: ...


class Subscriber(Protocol):  # Observer: stats, router feedback, planner history
    def on_event(self, payment_id: str, event: Event) -> None: ...
