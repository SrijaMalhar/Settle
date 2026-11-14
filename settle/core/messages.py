"""Inputs (what happened outside), events (facts Settle stores) and commands (what to do next).

Every one is a small frozen dataclass. Events carry the time they happened, taken from the
input, which took it from the injected clock; the core never reads a clock itself.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class PayRequested:
    """The customer taps "pay". The router has already chosen the PSP (None if none is healthy)."""

    now: float
    attempt_id: str
    psp: str | None
    amount: int  # paise
    hold_seconds: float = 600.0  # a food order is held for about 10 minutes
    payment_id: str = ""  # used only by the first call, which creates the payment
    order_id: str = ""
    shard: int = 0


@dataclass(frozen=True, kw_only=True)
class CancelRequested:
    now: float


@dataclass(frozen=True, kw_only=True)
class PspAnswer:
    """An HTTP reply, a callback, or a status-check result."""

    now: float
    attempt_id: str
    signal: str
    code: str | None = None
    via: str = "reply"  # reply, callback, check or settlement
    n: int = 0  # which status check this answers
    plan: tuple[float, ...] = ()  # check offsets the planner wants if the attempt goes pending


@dataclass(frozen=True, kw_only=True)
class RefundAnswer:
    now: float
    refund_id: str
    confirmed: bool


@dataclass(frozen=True, kw_only=True)
class TimerFired:
    now: float
    kind: str  # hold, check, give_up, refund_retry
    ref: str | None = None  # the attempt or refund the timer is about
    n: int = 0


@dataclass(frozen=True, kw_only=True)
class Recover:
    """After a restart or a takeover: rebuild timers and treat SENDING attempts as unknown."""

    now: float
    plans: tuple[tuple[str, tuple[float, ...]], ...] = ()  # (psp, offsets) from the planner

    def plan_for(self, psp: str) -> tuple[float, ...]:
        return dict(self.plans).get(psp, ())


Input = PayRequested | CancelRequested | PspAnswer | RefundAnswer | TimerFired | Recover

# ---------------------------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------------------------

EVENT_TYPES: dict[str, type[Event]] = {}


@dataclass(frozen=True, kw_only=True)
class Event:
    at: float

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        EVENT_TYPES[cls.__name__] = cls

    @property
    def kind(self) -> str:
        return type(self).__name__

    def data(self) -> dict[str, Any]:
        """Everything except the time, ready for a JSONB column."""
        out = {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if f.name != "at"}
        return {k: list(v) if isinstance(v, tuple) else v for k, v in out.items()}


def event_from(kind: str, data: dict[str, Any], at: float) -> Event:
    cls = EVENT_TYPES[kind]
    names = {f.name for f in dataclasses.fields(cls)}
    args = {k: tuple(v) if isinstance(v, list) else v for k, v in data.items() if k in names}
    return cls(at=at, **args)


@dataclass(frozen=True, kw_only=True)
class PaymentCreated(Event):
    payment_id: str
    order_id: str
    shard: int
    amount: int
    hold_until: float


@dataclass(frozen=True, kw_only=True)
class AttemptStarted(Event):
    attempt: str
    psp: str


@dataclass(frozen=True, kw_only=True)
class AttemptMoved(Event):
    attempt: str
    psp: str
    to: str
    signal: str
    via: str
    code: str | None = None
    plan: tuple[float, ...] = ()  # set when the attempt goes pending
    took: float | None = None  # seconds from sending to the final answer
    outcome: str | None = None  # clean, pending_success or failed, for the router


@dataclass(frozen=True, kw_only=True)
class CheckSpent(Event):
    attempt: str
    n: int


@dataclass(frozen=True, kw_only=True)
class ChecksStopped(Event):
    attempt: str
    reason: str
    give_up_at: float


@dataclass(frozen=True, kw_only=True)
class PaymentPaid(Event):
    attempt: str


@dataclass(frozen=True, kw_only=True)
class PaymentExpired(Event):
    reason: str  # hold or cancelled


@dataclass(frozen=True, kw_only=True)
class RefundStarted(Event):
    attempt: str
    refund: str
    amount: int


@dataclass(frozen=True, kw_only=True)
class RefundRetried(Event):
    refund: str
    n: int


@dataclass(frozen=True, kw_only=True)
class RefundConfirmed(Event):
    refund: str


@dataclass(frozen=True, kw_only=True)
class PaymentClosed(Event):
    pass


@dataclass(frozen=True, kw_only=True)
class EscalatedToOps(Event):
    reason: str  # unresolved, refund_unconfirmed or contradiction
    attempt: str | None = None
    first: str | None = None  # for a contradiction: what the PSP said first...
    then: str | None = None  # ...and what it said after
    code: str | None = None


# ---------------------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class Command:
    @property
    def kind(self) -> str:
        return type(self).__name__


@dataclass(frozen=True, kw_only=True)
class SendToPsp(Command):
    attempt: str
    psp: str
    order_id: str
    amount: int


@dataclass(frozen=True, kw_only=True)
class CheckStatus(Command):
    attempt: str
    psp: str
    n: int


@dataclass(frozen=True, kw_only=True)
class SendRefund(Command):
    refund: str
    attempt: str
    psp: str
    amount: int


@dataclass(frozen=True, kw_only=True)
class SetTimer(Command):
    timer: str  # hold, check, give_up, refund_retry
    at: float
    ref: str | None = None
    n: int = 0


@dataclass(frozen=True, kw_only=True)
class Notify(Command):
    what: str  # paid, failed, cancelled, refunded
    attempt: str | None = None
    code: str | None = None
    blame: str | None = None
    message: str = ""


@dataclass(frozen=True, kw_only=True)
class AlertOps(Command):
    reason: str
    attempt: str | None = None


Commands = list[Command]
Events = list[Event]
