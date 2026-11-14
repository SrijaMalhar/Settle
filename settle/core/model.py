"""The two state machines, written as tables, and the immutable payment they describe.

A payment (one merchant order) has seven states. Each attempt (one request to one PSP) has
five. Every legal move is a row in a table, so an illegal move cannot happen silently, and a
reviewer can see every legal move in one place.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, StrEnum

from settle.core.money import Money


class IllegalMove(Exception):
    """A transition that is not in the table. For PSP answers this means the PSP contradicted itself."""


# ---------------------------------------------------------------------------------------------
# Attempts
# ---------------------------------------------------------------------------------------------


class AttemptStatus(Enum):
    SENDING = "sending"  # recorded; request on its way to the PSP
    PENDING = "pending"  # the PSP may have it: outcome unknown
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNRESOLVED = "unresolved"  # checks used up, still no answer


S = AttemptStatus

ATTEMPT_MOVES: dict[tuple[AttemptStatus, str], AttemptStatus] = {
    (S.SENDING, "accepted"): S.PENDING,
    (S.SENDING, "read_timeout"): S.PENDING,  # unknown, never "failed"
    (S.SENDING, "deemed"): S.PENDING,  # RB: the PSP itself does not know yet
    (S.SENDING, "recovered"): S.PENDING,  # restarted mid-send: it may or may not have gone out
    (S.SENDING, "not_delivered"): S.FAILED,  # connection refused: the PSP never saw it
    (S.SENDING, "declined"): S.FAILED,
    (S.SENDING, "success"): S.SUCCEEDED,  # a callback can beat the HTTP reply
    (S.SENDING, "failure"): S.FAILED,  # so can a failure callback
    (S.PENDING, "success"): S.SUCCEEDED,
    (S.PENDING, "failure"): S.FAILED,
    (S.PENDING, "checks_exhausted"): S.UNRESOLVED,
    (S.UNRESOLVED, "success"): S.SUCCEEDED,  # a late answer is still the truth
    (S.UNRESOLVED, "failure"): S.FAILED,
}

# The same answer twice. Nothing changes.
REPEATS: frozenset[tuple[AttemptStatus, str]] = frozenset(
    {
        (S.SUCCEEDED, "success"),
        (S.FAILED, "failure"),
        (S.FAILED, "declined"),
        (S.UNRESOLVED, "checks_exhausted"),
    }
)

# Signals that only describe the hand-off to the PSP. Once an attempt has moved past SENDING
# (say a callback beat the HTTP reply), a late "accepted" or "timed out" reply adds nothing.
HANDOFF: frozenset[str] = frozenset({"accepted", "read_timeout", "deemed", "recovered"})

# Attempts that might still take the customer's money.
LIVE: frozenset[AttemptStatus] = frozenset({S.SENDING, S.PENDING, S.UNRESOLVED})


def move(status: AttemptStatus, signal: str) -> AttemptStatus:
    """The next status. A duplicate answer returns the same status; a contradiction raises."""
    if (status, signal) in ATTEMPT_MOVES:
        return ATTEMPT_MOVES[(status, signal)]
    if (status, signal) in REPEATS:
        return status  # duplicate answer: nothing changes
    if status is not S.SENDING and signal in HANDOFF:
        return status  # a stale reply about the hand-off
    raise IllegalMove(f"{status.value} + {signal}")  # e.g. failure after success: never applied


def attempt_edges() -> frozenset[tuple[AttemptStatus, AttemptStatus]]:
    """Every (from, to) pair the table allows, for checkers that watch from the outside."""
    return frozenset((frm, to) for (frm, _), to in ATTEMPT_MOVES.items())


# ---------------------------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------------------------


class PaymentStatus(StrEnum):
    AWAITING = "AWAITING"  # order open, the customer may pay
    PAID = "PAID"  # final: fulfil the order
    EXPIRED = "EXPIRED"  # order given up; attempts may still be live
    REFUNDING = "REFUNDING"  # a success arrived after the order was given up
    CLOSED = "CLOSED"  # final: nothing charged
    REFUNDED = "REFUNDED"  # final: money returned
    NEEDS_OPS = "NEEDS_OPS"  # Settle does not know; a person or a late PSP answer decides


P = PaymentStatus

FINAL: frozenset[PaymentStatus] = frozenset({P.PAID, P.CLOSED, P.REFUNDED})

# AWAITING + "an attempt fails for certain" stays AWAITING (the customer may pay again), so it
# needs no row: the payment does not move.
PAYMENT_MOVES: dict[tuple[PaymentStatus, str], PaymentStatus] = {
    (P.AWAITING, "paid"): P.PAID,
    (P.AWAITING, "expired"): P.EXPIRED,  # hold time over, or the merchant cancels
    (P.EXPIRED, "closed"): P.CLOSED,  # every attempt ended, none succeeded
    (P.EXPIRED, "refund_started"): P.REFUNDING,  # a late success
    (P.EXPIRED, "escalated"): P.NEEDS_OPS,  # an attempt still unknown after all checks
    (P.NEEDS_OPS, "refund_started"): P.REFUNDING,  # a late success
    (P.NEEDS_OPS, "closed"): P.CLOSED,  # a late failure
    (P.NEEDS_OPS, "refund_confirmed"): P.REFUNDED,  # a refund confirmed after ops took it
    (P.REFUNDING, "refund_confirmed"): P.REFUNDED,
    (P.REFUNDING, "escalated"): P.NEEDS_OPS,  # still unconfirmed after 5 tries
}


def pay_move(status: PaymentStatus, signal: str) -> PaymentStatus:
    """The next payment status. "contradiction" is the one move allowed from any state."""
    if signal == "contradiction":
        return P.NEEDS_OPS  # the PSP contradicted itself: alert ops, keep both answers
    if (status, signal) in PAYMENT_MOVES:
        return PAYMENT_MOVES[(status, signal)]
    raise IllegalMove(f"payment {status} + {signal}")


# ---------------------------------------------------------------------------------------------
# The aggregate
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Attempt:
    """One request to one PSP."""

    id: str
    psp: str
    status: AttemptStatus
    started_at: float
    went_pending: bool = False  # a timeout, a deemed answer, a restart, or a status check
    plan: tuple[float, ...] = ()  # when its status checks are due (absolute times)
    checks: tuple[float, ...] = ()  # when status checks were actually spent
    answered: int = 0  # how many of those checks came back with an answer Settle stored
    checks_stopped: bool = False  # a check hit a transport error: NPCI says stop
    give_up_at: float | None = None
    code: str | None = None  # the NPCI code of a failure
    resolved_at: float | None = None

    @property
    def live(self) -> bool:
        return self.status in LIVE

    def next_check_at(self) -> float | None:
        if self.status is not S.PENDING or self.checks_stopped:
            return None
        n = len(self.checks)
        return self.plan[n] if n < len(self.plan) else None


@dataclass(frozen=True)
class Refund:
    id: str
    attempt: str
    amount: int
    tries: int = 1
    confirmed: bool = False


@dataclass(frozen=True)
class Payment:
    """One merchant order. Built only by replaying its events; never updated in place."""

    payment_id: str
    order_id: str
    shard: int
    amount: Money
    status: PaymentStatus
    created_at: float
    hold_until: float
    attempts: tuple[Attempt, ...] = ()
    refunds: tuple[Refund, ...] = ()
    ops: tuple[str, ...] = ()  # why it was sent to a person, oldest first
    contradictions: tuple[tuple[str, str], ...] = ()  # (attempt, the signal that contradicted it)

    @property
    def contradicted(self) -> bool:
        return bool(self.contradictions)

    @property
    def final(self) -> bool:
        return self.status in FINAL

    def attempt(self, attempt_id: str) -> Attempt | None:
        for a in self.attempts:
            if a.id == attempt_id:
                return a
        return None

    def refund(self, refund_id: str) -> Refund | None:
        for r in self.refunds:
            if r.id == refund_id:
                return r
        return None

    def live_attempt(self) -> Attempt | None:
        for a in self.attempts:
            if a.live:
                return a
        return None

    def succeeded(self) -> Attempt | None:
        for a in self.attempts:
            if a.status is S.SUCCEEDED:
                return a
        return None

    def next_attempt_id(self) -> str:
        """Attempt ids carry the payment id, and the payment id carries the logical shard."""
        return f"{self.payment_id}.{len(self.attempts) + 1}"
