"""The guarantees, checked from outside the engine.

    I1  No double charge: per order, money taken minus money refunded is between 0 and one payment
    I2  No charge without goods: once faults stop, every order is charged exactly when it was paid
    I3  NPCI rules: at most 3 checks, the first after 90 s, 90 s apart, within 2 hours, none after
        a transport error
    I4  Only legal transitions, and final states never change (except a PSP contradiction -> ops)
    I5  At most one live attempt per order, in Settle's view and in the PSPs' reality
    I6  Settle's final answer for an attempt matches the PSP's truth; no debit is unknown to Settle
    I7  The same seed gives the same trace hash (checked by the runner)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from settle.core.decide import apply
from settle.core.messages import AttemptMoved, Event, PaymentCreated, PaymentPaid
from settle.core.model import FINAL, PAYMENT_MOVES, AttemptStatus, Payment, PaymentStatus, attempt_edges
from settle.policies.checks import FIRST, GAP, MAX_CHECKS, WINDOW
from settle.sharding import payment_of_attempt

if TYPE_CHECKING:
    from sim.world import World

S = AttemptStatus
P = PaymentStatus
EDGES = attempt_edges()
PAY_EDGES = {(frm, to) for (frm, _), to in PAYMENT_MOVES.items()}


@dataclass(frozen=True)
class Violation:
    invariant: str
    detail: str
    at: float

    def __str__(self) -> str:
        return f"{self.invariant} at +{self.at:.1f}s: {self.detail}"


class Checker:
    def __init__(self, world: World) -> None:
        self.w = world
        self.state: dict[str, Payment] = {}
        self.paid: set[str] = set()  # orders whose goods were handed over
        self.touched_orders: list[str] = []
        self.steps = 0

    # ------------------------------------------------------------------ per stored batch (I4, I5, I6)

    def on_append(self, pid: str, events: list[Event]) -> None:
        p = self.state.get(pid)
        for e in events:
            before = p
            try:
                p = apply(p, e)
            except Exception as err:  # noqa: BLE001 - any failure to fold is an illegal transition
                self.w.violation("I4", f"{pid}: {e.kind} cannot be applied: {err}")
                return
            if isinstance(e, PaymentCreated):
                continue
            assert before is not None
            if isinstance(e, AttemptMoved):
                old = before.attempt(e.attempt)
                if old is not None and (old.status, S(e.to)) not in EDGES:
                    self.w.violation("I4", f"{e.attempt}: {old.status.value} -> {e.to} is not a legal move")
            if before.status is not p.status:
                if before.status in FINAL and not (p.status is P.NEEDS_OPS and p.contradicted):
                    self.w.violation("I4", f"{pid}: final state {before.status} changed to {p.status}")
                elif (before.status, p.status) not in PAY_EDGES and not (p.status is P.NEEDS_OPS and p.contradicted):
                    self.w.violation("I4", f"{pid}: {before.status} -> {p.status} is not a legal move")
            if isinstance(e, PaymentPaid):
                if p.order_id in self.paid:
                    self.w.violation("I4", f"{p.order_id}: paid twice")
                self.paid.add(p.order_id)
        assert p is not None
        self.state[pid] = p
        if sum(a.live for a in p.attempts) > 1:
            self.w.violation("I5", f"{p.order_id}: {sum(a.live for a in p.attempts)} live attempts in Settle")
        self._i6(p)
        self.touched_orders.append(p.order_id)

    def _i6(self, p: Payment) -> None:
        now = self.w.now()
        for a in p.attempts:
            if a.status not in (S.SUCCEEDED, S.FAILED):
                continue
            truth = self.w.truth(a.psp, a.id, now)
            if a.status is S.SUCCEEDED and truth != "success":
                self.w.violation("I6", f"{a.id}: Settle says succeeded, the PSP says {truth}")
            if a.status is S.FAILED and truth == "success":
                self.w.violation("I6", f"{a.id}: Settle says failed, but the PSP took the money")

    # ------------------------------------------------------------------ per step (I1, I5 in reality)

    def after_step(self) -> None:
        self.steps += 1
        orders, self.touched_orders = self.touched_orders, []
        for order in dict.fromkeys(orders):
            self.money(order)

    def money(self, order: str) -> None:
        now = self.w.now()
        amount = self.w.amounts.get(order)
        if amount is None:
            return
        taken, refunded, live = self.w.ledger(order, now)
        net = taken - refunded
        if net > amount:
            self.w.violation("I1", f"{order}: charged {taken} and refunded {refunded} for an order of {amount}")
        if net < 0:
            self.w.violation("I1", f"{order}: refunded {refunded} but only {taken} was taken")
        if live > 1:
            self.w.violation("I5", f"{order}: {live} attempts the PSPs could still charge")

    def sweep(self) -> None:
        for order in self.w.amounts:
            self.money(order)

    # ------------------------------------------------------------------ NPCI (I3), checked as each check is sent

    def on_check(self, attempt: str, checks: list[float], stopped_at: float | None) -> None:
        """NPCI times everything from the transaction: when Settle started the attempt (which is
        also when it was sent, unless Settle crashed in between and never sent it)."""
        now = self.w.now()
        p = self.state.get(payment_of_attempt(attempt))
        a = p.attempt(attempt) if p else None
        if a is None:
            self.w.violation("I3", f"{attempt}: a status check for an attempt Settle never stored")
            return
        sent_at = a.started_at
        if stopped_at is not None and now >= stopped_at:
            self.w.violation("I3", f"{attempt}: checked again after a transport error")
        if len(checks) > MAX_CHECKS:
            self.w.violation("I3", f"{attempt}: {len(checks)} status checks")
        if len(checks) == 1 and now < sent_at + FIRST:
            self.w.violation("I3", f"{attempt}: first check {now - sent_at:.0f} s after sending")
        if len(checks) > 1 and now < checks[-2] + GAP:
            self.w.violation("I3", f"{attempt}: checks {now - checks[-2]:.0f} s apart")
        if now > sent_at + WINDOW:
            self.w.violation("I3", f"{attempt}: check {now - sent_at:.0f} s after sending, past 2 hours")

    # ------------------------------------------------------------------ at the end (I2, I6)

    def final(self) -> None:
        now = self.w.now()
        self.sweep()
        known = {a.id for p in self.state.values() for a in p.attempts}
        for psp, attempt in self.w.debited_attempts(now):
            if attempt not in known:
                self.w.violation("I6", f"{attempt}: {psp} took the money for an attempt Settle has no record of")
        for order, amount in self.w.amounts.items():
            taken, refunded, _ = self.w.ledger(order, now)
            net = taken - refunded
            expected = amount if order in self.paid else 0
            if net != expected:
                self.w.violation("I2", f"{order}: net charge {net}, expected {expected} "
                                       f"({'paid' if order in self.paid else 'not paid'})")
        for p in self.state.values():
            self._i6(p)
            if p.status not in FINAL and not p.contradicted:
                self.w.violation("I2", f"{p.order_id}: still {p.status} after faults stopped and settlement")
