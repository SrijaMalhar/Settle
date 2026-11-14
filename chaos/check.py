"""The simulator's guarantees, checked over a live system: Settle's stored event log on one side,
each mock PSP's ledger of what it really took and gave back on the other."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from settle.core.decide import apply
from settle.core.messages import AttemptMoved, AttemptStarted, CheckSpent, ChecksStopped, Event, PaymentPaid, event_from
from settle.core.model import FINAL, PAYMENT_MOVES, AttemptStatus, Payment, PaymentStatus, attempt_edges
from settle.policies.checks import FIRST, GAP, MAX_CHECKS, WINDOW

S = AttemptStatus
P = PaymentStatus
EDGES = attempt_edges()
PAY_EDGES = {(frm, to) for (frm, _), to in PAYMENT_MOVES.items()}


@dataclass
class Report:
    payments: int = 0
    violations: list[str] = field(default_factory=list)
    status: dict[str, int] = field(default_factory=dict)
    needs_ops: list[str] = field(default_factory=list)

    def fail(self, invariant: str, detail: str) -> None:
        if len(self.violations) < 30:
            self.violations.append(f"{invariant}: {detail}")


@dataclass
class Ledgers:
    debited: dict[str, int]  # attempt -> amount really taken
    refunded: dict[str, int]  # attempt -> amount really given back
    received: set[str]  # every attempt id any PSP has a record of
    pending: set[str]
    repeated: dict[str, int]

    @classmethod
    def merge(cls, raw: list[dict[str, Any]]) -> Ledgers:
        debited: dict[str, int] = {}
        refunded: dict[str, int] = {}
        received: set[str] = set()
        pending: set[str] = set()
        repeated: dict[str, int] = {}
        for led in raw:
            debited.update(led["debited"])
            for r in led["refunded"].values():
                refunded[r["attempt"]] = refunded.get(r["attempt"], 0) + r["amount"]
            received |= set(led["debited"]) | set(led["pending"]) | set(led.get("failed", []))
            pending |= set(led["pending"])
            repeated.update(led.get("repeated", {}))
        return cls(debited, refunded, received, pending, repeated)


def check(rows: list[tuple[str, int, str, dict[str, Any], float]], ledgers: Ledgers, final: bool = False) -> Report:
    """rows: (payment_id, seq, kind, data, at) in order. With final=True the system is quiet and
    the settlement replay has run, so I2 is checked too."""
    rep = Report()
    by_payment: dict[str, list[Event]] = {}
    for pid, _seq, kind, data, at in rows:
        by_payment.setdefault(pid, []).append(event_from(kind, data, at))
    known: set[str] = set()
    for pid, events in by_payment.items():
        rep.payments += 1
        p = _replay(pid, events, rep)
        if p is None:
            continue
        rep.status[p.status.value] = rep.status.get(p.status.value, 0) + 1
        known |= {a.id for a in p.attempts}
        _money(p, events, ledgers, rep, final)
    for attempt in ledgers.debited:  # I6: no debit Settle has no record of
        if attempt not in known:
            rep.fail("I6", f"{attempt}: a PSP took money for an attempt Settle never stored")
    for attempt, n in ledgers.repeated.items():  # ask, never resend
        rep.fail("I1", f"{attempt}: the PSP received the same payment request {n} times")
    return rep


def _replay(pid: str, events: list[Event], rep: Report) -> Payment | None:
    p: Payment | None = None
    started: dict[str, float] = {}
    checks: dict[str, list[float]] = {}
    stopped: set[str] = set()
    for e in events:
        before = p
        try:
            p = apply(p, e)
        except Exception as err:  # noqa: BLE001
            rep.fail("I4", f"{pid}: {e.kind} cannot be applied: {err}")
            return None
        if isinstance(e, AttemptStarted):
            started[e.attempt] = e.at
        if isinstance(e, ChecksStopped):
            stopped.add(e.attempt)
        if isinstance(e, CheckSpent):  # I3, check by check
            if e.attempt in stopped:
                rep.fail("I3", f"{e.attempt}: checked after a transport error")
            done = checks.setdefault(e.attempt, [])
            t0 = started[e.attempt]
            if not done and e.at < t0 + FIRST:
                rep.fail("I3", f"{e.attempt}: first check {e.at - t0:.0f} s after sending")
            if done and e.at < done[-1] + GAP:
                rep.fail("I3", f"{e.attempt}: checks {e.at - done[-1]:.0f} s apart")
            if e.at > t0 + WINDOW or len(done) >= MAX_CHECKS:
                rep.fail("I3", f"{e.attempt}: check number {len(done) + 1} at {e.at - t0:.0f} s")
            done.append(e.at)
        if before is None or p is None:
            continue
        if isinstance(e, AttemptMoved):  # I4
            old = before.attempt(e.attempt)
            if old is not None and (old.status, S(e.to)) not in EDGES:
                rep.fail("I4", f"{e.attempt}: {old.status.value} -> {e.to}")
        if before.status is not p.status:
            contradiction = p.status is P.NEEDS_OPS and p.contradicted
            if before.status in FINAL and not contradiction:
                rep.fail("I4", f"{pid}: final {before.status} became {p.status}")
            elif (before.status, p.status) not in PAY_EDGES and not contradiction:
                rep.fail("I4", f"{pid}: {before.status} -> {p.status}")
        if sum(a.live for a in p.attempts) > 1:  # I5
            rep.fail("I5", f"{pid}: two live attempts")
    return p


def _money(p: Payment, events: list[Event], led: Ledgers, rep: Report, final: bool) -> None:
    amount = p.amount.paise
    taken = sum(led.debited.get(a.id, 0) for a in p.attempts)
    back = sum(led.refunded.get(a.id, 0) for a in p.attempts)
    net = taken - back
    if not 0 <= net <= amount:  # I1
        rep.fail("I1", f"{p.order_id}: taken {taken}, refunded {back}, order of {amount}")
    live_at_psp = [a.id for a in p.attempts if a.id in led.pending or (a.id in led.debited and not led.refunded.get(a.id))]
    if len(live_at_psp) > 1:  # I5 in reality
        rep.fail("I5", f"{p.order_id}: {len(live_at_psp)} attempts the PSPs could still charge")
    for a in p.attempts:  # I6
        if a.status is S.SUCCEEDED and a.id not in led.debited:
            rep.fail("I6", f"{a.id}: Settle says succeeded, the PSP did not take the money")
        if a.status is S.FAILED and a.id in led.debited:
            rep.fail("I6", f"{a.id}: Settle says failed, the PSP took the money")
    if not final:
        return
    paid = any(isinstance(e, PaymentPaid) for e in events)
    if net != (amount if paid else 0):  # I2
        rep.fail("I2", f"{p.order_id}: net charge {net}, {'paid' if paid else 'not paid'}")
    if p.status is P.NEEDS_OPS:
        live = p.live_attempt()
        unknown = live is not None and live.id not in led.received
        rep.needs_ops.append(f"{p.order_id} ({'not in any settlement file: ops closes it' if unknown else ', '.join(p.ops)})")
    elif not p.final:
        rep.fail("I2", f"{p.order_id}: still {p.status} after faults stopped and settlement")
