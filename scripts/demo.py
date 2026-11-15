"""Three payments, start to finish, printed as the events Settle stores.

    python scripts/demo.py

1. A normal payment: accepted, then the PSP's callback says success.
2. A timeout resolved by a status check: the reply times out, the callback is lost, and the
   first check (90 s later, as NPCI requires) finds the answer.
3. A late success refunded: three status checks say "pending", the hold time runs out and the
   order goes to ops, then the money arrives anyway, so Settle refunds it and resends the refund
   with the same id until the PSP confirms.

It runs the real engine and the real decide() with a fake clock, so the 90-second and
10-minute waits take no time. The PSP's answers are scripted, which is why each story reads the
same every time.
"""

from __future__ import annotations

import itertools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from settle.adapters.local_health import LocalHealth  # noqa: E402
from settle.adapters.memory_store import MemoryEventStore  # noqa: E402
from settle.clock import FakeClock  # noqa: E402
from settle.core.decide import Rejected  # noqa: E402
from settle.core.messages import (  # noqa: E402
    AttemptMoved,
    AttemptStarted,
    CheckAnswered,
    CheckSpent,
    CheckStatus,
    EscalatedToOps,
    Event,
    PaymentCreated,
    PaymentExpired,
    PaymentPaid,
    RefundConfirmed,
    RefundRetried,
    RefundStarted,
    SendRefund,
    SendToPsp,
)
from settle.core.money import Money  # noqa: E402
from settle.policies.checks import PercentilePlanner  # noqa: E402
from settle.policies.routing import FixedOrderRouter  # noqa: E402
from settle.shard.engine import PaymentEngine  # noqa: E402


class Script:
    """Plays the PSP: remembers what Settle asked for, and lets the story decide the answers."""

    def __init__(self) -> None:
        self.asked: list[str] = []
        self.checks: list[CheckStatus] = []

    def initiate(self, payment_id: str, c: SendToPsp) -> None:
        self.asked.append(f"send {c.attempt} to {c.psp}")

    def check(self, payment_id: str, c: CheckStatus) -> None:
        self.asked.append(f"status check #{c.n} for {c.attempt}")
        self.checks.append(c)

    def refund(self, payment_id: str, c: SendRefund) -> None:
        self.asked.append(f"refund {c.refund}")


class Printer:
    """Prints each stored event as one line: time since the order was created, kind, detail."""

    def __init__(self, clock: FakeClock) -> None:
        self.start = clock.now()

    def detail(self, e: Event) -> str:
        match e:
            case PaymentCreated():
                return f"{e.order_id}  {Money(e.amount)}, held {e.hold_until - e.at:.0f} s"
            case AttemptStarted():
                return f"{e.attempt}  to {e.psp}"
            case AttemptMoved():
                line = f"{e.attempt}  {e.signal} ({e.via}) -> {e.to}"
                if e.plan:
                    line += "   checks due " + "/".join(f"+{t - self.start:.0f}" for t in e.plan) + " s"
                return line
            case CheckSpent():
                return f"{e.attempt}  status check #{e.n}"
            case CheckAnswered():
                return f"{e.attempt}  check #{e.n}: still pending"
            case PaymentPaid():
                return "fulfil the order"
            case PaymentExpired():
                return "hold time over" if e.reason == "hold" else e.reason
            case EscalatedToOps():
                return f"{e.reason}: waiting for a person"
            case RefundStarted():
                return f"{e.refund}  {Money(e.amount)}"
            case RefundRetried():
                return f"{e.refund}  try {e.n}, same refund id"
            case RefundConfirmed():
                return f"{e.refund}  money returned"
        return ""

    def on_event(self, payment_id: str, e: Event) -> None:
        print(f"  +{e.at - self.start:4.0f}s  {e.kind:<16} {self.detail(e)}")


def story(title: str) -> tuple[PaymentEngine, FakeClock, Script]:
    print(f"\n{title}")
    clock, script = FakeClock(), Script()
    ids = itertools.count(1)
    engine = PaymentEngine(
        store=MemoryEventStore(), psps=["axis", "icici"], router=FixedOrderRouter(["axis", "icici"]),
        planner=PercentilePlanner(), health=LocalHealth(), clock=clock, dispatcher=script,
        subscribers=[Printer(clock)], ids=lambda: f"{next(ids):04d}",
    )
    return engine, clock, script


def wait(engine: PaymentEngine, clock: FakeClock, seconds: float, script: Script | None = None) -> None:
    """Move time forward, firing every timer at its own due time. If a script is given, its
    status checks come back "still pending"."""
    end = clock.now() + seconds
    while (due := engine.timers.next_due()) is not None and due <= end:
        clock.set(due)
        engine.fire_due()
        while script and script.checks:
            c = script.checks.pop(0)
            engine.psp_answer(c.attempt, "pending", via="check", n=c.n)
    clock.set(end)


def outcome(e: PaymentEngine, order: str, psp: Script) -> str:
    p = e.get(order)
    assert p is not None
    sends = sum(a.startswith("send") for a in psp.asked)
    checks = sum(a.startswith("status") for a in psp.asked)
    refunds = sum(a.startswith("refund") for a in psp.asked)
    parts = [f"sent to the PSP {sends}x"]
    if checks:
        parts.append(f"{checks} status check{'s' if checks > 1 else ''}")
    if refunds:
        parts.append(f"refund sent {refunds}x with one id")
    return f"  => {p.status}: " + ", ".join(parts)


def main() -> None:
    e, clock, psp = story("1. A normal payment")
    a = e.pay("order-1001", 49900).attempts[0].id
    e.psp_answer(a, "accepted", via="reply")
    wait(e, clock, 12)
    e.psp_answer(a, "success", via="callback")
    print(outcome(e, "order-1001", psp))

    e, clock, psp = story("2. A timeout, resolved by a status check")
    a = e.pay("order-1002", 25000).attempts[0].id
    e.psp_answer(a, "read_timeout", via="reply")  # unknown, never "failed"
    wait(e, clock, 30)
    try:
        e.pay("order-1002", 25000)  # the customer taps pay again
    except Rejected as err:
        print(f"  +  30s  {'(second tap)':<16} refused: {err}")
    wait(e, clock, 60)  # the first check is due 90 s after sending
    e.psp_answer(a, "success", via="check", n=1)  # the callback was lost; the check finds the answer
    print(outcome(e, "order-1002", psp))

    e, clock, psp = story("3. A success after the order was given up, refunded")
    a = e.pay("order-1003", 79900).attempts[0].id
    e.psp_answer(a, "accepted", via="reply")
    wait(e, clock, 640, psp)  # three checks say "pending"; the 10-minute hold runs out
    e.psp_answer(a, "success", via="callback")  # the money arrives anyway
    refund = f"rf-{a}"
    e.refund_answer(refund, confirmed=False)  # the refund reply is lost...
    wait(e, clock, 30)  # ...so it is resent with the same refund id
    e.refund_answer(refund, confirmed=True)
    print(outcome(e, "order-1003", psp))


if __name__ == "__main__":
    main()
