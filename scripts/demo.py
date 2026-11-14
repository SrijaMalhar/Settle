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

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from settle.adapters.local_health import LocalHealth  # noqa: E402
from settle.adapters.memory_store import MemoryEventStore  # noqa: E402
from settle.clock import FakeClock  # noqa: E402
from settle.core.messages import AttemptMoved, CheckStatus, Event, SendRefund, SendToPsp  # noqa: E402
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
    def __init__(self, clock: FakeClock) -> None:
        self.clock, self.start = clock, clock.now()

    def on_event(self, payment_id: str, e: Event) -> None:
        detail = ""
        if isinstance(e, AttemptMoved):
            detail = f"{e.attempt}: {e.signal} via {e.via} -> {e.to}"
            if e.plan:
                detail += f"  (checks planned at +{', +'.join(str(round(t - self.start)) for t in e.plan)} s)"
        else:
            detail = ", ".join(f"{k}={v}" for k, v in e.data().items() if k not in ("payment_id", "amount", "hold_until") and v is not None)
        print(f"  +{e.at - self.start:6.0f} s  {e.kind:<16} {detail}")


def story(title: str) -> tuple[PaymentEngine, FakeClock, Script]:
    print(f"\n{title}\n" + "-" * len(title))
    clock, script = FakeClock(), Script()
    engine = PaymentEngine(
        store=MemoryEventStore(), psps=["axis", "icici"], router=FixedOrderRouter(["axis", "icici"]),
        planner=PercentilePlanner(), health=LocalHealth(), clock=clock, dispatcher=script,
        subscribers=[Printer(clock)],
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


def main() -> None:
    e, clock, psp = story("1. A normal payment")
    p = e.pay("order-1001", 49900)
    a = p.attempts[0].id
    e.psp_answer(a, "accepted", via="reply")
    wait(e, clock, 12)
    e.psp_answer(a, "success", via="callback")
    print(f"  => {e.get('order-1001').status}; merchant told: {e.notifier.recent[-1]['message']}")  # type: ignore[union-attr]

    e, clock, psp = story("2. A timeout resolved by a status check")
    p = e.pay("order-1002", 25000)
    a = p.attempts[0].id
    e.psp_answer(a, "read_timeout", via="reply")  # unknown, never "failed"
    wait(e, clock, 30)
    try:
        e.pay("order-1002", 25000)
    except Exception as err:  # the customer taps pay again
        print(f"  +    30 s  (second tap refused: {err})")
    wait(e, clock, 60)  # the first check is due 90 s after sending
    e.psp_answer(a, "success", via="check", n=1)  # the callback was lost; the check finds the answer
    print(f"  => {e.get('order-1002').status}; PSP was asked: {psp.asked}")  # type: ignore[union-attr]

    e, clock, psp = story("3. A late success, refunded")
    p = e.pay("order-1003", 79900)
    a = p.attempts[0].id
    e.psp_answer(a, "accepted", via="reply")
    wait(e, clock, 640, psp)  # three checks say "pending"; the 10-minute hold runs out
    e.psp_answer(a, "success", via="callback")  # the money arrives after the order was cancelled
    refund = e.get("order-1003").refunds[0].id  # type: ignore[union-attr]
    e.refund_answer(refund, confirmed=False)  # the refund times out...
    wait(e, clock, 30)  # ...and is resent with the same refund id
    e.refund_answer(refund, confirmed=True)
    final = e.store.load(e.store.payment_of("order-1003") or "")
    print(f"  => {final[-1].kind}; PSP was asked: {psp.asked}")


if __name__ == "__main__":
    main()
