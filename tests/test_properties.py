"""Property-based tests: Hypothesis fires random inputs at one payment and checks invariants
after every step. When it finds a failure it shrinks it to the shortest sequence that breaks."""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule, run_state_machine_as_test

from settle.core.decide import Rejected, UnknownAttempt, apply, decide, replay
from settle.core.messages import (
    CancelRequested,
    Event,
    Input,
    PayRequested,
    PspAnswer,
    Recover,
    RefundAnswer,
    TimerFired,
    event_from,
)
from settle.core.model import FINAL, LIVE, AttemptStatus, IllegalMove, Payment, PaymentStatus

S = AttemptStatus
P = PaymentStatus
SIGNALS = ["accepted", "read_timeout", "not_delivered", "declined", "success", "failure", "deemed"]
CHECK_SIGNALS = ["pending", "success", "failure", "transport_error"]
CODES = [None, "ZM", "Z9", "U30", "BT", "U28"]
T0 = 1_700_000_000.0


def machine(bugs: frozenset[str] = frozenset()) -> type[RuleBasedStateMachine]:
    class OnePayment(RuleBasedStateMachine):
        def __init__(self) -> None:
            super().__init__()
            self.p: Payment | None = None
            self.log: list[Event] = []
            self.n = 0
            self.now = T0
            self.finals: dict[str, PaymentStatus] = {}

        def step(self, inp: Input) -> None:
            try:
                events, _ = decide(self.p, inp, bugs)
            except (Rejected, IllegalMove, UnknownAttempt):
                return
            for e in events:
                before = self.p.status if self.p else None
                self.p = apply(self.p, e)
                if before in FINAL and self.p.status is not before:
                    # the one way out of a final state is a PSP contradicting itself
                    assert self.p.status is P.NEEDS_OPS and self.p.contradicted
            self.log += events

        def tick(self, seconds: float) -> float:
            self.now += seconds
            return self.now

        @initialize()
        def create(self) -> None:
            self.n = 1
            self.step(
                PayRequested(now=self.now, attempt_id="p00x.1", psp="axis", amount=100, payment_id="p00x", order_id="ord")
            )

        @rule(psp=st.sampled_from(["axis", "icici", "yes"]))
        def pay(self, psp: str) -> None:
            self.n += 1
            self.step(PayRequested(now=self.tick(1), attempt_id=f"p00x.{self.n}", psp=psp, amount=100))

        @rule(which=st.integers(0, 5), signal=st.sampled_from(SIGNALS), code=st.sampled_from(CODES),
              via=st.sampled_from(["reply", "callback"]))
        def answer(self, which: int, signal: str, code: str | None, via: str) -> None:
            if self.p and self.p.attempts:
                a = self.p.attempts[which % len(self.p.attempts)]
                self.step(PspAnswer(now=self.tick(3), attempt_id=a.id, signal=signal, code=code, via=via))

        @rule(which=st.integers(0, 5), signal=st.sampled_from(CHECK_SIGNALS))
        def check(self, which: int, signal: str) -> None:
            if self.p and self.p.attempts:
                a = self.p.attempts[which % len(self.p.attempts)]
                self.step(TimerFired(now=self.tick(91), kind="check", ref=a.id, n=len(a.checks) + 1))
                a2 = self.p.attempt(a.id)
                assert a2 is not None
                self.step(PspAnswer(now=self.tick(1), attempt_id=a.id, signal=signal, via="check", n=len(a2.checks)))

        @rule()
        def expire(self) -> None:
            if self.p:
                self.step(TimerFired(now=max(self.tick(1), self.p.hold_until), kind="hold"))

        @rule()
        def cancel(self) -> None:
            self.step(CancelRequested(now=self.tick(1)))

        @rule(confirmed=st.booleans())
        def refund_answer(self, confirmed: bool) -> None:
            if self.p and self.p.refunds:
                self.step(RefundAnswer(now=self.tick(2), refund_id=self.p.refunds[-1].id, confirmed=confirmed))

        @rule()
        def refund_retry(self) -> None:
            if self.p and self.p.refunds:
                r = self.p.refunds[-1]
                self.step(TimerFired(now=self.tick(300), kind="refund_retry", ref=r.id, n=r.tries + 1))

        @rule()
        def restart(self) -> None:
            self.step(Recover(now=self.tick(5)))

        @invariant()
        def at_most_one_live_attempt(self) -> None:
            if self.p:
                assert sum(a.status in LIVE for a in self.p.attempts) <= 1

        @invariant()
        def never_two_successes(self) -> None:
            if self.p:
                assert sum(a.status is S.SUCCEEDED for a in self.p.attempts) <= 1

        @invariant()
        def paid_means_exactly_one_success(self) -> None:
            if self.p and self.p.status is P.PAID:
                assert self.p.succeeded() is not None and not self.p.refunds

        @invariant()
        def at_most_three_checks_within_npci_rules(self) -> None:
            if self.p:
                for a in self.p.attempts:
                    assert len(a.checks) <= 3
                    if a.checks:
                        assert a.checks[0] >= a.started_at + 90
                    for x, y in zip(a.checks, a.checks[1:]):
                        assert y - x >= 90

        @invariant()
        def a_success_is_either_paid_or_refunded(self) -> None:
            p = self.p
            if p and p.succeeded() is not None and not p.contradicted:
                won = p.succeeded()
                assert won is not None
                assert p.status is P.PAID or any(r.attempt == won.id for r in p.refunds)

        @invariant()
        def replay_gives_the_same_state(self) -> None:
            if self.log:
                assert replay([event_from(e.kind, e.data(), e.at) for e in self.log]) == self.p

    return OnePayment


TestOnePayment: Any = machine().TestCase
TestOnePayment.settings = settings(max_examples=400, stateful_step_count=40, deadline=None)


def test_removing_the_live_attempt_check_is_caught_and_shrunk() -> None:
    """The story from the spec: take the one-live-attempt check out of decide() and Hypothesis
    finds the double charge in its smallest form: pay, then pay again."""
    broken = machine(frozenset({"no_live_check"}))
    with pytest.raises(AssertionError):
        run_state_machine_as_test(
            broken,
            settings=settings(
                max_examples=500, stateful_step_count=20, deadline=None, database=None,
                suppress_health_check=list(HealthCheck), print_blob=False,
            ),
        )
