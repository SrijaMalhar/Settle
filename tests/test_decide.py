"""decide() with no mocks: give it a state and an input, check what comes out."""

from __future__ import annotations

import pytest

from settle.core.decide import Rejected, UnknownAttempt, apply, decide, replay
from settle.core.messages import (
    AlertOps,
    CancelRequested,
    CheckSpent,
    CheckStatus,
    Command,
    EscalatedToOps,
    Event,
    Input,
    Notify,
    PaymentClosed,
    PaymentPaid,
    PayRequested,
    PspAnswer,
    Recover,
    RefundAnswer,
    RefundConfirmed,
    RefundRetried,
    RefundStarted,
    SendRefund,
    SendToPsp,
    SetTimer,
    TimerFired,
    event_from,
)
from settle.core.model import AttemptStatus, Payment, PaymentStatus

S = AttemptStatus
P = PaymentStatus
T0 = 1_700_000_000.0


class Story:
    """Feeds inputs through decide() and apply(), like the engine does, minus the I/O."""

    def __init__(self) -> None:
        self.p: Payment | None = None
        self.log: list[Event] = []

    def __call__(self, inp: Input, bugs: frozenset[str] = frozenset()) -> tuple[list[Event], list[Command]]:
        events, commands = decide(self.p, inp, bugs)
        for e in events:
            self.p = apply(self.p, e)
        self.log += events
        return events, commands

    @property
    def pay(self) -> Payment:
        assert self.p is not None
        return self.p

    def attempt(self, n: int = 1) -> AttemptStatus:
        a = self.pay.attempt(f"p00x.{n}")
        assert a is not None
        return a.status


def kinds(items: list[Event] | list[Command]) -> list[str]:
    return [i.kind for i in items]


def pay(now: float = T0, n: int = 1, psp: str | None = "axis", amount: int = 49900) -> PayRequested:
    return PayRequested(
        now=now, attempt_id=f"p00x.{n}", psp=psp, amount=amount, payment_id="p00x", order_id="ord-1", shard=0
    )


def answer(signal: str, n: int = 1, now: float = T0 + 1, via: str = "reply", code: str | None = None, check: int = 0) -> PspAnswer:
    return PspAnswer(now=now, attempt_id=f"p00x.{n}", signal=signal, via=via, code=code, n=check)


@pytest.fixture
def story() -> Story:
    return Story()


def test_first_pay_creates_the_payment_stores_the_attempt_then_sends(story: Story) -> None:
    events, commands = story(pay())
    assert kinds(events) == ["PaymentCreated", "AttemptStarted"]
    assert kinds(commands) == ["SetTimer", "SendToPsp"]
    assert story.pay.status is P.AWAITING
    assert story.attempt() is S.SENDING


def test_happy_path_accepted_then_callback(story: Story) -> None:
    story(pay())
    events, commands = story(answer("accepted"))
    assert story.attempt() is S.PENDING
    assert [c for c in commands if isinstance(c, SetTimer)][0].at == T0 + 90
    events, commands = story(answer("success", via="callback", now=T0 + 20))
    assert kinds(events) == ["AttemptMoved", "PaymentPaid"]
    assert isinstance(commands[0], Notify) and commands[0].what == "paid"
    assert story.pay.status is P.PAID
    moved = events[0]
    assert getattr(moved, "outcome") == "clean"


def test_one_live_attempt_per_order(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    with pytest.raises(Rejected) as e:
        story(pay(n=2))
    assert e.value.status == 409
    assert e.value.retry_at == T0 + 90  # the next status-check time


def test_a_definite_failure_lets_the_customer_pay_again(story: Story) -> None:
    story(pay())
    _, commands = story(answer("declined", code="ZM"))
    note = commands[0]
    assert isinstance(note, Notify) and note.blame == "customer" and "PIN" in note.message
    assert story.pay.status is P.AWAITING
    events, _ = story(pay(n=2, psp="icici", now=T0 + 30))
    assert kinds(events) == ["AttemptStarted"]


def test_paying_a_paid_order_again_returns_it_unchanged(story: Story) -> None:
    story(pay())
    story(answer("success", via="callback"))
    assert story(pay(n=2)) == ([], [])


def test_a_different_amount_for_the_same_order_is_refused(story: Story) -> None:
    story(pay())
    story(answer("declined", code="Z9"))
    with pytest.raises(Rejected):
        story(pay(n=2, amount=100))


def test_no_healthy_psp_creates_nothing(story: Story) -> None:
    with pytest.raises(Rejected) as e:
        story(pay(psp=None))
    assert e.value.status == 503
    assert story.p is None


def test_hold_expiry_with_nothing_live_closes(story: Story) -> None:
    story(pay())
    story(answer("declined", code="Z9"))
    events, _ = story(TimerFired(now=T0 + 600, kind="hold"))
    assert kinds(events) == ["PaymentExpired", "PaymentClosed"]
    assert story.pay.status is P.CLOSED


def test_a_success_after_expiry_is_refunded_and_resent_until_confirmed(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    story(CancelRequested(now=T0 + 10))
    assert story.pay.status is P.EXPIRED
    events, commands = story(answer("success", via="callback", now=T0 + 40))
    assert kinds(events) == ["AttemptMoved", "RefundStarted"]
    refund = commands[0]
    assert isinstance(refund, SendRefund) and refund.refund == "rf-p00x.1"
    assert story.pay.status is P.REFUNDING

    _, commands = story(RefundAnswer(now=T0 + 41, refund_id="rf-p00x.1", confirmed=False))
    timer = commands[0]
    assert isinstance(timer, SetTimer) and timer.timer == "refund_retry" and timer.n == 2
    events, commands = story(TimerFired(now=timer.at, kind="refund_retry", ref=timer.ref, n=2))
    assert isinstance(events[0], RefundRetried)
    assert isinstance(commands[0], SendRefund) and commands[0].refund == "rf-p00x.1"  # same refund id
    events, commands = story(RefundAnswer(now=T0 + 80, refund_id="rf-p00x.1", confirmed=True))
    assert isinstance(events[0], RefundConfirmed)
    assert story.pay.status is P.REFUNDED


def test_a_refund_unconfirmed_after_five_tries_goes_to_ops(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    story(CancelRequested(now=T0 + 5))
    story(answer("success", via="callback", now=T0 + 10))
    now = T0 + 10
    for n in range(2, 6):
        _, commands = story(RefundAnswer(now=now, refund_id="rf-p00x.1", confirmed=False))
        t = commands[0]
        assert isinstance(t, SetTimer)
        now = t.at
        story(TimerFired(now=now, kind="refund_retry", ref=t.ref, n=n))
    _, commands = story(RefundAnswer(now=now, refund_id="rf-p00x.1", confirmed=False))
    t = commands[0]
    assert isinstance(t, SetTimer)
    events, commands = story(TimerFired(now=t.at, kind="refund_retry", ref=t.ref, n=6))
    assert isinstance(events[0], EscalatedToOps) and events[0].reason == "refund_unconfirmed"
    assert isinstance(commands[0], AlertOps)
    assert story.pay.status is P.NEEDS_OPS


def test_a_duplicate_callback_changes_nothing(story: Story) -> None:
    story(pay())
    story(answer("success", via="callback"))
    assert story(answer("success", via="callback", now=T0 + 5)) == ([], [])


def test_failure_after_success_is_never_applied(story: Story) -> None:
    story(pay())
    story(answer("success", via="callback"))
    events, commands = story(answer("failure", via="callback", code="U28", now=T0 + 5))
    e = events[0]
    assert isinstance(e, EscalatedToOps) and e.first == "succeeded" and e.then == "failure"  # both answers kept
    assert isinstance(commands[0], AlertOps)
    assert story.attempt() is S.SUCCEEDED
    assert story.pay.status is P.NEEDS_OPS
    assert story(answer("failure", via="callback", code="U28", now=T0 + 6)) == ([], [])  # once is enough


def test_status_checks_obey_npci(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    # a timer that fires early is pushed back, not obeyed
    _, commands = story(TimerFired(now=T0 + 30, kind="check", ref="p00x.1", n=1))
    t = commands[0]
    assert isinstance(t, SetTimer) and t.at == T0 + 90
    events, commands = story(TimerFired(now=T0 + 90, kind="check", ref="p00x.1", n=1))
    assert isinstance(events[0], CheckSpent) and isinstance(commands[0], CheckStatus)
    _, commands = story(answer("pending", via="check", check=1, now=T0 + 91))
    t = commands[0]
    assert isinstance(t, SetTimer) and t.at == T0 + 270 and t.n == 2
    story(TimerFired(now=T0 + 270, kind="check", ref="p00x.1", n=2))
    story(answer("pending", via="check", check=2, now=T0 + 271))
    story(TimerFired(now=T0 + 540, kind="check", ref="p00x.1", n=3))
    events, _ = story(answer("pending", via="check", check=3, now=T0 + 541))
    assert story.attempt() is S.UNRESOLVED  # three checks spent: never a fourth
    assert story.pay.status is P.AWAITING  # still live, so it waits for the hold time
    events, _ = story(TimerFired(now=T0 + 600, kind="hold"))
    assert kinds(events) == ["PaymentExpired", "EscalatedToOps"]
    assert story.pay.status is P.NEEDS_OPS
    # a late answer is still the truth: the money goes back
    events, _ = story(answer("success", via="callback", now=T0 + 3000))
    assert kinds(events) == ["AttemptMoved", "RefundStarted"]


def test_a_late_failure_closes_an_order_with_ops(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    story(CancelRequested(now=T0 + 1))
    for n, t in ((1, 90), (2, 270), (3, 540)):
        story(TimerFired(now=T0 + t, kind="check", ref="p00x.1", n=n))
        story(answer("pending", via="check", check=n, now=T0 + t + 1))
    assert story.pay.status is P.NEEDS_OPS
    events, _ = story(answer("failure", via="callback", code="U30", now=T0 + 4000))
    assert kinds(events) == ["AttemptMoved", "PaymentClosed"]
    assert story.pay.status is P.CLOSED


def test_a_transport_error_on_a_check_stops_checking(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    story(TimerFired(now=T0 + 90, kind="check", ref="p00x.1", n=1))
    events, commands = story(answer("transport_error", via="check", check=1, now=T0 + 91))
    assert kinds(events) == ["ChecksStopped"]
    t = commands[0]
    assert isinstance(t, SetTimer) and t.timer == "give_up"
    # the planned second check now does nothing
    assert story(TimerFired(now=T0 + 270, kind="check", ref="p00x.1", n=2)) == ([], [])
    story(TimerFired(now=t.at, kind="give_up", ref="p00x.1"))
    assert story.attempt() is S.UNRESOLVED


def test_recovery_treats_sending_as_unknown_and_never_resends(story: Story) -> None:
    story(pay())  # stored, then the process died before or after sending
    events, commands = story(Recover(now=T0 + 5))
    assert kinds(events) == ["AttemptMoved"]
    assert story.attempt() is S.PENDING
    assert not any(isinstance(c, SendToPsp) for c in commands)
    assert {c.timer for c in commands if isinstance(c, SetTimer)} == {"check", "hold"}


def test_a_check_whose_answer_died_in_a_crash_stops_checking(story: Story) -> None:
    """The lost answer may have been a transport error, after which NPCI forbids another check."""
    story(pay())
    story(answer("read_timeout"))
    story(TimerFired(now=T0 + 90, kind="check", ref="p00x.1", n=1))
    story(answer("pending", via="check", check=1, now=T0 + 91))
    story(TimerFired(now=T0 + 270, kind="check", ref="p00x.1", n=2))  # then the crash
    events, commands = story(Recover(now=T0 + 275))
    assert kinds(events) == ["ChecksStopped"]
    assert getattr(events[0], "reason") == "check_answer_lost"
    assert not any(isinstance(c, SetTimer) and c.timer == "check" for c in commands)
    give_up = [c for c in commands if isinstance(c, SetTimer) and c.timer == "give_up"][0]
    story(answer("success", via="callback", now=T0 + 300))  # a callback still settles it
    assert story.attempt() is S.SUCCEEDED
    assert give_up.at >= T0 + 365


def test_recovery_resumes_checks_when_every_answer_was_stored(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    story(TimerFired(now=T0 + 90, kind="check", ref="p00x.1", n=1))
    story(answer("pending", via="check", check=1, now=T0 + 91))
    _, commands = story(Recover(now=T0 + 100))
    check = [c for c in commands if isinstance(c, SetTimer) and c.timer == "check"][0]
    assert (check.at, check.n) == (T0 + 270, 2)


def test_recovery_resends_an_unconfirmed_refund_with_the_same_id(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    story(CancelRequested(now=T0 + 1))
    story(answer("success", via="callback", now=T0 + 2))
    events, commands = story(Recover(now=T0 + 3))
    refund = [c for c in commands if isinstance(c, SendRefund)][0]
    assert refund.refund == "rf-p00x.1"


def test_unknown_attempt_is_rejected(story: Story) -> None:
    story(pay())
    with pytest.raises(UnknownAttempt):
        story(PspAnswer(now=T0, attempt_id="p00x.9", signal="success", via="callback"))


def test_a_pending_success_counts_less_than_a_clean_one(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    events, _ = story(answer("success", via="callback", now=T0 + 30))
    assert getattr(events[0], "outcome") == "pending_success"


def test_replaying_the_stored_events_gives_the_same_state(story: Story) -> None:
    story(pay())
    story(answer("read_timeout"))
    story(CancelRequested(now=T0 + 10))
    story(answer("success", via="callback", now=T0 + 40))
    story(RefundAnswer(now=T0 + 41, refund_id="rf-p00x.1", confirmed=True))
    rebuilt = replay([event_from(e.kind, e.data(), e.at) for e in story.log])
    assert rebuilt == story.p


@pytest.mark.parametrize("bug", ["no_live_check", "timeout_is_failure"])
def test_the_planted_bugs_really_break_the_rule(story: Story, bug: str) -> None:
    story(pay())
    story(answer("read_timeout"), frozenset({bug}))
    events, _ = story(pay(n=2), frozenset({bug}))
    assert kinds(events) == ["AttemptStarted"]  # a second attempt while the first may still succeed


def test_paid_then_cancel_is_refused(story: Story) -> None:
    story(pay())
    story(answer("success", via="callback"))
    with pytest.raises(Rejected):
        story(CancelRequested(now=T0 + 5))


def test_closed_payment_events() -> None:
    s = Story()
    s(pay())
    s(answer("not_delivered"))
    events, _ = s(CancelRequested(now=T0 + 1))
    assert isinstance(events[-1], PaymentClosed)
    assert not isinstance(events[-1], PaymentPaid)
    assert not any(isinstance(e, RefundStarted) for e in s.log)
