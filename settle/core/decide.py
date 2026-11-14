"""Every business rule in one pure function.

    decide(payment, input) -> (events, commands)

No database, no network, no clock and no randomness in here. The time and the router's PSP
choice come in with the input. The engine around it loads events, calls decide(), stores the
new events, and only then runs the commands. A payment's state is apply() folded over its
events, so any history can be rebuilt and replayed exactly.
"""

from __future__ import annotations

from dataclasses import replace

from settle.core.messages import (
    AlertOps,
    AttemptMoved,
    AttemptStarted,
    CancelRequested,
    CheckAnswered,
    CheckSpent,
    ChecksStopped,
    CheckStatus,
    Command,
    EscalatedToOps,
    Event,
    Input,
    Notify,
    PaymentClosed,
    PaymentCreated,
    PaymentExpired,
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
)
from settle.core.model import (
    Attempt,
    AttemptStatus,
    IllegalMove,
    Payment,
    PaymentStatus,
    Refund,
    move,
    pay_move,
)
from settle.core.money import Money
from settle.policies.checks import FIRST, FIXED, GAP, MAX_CHECKS, WINDOW, fit_to_rules
from settle.policies.routing import NOT_DELIVERED, TIMEOUT, blame, reason

S = AttemptStatus
P = PaymentStatus

MAX_REFUND_TRIES = 5
REFUND_BACKOFF = (30.0, 60.0, 120.0, 240.0)  # seconds before tries 2, 3, 4 and 5

# Signals after which the customer is left looking at "pending".
PENDING_SIGNALS = frozenset({"read_timeout", "deemed", "recovered"})

# Classic payment bugs, planted behind flags so the simulator can prove it catches each one.
# Production never sets any of them.
BUGS = frozenset(
    {
        "timeout_is_failure",  # a read timeout treated as a failure: the customer may pay again
        "resend_on_timeout",  # the payment sent again after a timeout instead of a status check
        "duplicate_twice",  # a duplicate success callback processed as if it were new
        "early_check",  # the first status check 30 seconds after sending
        "no_refund_after_expiry",  # a success after expiry never refunded
        "no_live_check",  # the one-live-attempt check removed
        "send_before_store",  # (engine) the PSP called before "attempt started" is stored
    }
)


class Rejected(Exception):
    """The input is refused and nothing is stored. Carries what the merchant should see."""

    def __init__(self, message: str, *, status: int = 409, retry_at: float | None = None) -> None:
        super().__init__(message)
        self.message, self.status, self.retry_at = message, status, retry_at


class UnknownAttempt(Exception):
    """An answer for an attempt or refund this payment never made."""


class _Out:
    """Collects events and commands, folding each event in as it goes so later rules see it."""

    def __init__(self, p: Payment | None, now: float, bugs: frozenset[str]) -> None:
        self.p, self.now, self.bugs = p, now, bugs
        self.events: list[Event] = []
        self.commands: list[Command] = []

    @property
    def pay(self) -> Payment:
        assert self.p is not None
        return self.p

    def emit(self, e: Event) -> None:
        self.p = apply(self.p, e)
        self.events.append(e)

    def do(self, c: Command) -> None:
        self.commands.append(c)


def decide(
    p: Payment | None, inp: Input, bugs: frozenset[str] = frozenset()
) -> tuple[list[Event], list[Command]]:
    match inp:
        case PayRequested():
            out = _pay(p, inp, bugs)
        case CancelRequested():
            out = _cancel(_existing(p), inp, bugs)
        case PspAnswer():
            out = _answer(_existing(p), inp, bugs)
        case RefundAnswer():
            out = _refund_answer(_existing(p), inp, bugs)
        case TimerFired():
            out = _timer(_existing(p), inp, bugs)
        case Recover():
            out = _recover(_existing(p), inp, bugs)
        case _:
            raise TypeError(inp)
    return out.events, out.commands


def _existing(p: Payment | None) -> Payment:
    if p is None:
        raise Rejected("no such order", status=404)
    return p


# ---------------------------------------------------------------------------------------------
# The customer taps pay; the merchant cancels
# ---------------------------------------------------------------------------------------------


def _pay(p: Payment | None, inp: PayRequested, bugs: frozenset[str]) -> _Out:
    try:
        Money(inp.amount)
    except (TypeError, ValueError) as e:
        raise Rejected(str(e), status=400) from e
    out = _Out(p, inp.now, bugs)
    psp = inp.psp
    if p is None:
        if psp is None:
            raise Rejected("no healthy PSP right now", status=503)
        hold_until = inp.now + inp.hold_seconds
        out.emit(
            PaymentCreated(
                at=inp.now,
                payment_id=inp.payment_id,
                order_id=inp.order_id,
                shard=inp.shard,
                amount=inp.amount,
                hold_until=hold_until,
            )
        )
        out.do(SetTimer(timer="hold", at=hold_until))
    else:
        if inp.amount != p.amount.paise:
            raise Rejected("this order was first paid with a different amount", status=422)
        if p.status is P.PAID:
            return out  # already paid: the payment unchanged
        if p.status is not P.AWAITING:
            raise Rejected(f"order is {p.status}")
        if inp.now >= p.hold_until:
            raise Rejected("the hold time is over")
        live = p.live_attempt()
        if live is not None and "no_live_check" not in bugs:  # one live attempt per order
            raise Rejected("an earlier attempt may still succeed", retry_at=live.next_check_at())
        if psp is None:
            raise Rejected("no healthy PSP right now", status=503)
    out.emit(AttemptStarted(at=inp.now, attempt=inp.attempt_id, psp=psp))  # stored first...
    out.do(SendToPsp(attempt=inp.attempt_id, psp=psp, order_id=out.pay.order_id, amount=inp.amount))  # ...then sent
    return out


def _cancel(p: Payment, inp: CancelRequested, bugs: frozenset[str]) -> _Out:
    out = _Out(p, inp.now, bugs)
    if p.status is P.AWAITING:
        _expire(out, "cancelled")
    elif p.status is P.PAID:
        raise Rejected("the order is already paid; cancelling it is not a refund")
    return out  # already given up: nothing more to do


def _expire(out: _Out, why: str) -> None:
    out.emit(PaymentExpired(at=out.now, reason=why))
    out.do(Notify(what="cancelled", message="The order is cancelled. A late payment will be refunded."))
    live = out.pay.live_attempt()
    if live is None:
        out.emit(PaymentClosed(at=out.now))  # every attempt ended, none succeeded
    elif live.status is S.UNRESOLVED:
        _escalate(out, "unresolved", live)  # still unknown after all checks


# ---------------------------------------------------------------------------------------------
# A PSP answers: an HTTP reply, a callback, or a status-check result
# ---------------------------------------------------------------------------------------------


def _answer(p: Payment, inp: PspAnswer, bugs: frozenset[str]) -> _Out:
    a = p.attempt(inp.attempt_id)
    if a is None:
        raise UnknownAttempt(inp.attempt_id)
    out = _Out(p, inp.now, bugs)
    signal, code = inp.signal, inp.code
    if inp.via == "check":
        if signal == "pending":
            return _check_pending(out, a, inp.n)
        if signal == "transport_error":
            return _check_stopped(out, a)
    if "timeout_is_failure" in bugs and signal == "read_timeout":
        signal, code = "failure", TIMEOUT
    try:
        new = move(a.status, signal)
    except IllegalMove:
        return _contradiction(out, a, signal, code)
    if new is a.status:  # duplicate: no-op
        if "duplicate_twice" in bugs and signal == "success":
            _on_success(out, a, again=True)
        return out
    _moved(out, a, new, signal, inp.via, code, inp.plan)
    if "resend_on_timeout" in bugs and signal == "read_timeout":
        out.do(SendToPsp(attempt=a.id + "r", psp=a.psp, order_id=p.order_id, amount=p.amount.paise))
    return out


def _moved(
    out: _Out,
    a: Attempt,
    new: AttemptStatus,
    signal: str,
    via: str,
    code: str | None,
    offsets: tuple[float, ...],
) -> None:
    """Store one attempt transition, then apply what it means for the payment."""
    now = out.now
    plan: tuple[float, ...] = ()
    if new is S.PENDING:
        wanted = list(offsets) or list(FIXED)
        if "early_check" in out.bugs:
            wanted = [30.0] + wanted[1:]  # the planted bug: a first check 30 seconds after sending
        first = _first_offset(out.bugs)
        rel = fit_to_rules(wanted, first=first) or fit_to_rules(list(FIXED), first=first)
        plan = tuple(a.started_at + t for t in rel)
    went_pending = a.went_pending or signal in PENDING_SIGNALS or via in ("check", "settlement")
    outcome = None
    if new is S.SUCCEEDED:
        outcome = "pending_success" if went_pending else "clean"
    elif new in (S.FAILED, S.UNRESOLVED):
        outcome = "failed"
    if new is S.FAILED and code is None:
        code = NOT_DELIVERED if signal == "not_delivered" else None
    if new is S.UNRESOLVED:
        code = TIMEOUT  # the PSP never gave an answer
    # When the PSP's answer was ready. Only a callback or the PSP's own reply says that; a check or a
    # settlement file only says when Settle found out, which would teach the planner the wrong times.
    took = round(now - a.started_at, 6) if signal in ("success", "failure") and via in ("callback", "reply") else None
    out.emit(
        AttemptMoved(
            at=now,
            attempt=a.id,
            psp=a.psp,
            to=new.value,
            signal=signal,
            via=via,
            code=code if new in (S.FAILED, S.UNRESOLVED) else None,
            plan=plan,
            took=took,
            outcome=outcome,
        )
    )
    moved = out.pay.attempt(a.id)
    assert moved is not None
    if new is S.PENDING:
        out.do(SetTimer(timer="check", at=moved.plan[0], ref=moved.id, n=1))
    elif new is S.SUCCEEDED:
        _on_success(out, moved)
    elif new is S.FAILED:
        _on_failure(out, moved)
    elif new is S.UNRESOLVED:
        if out.pay.status is P.EXPIRED:
            _escalate(out, "unresolved", moved)
        # AWAITING: the attempt is still live, so the customer waits until the hold time is over


def _on_success(out: _Out, a: Attempt, again: bool = False) -> None:
    p = out.pay
    if p.status is P.AWAITING and not again:
        out.emit(PaymentPaid(at=out.now, attempt=a.id))
        out.do(Notify(what="paid", attempt=a.id, message="Fulfil the order."))
        return
    # The order was given up (or is with ops): the money goes back.
    if "no_refund_after_expiry" in out.bugs:
        return
    n = sum(1 for r in p.refunds if r.attempt == a.id)
    refund = f"rf-{a.id}" if n == 0 else f"rf-{a.id}-{n + 1}"
    out.emit(RefundStarted(at=out.now, attempt=a.id, refund=refund, amount=p.amount.paise))
    out.do(SendRefund(refund=refund, attempt=a.id, psp=a.psp, amount=p.amount.paise))


def _on_failure(out: _Out, a: Attempt) -> None:
    p = out.pay
    if p.status is P.AWAITING:
        out.do(Notify(what="failed", attempt=a.id, code=a.code, blame=blame(a.code), message=reason(a.code)))
        return  # the customer may pay again
    if (
        p.status in (P.EXPIRED, P.NEEDS_OPS)
        and not p.contradicted
        and p.live_attempt() is None
        and p.succeeded() is None
    ):
        out.emit(PaymentClosed(at=out.now))  # nothing was charged


def _contradiction(out: _Out, a: Attempt, signal: str, code: str | None) -> _Out:
    """The PSP said one thing, then the opposite. Never applied: both answers go to ops."""
    if (a.id, signal) in out.pay.contradictions:
        return out  # the same contradiction again
    out.emit(
        EscalatedToOps(
            at=out.now, reason="contradiction", attempt=a.id, first=a.status.value, then=signal, code=code
        )
    )
    out.do(AlertOps(reason="contradiction", attempt=a.id))
    return out


def _escalate(out: _Out, why: str, a: Attempt | None) -> None:
    attempt = a.id if a is not None else None
    out.emit(EscalatedToOps(at=out.now, reason=why, attempt=attempt))
    out.do(AlertOps(reason=why, attempt=attempt))


# ---------------------------------------------------------------------------------------------
# Status checks: at most three, first after 90 s, 90 s apart, within 2 hours
# ---------------------------------------------------------------------------------------------


def _first_offset(bugs: frozenset[str]) -> int:
    return 30 if "early_check" in bugs else FIRST


def _next_check_time(a: Attempt, spent: int, bugs: frozenset[str]) -> float | None:
    if spent >= MAX_CHECKS or spent >= len(a.plan):
        return None
    t = a.plan[spent]
    t = max(t, a.checks[-1] + GAP) if a.checks else max(t, a.started_at + _first_offset(bugs))
    if t > a.started_at + WINDOW:
        return None
    return t


def _check_due(out: _Out, inp: TimerFired) -> None:
    a = out.pay.attempt(inp.ref or "")
    if a is None or a.status is not S.PENDING or a.checks_stopped:
        return  # answered already, or checking was stopped
    n = len(a.checks) + 1
    if inp.n != n or n > MAX_CHECKS:
        return  # a timer for a check that is already spent
    earliest = a.started_at + _first_offset(out.bugs) if n == 1 else a.checks[-1] + GAP
    if out.now < earliest:
        out.do(SetTimer(timer="check", at=earliest, ref=a.id, n=n))  # too early: NPCI says wait
        return
    if out.now > a.started_at + WINDOW:
        _exhaust(out, a)
        return
    out.emit(CheckSpent(at=out.now, attempt=a.id, n=n))  # counts against the budget even if we crash now
    out.do(CheckStatus(attempt=a.id, psp=a.psp, n=n))


def _check_pending(out: _Out, a: Attempt, n: int) -> _Out:
    """A check came back "still pending"."""
    if a.status is not S.PENDING or a.checks_stopped or n != len(a.checks) or a.answered >= n:
        return out  # stale
    out.emit(CheckAnswered(at=out.now, attempt=a.id, n=n))
    nxt = _next_check_time(a, len(a.checks), out.bugs)
    if nxt is None:
        return _exhaust(out, a)
    out.do(SetTimer(timer="check", at=nxt, ref=a.id, n=len(a.checks) + 1))
    return out


def _check_stopped(out: _Out, a: Attempt, why: str = "transport_error") -> _Out:
    """A check hit a transport error. NPCI: stop checking; wait for a callback, then ops."""
    if a.status is not S.PENDING or a.checks_stopped:
        return out
    give_up_at = max(a.plan[-1] if a.plan else out.now, out.now + GAP)
    out.emit(ChecksStopped(at=out.now, attempt=a.id, reason=why, give_up_at=give_up_at))
    out.do(SetTimer(timer="give_up", at=give_up_at, ref=a.id))
    return out


def _exhaust(out: _Out, a: Attempt) -> _Out:
    _moved(out, a, move(a.status, "checks_exhausted"), "checks_exhausted", "planner", TIMEOUT, ())
    return out


# ---------------------------------------------------------------------------------------------
# Timers and refunds
# ---------------------------------------------------------------------------------------------


def _timer(p: Payment, inp: TimerFired, bugs: frozenset[str]) -> _Out:
    out = _Out(p, inp.now, bugs)
    if inp.kind == "hold":
        if p.status is P.AWAITING and inp.now >= p.hold_until:
            _expire(out, "hold")
    elif inp.kind == "check":
        _check_due(out, inp)
    elif inp.kind == "give_up":
        a = p.attempt(inp.ref or "")
        if a is not None and a.status is S.PENDING and a.checks_stopped:
            _exhaust(out, a)
    elif inp.kind == "refund_retry":
        _refund_due(out, inp.ref or "", inp.n)
    else:
        raise TypeError(f"unknown timer {inp.kind!r}")
    return out


def _refund_due(out: _Out, refund_id: str, n: int) -> None:
    p = out.pay
    r = p.refund(refund_id)
    if r is None or r.confirmed or n != r.tries + 1 or "refund_unconfirmed" in p.ops:
        return  # confirmed, stale, or already with ops
    if r.tries >= MAX_REFUND_TRIES:
        _escalate(out, "refund_unconfirmed", p.attempt(r.attempt))
        return
    a = p.attempt(r.attempt)
    assert a is not None
    out.emit(RefundRetried(at=out.now, refund=r.id, n=r.tries + 1))
    out.do(SendRefund(refund=r.id, attempt=r.attempt, psp=a.psp, amount=r.amount))  # same refund id: the PSP deduplicates


def _refund_answer(p: Payment, inp: RefundAnswer, bugs: frozenset[str]) -> _Out:
    r = p.refund(inp.refund_id)
    if r is None:
        raise UnknownAttempt(inp.refund_id)
    out = _Out(p, inp.now, bugs)
    if r.confirmed:
        return out
    if inp.confirmed:
        out.emit(RefundConfirmed(at=inp.now, refund=r.id))
        if out.pay.status is P.REFUNDED:
            out.do(Notify(what="refunded", attempt=r.attempt, message="The customer's money is on its way back."))
        return out
    delay = REFUND_BACKOFF[min(r.tries - 1, len(REFUND_BACKOFF) - 1)]
    out.do(SetTimer(timer="refund_retry", at=inp.now + delay, ref=r.id, n=r.tries + 1))
    return out


def _recover(p: Payment, inp: Recover, bugs: frozenset[str]) -> _Out:
    """After a restart: SENDING becomes unknown (never resent), and every timer comes back."""
    out = _Out(p, inp.now, bugs)
    for a in p.attempts:
        if a.status is S.SENDING:
            # It may or may not have gone out. Ask, never resend.
            _moved(out, a, move(a.status, "recovered"), "recovered", "recover", None, inp.plan_for(a.psp))
        elif a.status is S.PENDING:
            if a.checks_stopped:
                out.do(SetTimer(timer="give_up", at=a.give_up_at or inp.now, ref=a.id))
                continue
            if len(a.checks) > a.answered:
                # A check went out and its answer died with the old process. It may have hit a
                # transport error, after which NPCI forbids another check, so stop and wait.
                _check_stopped(out, a, "check_answer_lost")
                continue
            nxt = _next_check_time(a, len(a.checks), bugs)
            if nxt is None:
                _exhaust(out, a)
            else:
                out.do(SetTimer(timer="check", at=nxt, ref=a.id, n=len(a.checks) + 1))
    if out.pay.status is P.AWAITING:
        out.do(SetTimer(timer="hold", at=out.pay.hold_until))
    for r in out.pay.refunds:
        if not r.confirmed:
            _refund_due(out, r.id, r.tries + 1)
    return out


# ---------------------------------------------------------------------------------------------
# State = apply(...apply(apply(empty, e1), e2)..., en)
# ---------------------------------------------------------------------------------------------


def _with_attempt(p: Payment, attempt_id: str, **changes: object) -> Payment:
    if p.attempt(attempt_id) is None:
        raise IllegalMove(f"no attempt {attempt_id}")
    return replace(p, attempts=tuple(replace(a, **changes) if a.id == attempt_id else a for a in p.attempts))  # type: ignore[arg-type]


def _with_refund(p: Payment, refund_id: str, **changes: object) -> Payment:
    if p.refund(refund_id) is None:
        raise IllegalMove(f"no refund {refund_id}")
    return replace(p, refunds=tuple(replace(r, **changes) if r.id == refund_id else r for r in p.refunds))  # type: ignore[arg-type]


def apply(p: Payment | None, e: Event) -> Payment:
    """Fold one stored fact into the state. Payment moves go through the table, so an illegal
    one raises instead of slipping through."""
    if isinstance(e, PaymentCreated):
        if p is not None:
            raise IllegalMove("payment created twice")
        return Payment(
            payment_id=e.payment_id,
            order_id=e.order_id,
            shard=e.shard,
            amount=Money(e.amount),
            status=P.AWAITING,
            created_at=e.at,
            hold_until=e.hold_until,
        )
    if p is None:
        raise IllegalMove(f"{e.kind} before PaymentCreated")
    match e:
        case AttemptStarted():
            return replace(p, attempts=p.attempts + (Attempt(e.attempt, e.psp, S.SENDING, started_at=e.at),))
        case AttemptMoved():
            a = p.attempt(e.attempt)
            if a is None:
                raise IllegalMove(f"no attempt {e.attempt}")
            to = S(e.to)
            return _with_attempt(
                p,
                e.attempt,
                status=to,
                went_pending=a.went_pending or e.signal in PENDING_SIGNALS or e.via in ("check", "settlement"),
                plan=e.plan or a.plan,
                code=e.code if e.code is not None else a.code,
                resolved_at=e.at if to in (S.SUCCEEDED, S.FAILED) else a.resolved_at,
            )
        case CheckSpent():
            a = p.attempt(e.attempt)
            if a is None:
                raise IllegalMove(f"no attempt {e.attempt}")
            return _with_attempt(p, e.attempt, checks=a.checks + (e.at,))
        case CheckAnswered():
            return _with_attempt(p, e.attempt, answered=e.n)
        case ChecksStopped():
            return _with_attempt(p, e.attempt, checks_stopped=True, give_up_at=e.give_up_at)
        case PaymentPaid():
            return replace(p, status=pay_move(p.status, "paid"))
        case PaymentExpired():
            return replace(p, status=pay_move(p.status, "expired"))
        case PaymentClosed():
            return replace(p, status=pay_move(p.status, "closed"))
        case RefundStarted():
            status = p.status if p.contradicted else pay_move(p.status, "refund_started")
            return replace(p, status=status, refunds=p.refunds + (Refund(e.refund, e.attempt, e.amount),))
        case RefundRetried():
            return _with_refund(p, e.refund, tries=e.n)
        case RefundConfirmed():
            status = p.status if p.contradicted else pay_move(p.status, "refund_confirmed")
            return replace(_with_refund(p, e.refund, confirmed=True), status=status)
        case EscalatedToOps():
            if e.reason == "contradiction":
                pair = (e.attempt or "", e.then or "")
                return replace(
                    p,
                    status=pay_move(p.status, "contradiction"),
                    ops=p.ops + (e.reason,),
                    contradictions=p.contradictions + (pair,),
                )
            status = p.status if p.status is P.NEEDS_OPS else pay_move(p.status, "escalated")
            return replace(p, status=status, ops=p.ops + (e.reason,))
    raise TypeError(f"unknown event {e.kind}")


def replay(events: list[Event]) -> Payment | None:
    p: Payment | None = None
    for e in events:
        p = apply(p, e)
    return p
