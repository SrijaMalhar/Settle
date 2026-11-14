"""The imperative shell around decide().

PaymentEngine does one step at a time: load the payment, call decide(), append the new events
in one transaction, and only then run the commands. It is single-threaded on purpose. In a
shard process the ShardEngine below owns it: ONE writer thread, an inbox, and a pool of I/O
threads for slow PSP calls. In the simulator the same PaymentEngine runs on one thread with a
fake clock, so production and simulation share every rule.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from settle.adapters.errors import StoreFailed
from settle.adapters.notifier import MerchantNotifier
from settle.core.decide import Rejected, UnknownAttempt, apply, decide, replay
from settle.core.messages import (
    AlertOps,
    CancelRequested,
    CheckStatus,
    Command,
    Input,
    Notify,
    PayRequested,
    PspAnswer,
    Recover,
    RefundAnswer,
    SendRefund,
    SendToPsp,
    SetTimer,
    TimerFired,
)
from settle.core.model import Payment, PaymentStatus
from settle.policies.breaker import CircuitBreaker
from settle.policies.checks import CheckPlanner
from settle.policies.routing import Router, blame
from settle.ports import Clock, EventStore, HealthStore, PspGateway, PspReply, Subscriber
from settle.shard.feeds import HealthFeed, PlannerFeed
from settle.shard.timers import TimerQueue
from settle.sharding import logical_shard, payment_of_attempt, shard_of_payment

log = logging.getLogger("settle.engine")

T = TypeVar("T")

EXTERNAL = (SendToPsp, CheckStatus, SendRefund)


class NotOwner(Exception):
    """This process does not hold the lock for that logical shard (any more, or yet)."""


class Dispatcher(Protocol):
    """Runs the slow commands. Threads in production; scheduled events in the simulator."""

    def initiate(self, payment_id: str, c: SendToPsp) -> None: ...

    def check(self, payment_id: str, c: CheckStatus) -> None: ...

    def refund(self, payment_id: str, c: SendRefund) -> None: ...


@dataclass
class _Cached:
    payment: Payment
    seq: int  # how many events are stored


def random_ids() -> str:
    return uuid.uuid4().hex[:16]


class PaymentEngine:
    def __init__(
        self,
        *,
        store: EventStore,
        psps: list[str],
        router: Router,
        planner: CheckPlanner,
        health: HealthStore,
        clock: Clock,
        dispatcher: Dispatcher,
        notifier: MerchantNotifier | None = None,
        subscribers: Iterable[Subscriber] = (),
        ids: Callable[[], str] = random_ids,
        bugs: frozenset[str] = frozenset(),
        owned: Iterable[int] | None = None,
        hold_seconds: float = 600.0,
        breaker_threshold: int = 5,
        breaker_cooldown: float = 30.0,
    ) -> None:
        self.store, self.psps, self.router, self.planner = store, list(psps), router, planner
        self.health, self.clock, self.dispatcher = health, clock, dispatcher
        self.notifier = notifier or MerchantNotifier()
        self.ids, self.bugs, self.hold_seconds = ids, bugs, hold_seconds
        self.owned: set[int] | None = set(owned) if owned is not None else None  # None: every shard
        self.timers = TimerQueue()
        self.breakers = {p: CircuitBreaker(p, health, breaker_threshold, breaker_cooldown) for p in self.psps}
        self.subscribers: list[Subscriber] = [HealthFeed(router, self.breakers), PlannerFeed(planner), *subscribers]
        self.cache: dict[str, _Cached] = {}
        self.by_order: dict[str, str] = {}
        self.crash_point: Callable[[str], None] | None = None  # the simulator's crash fault
        self.steps = 0

    # -------------------------------------------------------------------------------- inputs

    def pay(self, order_id: str, amount: int, hold_seconds: float | None = None) -> Payment:
        shard = self._own(logical_shard(order_id))
        pid = self._payment_id(order_id)
        now = self.clock.now()
        current = self._load(pid).payment if pid else None
        if pid is None:
            pid = f"p{shard:02d}{self.ids()}"
        attempt_id = current.next_attempt_id() if current else f"{pid}.1"
        psp = self.choose_psp(current, now)
        inp = PayRequested(
            now=now,
            attempt_id=attempt_id,
            psp=psp,
            amount=amount,
            hold_seconds=self.hold_seconds if hold_seconds is None else hold_seconds,
            payment_id=pid,
            order_id=order_id,
            shard=shard,
        )
        p = self._step(pid, inp, new=current is None)
        assert p is not None
        if psp is not None and p.attempt(attempt_id) is not None:
            self.breakers[psp].on_attempt(now)
        return p

    def cancel(self, order_id: str) -> Payment:
        self._own(logical_shard(order_id))
        pid = self._payment_id(order_id)
        if pid is None:
            raise Rejected("no such order", status=404)
        p = self._step(pid, CancelRequested(now=self.clock.now()))
        assert p is not None
        return p

    def get(self, order_id: str) -> Payment | None:
        self._own(logical_shard(order_id))
        pid = self._payment_id(order_id)
        return self._load(pid).payment if pid else None

    def psp_answer(self, attempt_id: str, signal: str, code: str | None = None, via: str = "callback",
                   n: int = 0) -> Payment | None:
        """An HTTP reply (from an I/O thread), a PSP callback, or a status-check result."""
        pid = payment_of_attempt(attempt_id)
        self._own(shard_of_payment(pid))
        cached = self._load(pid)
        a = cached.payment.attempt(attempt_id)
        plan = tuple(float(t) for t in self.planner.plan(a.psp)) if a is not None else ()
        inp = PspAnswer(now=self.clock.now(), attempt_id=attempt_id, signal=signal, code=code, via=via, n=n,
                        plan=plan)
        return self._step(pid, inp)

    def refund_answer(self, refund_id: str, confirmed: bool) -> Payment | None:
        pid = payment_of_attempt(refund_id)
        self._own(shard_of_payment(pid))
        return self._step(pid, RefundAnswer(now=self.clock.now(), refund_id=refund_id, confirmed=confirmed))

    def fire_due(self) -> int:
        """Run every timer that is due. Returns how many fired."""
        now = self.clock.now()
        fired = 0
        for t in self.timers.pop_due(now):
            fired += 1
            try:
                self._step(t.payment_id, TimerFired(now=now, kind=t.kind, ref=t.ref, n=t.n))
            except (Rejected, UnknownAttempt) as e:
                log.debug("timer %s for %s ignored: %s", t.kind, t.payment_id, e)
        return fired

    def recover(self, shard: int) -> int:
        """Rebuild state and timers for one logical shard from its events, then take its traffic."""
        if self.owned is not None:
            self.owned.add(shard)
        pids = self.store.open_payments(shard)
        plans = tuple((psp, tuple(float(t) for t in self.planner.plan(psp))) for psp in self.psps)
        for pid in pids:
            self.cache.pop(pid, None)
            self._step(pid, Recover(now=self.clock.now(), plans=plans))
        return len(pids)

    def warm_planner(self, history: list[tuple[str, float]]) -> None:
        """Per-PSP resolution times, rebuilt from stored events at startup (oldest first)."""
        for psp, took in reversed(history):
            self.planner.observe(psp, took)

    # -------------------------------------------------------------------------------- routing

    def choose_psp(self, current: Payment | None, now: float) -> str | None:
        """The router's pick among PSPs whose breaker allows a call. A PSP that already failed
        this order on its own side is skipped while another is available."""
        healthy = [p for p in self.psps if self.breakers[p].available(now)]
        if current is not None:
            burnt = {a.psp for a in current.attempts if a.code is not None and blame(a.code) == "psp"}
            healthy = [p for p in healthy if p not in burnt] or healthy
        return self.router.choose(healthy, now)

    # -------------------------------------------------------------------------------- one step

    def _step(self, pid: str, inp: Input, new: bool = False) -> Payment | None:
        """Load, decide, store, then act. The only place state changes."""
        cached = None if new else self._load(pid)
        p, seq = (cached.payment, cached.seq) if cached else (None, 0)
        events, commands = decide(p, inp, self.bugs)
        self.steps += 1
        if not events and not commands:
            return p
        after = p
        for e in events:
            after = apply(after, e)
        assert after is not None
        if "send_before_store" in self.bugs:  # the planted bug: act, then store
            for c in commands:
                if isinstance(c, SendToPsp):
                    self._run(after, c)
            commands = [c for c in commands if not isinstance(c, SendToPsp)]
            if self.crash_point:
                self.crash_point("after_send")
        if events:
            # Write before you act: all of them or none, through the shard lock's connection.
            self.store.append(pid, events, expected=seq, open=not after.final)
            seq += len(events)
            self._remember(after, seq)
            for e in events:
                for s in self.subscribers:
                    s.on_event(pid, e)
        if self.crash_point:
            self.crash_point("after_store")
        for c in commands:
            self._run(after, c)
        return after

    def _run(self, p: Payment, c: Command) -> None:
        pid = p.payment_id
        match c:
            case SendToPsp():
                self.dispatcher.initiate(pid, c)
            case CheckStatus():
                self.dispatcher.check(pid, c)
            case SendRefund():
                self.dispatcher.refund(pid, c)
            case SetTimer():
                self.timers.push(c.at, pid, c.timer, c.ref, c.n)
            case Notify():
                self.notifier.notify(p.order_id, pid, c, self.clock.now())
            case AlertOps():
                self.notifier.alert(p.order_id, pid, c, self.clock.now())
            case _:
                raise TypeError(c)

    # -------------------------------------------------------------------------------- state

    def _own(self, shard: int) -> int:
        if self.owned is not None and shard not in self.owned:
            raise NotOwner(f"logical shard {shard} is not owned here")
        return shard

    def _payment_id(self, order_id: str) -> str | None:
        pid = self.by_order.get(order_id)
        if pid is None:
            pid = self.store.payment_of(order_id)
        return pid

    def _load(self, pid: str) -> _Cached:
        cached = self.cache.get(pid)
        if cached is None:
            events = self.store.load(pid)
            p = replay(events)
            if p is None:
                raise Rejected("no such payment", status=404)
            cached = _Cached(p, len(events))
            self._remember(p, len(events))
        return cached

    def _remember(self, p: Payment, seq: int) -> None:
        if p.final:  # final payments only change on a PSP contradiction: reload them if that happens
            self.cache.pop(p.payment_id, None)
            self.by_order.pop(p.order_id, None)
            return
        self.cache[p.payment_id] = _Cached(p, seq)
        self.by_order[p.order_id] = p.payment_id

    def forget(self) -> None:
        """Drop everything in memory, as a crash would."""
        self.cache.clear()
        self.by_order.clear()
        self.timers.clear()

    # -------------------------------------------------------------------------------- stats

    def stats(self) -> dict[str, Any]:
        now = self.clock.now()
        psps = {}
        for name in self.psps:
            good, n = self.health.window(name, now)
            b = self.breakers[name]
            b.available(now)
            psps[name] = {
                "score": round(good / n, 4) if n else None,
                "samples": n,
                "breaker": b.state.value,
                "check_plan": list(self.planner.plan(name)),
            }
        waiting = sorted(
            (c.payment.order_id for c in self.cache.values() if c.payment.status is PaymentStatus.NEEDS_OPS)
        )
        return {
            "owned": sorted(self.owned) if self.owned is not None else "all",
            "psps": psps,
            "needs_ops": waiting,
            "open_in_memory": len(self.cache),
            "timers": len(self.timers),
            "recent_alerts": list(self.notifier.alerts)[-20:],
        }


# ------------------------------------------------------------------------------------------------
# The actor: one writer thread per shard process
# ------------------------------------------------------------------------------------------------


class ShardEngine:
    """Actor model: ONE writer thread owns every payment in this shard. HTTP threads and I/O
    threads never touch payment state; they send messages to the writer's inbox and get answers
    back through Futures. No locks in application code: the only shared object is the queue."""

    def __init__(self, engine: PaymentEngine, *, on_fatal: Callable[[BaseException], None],
                 heartbeat: Callable[[], None] | None = None, heartbeat_every: float = 1.0) -> None:
        self.engine = engine
        self.inbox: queue.Queue[tuple[Callable[[PaymentEngine], Any], Future[Any] | None]] = queue.Queue()
        self.on_fatal = on_fatal
        self.heartbeat, self.heartbeat_every = heartbeat, heartbeat_every
        self.writer = threading.Thread(target=self._run, name="writer", daemon=True)
        self._stopping = threading.Event()
        self.processed = 0

    def start(self) -> ShardEngine:
        self.writer.start()
        return self

    def stop(self, timeout: float = 5.0) -> None:
        self._stopping.set()
        self.inbox.put((lambda _e: None, None))
        self.writer.join(timeout)

    def submit(self, fn: Callable[[PaymentEngine], T]) -> Future[T]:  # any thread may call this
        reply: Future[T] = Future()
        self.inbox.put((fn, reply))
        return reply

    def post(self, fn: Callable[[PaymentEngine], Any]) -> None:
        """A message with no one waiting for the answer (I/O results)."""
        self.inbox.put((fn, None))

    def _run(self) -> None:  # the only code that touches payment state
        next_beat = time.monotonic() + self.heartbeat_every
        while not self._stopping.is_set():
            due = self.engine.timers.next_due()
            wait = 0.5 if due is None else min(max(due - self.engine.clock.now(), 0.0), 0.5)
            try:
                fn, reply = self.inbox.get(timeout=wait)
            except queue.Empty:
                fn, reply = None, None
            try:
                if fn is not None:
                    self._handle(fn, reply)
                self.engine.fire_due()
                if self.heartbeat and time.monotonic() >= next_beat:
                    self.heartbeat()  # a lost lock connection must stop this process even when idle
                    next_beat = time.monotonic() + self.heartbeat_every
            except BaseException as e:  # crash-only: the store or the lock is gone
                self.on_fatal(e)
                return

    def _handle(self, fn: Callable[[PaymentEngine], Any], reply: Future[Any] | None) -> None:
        self.processed += 1
        try:
            result = fn(self.engine)
        except (Rejected, UnknownAttempt, NotOwner) as e:
            if reply is not None:
                reply.set_exception(e)
            return
        except StoreFailed as e:
            if reply is not None:
                reply.set_exception(e)
            raise
        except Exception as e:
            log.exception("step failed")
            if reply is not None:
                reply.set_exception(e)
            raise
        if reply is not None:
            reply.set_result(result)


class ThreadDispatcher:
    """Slow PSP calls run on a pool of I/O threads, never on the writer. Each result comes back
    as an ordinary message in the writer's inbox."""

    def __init__(self, psps: dict[str, PspGateway], io_threads: int = 16) -> None:
        self.psps = psps
        self.pool = ThreadPoolExecutor(io_threads, thread_name_prefix="psp-io")
        self.shard: ShardEngine | None = None

    def bind(self, shard: ShardEngine) -> None:
        self.shard = shard

    def _post(self, fn: Callable[[PaymentEngine], Any]) -> None:
        assert self.shard is not None
        self.shard.post(fn)

    def initiate(self, payment_id: str, c: SendToPsp) -> None:
        self.pool.submit(self._initiate, c)

    def check(self, payment_id: str, c: CheckStatus) -> None:
        self.pool.submit(self._check, c)

    def refund(self, payment_id: str, c: SendRefund) -> None:
        self.pool.submit(self._refund, c)

    def _initiate(self, c: SendToPsp) -> None:  # on an I/O thread
        try:
            reply = self.psps[c.psp].initiate(c.attempt, c.order_id, c.amount)
        except Exception:
            log.exception("initiate %s", c.attempt)
            reply = PspReply("read_timeout")  # unknown: never assume it failed
        self._post(lambda e: _quiet(e.psp_answer, c.attempt, reply.signal, reply.code, "reply"))

    def _check(self, c: CheckStatus) -> None:
        try:
            reply = self.psps[c.psp].check_status(c.attempt)
        except Exception:
            log.exception("check %s", c.attempt)
            reply = PspReply("transport_error")
        self._post(lambda e: _quiet(e.psp_answer, c.attempt, reply.signal, reply.code, "check", c.n))

    def _refund(self, c: SendRefund) -> None:
        try:
            reply = self.psps[c.psp].refund(c.refund, c.attempt, c.amount)
        except Exception:
            log.exception("refund %s", c.refund)
            reply = PspReply("unconfirmed")
        self._post(lambda e: _quiet(e.refund_answer, c.refund, reply.signal == "confirmed"))


def _quiet(fn: Callable[..., Any], *args: Any) -> Any:
    """Answers about payments that moved on (or that another process now owns) are dropped."""
    try:
        return fn(*args)
    except (Rejected, UnknownAttempt, NotOwner) as e:
        log.info("answer dropped: %s", e)
        return None

