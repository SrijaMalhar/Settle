"""The engine: one writer thread, I/O threads, write before you act, and recovery."""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future
from typing import Any

import pytest

from settle.adapters.errors import StoreFailed
from settle.adapters.local_health import LocalHealth
from settle.adapters.memory_store import MemoryEventStore
from settle.clock import FakeClock, SystemClock
from settle.core.decide import Rejected
from settle.core.messages import CheckStatus, Event, SendRefund, SendToPsp
from settle.core.model import AttemptStatus, PaymentStatus
from settle.policies.checks import FixedPlanner
from settle.policies.routing import FixedOrderRouter
from settle.ports import PspReply
from settle.shard.engine import PaymentEngine, ShardEngine, ThreadDispatcher
from settle.sharding import logical_shard

S = AttemptStatus
P = PaymentStatus


class FakePsp:
    def __init__(self, name: str, reply: str = "accepted", delay: float = 0.0) -> None:
        self.name, self.reply, self.delay = name, reply, delay
        self.initiated: list[str] = []
        self.checked: list[str] = []
        self.lock = threading.Lock()

    def initiate(self, attempt_id: str, order_id: str, amount: int) -> PspReply:
        with self.lock:
            self.initiated.append(attempt_id)
        time.sleep(self.delay)
        return PspReply(self.reply)

    def check_status(self, attempt_id: str) -> PspReply:
        self.checked.append(attempt_id)
        return PspReply("success")

    def refund(self, refund_id: str, attempt_id: str, amount: int) -> PspReply:
        return PspReply("confirmed")


class Recorder:
    """A dispatcher that only writes down what it was asked to do (for single-threaded tests)."""

    def __init__(self) -> None:
        self.sent: list[SendToPsp] = []
        self.checks: list[CheckStatus] = []
        self.refunds: list[SendRefund] = []

    def initiate(self, payment_id: str, c: SendToPsp) -> None:
        self.sent.append(c)

    def check(self, payment_id: str, c: CheckStatus) -> None:
        self.checks.append(c)

    def refund(self, payment_id: str, c: SendRefund) -> None:
        self.refunds.append(c)


def engine(store: Any, dispatcher: Any, clock: Any, **kw: Any) -> PaymentEngine:
    health = LocalHealth()
    return PaymentEngine(store=store, psps=["axis", "icici"], router=FixedOrderRouter(["axis", "icici"]),
                         planner=FixedPlanner(), health=health, clock=clock, dispatcher=dispatcher, **kw)


def actor(psps: dict[str, FakePsp], store: Any = None, io_threads: int = 16) -> ShardEngine:
    d = ThreadDispatcher(dict(psps), io_threads)  # type: ignore[arg-type]
    e = engine(store or MemoryEventStore(), d, SystemClock())
    fatal: list[BaseException] = []
    shard = ShardEngine(e, on_fatal=fatal.append)
    shard.fatal = fatal  # type: ignore[attr-defined]
    d.bind(shard)
    return shard.start()


def wait_for(cond: Any, timeout: float = 5.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met in time")


def test_fifty_simultaneous_pay_taps_make_exactly_one_attempt() -> None:
    psps = {"axis": FakePsp("axis"), "icici": FakePsp("icici")}
    shard = actor(psps)
    barrier = threading.Barrier(50)
    results: list[str] = []
    lock = threading.Lock()

    def tap() -> None:
        barrier.wait()
        try:
            shard.submit(lambda e: e.pay("ord-1", 49900)).result(5)
            outcome = "ok"
        except Rejected:
            outcome = "409"
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=tap) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    p = shard.submit(lambda e: e.get("ord-1")).result(5)
    assert p is not None and len(p.attempts) == 1
    assert results.count("ok") == 1 and results.count("409") == 49
    wait_for(lambda: len(psps["axis"].initiated) == 1)
    shard.stop()


def test_slow_bank_calls_never_block_the_writer() -> None:
    psps = {"axis": FakePsp("axis", delay=2.0), "icici": FakePsp("icici", delay=2.0)}
    shard = actor(psps, io_threads=40)
    for i in range(30):
        shard.submit(lambda e, i=i: e.pay(f"slow-{i}", 100)).result(5)
    wait_for(lambda: len(psps["axis"].initiated) == 30)  # 30 slow calls now in flight
    start = time.perf_counter()
    shard.submit(lambda e: e.pay("fresh", 100)).result(5)
    took = time.perf_counter() - start
    assert took < 0.05, f"a new request took {took * 1000:.1f} ms with 30 slow calls in flight"
    shard.stop()


def test_reply_then_callback_pays_the_order() -> None:
    psps = {"axis": FakePsp("axis"), "icici": FakePsp("icici")}
    shard = actor(psps)
    p = shard.submit(lambda e: e.pay("ord-2", 100)).result(5)
    attempt = p.attempts[0].id
    wait_for(lambda: shard.submit(lambda e: e.get("ord-2")).result(5).attempts[0].status is S.PENDING)
    shard.submit(lambda e: e.psp_answer(attempt, "success", via="callback")).result(5)
    p = shard.submit(lambda e: e.get("ord-2")).result(5)
    assert p.status is P.PAID
    assert shard.engine.notifier.recent[-1]["what"] == "paid"
    shard.stop()


def test_a_refused_connection_sends_the_next_attempt_elsewhere() -> None:
    psps = {"axis": FakePsp("axis", reply="not_delivered"), "icici": FakePsp("icici")}
    shard = actor(psps)
    shard.submit(lambda e: e.pay("ord-3", 100)).result(5)
    wait_for(lambda: shard.submit(lambda e: e.get("ord-3")).result(5).attempts[0].status is S.FAILED)
    p = shard.submit(lambda e: e.pay("ord-3", 100)).result(5)
    assert [a.psp for a in p.attempts] == ["axis", "icici"]
    shard.stop()


def test_a_lost_store_stops_the_process() -> None:
    class Broken(MemoryEventStore):
        def append(self, payment_id: str, events: list[Event], *, expected: int, open: bool,
                   cause: object = None) -> None:
            raise StoreFailed("connection lost")

    psps = {"axis": FakePsp("axis"), "icici": FakePsp("icici")}
    shard = actor(psps, store=Broken())
    f: Future[Any] = shard.submit(lambda e: e.pay("ord-4", 100))
    with pytest.raises(StoreFailed):
        f.result(5)
    wait_for(lambda: bool(shard.fatal))  # type: ignore[attr-defined]
    assert not psps["axis"].initiated  # nothing was stored, so nothing was sent


class Crash(Exception):
    pass


def test_a_crash_between_storing_and_sending_is_checked_never_resent() -> None:
    store, clock = MemoryEventStore(), FakeClock()
    first = Recorder()
    e1 = engine(store, first, clock)

    def crash(where: str) -> None:
        if where == "after_store":
            raise Crash

    e1.crash_point = crash
    with pytest.raises(Crash):
        e1.pay("ord-5", 100)
    assert first.sent == []  # stored, never sent
    # a new process takes over from the same events
    second = Recorder()
    e2 = engine(store, second, clock)
    assert e2.recover(logical_shard("ord-5")) == 1
    p = e2.get("ord-5")
    assert p is not None and p.attempts[0].status is S.PENDING
    clock.advance(90)
    e2.fire_due()
    assert second.sent == []  # never resent...
    assert [c.n for c in second.checks] == [1]  # ...asked about instead


def test_the_planted_send_before_store_bug_loses_the_attempt() -> None:
    store, clock, d = MemoryEventStore(), FakeClock(), Recorder()
    e = engine(store, d, clock, bugs=frozenset({"send_before_store"}))

    def crash(where: str) -> None:
        if where == "after_send":
            raise Crash

    e.crash_point = crash
    with pytest.raises(Crash):
        e.pay("ord-6", 100)
    assert len(d.sent) == 1  # the PSP has it...
    assert store.payment_of("ord-6") is None  # ...and Settle has no record of it


def test_recovery_rebuilds_hold_timers() -> None:
    store, clock, d = MemoryEventStore(), FakeClock(), Recorder()
    e1 = engine(store, d, clock)
    e1.pay("ord-7", 100)
    e1.psp_answer(d.sent[0].attempt, "declined", "Z9", via="reply")
    e2 = engine(store, Recorder(), clock)
    e2.recover(logical_shard("ord-7"))
    clock.advance(600)
    e2.fire_due()
    p = e2.get("ord-7")
    assert p is not None and p.status is P.CLOSED
