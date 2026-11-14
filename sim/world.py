"""The simulated world: customers, PSPs that know the truth, a network that misbehaves on
purpose, and the real PaymentEngine running on one thread with a fake clock.

One seeded random generator drives everything, so the same seed gives the same run. The trace
hash covers every stored event and every request a PSP receives.
"""

from __future__ import annotations

import hashlib
import heapq
import itertools
import json
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from settle import LOGICAL_SHARDS
from settle.adapters.local_health import LocalHealth
from settle.adapters.memory_store import MemoryEventStore
from settle.adapters.notifier import MerchantNotifier
from settle.clock import FakeClock
from settle.core.decide import Rejected, UnknownAttempt
from settle.core.messages import CheckStatus, Event, Notify, SendRefund, SendToPsp
from settle.core.model import IllegalMove, PaymentStatus
from settle.mockpsp import Decision, Profile, draw
from settle.policies.checks import CheckPlanner, FixedPlanner, PercentilePlanner
from settle.policies.routing import CleanSuccessRouter, FixedOrderRouter, Router
from settle.shard.engine import PaymentEngine
from sim.invariants import Checker, Violation
from sim.profiles import Faults, SimConfig

T0 = 1_700_000_000.0
P = PaymentStatus


class SimCrash(Exception):
    """Settle's process dies: everything in memory is gone."""


# ------------------------------------------------------------------------------------------------
# PSPs that know the truth
# ------------------------------------------------------------------------------------------------


@dataclass
class Txn:
    attempt: str
    order: str
    amount: int
    received_at: float
    d: Decision
    callback_lost: bool = False

    @property
    def decided_at(self) -> float:
        return self.received_at + (0.0 if self.d.first == "declined" else self.d.after)

    def state(self, now: float) -> str:
        if self.d.first == "declined":
            return "failure"
        return self.d.final if now >= self.decided_at else "pending"


class SimPsp:
    def __init__(self, name: str, profile: Profile, world: World) -> None:
        self.name, self.profile, self.w = name, profile, world
        self.txns: dict[str, Txn] = {}
        self.refunds: dict[str, tuple[str, int]] = {}

    def receive(self, attempt: str, order: str, amount: int, now: float) -> Txn:
        txn = self.txns.get(attempt)
        if txn is not None:
            return txn  # the same attempt id twice is the same payment
        txn = Txn(attempt, order, amount, now, draw(self.profile, self.w.rng))
        self.txns[attempt] = txn
        self.w.trace(f"recv {self.name} {attempt} {now:.6f} {txn.d.first} {txn.d.final} {txn.d.code}")
        self.w.by_order.setdefault(order, []).append((self.name, attempt))
        if txn.d.first != "declined":
            self._callbacks(txn)
        return txn

    def _callbacks(self, txn: Txn) -> None:
        w, f = self.w, self.w.faults
        lost = txn.d.callback_lost or w.rng.random() < f.callback_loss
        txn.callback_lost = lost
        delay = txn.d.callback_delay + (w.rng.uniform(0, f.callback_delay) if f.callback_delay else 0.0)
        at = txn.decided_at + delay
        if not lost:
            w.at(at, lambda: w.callback(self.name, txn.attempt, txn.d.final, txn.d.code))
            if txn.d.callback_dup or w.rng.random() < f.callback_dup:
                w.at(at + w.rng.uniform(0.1, 30), lambda: w.callback(self.name, txn.attempt, txn.d.final, txn.d.code))
            if f.contradiction and w.rng.random() < f.contradiction:  # the PSP later says the opposite
                lie = "failure" if txn.d.final == "success" else "success"
                w.at(at + w.rng.uniform(30, 300), lambda: w.callback(self.name, txn.attempt, lie, "U28"))

    def status(self, attempt: str, now: float) -> tuple[str, str | None]:
        txn = self.txns.get(attempt)
        if txn is None:
            return "pending", None  # "not found" proves nothing: NPCI sends these to settlement files
        state = txn.state(now)
        return state, (txn.d.code if state == "failure" else None)

    def refund(self, refund_id: str, attempt: str, now: float) -> bool:
        if refund_id in self.refunds:
            return True  # deduplicated by refund id
        txn = self.txns.get(attempt)
        if txn is None or txn.state(now) != "success":
            return False
        self.refunds[refund_id] = (attempt, txn.amount)
        self.w.trace(f"refund {self.name} {refund_id} {now:.6f}")
        return True


# ------------------------------------------------------------------------------------------------
# The engine's view of the world
# ------------------------------------------------------------------------------------------------


class TracingStore(MemoryEventStore):
    """The in-memory store, plus a trace hash and the invariant checks on every append."""

    def __init__(self, world: World) -> None:
        super().__init__()
        self.w = world

    def append(self, payment_id: str, events: list[Event], *, expected: int, open: bool) -> None:
        super().append(payment_id, events, expected=expected, open=open)
        for i, e in enumerate(events):
            self.w.trace(f"{payment_id} {expected + i + 1} {e.kind} {json.dumps(e.data(), sort_keys=True)} {e.at:.6f}")
        self.w.checker.on_append(payment_id, events)


class SimNotifier(MerchantNotifier):
    def __init__(self, world: World) -> None:
        super().__init__(keep=10)
        self.w = world

    def notify(self, order_id: str, payment_id: str, c: Notify, at: float) -> None:
        super().notify(order_id, payment_id, c, at)
        self.w.on_notify(order_id, c)


class SimDispatcher:
    """PSP calls become scheduled events. A reply only reaches the process that made the request:
    if Settle crashed in between, the connection died with it."""

    def __init__(self, world: World) -> None:
        self.w = world

    def initiate(self, payment_id: str, c: SendToPsp) -> None:
        w = self.w
        now, epoch, rt = w.now(), w.epoch, w.cfg.read_timeout
        w.sent_at.setdefault(c.attempt, now)
        w.attempt_psp[c.attempt] = c.psp
        fate = w.network(c.psp)
        if fate == "refused":
            w.at(now + 0.005, lambda: w.reply(epoch, c.attempt, "not_delivered"))
            return
        if fate == "black_hole":
            w.at(now + rt, lambda: w.reply(epoch, c.attempt, "read_timeout"))
            return
        txn = w.psps[c.psp].receive(c.attempt, c.order_id, c.amount, now)
        if fate == "reset":
            w.at(now + w.rng.uniform(0.01, rt), lambda: w.reply(epoch, c.attempt, "read_timeout"))
        elif txn.d.reply_latency > rt:
            w.at(now + rt, lambda: w.reply(epoch, c.attempt, "read_timeout"))
        else:
            first = txn.d.first
            code = txn.d.code if first == "declined" else ("RB" if first == "deemed" else None)
            w.at(now + txn.d.reply_latency, lambda: w.reply(epoch, c.attempt, first, code))

    def check(self, payment_id: str, c: CheckStatus) -> None:
        w = self.w
        now, epoch = w.now(), w.epoch
        checks = w.checks.setdefault(c.attempt, [])
        checks.append(now)
        w.checker.on_check(c.attempt, checks, w.stopped.get(c.attempt))
        if w.network(c.psp) == "refused" or (w.faults_on and w.rng.random() < w.faults.check_transport_error):
            w.stopped.setdefault(c.attempt, now)
            w.at(now + 0.5, lambda: w.reply(epoch, c.attempt, "transport_error", via="check", n=c.n))
            return
        signal, code = w.psps[c.psp].status(c.attempt, now)
        w.at(now + w.rng.uniform(0.05, 0.3), lambda: w.reply(epoch, c.attempt, signal, code, via="check", n=c.n))

    def refund(self, payment_id: str, c: SendRefund) -> None:
        w = self.w
        now, epoch, f = w.now(), w.epoch, w.faults
        if w.network(c.psp) == "refused" or (w.faults_on and w.rng.random() < f.refund_refused):
            w.at(now + 0.005, lambda: w.refund_reply(epoch, c.refund, False))
            return
        done = w.psps[c.psp].refund(c.refund, c.attempt, now)
        if w.faults_on and w.rng.random() < f.refund_lost_reply:
            w.at(now + w.cfg.read_timeout, lambda: w.refund_reply(epoch, c.refund, False))
        else:
            w.at(now + w.rng.uniform(0.05, 0.5), lambda: w.refund_reply(epoch, c.refund, done))


# ------------------------------------------------------------------------------------------------
# The world
# ------------------------------------------------------------------------------------------------


@dataclass
class Result:
    seed: int
    hash: str
    violations: list[Violation]
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.violations


class World:
    def __init__(self, seed: int, cfg: SimConfig | None = None) -> None:
        self.seed, self.cfg = seed, cfg or SimConfig()
        self.rng = random.Random(seed)
        self.clock = FakeClock(T0)
        self._queue: list[tuple[float, int, Callable[[], object]]] = []
        self._seq = itertools.count()
        self._hash = hashlib.sha256()
        self.trace_lines: list[str] | None = [] if self.cfg.keep_trace else None
        cust = self.cfg.customers
        self.horizon = cust.orders / cust.rate
        self.faults: Faults = self.cfg.faults or Faults.random(self.rng, list(self.cfg.profiles), self.horizon)
        self.planned_faults = self.faults
        self.faults_on = True
        self.psps = {n: SimPsp(n, p, self) for n, p in self.cfg.profiles.items()}
        self.amounts: dict[str, int] = {}
        self.by_order: dict[str, list[tuple[str, str]]] = {}
        self.sent_at: dict[str, float] = {}
        self.attempt_psp: dict[str, str] = {}
        self.checks: dict[str, list[float]] = {}
        self.stopped: dict[str, float] = {}
        self.violations: list[Violation] = []
        self.epoch = 0
        self.crashes = 0
        self._next_id = 0
        self._recovering = False
        self.store = TracingStore(self)
        self.checker = Checker(self)
        self.engine = self._engine()

    # ------------------------------------------------------------------ plumbing

    def now(self) -> float:
        return self.clock.now()

    def at(self, t: float, fn: Callable[[], object]) -> None:
        heapq.heappush(self._queue, (round(t, 6), next(self._seq), fn))

    def trace(self, line: str) -> None:
        self._hash.update(line.encode())
        self._hash.update(b"\n")
        if self.trace_lines is not None:
            self.trace_lines.append(line)

    def violation(self, invariant: str, detail: str) -> None:
        if len(self.violations) < 50:
            self.violations.append(Violation(invariant, detail, self.now() - T0))

    def _ids(self) -> str:
        self._next_id += 1
        return f"{self._next_id:010x}"

    def _engine(self) -> PaymentEngine:
        cfg = self.cfg
        health = LocalHealth()
        router: Router
        if cfg.router == "fixed":
            router = FixedOrderRouter(list(cfg.profiles))
        else:
            router = CleanSuccessRouter(health, w=cfg.weight, seed=self.seed * 1000 + self.epoch)
        planner: CheckPlanner = FixedPlanner() if cfg.planner == "fixed" else PercentilePlanner()
        e = PaymentEngine(
            store=self.store, psps=list(cfg.profiles), router=router, planner=planner, health=health,
            clock=self.clock, dispatcher=SimDispatcher(self), notifier=SimNotifier(self), ids=self._ids,
            bugs=cfg.bugs, hold_seconds=cfg.customers.hold_seconds,
        )
        e.crash_point = self._maybe_crash
        if self.epoch:
            e.warm_planner(self.store.resolutions(3000))
        return e

    def _maybe_crash(self, where: str) -> None:
        if self.faults_on and not self._recovering and self.faults.crash and self.rng.random() < self.faults.crash:
            raise SimCrash(where)

    def _do(self, fn: Callable[[], Any]) -> None:
        try:
            fn()
        except SimCrash as crash:
            self._crash(str(crash))
        except (Rejected, UnknownAttempt):
            pass
        except IllegalMove as e:
            self.violation("I4", f"the engine raised {e}")
        self.checker.after_step()

    def _crash(self, where: str) -> None:
        """Throw away the engine's memory and restart from the event store, like a standby would."""
        self.crashes += 1
        self.epoch += 1
        self.trace(f"crash {where} {self.now():.6f}")
        self.engine = self._engine()
        self._recovering = True
        try:
            for k in range(LOGICAL_SHARDS):
                self.engine.recover(k)
        finally:
            self._recovering = False

    def network(self, psp: str) -> str:
        """What happens to one connection: refused, black_hole, reset (after sending) or ok."""
        now = self.now()
        for name, start, end in self.faults.outages:
            if name == psp and start <= now - T0 < end:
                return "refused"
        if not self.faults_on:
            return "ok"
        f, r = self.faults, self.rng.random()
        if r < f.refused:
            return "refused"
        if r < f.refused + f.black_hole:
            return "black_hole"
        if r < f.refused + f.black_hole + f.reset_after_send:
            return "reset"
        return "ok"

    # ------------------------------------------------------------------ deliveries

    def reply(self, epoch: int, attempt: str, signal: str, code: str | None = None, via: str = "reply",
              n: int = 0) -> None:
        if epoch != self.epoch:
            return  # the process that asked is gone, and its connection with it
        self.engine.psp_answer(attempt, signal, code, via=via, n=n)

    def refund_reply(self, epoch: int, refund: str, confirmed: bool) -> None:
        if epoch == self.epoch:
            self.engine.refund_answer(refund, confirmed)

    def callback(self, psp: str, attempt: str, signal: str, code: str | None) -> None:
        self.engine.psp_answer(attempt, signal, code, via="callback")  # through a gateway: any process

    # ------------------------------------------------------------------ customers

    def arrive(self, order: str, amount: int) -> None:
        cust = self.cfg.customers
        self.amounts[order] = amount
        self.pay(order, amount)
        if self.rng.random() < cust.double_tap:
            self.at(self.now(), lambda: self.pay(order, amount))  # the same instant
        if self.rng.random() < cust.cancel:
            self.at(self.now() + self.rng.uniform(20, 400), lambda: self.engine.cancel(order))

    def pay(self, order: str, amount: int, tries: int = 0) -> None:
        try:
            self.engine.pay(order, amount)
        except Rejected as e:
            if e.status == 503 and tries < 3:
                self.at(self.now() + 15, lambda: self.pay(order, amount, tries + 1))

    def on_notify(self, order: str, c: Notify) -> None:
        if c.what == "failed" and self.rng.random() < self.cfg.customers.retry_after_failure:
            amount = self.amounts[order]
            self.at(self.now() + self.rng.uniform(3, 20), lambda: self.pay(order, amount))

    # ------------------------------------------------------------------ truth

    def truth(self, psp: str, attempt: str, now: float) -> str:
        txn = self.psps[psp].txns.get(attempt)
        return "never received" if txn is None else txn.state(now)

    def ledger(self, order: str, now: float) -> tuple[int, int, int]:
        """(money taken, money refunded, attempts that could still take money) for one order."""
        taken = refunded = live = 0
        for psp, attempt in self.by_order.get(order, ()):
            p = self.psps[psp]
            txn = p.txns[attempt]
            state = txn.state(now)
            back = sum(amt for a, amt in p.refunds.values() if a == attempt)
            if state == "success":
                taken += txn.amount
            refunded += back
            if state == "pending" or (state == "success" and not back):
                live += 1
        return taken, refunded, live

    def debited_attempts(self, now: float) -> list[tuple[str, str]]:
        return [(n, a) for n, p in self.psps.items() for a, t in p.txns.items() if t.state(now) == "success"]

    # ------------------------------------------------------------------ the run

    def loop(self, until: float) -> None:
        while True:
            tq = self._queue[0][0] if self._queue else float("inf")
            te = self.engine.timers.next_due()
            te = float("inf") if te is None else te
            t = min(tq, te)
            if t == float("inf") or t > until:
                return
            if t > self.now():
                self.clock.set(t)
            if te <= tq:
                self._do(self.engine.fire_due)
            else:
                _, _, fn = heapq.heappop(self._queue)
                self._do(fn)
            if self.checker.steps % 200 == 0:
                self.checker.sweep()

    def run(self) -> Result:
        cust = self.cfg.customers
        t = T0
        for i in range(cust.orders):
            t += self.rng.expovariate(cust.rate)
            order = f"o{self.seed}-{i}"
            amount = self.rng.randint(*cust.amounts)
            self.at(t, partial(self.arrive, order, amount))
        calm_at = t + cust.hold_seconds + 60
        self.at(calm_at, self.calm)
        self.loop(until=T0 + 48 * 3600)
        before = self.metrics()
        self.settle()
        self.loop(until=T0 + 96 * 3600)
        self.checker.final()
        after = self.metrics()
        before["needs_ops_at_end"] = after["status"].get("NEEDS_OPS", 0)
        before["final_status"] = after["status"]
        return Result(self.seed, self._hash.hexdigest(), self.violations, before)

    def calm(self) -> None:
        """Faults stop. Outages end, the network behaves, Settle stops crashing."""
        self.faults_on = False
        self.faults = Faults.calm()
        self.trace(f"calm {self.now():.6f}")

    def settle(self) -> None:
        """What NPCI's settlement files and the ops queue do for payments Settle couldn't resolve:
        deliver the truth for every attempt still unknown, and resend refunds ops was handed."""
        latest = max((t.decided_at for p in self.psps.values() for t in p.txns.values()), default=self.now())
        self.clock.set(max(self.now(), latest + 1))
        for pid in sorted(self.store.events):
            p = self.checker.state.get(pid)
            if p is None:
                continue
            for a in p.attempts:
                if a.live:
                    txn = self.psps[a.psp].txns.get(a.id)
                    signal, code = ("failure", "NF") if txn is None else (txn.state(self.now()), txn.d.code)
                    self._do(partial(self._settlement_answer, a.id, signal, code))
            if "refund_unconfirmed" in p.ops:
                for r in p.refunds:
                    if not r.confirmed:
                        cmd = SendRefund(refund=r.id, attempt=r.attempt, psp=self.attempt_psp[r.attempt], amount=r.amount)
                        self._do(partial(self.engine.dispatcher.refund, pid, cmd))

    def _settlement_answer(self, attempt: str, signal: str, code: str | None) -> None:
        self.engine.psp_answer(attempt, signal, code, via="settlement")

    # ------------------------------------------------------------------ what happened

    def metrics(self) -> dict[str, Any]:
        status: dict[str, int] = {}
        attempts = pending = clean = pending_success = checks = 0
        lost = lost_by_check = lost_to_ops = 0
        certainty: list[float] = []
        for p in self.checker.state.values():
            status[p.status.value] = status.get(p.status.value, 0) + 1
            for a in p.attempts:
                attempts += 1
                checks += len(a.checks)
                pending += a.went_pending
                if a.status.value == "succeeded":
                    if a.went_pending:
                        pending_success += 1
                    else:
                        clean += 1
                txn = self.psps[a.psp].txns.get(a.id)
                if txn is not None and txn.callback_lost and txn.d.first != "declined":
                    lost += 1  # with no callback, only a status check can resolve it before settlement
                    if a.status.value in ("succeeded", "failed"):
                        lost_by_check += 1
                    else:
                        lost_to_ops += 1
                if a.resolved_at is not None and a.went_pending:
                    certainty.append(a.resolved_at - a.started_at)
        successes = clean + pending_success
        return {
            "orders": len(self.amounts),
            "status": status,
            "attempts": attempts,
            "went_pending": pending,
            "clean_successes": clean,
            "pending_successes": pending_success,
            "eventual_successes": successes,
            "completed_if_30pct_abandon": round(clean + 0.7 * pending_success, 1),
            "checks_spent": checks,
            "lost_callbacks": lost,
            "lost_resolved_by_check": lost_by_check,
            "lost_sent_to_ops": lost_to_ops,
            "mean_time_to_certainty": round(sum(certainty) / len(certainty), 1) if certainty else None,
            "crashes": self.crashes,
        }


def run(seed: int, cfg: SimConfig | None = None) -> Result:
    return World(seed, cfg).run()
