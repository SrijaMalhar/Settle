"""The simulator, live: a world that keeps running while you watch it, for the browser demo.

The engine, router, breakers, check planner and invariant checks are the same code the
simulator and production run. What is new is only the steering: customers arrive as an endless
stream, and PSP outages, slow PSPs, lost callbacks, crashes and planted bugs are switched on and
off while it runs. Two checks the batch simulator makes once at the end are made continuously
here: a payment in a final state is charged exactly what it should be (I2), and no PSP holds
money for an attempt Settle never stored (I6).
"""

from __future__ import annotations

import heapq
import json
import re
import time
from collections import Counter, OrderedDict, deque
from dataclasses import dataclass, replace
from typing import Any

from settle import LOGICAL_SHARDS
from settle.adapters.local_health import LocalHealth
from settle.core.decide import BUGS as PLANTABLE
from settle.core.decide import apply, decide
from settle.core.messages import (
    AttemptMoved,
    AttemptStarted,
    CheckAnswered,
    CheckSpent,
    ChecksStopped,
    EscalatedToOps,
    Event,
    Input,
    PaymentClosed,
    PaymentCreated,
    PaymentExpired,
    PaymentPaid,
    RefundConfirmed,
    RefundRetried,
    RefundStarted,
    SendToPsp,
)
from settle.core.model import FINAL, AttemptStatus, Payment, PaymentStatus
from settle.policies.checks import PercentilePlanner
from settle.policies.routing import CleanSuccessRouter, blame
from settle.replay import rerun
from settle.shard.engine import PaymentEngine
from sim.profiles import SIM_PROFILES, Customers, Faults, SimConfig
from sim.world import T0, SimDispatcher, SimNotifier, SimPsp, TracingStore, Txn, World

S = AttemptStatus
P = PaymentStatus

PSPS = {
    "alpha": (replace(SIM_PROFILES["fast"], name="alpha"), "Fast approvals, rarely fails"),
    "bravo": (replace(SIM_PROFILES["slow"], name="bravo"), "Slow approvals, about 5 minutes"),
    "charlie": (replace(SIM_PROFILES["flaky"], name="charlie"), "Loses callbacks, fails more often"),
}

# A rough network: duplicated and reordered callbacks, refused and reset connections, failed
# status checks, lost refund replies.
CHAOS = Faults(callback_dup=0.10, callback_delay=30.0, refused=0.02, reset_after_send=0.05, black_hole=0.01,
               check_transport_error=0.05, refund_lost_reply=0.20, refund_refused=0.05)
CRASHES = 0.002  # chance per step that Settle dies between storing and acting
LIES = 0.02  # chance that a PSP later contradicts its own answer


@dataclass(frozen=True)
class Bug:
    title: str
    what: str
    faults: Faults  # what gives the bug a chance to bite
    customers: dict[str, float]


BUGS: dict[str, Bug] = {
    "timeout_is_failure": Bug(
        "Timeout treated as a failure",
        "A read timeout is taken as a decline, so the customer pays again while the first attempt can still succeed.",
        Faults(reset_after_send=0.3), {"retry_after_failure": 1.0}),
    "resend_on_timeout": Bug(
        "Resend after a timeout",
        "After a timeout the payment is sent again instead of checking its status.",
        Faults(reset_after_send=0.3), {}),
    "duplicate_twice": Bug(
        "Duplicate callback processed twice",
        "A repeated success callback is treated as a new answer.",
        Faults(callback_dup=0.5), {"cancel": 0.3}),
    "early_check": Bug(
        "Status check after 30 seconds",
        "The first status check goes out at 30 s, inside NPCI's 90-second wait.",
        Faults(callback_loss=0.5), {}),
    "no_refund_after_expiry": Bug(
        "Late success never refunded",
        "Money that arrives after the order was given up is kept.",
        Faults(), {"cancel": 0.5}),
    "send_before_store": Bug(
        "PSP called before the attempt is stored",
        "The request goes out first and is written down after, so a crash in between loses it.",
        Faults(crash=0.01), {}),
    "no_live_check": Bug(
        "One-live-attempt check removed",
        "A second tap starts a second attempt while the first is still live.",
        Faults(reset_after_send=0.2), {"double_tap": 0.5}),
}
assert set(BUGS) == PLANTABLE

INVARIANTS = {
    "I1": "No double charge",
    "I2": "Charged exactly when paid",
    "I3": "NPCI status-check rules",
    "I4": "Only legal state moves",
    "I5": "One live attempt per order",
    "I6": "Settle agrees with the PSP",
}

AMOUNT = re.compile(r"\b(charged|refunded|order of|net charge|expected|only|hold) (-?\d+)(?: paise)?")
BUCKET = 10.0  # seconds of simulated time per chart bucket
BUCKETS = 90
STEP_LIMIT = 20_000  # per call to advance(), so the browser stays responsive


def _merge(*faults: Faults) -> Faults:
    """Every switch at its highest setting among the given mixes."""
    out: dict[str, Any] = {}
    for f in faults:
        for k, v in vars(f).items():
            if k == "outages":
                continue
            out[k] = max(out.get(k, 0.0), v)
    return Faults(**out)


class LivePsp(SimPsp):
    def receive(self, attempt: str, order: str, amount: int, now: float) -> Txn:
        fresh = attempt not in self.txns
        txn = super().receive(attempt, order, amount, now)
        if fresh:
            w = self.w
            assert isinstance(w, LiveWorld)
            w.received.append((now, self.name, attempt))
            if txn.d.first != "declined" and txn.d.final == "success":
                heapq.heappush(w.debits, (txn.decided_at, txn.amount))
        return txn

    def refund(self, refund_id: str, attempt: str, now: float) -> bool:
        fresh = refund_id not in self.refunds
        done = super().refund(refund_id, attempt, now)
        if fresh and done:
            w = self.w
            assert isinstance(w, LiveWorld)
            w.money["refunded"] += self.refunds[refund_id][1]
        return done

    def _callbacks(self, txn: Txn) -> None:
        w = self.w
        assert isinstance(w, LiveWorld)
        if self.name in w.dropping:
            txn.callback_lost = True
            return
        super()._callbacks(txn)


class LiveDispatcher(SimDispatcher):
    def initiate(self, payment_id: str, c: SendToPsp) -> None:
        w = self.w
        assert isinstance(w, LiveWorld)
        if c.psp in w.slow and c.psp not in w.down:  # the PSP gets it; the reply never comes in time
            now, epoch = w.now(), w.epoch
            w.sent_at.setdefault(c.attempt, now)
            w.attempt_psp[c.attempt] = c.psp
            w.psps[c.psp].receive(c.attempt, c.order_id, c.amount, now)
            w.at(now + w.cfg.read_timeout, lambda: w.reply(epoch, c.attempt, "read_timeout"))
            return
        super().initiate(payment_id, c)


class LiveStore(TracingStore):
    """The simulator's store without the trace hash, feeding the live views."""

    def append(self, payment_id: str, events: list[Event], *, expected: int, open: bool,
               cause: Input | None = None) -> None:
        w = self.w
        assert isinstance(w, LiveWorld)
        before = w.checker.state.get(payment_id)
        super(TracingStore, self).append(payment_id, events, expected=expected, open=open, cause=cause)
        w.checker.on_append(payment_id, events)
        w.on_stored(payment_id, before, w.checker.state.get(payment_id), events)


class LiveWorld(World):
    def __init__(self, seed: int = 7, rate: float = 0.5) -> None:
        self.health = LocalHealth()  # Redis in production: it outlives a crashed shard
        self.down: set[str] = set()
        self.slow: set[str] = set()
        self.dropping: set[str] = set()
        self.chaos = self.crashing = self.lying = False
        self.bug: str | None = None
        self.rate = rate
        self.received: deque[tuple[float, str, str]] = deque()
        self.debits: list[tuple[float, int]] = []  # (when the PSP takes it, paise), for successes to come
        self.money: Counter[str] = Counter()
        cfg = SimConfig(profiles={n: p for n, (p, _) in PSPS.items()}, faults=Faults(),
                        customers=Customers(orders=1, rate=rate))
        super().__init__(seed, cfg)
        self.psps = {n: LivePsp(n, p, self) for n, p in self.cfg.profiles.items()}
        self.store = LiveStore(self)
        self.engine = self._engine()

        self.feed: deque[dict[str, Any]] = deque(maxlen=80)
        self.feed_id = 0
        self.counts: Counter[str] = Counter()
        self.status: Counter[str] = Counter()
        self.by_status: dict[str, OrderedDict[str, None]] = {s.value: OrderedDict() for s in P}
        self.unknown: OrderedDict[str, None] = OrderedDict()  # payments with an attempt in doubt
        self.newest: deque[str] = deque(maxlen=400)
        self.flagged: OrderedDict[str, None] = OrderedDict()
        self.by_invariant: Counter[str] = Counter()
        self.seen: set[tuple[str, str]] = set()
        self.recent_violations: deque[dict[str, Any]] = deque(maxlen=30)
        self.final_due: deque[tuple[float, str]] = deque()  # (when to check, payment id): live I2
        self.reported: set[str] = set()  # payments already failed by the live I2 check
        self.unstored: dict[str, str] = {}  # attempts a PSP received that Settle has no record of
        self.known: set[str] = set()
        self.buckets: deque[dict[str, Any]] = deque(maxlen=BUCKETS)
        self.breakers: dict[str, str] = {n: "closed" for n in self.psps}
        self.last_recovery: dict[str, Any] | None = None
        self.order_no = 0
        self.lag = False
        self._bucket(self.now())
        self.at(self.now() + 1.0, self._arrival)
        self.say("info", "Settle is up: 12 logical shards, 3 PSPs, customers arriving.")

    # ------------------------------------------------------------------ the engine, live

    def _engine(self) -> PaymentEngine:
        cfg = self.cfg
        router = CleanSuccessRouter(self.health, w=cfg.weight, seed=self.seed * 1000 + self.epoch)
        e = PaymentEngine(
            store=self.store, psps=list(cfg.profiles), router=router, planner=PercentilePlanner(),
            health=self.health, clock=self.clock, dispatcher=LiveDispatcher(self), notifier=SimNotifier(self),
            ids=self._ids, bugs=frozenset({self.bug}) if self.bug else frozenset(),
            hold_seconds=cfg.customers.hold_seconds,
        )
        e.crash_point = self._maybe_crash
        if self.epoch:
            e.warm_planner(self.store.resolutions(3000))
        return e

    def trace(self, line: str) -> None:
        pass  # no trace hash: this world is not replayed by seed

    def _crash(self, where: str) -> None:
        self.crashes += 1
        self.epoch += 1
        started = time.perf_counter()
        self.engine = self._engine()
        self._recovering = True
        try:
            recovered = sum(self.engine.recover(k) for k in range(LOGICAL_SHARDS))
        finally:
            self._recovering = False
        ms = (time.perf_counter() - started) * 1000
        self.last_recovery = {"t": self.t(), "open": recovered, "ms": round(ms, 1), "where": where}
        self.counts["recovered"] += recovered
        self.say("warn" if where == "manual" else "info",
                 f"Settle crashed ({'by hand' if where == 'manual' else where.replace('_', ' ')}): memory wiped. "
                 f"Recovered {recovered} open payments from the event log in {ms:.0f} ms.")

    def network(self, psp: str) -> str:
        if psp in self.down:
            return "refused"
        return super().network(psp)

    def _apply_faults(self) -> None:
        parts = [Faults()]
        if self.chaos:
            parts.append(CHAOS)
        if self.crashing:
            parts.append(Faults(crash=CRASHES))
        if self.lying:
            parts.append(Faults(contradiction=LIES))
        customers: dict[str, Any] = {"orders": 1, "rate": self.rate}
        if self.bug:
            parts.append(BUGS[self.bug].faults)
            customers.update(BUGS[self.bug].customers)
        self.faults = _merge(*parts)
        self.cfg = replace(self.cfg, customers=replace(Customers(), **customers))
        self.engine.bugs = frozenset({self.bug}) if self.bug else frozenset()

    # ------------------------------------------------------------------ customers, forever

    def _arrival(self) -> None:
        if self.rate > 0:  # the next customer first: this one may crash Settle
            self.at(self.now() + self.rng.expovariate(self.rate), self._arrival)
        self.order_no += 1
        self.arrive(f"o{self.order_no:05d}", self.rng.randint(*self.cfg.customers.amounts))

    # ------------------------------------------------------------------ what was stored

    def on_stored(self, pid: str, before: Payment | None, after: Payment | None, events: list[Event]) -> None:
        if after is None:
            return
        old = before.status.value if before else None
        new = after.status.value
        if old != new:
            if old:
                self.status[old] -= 1
                self.by_status[old].pop(pid, None)
            self.status[new] += 1
            self.by_status[new][pid] = None
            if old == "PAID":
                self.money["owed"] -= after.amount.paise
            if new == "PAID":
                self.money["owed"] += after.amount.paise
            if after.status in FINAL and not after.contradicted:
                self.final_due.append((self.now() + 30.0, pid))
        if after.status is P.EXPIRED and after.live_attempt() is None and not after.contradicted:
            self.final_due.append((self.now() + 30.0, pid))  # nothing in flight: it must close or refund
        doubt = any(a.live and a.went_pending for a in after.attempts)
        if doubt:
            self.unknown[pid] = None
            self.unknown.move_to_end(pid)
        else:
            self.unknown.pop(pid, None)
        b = self.buckets[-1]
        for e in events:
            self.counts["events"] += 1
            match e:
                case PaymentCreated():
                    self.newest.appendleft(pid)
                    self.counts["orders"] += 1
                case AttemptStarted():
                    self.known.add(e.attempt)
                    self.counts["attempts"] += 1
                    b["sent"][e.psp] = b["sent"].get(e.psp, 0) + 1
                case AttemptMoved() if e.outcome is not None:
                    out = b["out"].setdefault(e.psp, Counter())
                    out["failed_psp" if e.outcome == "failed" and e.code and blame(e.code) == "psp" else e.outcome] += 1
                    self._feed_moved(after, e, pid)
                case AttemptMoved():
                    self._feed_moved(after, e, pid)
                case CheckSpent():
                    self.counts["checks"] += 1
                    b["checks"] += 1
                case PaymentPaid():
                    self.counts["paid"] += 1
                case RefundStarted():
                    self.counts["refunds"] += 1
                    self.say("warn", f"{after.order_id}: money arrived after the order was given up. "
                                     f"Refunding ₹{e.amount / 100:,.2f}.", pid)
                case RefundConfirmed():
                    self.say("good", f"{after.order_id}: refund confirmed by the PSP.", pid)
                case EscalatedToOps():
                    self.counts["ops"] += 1
                    why = {"unresolved": "still unknown after every allowed status check",
                           "refund_unconfirmed": "refund unconfirmed after five tries",
                           "contradiction": f"the PSP said {e.first}, then {e.then}"}.get(e.reason, e.reason)
                    self.say("warn", f"{after.order_id}: sent to the ops queue: {why}.", pid)
                case _:
                    pass

    def _feed_moved(self, after: Payment, e: AttemptMoved, pid: str) -> None:
        if e.to == "pending" and e.signal in ("read_timeout", "recovered"):
            self.counts["unknown"] += 1
            why = "timed out" if e.signal == "read_timeout" else "was in flight during a crash"
            nxt = f"; first status check in {e.plan[0] - e.at:.0f} s" if e.plan else ""
            self.say("info", f"{after.order_id}: attempt on {e.psp} {why}. Outcome unknown, "
                             f"so it is checked, never resent{nxt}.", pid)
        elif e.to in ("succeeded", "failed") and e.via == "check":
            self.counts["resolved_by_check"] += 1
            self.say("good", f"{after.order_id}: status check resolved the attempt on {e.psp}: {e.to}.", pid)
        elif e.to == "failed" and e.signal == "not_delivered":
            self.say("info", f"{after.order_id}: {e.psp} refused the connection, so it never had the "
                             f"request. Safe to try another PSP.", pid)

    # ------------------------------------------------------------------ violations and live checks

    def violation(self, invariant: str, detail: str) -> None:
        ref = detail.split(":", 1)[0].strip()
        if (invariant, ref) in self.seen:
            return  # one count per invariant per order or attempt, however many steps it stays broken
        self.seen.add((invariant, ref))
        detail = AMOUNT.sub(lambda m: f"{m[1]} ₹{int(m[2]) / 100:,.2f}", detail)
        super().violation(invariant, detail)
        self.by_invariant[invariant] += 1
        pid = ref.split(".")[0] if ref.startswith("p") else self.store.payment_of(ref)
        if pid:
            self.flagged[pid] = None
            self.flagged.move_to_end(pid)
        v = {"invariant": invariant, "name": INVARIANTS.get(invariant, ""), "detail": detail, "t": self.t(), "pid": pid}
        self.recent_violations.appendleft(v)
        if self.by_invariant[invariant] <= 3 or self.by_invariant[invariant] % 25 == 0:
            self.say("bad", f"{invariant} violated ({INVARIANTS.get(invariant, '')}): {detail}", pid)

    def live_checks(self) -> None:
        now = self.now()
        while self.final_due and self.final_due[0][0] <= now:
            _, pid = self.final_due.popleft()
            p = self.checker.state[pid]
            if p.contradicted or pid in self.reported:
                continue
            if p.status is P.EXPIRED and p.live_attempt() is None:
                taken, refunded, _ = self.ledger(p.order_id, now)
                self.reported.add(pid)
                self.violation("I2", f"{p.order_id}: given up with nothing in flight, but neither closed nor "
                                     f"refunded; the PSPs hold {taken - refunded} paise")
                continue
            if p.status not in FINAL:
                continue
            taken, refunded, _ = self.ledger(p.order_id, now)
            expected = p.amount.paise if p.status is P.PAID else 0
            if taken - refunded != expected:
                self.reported.add(pid)
                self.violation("I2", f"{p.order_id}: {p.status.value}, but the PSPs hold {taken - refunded} paise "
                                     f"(expected {expected})")
        while self.received and self.received[0][0] <= now - 5.0:
            _, psp, attempt = self.received.popleft()
            if attempt not in self.known:
                self.unstored[attempt] = psp
        for attempt, psp in list(self.unstored.items()):
            state = self.psps[psp].txns[attempt].state(now)
            if state == "success":
                self.violation("I6", f"{attempt}: {psp} took the money for an attempt Settle never stored")
            if state != "pending":
                del self.unstored[attempt]

    # ------------------------------------------------------------------ running

    def advance(self, seconds: float) -> None:
        """Run the world forward. Stops early if that would take too long."""
        until = self.now() + seconds
        steps = 0
        while True:
            tq = self._queue[0][0] if self._queue else float("inf")
            te = self.engine.timers.next_due()
            t = min(tq, float("inf") if te is None else te)
            if t > until:
                break
            if steps >= STEP_LIMIT:
                self.lag = True
                return
            if t > self.now():
                self._tick(t)
            if te is not None and te <= tq:
                self._do(self.engine.fire_due)
            else:
                _, _, fn = heapq.heappop(self._queue)
                self._do(fn)
            steps += 1
        self._tick(until)
        self.lag = False

    def _tick(self, t: float) -> None:
        while t >= self.buckets[-1]["t"] + BUCKET:
            self.clock.set(self.buckets[-1]["t"] + BUCKET)
            self.live_checks()
            self._bucket(self.buckets[-1]["t"] + BUCKET)
        self.clock.set(t)

    def _bucket(self, start: float) -> None:
        live = sum(1 for _ in self.unknown)
        self.buckets.append({"t": start, "sent": {}, "checks": 0, "unknown": live, "out": {}})

    def t(self) -> float:
        return round(self.now() - T0, 1)

    def say(self, level: str, text: str, pid: str | None = None) -> None:
        self.feed_id += 1
        self.feed.append({"id": self.feed_id, "t": self.t(), "level": level, "text": text, "pid": pid})


class Demo:
    """What the browser calls. Every method takes and returns plain values or JSON strings."""

    def __init__(self, seed: int = 7) -> None:
        self.w = LiveWorld(seed)

    # ------------------------------------------------------------------ steering

    def advance(self, seconds: float) -> bool:
        self.w.advance(seconds)
        return self.w.lag

    def control(self, raw: str) -> str:
        c = json.loads(raw)
        w = self.w
        what = c.get("what")
        if what == "psp":
            psp, fault, on = c["psp"], c["fault"], bool(c["on"])
            group = {"down": w.down, "slow": w.slow, "dropping": w.dropping}[fault]
            (group.add if on else group.discard)(psp)
            text = {"down": ("is down: every connection refused", "is back up"),
                    "slow": ("is slow: replies miss the 2 s timeout", "replies on time again"),
                    "dropping": ("is dropping every callback", "sends callbacks again")}[fault][0 if on else 1]
            w.say("warn" if on else "good", f"{psp} {text}.")
        elif what in ("chaos", "crashing", "lying"):
            setattr(w, what, bool(c["on"]))
            text = {"chaos": "Network chaos", "crashing": "Random crashes", "lying": "Lying PSPs"}[what]
            w.say("warn" if c["on"] else "good", f"{text} {'on' if c['on'] else 'off'}.")
        elif what == "bug":
            bug = c.get("bug") or None
            if bug is not None and bug not in BUGS:
                raise ValueError(bug)
            w.bug = bug
            if bug:
                w.say("bad", f"Planted bug: {BUGS[bug].title}. {BUGS[bug].what} The invariants should catch it.")
            else:
                w.say("good", "Bug removed: production rules again.")
        elif what == "rate":
            old, w.rate = w.rate, max(0.0, min(5.0, float(c["rate"])))
            if old == 0 and w.rate > 0:
                w.at(w.now() + w.rng.expovariate(w.rate), w._arrival)
        elif what == "crash":
            w._crash("manual")
            w.checker.after_step()
        else:
            raise ValueError(what)
        w._apply_faults()
        return self.snapshot(0)

    # ------------------------------------------------------------------ views

    def snapshot(self, since: int = 0, view: str = "all") -> str:
        w = self.w
        stats = w.engine.stats()["psps"]
        totals = self._money()
        for name, s in stats.items():
            if s["breaker"] != w.breakers[name]:
                text = {"open": f"Circuit breaker for {name} opened: no traffic until a trial in 30 s.",
                        "half_open": f"Breaker for {name} half open: one trial request allowed.",
                        "closed": f"Breaker for {name} closed: {name} is healthy again."}[s["breaker"]]
                w.say("warn" if s["breaker"] == "open" else "good" if s["breaker"] == "closed" else "info", text)
                w.breakers[name] = s["breaker"]
        recent = [b for b in w.buckets]
        window = recent[-6:]
        sent_recent = {n: sum(b["sent"].get(n, 0) for b in window) for n in w.psps}
        total_recent = sum(sent_recent.values()) or 1
        psps = {}
        for name, (_, about) in PSPS.items():
            s = stats[name]
            psps[name] = {
                "about": about, "score": s["score"], "samples": s["samples"], "breaker": s["breaker"],
                "plan": [round(x) for x in s["check_plan"]], "share": round(sent_recent[name] / total_recent, 3),
                "sent_recent": sent_recent[name],
                "minute": dict(sum((Counter(bk["out"].get(name, {})) for bk in window), Counter())),
                "down": name in w.down, "slow": name in w.slow, "dropping": name in w.dropping,
            }
        return json.dumps({
            "t": w.t(),
            "lag": w.lag,
            "rate": w.rate,
            "counts": dict(w.counts),
            "status": {k: v for k, v in w.status.items() if v},
            "in_doubt": len(w.unknown),
            "money": totals,
            "psps": psps,
            "switches": {"chaos": w.chaos, "crashing": w.crashing, "lying": w.lying, "bug": w.bug},
            "invariants": {k: {"name": v, "violations": w.by_invariant.get(k, 0)} for k, v in INVARIANTS.items()},
            "violations": list(w.recent_violations)[:12],
            "recovery": w.last_recovery,
            "crashes": w.crashes,
            "series": [{"t": b["t"] - T0, "sent": b["sent"], "checks": b["checks"], "unknown": b["unknown"]}
                       for b in recent],
            "payments": [self._row(pid) for pid in self._pick(view)],
            "views": {"all": w.counts["orders"], "doubt": len(w.unknown),
                      "refunds": len(w.by_status["REFUNDING"]) + len(w.by_status["REFUNDED"]),
                      "ops": len(w.by_status["NEEDS_OPS"]), "flagged": len(w.flagged)},
            "feed": [f for f in w.feed if f["id"] > since],
        }, separators=(",", ":"))

    def _pick(self, view: str, n: int = 14) -> list[str]:
        w = self.w
        if view == "doubt":
            return list(reversed(w.unknown))[:n]
        if view == "refunds":
            both = list(w.by_status["REFUNDING"]) + list(w.by_status["REFUNDED"])
            return sorted(both, key=lambda p: w.checker.state[p].created_at, reverse=True)[:n]
        if view == "ops":
            return list(reversed(w.by_status["NEEDS_OPS"]))[:n]
        if view == "flagged":
            return list(reversed(w.flagged))[:n]
        return list(w.newest)[:n]

    def _row(self, pid: str) -> dict[str, Any]:
        p = self.w.checker.state[pid]
        return {
            "pid": pid, "order": p.order_id, "amount": p.amount.paise, "status": p.status.value,
            "age": round(self.w.now() - p.created_at), "flagged": pid in self.w.flagged,
            "attempts": [{"psp": a.psp, "status": a.status.value, "doubt": a.went_pending} for a in p.attempts],
        }

    def _money(self) -> dict[str, int]:
        """What the PSPs really hold, against what paid orders are worth."""
        w = self.w
        now = w.now()
        while w.debits and w.debits[0][0] <= now:
            w.money["taken"] += heapq.heappop(w.debits)[1]
        m = w.money
        return {"taken": m["taken"], "refunded": m["refunded"], "net": m["taken"] - m["refunded"], "owed": m["owed"]}

    def detail(self, pid: str) -> str:
        w = self.w
        p = w.checker.state.get(pid)
        if p is None:
            return json.dumps(None)
        events = w.store.load(pid)
        path: list[str] = []
        folded: Payment | None = None
        rows = []
        for seq, e in enumerate(events, 1):
            folded = apply(folded, e)
            if not path or path[-1] != folded.status.value:
                path.append(folded.status.value)
            rows.append({"seq": seq, "t": round(e.at - T0, 2), "kind": e.kind, "text": describe(e, p),
                         "data": e.data(), "status": folded.status.value})
        now = w.now()
        attempts = []
        for a in p.attempts:
            txn = w.psps[a.psp].txns.get(a.id)
            attempts.append({
                "id": a.id, "psp": a.psp, "status": a.status.value, "doubt": a.went_pending, "code": a.code,
                "blame": blame(a.code) if a.code else None,
                "started": round(a.started_at - T0, 1),
                "resolved": round(a.resolved_at - T0, 1) if a.resolved_at else None,
                "checks": [round(c - a.started_at) for c in a.checks],
                "plan": [round(c - a.started_at) for c in a.plan],
                "stopped": a.checks_stopped,
                "truth": "never received" if txn is None else txn.state(now),
                "truth_at": round(txn.decided_at - T0, 1) if txn else None,
                "callback_lost": bool(txn and txn.callback_lost),
            })
        taken, refunded, _ = w.ledger(p.order_id, now)
        known = {a.id for a in p.attempts}
        unrecorded = [{"id": a, "psp": psp, "truth": w.truth(psp, a, now)}
                      for psp, a in w.by_order.get(p.order_id, ()) if a not in known]
        return json.dumps({
            "pid": pid, "order": p.order_id, "amount": p.amount.paise, "status": p.status.value, "path": path,
            "now": w.t(),
            "created": round(p.created_at - T0, 1), "hold_until": round(p.hold_until - T0, 1),
            "attempts": attempts, "unrecorded": unrecorded,
            "refunds": [{"id": r.id, "amount": r.amount, "confirmed": r.confirmed, "tries": r.tries} for r in p.refunds],
            "ops": list(p.ops), "contradicted": p.contradicted,
            "ledger": {"taken": taken, "refunded": refunded},
            "events": rows, "inputs": len(w.store.load_inputs(pid)),
            "violations": [v for v in w.recent_violations if v["pid"] == pid],
            "flagged": pid in w.flagged,
        }, separators=(",", ":"))

    def replay(self, pid: str) -> str:
        """Re-run the stored inputs through decide() with production rules and compare."""
        w = self.w
        stored = w.store.load(pid)
        inputs = w.store.load_inputs(pid)
        started = time.perf_counter()
        _, replayed = rerun(inputs, decide, frozenset(), Counter())
        ms = (time.perf_counter() - started) * 1000
        diff = None
        for i, (a, b) in enumerate(zip(stored, replayed), 1):
            if a != b:
                diff = {"seq": i, "stored": f"{a.kind} {a.data()}", "replayed": f"{b.kind} {b.data()}"}
                break
        if diff is None and len(stored) != len(replayed):
            n = min(len(stored), len(replayed)) + 1
            longer = stored if len(stored) > len(replayed) else replayed
            e = longer[n - 1]
            diff = {"seq": n, "stored": f"{e.kind} {e.data()}" if longer is stored else "nothing",
                    "replayed": f"{e.kind} {e.data()}" if longer is replayed else "nothing"}
        return json.dumps({"inputs": len(inputs), "stored": len(stored), "replayed": len(replayed),
                           "identical": diff is None, "diff": diff, "ms": round(ms, 2)}, default=str)

    @staticmethod
    def catalogue() -> str:
        return json.dumps({
            "psps": {n: about for n, (_, about) in PSPS.items()},
            "bugs": {k: {"title": b.title, "what": b.what} for k, b in BUGS.items()},
            "invariants": INVARIANTS,
        })


def describe(e: Event, p: Payment) -> str:
    """One line per stored event, the way an engineer would read the log."""
    def n(attempt: str) -> str:
        return attempt.rsplit(".", 1)[-1]

    match e:
        case PaymentCreated():
            return f"order {e.order_id} for ₹{e.amount / 100:,.2f}, held for {e.hold_until - e.at:.0f} s"
        case AttemptStarted():
            return f"attempt {n(e.attempt)} stored, then sent to {e.psp}"
        case AttemptMoved():
            parts = [f"attempt {n(e.attempt)} → {e.to}", f"({e.signal} via {e.via}"]
            if e.code:
                parts[-1] += f", {e.code}, {blame(e.code)}'s side"
            parts[-1] += ")"
            a = p.attempt(e.attempt)
            if e.plan and a is not None:
                parts.append("checks due " + ", ".join(f"{x - a.started_at:.0f}" for x in e.plan) + " s after sending")
            if e.took is not None:
                parts.append(f"answered after {e.took:.1f} s")
            return " ".join(parts)
        case CheckSpent():
            return f"status check {e.n} sent for attempt {n(e.attempt)}"
        case CheckAnswered():
            return f"status check {e.n} answered: still pending"
        case ChecksStopped():
            return f"no more checks for attempt {n(e.attempt)}: {e.reason.replace('_', ' ')}"
        case PaymentPaid():
            return f"paid by attempt {n(e.attempt)}: hand over the goods"
        case PaymentExpired():
            return "order given up: " + ("hold ran out" if e.reason == "hold" else "merchant cancelled")
        case RefundStarted():
            return f"late success on attempt {n(e.attempt)}: refund of ₹{e.amount / 100:,.2f} sent"
        case RefundRetried():
            return f"refund retried with the same id (try {e.n})"
        case RefundConfirmed():
            return "refund confirmed"
        case PaymentClosed():
            return "closed: nothing charged"
        case EscalatedToOps():
            if e.reason == "contradiction":
                return f"to ops: the PSP said {e.first}, then {e.then}; neither is applied"
            return f"to ops: {e.reason.replace('_', ' ')}"
    return e.kind
