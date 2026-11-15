"""Prometheus metrics for shards and gateways.

Each process gets its own registry, passed in like everything else, so tests never share
counters. Counters are fed by an Observer on stored events (nothing in decide() or the engine
knows about metrics). Gauges that describe the shard's state are refreshed by the writer thread
itself once a second, because only the writer may read payment state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from settle.core.messages import (
    AttemptMoved,
    AttemptStarted,
    CheckSpent,
    EscalatedToOps,
    Event,
    PaymentClosed,
    PaymentPaid,
    RefundConfirmed,
    RefundRetried,
    RefundStarted,
)
from settle.core.model import AttemptStatus, PaymentStatus
from settle.policies.breaker import BreakerState
from settle.policies.routing import blame

if TYPE_CHECKING:
    from settle.shard.engine import PaymentEngine

BREAKER_VALUE = {BreakerState.CLOSED: 0, BreakerState.HALF_OPEN: 1, BreakerState.OPEN: 2}
PENDING_SIGNALS = {"read_timeout", "deemed", "recovered"}
FINAL = {AttemptStatus.SUCCEEDED.value, AttemptStatus.FAILED.value}
CERTAINTY_BUCKETS = (5, 10, 20, 30, 60, 90, 180, 300, 600, 1200, 3600)
STEP_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
REQUEST_BUCKETS = (0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class ShardMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        r = self.registry = registry or CollectorRegistry()
        self.events = Counter("settle_events_total", "Events stored, by kind", ["kind"], registry=r)
        self.attempts = Counter("settle_attempts_total", "Attempts started, by PSP", ["psp"], registry=r)
        self.outcomes = Counter("settle_attempt_outcomes_total", "Final attempt outcomes as the router sees them",
                                ["psp", "outcome", "blame"], registry=r)
        self.went_pending = Counter("settle_attempts_went_pending_total",
                                    "Attempts that left the customer at pending (timeout, deemed, restart)", ["psp"],
                                    registry=r)
        self.certainty = Histogram("settle_time_to_certainty_seconds",
                                   "From sending to a final answer, for attempts that went pending", ["psp"],
                                   buckets=CERTAINTY_BUCKETS, registry=r)
        self.checks = Counter("settle_status_checks_total", "Status checks spent (NPCI allows three)", ["psp"],
                              registry=r)
        self.finished = Counter("settle_payments_finished_total", "Payments that reached a final state", ["status"],
                                registry=r)
        self.escalations = Counter("settle_escalations_total", "Payments sent to a person, by reason", ["reason"],
                                   registry=r)
        self.refunds = Counter("settle_refunds_total", "Refunds started, retried and confirmed", ["state"], registry=r)
        self.breaker = Gauge("settle_breaker_state", "Circuit breaker: 0 closed, 1 half open, 2 open", ["psp"],
                             registry=r)
        self.score = Gauge("settle_router_score", "Share of good outcomes in the last minute (NaN without data)",
                           ["psp"], registry=r)
        self.live = Gauge("settle_live_attempts", "Attempts that might still take the customer's money", registry=r)
        self.needs_ops = Gauge("settle_needs_ops_payments", "Payments waiting for a person", registry=r)
        self.owned = Gauge("settle_owned_logical_shards", "Logical shards this process holds the lock for",
                           registry=r)
        self.inbox = Gauge("settle_inbox_depth", "Messages waiting for the writer thread", registry=r)
        self.timers = Gauge("settle_timers", "Timers waiting in the min-heap", registry=r)
        self.step = Histogram("settle_step_seconds", "One writer step: load, decide, store, act",
                              buckets=STEP_BUCKETS, registry=r)
        # attempt -> (psp, sent at, went pending), for live attempts only. Fed by events, and topped
        # up from state after a takeover, when this process never saw the attempt start.
        self._live: dict[str, tuple[str, float, bool]] = {}

    def on_event(self, payment_id: str, e: Event) -> None:
        self.events.labels(e.kind).inc()
        match e:
            case AttemptStarted():
                self.attempts.labels(e.psp).inc()
                self._live[e.attempt] = (e.psp, e.at, False)
            case AttemptMoved():
                known = self._live.get(e.attempt)
                if (e.signal in PENDING_SIGNALS or e.via in ("check", "settlement")) and known and not known[2]:
                    self.went_pending.labels(e.psp).inc()
                    known = self._live[e.attempt] = (known[0], known[1], True)
                if e.outcome:
                    self.outcomes.labels(e.psp, e.outcome, blame(e.code) if e.outcome == "failed" else "none").inc()
                if e.to in FINAL:
                    self._live.pop(e.attempt, None)
                    if known is not None and known[2]:
                        self.certainty.labels(e.psp).observe(e.at - known[1])
            case CheckSpent():
                known = self._live.get(e.attempt)
                self.checks.labels(known[0] if known else "unknown").inc()
            case PaymentPaid():
                self.finished.labels(PaymentStatus.PAID.value).inc()
            case PaymentClosed():
                self.finished.labels(PaymentStatus.CLOSED.value).inc()
            case RefundStarted():
                self.refunds.labels("started").inc()
            case RefundRetried():
                self.refunds.labels("retried").inc()
            case RefundConfirmed():
                self.refunds.labels("confirmed").inc()
                self.finished.labels(PaymentStatus.REFUNDED.value).inc()
            case EscalatedToOps():
                self.escalations.labels(e.reason).inc()

    def refresh(self, engine: PaymentEngine, inbox_depth: int) -> None:
        """On the writer thread: copy the shard's state into gauges."""
        now = engine.clock.now()
        for name, b in engine.breakers.items():
            b.available(now)
            self.breaker.labels(name).set(BREAKER_VALUE[b.state])
            good, n = engine.health.window(name, now)
            self.score.labels(name).set(good / n if n else float("nan"))
        live = needs_ops = 0
        for cached in engine.cache.values():
            p = cached.payment
            needs_ops += p.status is PaymentStatus.NEEDS_OPS
            for a in p.attempts:
                if a.live:
                    live += 1
                    self._live.setdefault(a.id, (a.psp, a.started_at, a.went_pending))
        self.live.set(live)
        self.needs_ops.set(needs_ops)
        self.owned.set(len(engine.owned) if engine.owned is not None else 12)
        self.inbox.set(inbox_depth)
        self.timers.set(len(engine.timers))

    def expose(self) -> bytes:
        return generate_latest(self.registry)


class GatewayMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        r = self.registry = registry or CollectorRegistry()
        self.requests = Counter("settle_gateway_requests_total", "Requests forwarded, by route and status code",
                                ["route", "code"], registry=r)
        self.seconds = Histogram("settle_gateway_request_seconds", "Time to answer, by route", ["route"],
                                 buckets=REQUEST_BUCKETS, registry=r)
        self.shed = Counter("settle_gateway_shed_total", "Requests refused because a shard's bulkhead was full",
                            registry=r)
        self.retries = Counter("settle_gateway_retries_total", "Forwards sent again after a dead or moved owner",
                               registry=r)
        self.owners = Gauge("settle_gateway_owners_known", "Logical shards with a known owner", registry=r)

    def expose(self) -> bytes:
        return generate_latest(self.registry)
