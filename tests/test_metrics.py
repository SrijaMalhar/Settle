"""Prometheus metrics: fed by stored events and the writer's own view of the shard."""

from __future__ import annotations

import pytest
import requests

from settle.adapters.memory_store import MemoryEventStore
from settle.clock import FakeClock
from settle.metrics import ShardMetrics
from tests.test_engine import Recorder, engine


def value(m: ShardMetrics, name: str, **labels: str) -> float:
    v = m.registry.get_sample_value(name, labels)
    return 0.0 if v is None else v


def test_a_payment_that_went_pending_is_counted_and_timed() -> None:
    m, clock, d = ShardMetrics(), FakeClock(), Recorder()
    e = engine(MemoryEventStore(), d, clock, subscribers=[m])
    p = e.pay("ord-1", 100)
    a = p.attempts[0].id
    e.psp_answer(a, "read_timeout", via="reply")
    clock.advance(90)
    e.fire_due()
    clock.advance(1)
    e.psp_answer(a, "success", via="check", n=1)
    assert value(m, "settle_attempts_total", psp="axis") == 1
    assert value(m, "settle_attempts_went_pending_total", psp="axis") == 1
    assert value(m, "settle_status_checks_total", psp="axis") == 1
    assert value(m, "settle_payments_finished_total", status="PAID") == 1
    assert value(m, "settle_attempt_outcomes_total", psp="axis", outcome="pending_success", blame="none") == 1
    assert value(m, "settle_time_to_certainty_seconds_count", psp="axis") == 1
    assert value(m, "settle_time_to_certainty_seconds_sum", psp="axis") == pytest.approx(91)


def test_a_clean_payment_is_not_counted_as_pending() -> None:
    m, clock, d = ShardMetrics(), FakeClock(), Recorder()
    e = engine(MemoryEventStore(), d, clock, subscribers=[m])
    a = e.pay("ord-2", 100).attempts[0].id
    e.psp_answer(a, "accepted", via="reply")
    e.psp_answer(a, "success", via="callback")
    assert value(m, "settle_attempts_went_pending_total", psp="axis") == 0
    assert value(m, "settle_time_to_certainty_seconds_count", psp="axis") == 0
    assert value(m, "settle_attempt_outcomes_total", psp="axis", outcome="clean", blame="none") == 1


def test_gauges_come_from_the_writers_view() -> None:
    m, clock, d = ShardMetrics(), FakeClock(), Recorder()
    e = engine(MemoryEventStore(), d, clock, subscribers=[m])
    e.pay("ord-3", 100)
    for _ in range(5):
        e.breakers["icici"].on_failure("U28", clock.now())
    m.refresh(e, inbox_depth=7)
    assert value(m, "settle_live_attempts") == 1
    assert value(m, "settle_breaker_state", psp="icici") == 2
    assert value(m, "settle_breaker_state", psp="axis") == 0
    assert value(m, "settle_inbox_depth") == 7
    assert b"settle_live_attempts 1.0" in m.expose()


def test_customer_failures_are_labelled_with_who_caused_them() -> None:
    m, clock, d = ShardMetrics(), FakeClock(), Recorder()
    e = engine(MemoryEventStore(), d, clock, subscribers=[m])
    e.psp_answer(e.pay("ord-4", 100).attempts[0].id, "declined", "ZM", via="reply")
    assert value(m, "settle_attempt_outcomes_total", psp="axis", outcome="failed", blame="customer") == 1


@pytest.mark.postgres
def test_both_servers_expose_metrics(pg_dsn: str, serve: object) -> None:
    from tests.test_api import Stack

    stack = Stack(pg_dsn, serve)
    try:
        stack.pay("ord-5")
        stack.wait_status("ord-5", "PAID")
        http = requests.Session()
        http.trust_env = False
        text = http.get(f"{stack.gateway.url}/metrics", timeout=5).text
        assert 'settle_gateway_requests_total{code="202",route="pay"} 1.0' in text
        assert "settle_gateway_request_seconds_bucket" in text
    finally:
        stack.owners.stop()
        stack.shard_engine.stop()
