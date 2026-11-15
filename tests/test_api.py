"""The HTTP surface end to end: merchant -> gateway -> shard -> mock PSP -> signed callback ->
gateway -> shard, with the event log in a real PostgreSQL."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
import requests

from settle.adapters.http_psp import HttpPsp
from settle.adapters.local_health import LocalHealth
from settle.adapters.postgres_store import PostgresEventStore
from settle.clock import SystemClock
from settle.gateway import Gateway, OwnerMap
from settle.gateway import create_app as gateway_app
from settle.mockpsp import MockPsp, Profile
from settle.mockpsp import create_app as psp_app
from settle.policies.checks import FixedPlanner
from settle.policies.routing import FixedOrderRouter
from settle.shard.engine import PaymentEngine, ShardEngine, ThreadDispatcher
from settle.shard.server import create_app as shard_app
from settle.signing import HEADER, sign

pytestmark = pytest.mark.postgres

SECRET = "test-secret"
QUICK = Profile("quick", (0.0, 0.01), 0.3, 0.1, 0, 0, 0, 0, 0, 0, 0, (0.0, 0.05))


class Stack:
    def __init__(self, dsn: str, serve: Any) -> None:
        self.owners = OwnerMap(dsn, every=0.2)
        self.gateway = serve(gateway_app(Gateway(self.owners.start(), deadline=1.5)))
        self.mock = MockPsp("axis", QUICK, callback_url=self.gateway.url, secret=SECRET, seed=3)
        self.psp = serve(psp_app(self.mock))
        conn = psycopg.connect(dsn, autocommit=True)
        dispatcher = ThreadDispatcher({"axis": HttpPsp("axis", self.psp.url)})
        self.engine = PaymentEngine(
            store=PostgresEventStore.single(conn), psps=["axis"], router=FixedOrderRouter(["axis"]),
            planner=FixedPlanner(), health=LocalHealth(), clock=SystemClock(), dispatcher=dispatcher,
        )
        self.fatal: list[BaseException] = []
        self.shard_engine = ShardEngine(self.engine, on_fatal=self.fatal.append)
        dispatcher.bind(self.shard_engine)
        self.shard_engine.start()
        self.shard = serve(shard_app(self.shard_engine, secret=SECRET))
        with psycopg.connect(dsn, autocommit=True) as c:
            for k in range(12):
                c.execute("INSERT INTO shard_owners VALUES (%s, %s, now())", (k, self.shard.url))
        self.owners.refresh()
        self.http = requests.Session()
        self.http.trust_env = False

    def pay(self, order: str, amount: int = 49900) -> requests.Response:
        return self.http.post(f"{self.gateway.url}/orders/{order}/pay", json={"amount": amount}, timeout=5)

    def get(self, order: str) -> dict[str, Any]:
        r = self.http.get(f"{self.gateway.url}/orders/{order}", timeout=5)
        assert r.status_code == 200, r.text
        body: dict[str, Any] = r.json()
        return body

    def wait_status(self, order: str, status: str, timeout: float = 5.0) -> dict[str, Any]:
        end = time.time() + timeout
        while time.time() < end:
            body = self.get(order)
            if body["status"] == status:
                return body
            time.sleep(0.05)
        raise AssertionError(f"{order} never reached {status}: {self.get(order)}")


@pytest.fixture
def stack(pg_dsn: str, serve: Any) -> Iterator[Stack]:
    s = Stack(pg_dsn, serve)
    yield s
    s.owners.stop()
    s.shard_engine.stop()


def test_pay_through_the_gateway_and_get_paid_by_a_signed_callback(stack: Stack) -> None:
    r = stack.pay("ord-100")
    assert r.status_code == 202, r.text
    assert r.json()["status"] == "AWAITING"
    body = stack.wait_status("ord-100", "PAID")
    assert body["attempts"][0]["status"] == "succeeded"
    assert stack.mock.ledger()["debited"] == {body["attempts"][0]["id"]: 49900}
    again = stack.pay("ord-100")
    assert again.status_code == 200 and again.json()["status"] == "PAID"  # paying a paid order: unchanged


def test_a_second_tap_while_the_first_may_succeed_gets_409_with_the_next_check_time(stack: Stack) -> None:
    stack.mock.profile = Profile("waiting", (0.0, 0.0), 60.0, 0.01, 0, 0, 0, 0, 0, 0, 0, (0.0, 0.0))
    assert stack.pay("ord-101").status_code == 202
    stack.wait_status("ord-101", "AWAITING")
    time.sleep(0.2)
    r = stack.pay("ord-101")
    assert r.status_code == 409
    assert r.json()["retry_at"] is not None


def test_unsigned_callbacks_are_refused(stack: Stack) -> None:
    body = json.dumps({"attempt_id": "p03abc.1", "status": "success"}).encode()
    r = stack.http.post(f"{stack.shard.url}/psp/axis/callback", data=body, timeout=5,
                        headers={"Content-Type": "application/json", HEADER: "forged"})
    assert r.status_code == 401
    r = stack.http.post(f"{stack.shard.url}/psp/axis/callback", data=body, timeout=5,
                        headers={"Content-Type": "application/json", HEADER: sign(SECRET, body)})
    assert r.status_code == 200 and r.json()["ignored"]  # signed, but not an attempt Settle made


def test_cancel_then_a_late_success_is_refunded(stack: Stack) -> None:
    stack.mock.profile = Profile("late", (0.0, 0.0), 0.8, 0.01, 0, 0, 0, 0, 0, 0, 0, (0.0, 0.0))
    stack.pay("ord-102")
    r = stack.http.post(f"{stack.gateway.url}/orders/ord-102/cancel", timeout=5)
    assert r.json()["status"] == "EXPIRED"
    body = stack.wait_status("ord-102", "REFUNDED")
    assert body["refunds"][0]["confirmed"]
    ledger = stack.mock.ledger()
    assert sum(ledger["debited"].values()) == sum(r["amount"] for r in ledger["refunded"].values())


def test_bad_input_is_rejected_at_the_edge(stack: Stack) -> None:
    assert stack.pay("ord-103", amount=0).status_code == 400
    assert stack.http.post(f"{stack.gateway.url}/orders/bad%20id/pay", json={"amount": 1}, timeout=5).status_code == 400
    assert stack.http.get(f"{stack.gateway.url}/orders/never-made", timeout=5).status_code == 404


def test_a_shard_that_does_not_own_the_order_answers_421_and_the_gateway_gives_up_cleanly(stack: Stack) -> None:
    stack.engine.owned = set()  # this process owns nothing now
    r = stack.pay("ord-104")
    assert r.status_code == 503
    assert "no reachable owner" in r.json()["error"]


def test_stats_through_the_gateway(stack: Stack) -> None:
    stack.pay("ord-105")
    stack.wait_status("ord-105", "PAID")
    stats = stack.http.get(f"{stack.gateway.url}/stats", timeout=5).json()
    shard = stats["shards"][stack.shard.url]
    assert shard["psps"]["axis"]["breaker"] == "closed"
    assert shard["psps"]["axis"]["check_plan"] == [90, 270, 540]
    assert stats["missing"] == []


def test_the_merchant_api_needs_a_key_but_psp_callbacks_do_not(pg_dsn: str, serve: Any) -> None:
    from settle.gateway import Gateway, OwnerMap
    from settle.gateway import create_app as gateway_app

    owners = OwnerMap(pg_dsn, every=60)
    gw = serve(gateway_app(Gateway(owners.start(), deadline=0.5), frozenset({"k-123"})))
    http = requests.Session()
    http.trust_env = False
    try:
        assert http.post(f"{gw.url}/orders/o-1/pay", json={"amount": 100}, timeout=5).status_code == 401
        wrong = {"Authorization": "Bearer nope"}
        assert http.post(f"{gw.url}/orders/o-1/pay", json={"amount": 100}, headers=wrong, timeout=5).status_code == 401
        assert http.get(f"{gw.url}/stats", timeout=5).status_code == 401
        right = {"Authorization": "Bearer k-123"}
        # with the key it gets through to routing (no shard owns anything here, hence 503)
        assert http.post(f"{gw.url}/orders/o-1/pay", json={"amount": 100}, headers=right, timeout=5).status_code == 503
        # callbacks are proven by the PSP's HMAC signature at the shard, not by a merchant key
        r = http.post(f"{gw.url}/psp/axis/callback", json={"attempt_id": "x", "status": "success"}, timeout=5)
        assert r.status_code == 200
        assert http.get(f"{gw.url}/health", timeout=5).status_code == 200
    finally:
        owners.stop()
