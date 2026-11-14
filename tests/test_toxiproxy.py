"""Real TCP faults between Settle and a mock PSP, injected with Toxiproxy.

The mock PSP records every request it receives, so each test can say whether the PSP really got
the request. The finding these tests pin down: a reset or a slow reply is NOT proof that the
request didn't arrive. Only a failure before the connection opened is.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from settle.adapters.http_psp import HttpPsp
from settle.adapters.local_health import LocalHealth
from settle.adapters.memory_store import MemoryEventStore
from settle.clock import SystemClock
from settle.core.decide import Rejected
from settle.core.model import AttemptStatus
from settle.mockpsp import MockPsp, Profile, create_app
from settle.policies.checks import FixedPlanner
from settle.policies.routing import FixedOrderRouter
from settle.shard.engine import PaymentEngine
from tests.services import Toxiproxy
from tests.test_engine import Recorder

pytestmark = pytest.mark.toxiproxy

# Always accepts, answers in 2 s, no callbacks: every outcome here comes from the network.
STEADY = Profile("steady", (0.0, 0.0), 2.0, 0.01, 0, 0, 0, 0, 0, 0, 0, (0.0, 0.0))


class Rig:
    def __init__(self, toxiproxy: Toxiproxy, serve: Any) -> None:
        self.tp = toxiproxy
        self.mock = MockPsp("axis", STEADY, callback_url=None, secret="s", seed=1)
        self.server = serve(create_app(self.mock))
        self.listen = toxiproxy.proxy("axis", f"127.0.0.1:{self.server.port}")
        self.psp = HttpPsp("axis", f"http://127.0.0.1:{self.listen}", connect_timeout=0.5, read_timeout=1.0)

    def received(self, attempt_id: str) -> bool:
        return attempt_id in self.mock.requests


@pytest.fixture
def rig(toxiproxy: Toxiproxy, serve: Any) -> Iterator[Rig]:
    toxiproxy.clear()
    yield Rig(toxiproxy, serve)
    toxiproxy.clear()


def test_psp_down_connection_refused_is_not_delivered(rig: Rig) -> None:
    rig.tp.enable("axis", False)
    reply = rig.psp.initiate("p00a.1", "ord", 100)
    assert reply.signal == "not_delivered"
    assert not rig.received("p00a.1")  # another PSP is safe


def test_slow_reply_is_unknown_and_the_psp_had_it(rig: Rig) -> None:
    rig.tp.toxic("axis", "latency", latency=3000)  # 3 s latency, 1 s read timeout
    reply = rig.psp.initiate("p00b.1", "ord", 100)
    assert reply.signal == "read_timeout"
    assert rig.received("p00b.1")  # check its status; never send it elsewhere


def test_reset_after_the_request_was_sent_is_unknown_and_the_psp_had_it(rig: Rig) -> None:
    rig.tp.toxic("axis", "latency", latency=1000)
    rig.tp.toxic("axis", "reset_peer", timeout=200)  # reset mid-request, after the bytes went out
    reply = rig.psp.initiate("p00c.1", "ord", 100)
    assert reply.signal == "read_timeout"
    assert rig.received("p00c.1")


def test_black_hole_is_unknown_even_though_the_psp_never_saw_it(rig: Rig) -> None:
    rig.tp.toxic("axis", "timeout", stream="upstream", timeout=0)  # data silently dropped
    reply = rig.psp.initiate("p00d.1", "ord", 100)
    assert reply.signal == "read_timeout"
    assert not rig.received("p00d.1")  # Settle can't know that, so it must ask, not resend


def test_a_status_check_that_hits_a_transport_error_stops_checking(rig: Rig) -> None:
    assert rig.psp.initiate("p00e.1", "ord", 100).signal == "accepted"
    assert rig.psp.check_status("p00e.1").signal == "pending"
    rig.tp.enable("axis", False)
    assert rig.psp.check_status("p00e.1").signal == "transport_error"


def test_only_a_refused_connection_lets_the_order_try_another_psp(rig: Rig, serve: Any) -> None:
    other = MockPsp("icici", STEADY, callback_url=None, secret="s", seed=2)
    other_server = serve(create_app(other))
    psps = {"axis": rig.psp, "icici": HttpPsp("icici", other_server.url)}
    d = Recorder()
    e = PaymentEngine(store=MemoryEventStore(), psps=["axis", "icici"], router=FixedOrderRouter(["axis", "icici"]),
                      planner=FixedPlanner(), health=LocalHealth(), clock=SystemClock(), dispatcher=d)

    def send_last() -> AttemptStatus:
        c = d.sent[-1]
        reply = psps[c.psp].initiate(c.attempt, c.order_id, c.amount)
        p = e.psp_answer(c.attempt, reply.signal, reply.code, via="reply")
        assert p is not None
        a = p.attempt(c.attempt)
        assert a is not None
        return a.status

    # a slow reply: unknown, so the customer may NOT pay again elsewhere
    rig.tp.toxic("axis", "latency", latency=3000)
    e.pay("ord-slow", 100)
    assert send_last() is AttemptStatus.PENDING
    with pytest.raises(Rejected):
        e.pay("ord-slow", 100)
    rig.tp.remove_toxics("axis")
    # a refused connection: never delivered, so the next attempt goes to the other PSP
    rig.tp.enable("axis", False)
    e.pay("ord-down", 100)
    assert send_last() is AttemptStatus.FAILED
    p = e.pay("ord-down", 100)
    assert [a.psp for a in p.attempts] == ["axis", "icici"]
    assert send_last() is AttemptStatus.PENDING
    assert other.requests == [p.attempts[1].id]
