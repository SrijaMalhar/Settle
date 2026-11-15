"""Replay check: stored inputs, run back through decide(), rebuild every history exactly; a
changed rule shows which payments would have ended differently."""

from __future__ import annotations

import psycopg
import pytest

from settle.adapters.postgres_store import PostgresEventStore
from settle.clock import FakeClock
from settle.core.messages import Recover, input_data, input_from
from settle.replay import check
from sim.profiles import Customers, Faults, SimConfig
from sim.world import World
from tests.test_engine import Recorder, engine


def test_inputs_survive_json() -> None:
    inp = Recover(now=1.5, plans=(("axis", (90.0, 270.0)), ("icici", (120.0,))))
    assert input_from("Recover", input_data(inp)) == inp


def test_simulated_histories_rebuild_exactly_from_their_inputs() -> None:
    for seed in range(15):
        w = World(seed)
        w.run()
        rep = check(w.store)
        assert rep.payments > 0
        assert rep.identical == rep.payments, rep.summary()
        assert not rep.changed


def test_a_changed_rule_lists_the_payments_it_would_change() -> None:
    w = World(4, SimConfig(faults=Faults(), customers=Customers(cancel=0.6)))
    w.run()
    rep = check(w.store, bugs=frozenset({"no_refund_after_expiry"}))
    assert rep.changed, "some late successes were refunded; without refunds they end differently"
    assert all(c["before"]["refunded"] > 0 and c["after"]["refunded"] == 0 for c in rep.changed)


@pytest.mark.postgres
def test_postgres_histories_rebuild_exactly(pg_dsn: str) -> None:
    conn = psycopg.connect(pg_dsn, autocommit=True)
    clock = FakeClock()
    e = engine(PostgresEventStore.single(conn), Recorder(), clock)
    for i in range(5):
        p = e.pay(f"o{i}", 1000 + i)
        e.psp_answer(p.attempts[0].id, "read_timeout", via="reply")
    e.cancel("o1")
    clock.advance(90)
    e.fire_due()
    for i in range(5):
        a = e.store.payment_of(f"o{i}")
        assert a is not None
        e.psp_answer(f"{a}.1", "success", via="check", n=1)
    rep = check(PostgresEventStore.single(conn))
    assert rep.payments == 5 and rep.identical == 5
