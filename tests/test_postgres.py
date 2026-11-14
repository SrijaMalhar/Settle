"""The event store against a real PostgreSQL."""

from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pytest

from settle.adapters.errors import Conflict
from settle.adapters.postgres_store import SCHEMA, PostgresEventStore
from settle.clock import FakeClock, SystemClock
from settle.core.decide import replay
from settle.core.messages import AttemptMoved, AttemptStarted, PaymentCreated
from settle.core.model import AttemptStatus, PaymentStatus
from settle.sharding import logical_shard
from tests.test_engine import Recorder, engine

pytestmark = pytest.mark.postgres

T0 = 1_700_000_000.123456


def store(dsn: str) -> PostgresEventStore:
    return PostgresEventStore.single(psycopg.connect(dsn, autocommit=True))


def created(pid: str = "p03abc", order: str = "ord-1") -> PaymentCreated:
    return PaymentCreated(at=T0, payment_id=pid, order_id=order, shard=3, amount=100, hold_until=T0 + 600)


def test_events_round_trip_exactly(pg_dsn: str) -> None:
    s = store(pg_dsn)
    events = [
        created(),
        AttemptStarted(at=T0 + 0.000001, attempt="p03abc.1", psp="axis"),
        AttemptMoved(at=T0 + 1.5, attempt="p03abc.1", psp="axis", to="pending", signal="read_timeout",
                     via="reply", plan=(T0 + 90, T0 + 270)),
    ]
    s.append("p03abc", events, expected=0, open=True)
    loaded = s.load("p03abc")
    assert loaded == events  # times survive TIMESTAMPTZ to the microsecond
    assert replay(loaded) == replay(events)
    assert s.payment_of("ord-1") == "p03abc"
    assert s.open_payments(3) == ["p03abc"]


def test_append_is_all_or_nothing_and_catches_a_second_writer(pg_dsn: str) -> None:
    s = store(pg_dsn)
    s.append("p03abc", [created()], expected=0, open=True)
    batch = [AttemptStarted(at=T0 + 1, attempt="p03abc.1", psp="axis")]
    s.append("p03abc", batch, expected=1, open=True)
    with pytest.raises(Conflict):  # a writer that thinks only 1 event exists
        s.append("p03abc", [AttemptStarted(at=T0 + 2, attempt="p03abc.2", psp="icici"),
                            AttemptStarted(at=T0 + 3, attempt="p03abc.3", psp="yes")], expected=1, open=True)
    assert len(s.load("p03abc")) == 2  # nothing from the failed batch


def test_closing_a_payment_removes_it_from_recovery(pg_dsn: str) -> None:
    s = store(pg_dsn)
    s.append("p03abc", [created()], expected=0, open=True)
    s.append("p03abc", [AttemptStarted(at=T0 + 1, attempt="p03abc.1", psp="axis")], expected=1, open=False)
    assert s.open_payments(3) == []


def test_initdb_sql_has_the_same_tables_as_the_code() -> None:
    sql = (Path(__file__).resolve().parents[1] / "deploy" / "initdb.sql").read_text()

    def tables(text: str) -> list[list[str]]:
        text = re.sub(r"--.*", "", text)
        return [m.split() for m in re.findall(r"CREATE (?:TABLE|INDEX) IF NOT EXISTS .*?;", text, re.S)]

    assert tables(sql) == tables(SCHEMA)


def test_engine_on_postgres_recovers_after_a_crash(pg_dsn: str) -> None:
    clock = FakeClock()
    first = Recorder()
    e1 = engine(store(pg_dsn), first, clock)
    p = e1.pay("ord-9", 4990)
    e1.psp_answer(p.attempts[0].id, "read_timeout", via="reply")
    del e1  # the process dies with a pending attempt
    second = Recorder()
    e2 = engine(store(pg_dsn), second, clock)
    e2.recover(logical_shard("ord-9"))
    clock.advance(90)
    e2.fire_due()
    assert [c.attempt for c in second.checks] == [p.attempts[0].id]
    e2.psp_answer(p.attempts[0].id, "success", via="check", n=1)
    paid = e2.get("ord-9")
    assert paid is not None and paid.status is PaymentStatus.PAID
    assert paid.attempts[0].status is AttemptStatus.SUCCEEDED
    assert not second.sent


def test_resolution_history_comes_back_from_events(pg_dsn: str) -> None:
    clock = FakeClock()
    d = Recorder()
    e = engine(store(pg_dsn), d, clock)
    for i in range(3):
        p = e.pay(f"o{i}", 100)
        clock.advance(40)
        e.psp_answer(p.attempts[0].id, "success", via="callback")
    hist = store(pg_dsn).resolutions(10)
    assert hist == [("axis", 40.0)] * 3


def test_system_clock_times_survive_the_database(pg_dsn: str) -> None:
    s = store(pg_dsn)
    now = SystemClock().now()
    e = PaymentCreated(at=now, payment_id="p03xyz", order_id="ord-x", shard=3, amount=1, hold_until=now + 600)
    s.append("p03xyz", [e], expected=0, open=True)
    assert s.load("p03xyz") == [e]
