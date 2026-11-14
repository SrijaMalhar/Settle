"""Failover against a real PostgreSQL, at the process level.

kill -9 is what `docker kill` sends; SIGSTOP is what `docker pause` (or a long GC pause) looks
like. The writes table shows exactly who wrote when.
"""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

pytestmark = [pytest.mark.postgres, pytest.mark.slow]

ROOT = Path(__file__).resolve().parents[1]
SHARD = 5


@pytest.fixture
def db(pg_dsn: str) -> Iterator[str]:
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS writes")
        c.execute("CREATE TABLE writes (who TEXT NOT NULL, at TIMESTAMPTZ NOT NULL)")
    yield pg_dsn


def start(dsn: str, name: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, "-m", "tests.lock_holder", dsn, str(SHARD), name], cwd=ROOT)


def writes(dsn: str, who: str) -> list[float]:
    with psycopg.connect(dsn, autocommit=True) as c:
        rows = c.execute("SELECT extract(epoch FROM at)::float8 FROM writes WHERE who = %s ORDER BY at", (who,))
        return [r[0] for r in rows.fetchall()]


def wait_until(cond: object, timeout: float = 10.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        if cond():  # type: ignore[operator]
            return
        time.sleep(0.01)
    raise AssertionError("timed out")


def stop(*procs: subprocess.Popen[bytes]) -> None:
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGCONT)
            p.kill()
        p.wait(5)


def test_owner_killed_the_standby_takes_over_and_the_old_owner_never_writes_again(db: str) -> None:
    owner = start(db, "owner")
    wait_until(lambda: len(writes(db, "owner")) > 5)
    standby = start(db, "standby")
    time.sleep(1.0)
    assert writes(db, "standby") == []  # blocked on the lock while the owner lives
    try:
        killed_at = time.time()
        owner.send_signal(signal.SIGKILL)
        owner.wait(5)
        wait_until(lambda: writes(db, "standby"))
        takeover = writes(db, "standby")[0] - killed_at
        print(f"\nstandby took over {takeover * 1000:.1f} ms after SIGKILL")
        assert takeover < 2.0
        assert all(t < killed_at + 0.05 for t in writes(db, "owner"))  # never two writers
    finally:
        stop(owner, standby)


def test_a_frozen_owner_keeps_its_shard(db: str) -> None:
    owner = start(db, "owner")
    wait_until(lambda: len(writes(db, "owner")) > 5)
    standby = start(db, "standby")
    time.sleep(0.5)
    try:
        owner.send_signal(signal.SIGSTOP)  # like docker pause, or a long GC pause
        time.sleep(2.0)
        assert writes(db, "standby") == []  # its session is alive, so it still holds the lock
        before = len(writes(db, "owner"))
        owner.send_signal(signal.SIGCONT)
        wait_until(lambda: len(writes(db, "owner")) > before + 5)  # it wakes and carries on
        assert writes(db, "standby") == []
    finally:
        stop(owner, standby)


def test_an_owner_whose_session_is_cut_exits_and_the_standby_takes_over(db: str) -> None:
    owner = start(db, "owner")
    wait_until(lambda: len(writes(db, "owner")) > 5)
    standby = start(db, "standby")
    time.sleep(0.5)
    try:
        with psycopg.connect(db, autocommit=True) as c:
            cut_at = time.time()
            c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = %s",
                      (f"owner-shard-{SHARD}",))
        assert owner.wait(5) == 1  # its next write failed and it exited: crash-only
        wait_until(lambda: writes(db, "standby"))
        print(f"\nstandby took over {(writes(db, 'standby')[0] - cut_at) * 1000:.1f} ms after the session was cut")
        assert all(t < cut_at + 0.05 for t in writes(db, "owner"))
    finally:
        stop(owner, standby)

