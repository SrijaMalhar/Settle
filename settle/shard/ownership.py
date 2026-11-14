"""One writer per logical shard, enforced by PostgreSQL.

A process owns logical shard k only while it holds pg_advisory_lock(k), and it writes shard k's
events only through the connection that holds that lock. PostgreSQL releases a session's
advisory locks when the session ends, which is what makes failover automatic: kill the owner
and its socket closes, its session ends, its locks are free, and the standby's blocked
pg_advisory_lock() call returns. If the owner's connection is lost, it exits instead of
reconnecting (crash-only), so it can never write without the lock.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

import psycopg

from settle.shard.engine import NotOwner

log = logging.getLogger("settle.ownership")

LOCK_SPACE = 5150  # the two-key form keeps Settle's locks apart from anyone else's on this database


def connect(dsn: str, name: str) -> psycopg.Connection[Any]:
    # Client-side keepalives too, so this process notices a vanished database as well.
    return psycopg.connect(
        dsn, autocommit=True, application_name=name, connect_timeout=5,
        keepalives=1, keepalives_idle=5, keepalives_interval=2, keepalives_count=3,
    )


class ShardOwnership:
    def __init__(self, dsn: str, shards: list[int], *, name: str = "settle", delay: float = 0.0) -> None:
        self.dsn, self.shards, self.name, self.delay = dsn, shards, name, delay
        self.conns: dict[int, psycopg.Connection[Any]] = {}
        self.on_acquired: Callable[[int], None] = lambda _k: None
        self._stop = threading.Event()

    def start(self) -> None:
        for k in self.shards:
            threading.Thread(target=self._wait_for, args=(k,), name=f"lock-{k}", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        for c in list(self.conns.values()):
            c.close()

    def _wait_for(self, k: int) -> None:
        if self._stop.wait(self.delay):
            return
        while not self._stop.is_set():
            try:
                conn = connect(self.dsn, f"{self.name}-shard-{k}")
                conn.execute("SELECT pg_advisory_lock(%s, %s)", (LOCK_SPACE, k))  # blocks until no one else owns it
            except psycopg.Error as e:
                log.warning("waiting for shard %d: %s", k, e)
                time.sleep(0.5)
                continue
            self.conns[k] = conn
            log.info("%s owns logical shard %d", self.name, k)
            self.on_acquired(k)
            return

    def conn(self, k: int) -> psycopg.Connection[Any]:
        c = self.conns.get(k)
        if c is None:
            raise NotOwner(f"logical shard {k} is not owned here")
        return c

    def owned(self) -> list[int]:
        return sorted(self.conns)

    def publish(self, k: int, url: str) -> None:
        """Tell the gateways, through the lock's own connection, that shard k lives at url now."""
        self.conn(k).execute(
            "INSERT INTO shard_owners (shard, url, since) VALUES (%s, %s, now()) "
            "ON CONFLICT (shard) DO UPDATE SET url = EXCLUDED.url, since = EXCLUDED.since",
            (k, url),
        )

    def heartbeat(self) -> None:
        """Raises if any lock's session is gone. The caller exits; it never reconnects."""
        for k, c in list(self.conns.items()):
            c.execute("SELECT 1")
