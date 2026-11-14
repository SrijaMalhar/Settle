"""A tiny shard owner for the failover tests: take logical shard k's lock, then write a row every
10 ms through the lock's connection. On any database error it exits, never reconnecting.

    python -m tests.lock_holder DSN SHARD NAME
"""

from __future__ import annotations

import sys
import threading
import time

import psycopg

from settle.shard.ownership import ShardOwnership


def main() -> None:
    dsn, shard, name = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    ownership = ShardOwnership(dsn, [shard], name=name)
    got = threading.Event()
    ownership.on_acquired = lambda _k: got.set()
    ownership.start()
    got.wait()
    conn = ownership.conn(shard)
    try:
        while True:  # every write goes through the lock's connection
            conn.execute("INSERT INTO writes (who, at) VALUES (%s, clock_timestamp())", (name,))
            time.sleep(0.01)
    except psycopg.Error:
        sys.exit(1)  # crash-only: never reconnect and carry on


if __name__ == "__main__":
    main()
