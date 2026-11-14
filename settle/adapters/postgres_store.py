"""The event store in PostgreSQL. Append-only events; a payment's status is never a column.

Every query for logical shard k goes through the connection that holds shard k's advisory
lock (see shard/ownership.py), so a process that has lost the lock cannot write at all.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from settle.adapters.errors import Conflict, StoreFailed
from settle.core.messages import Event, PaymentCreated, event_from
from settle.sharding import logical_shard, shard_of_payment

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    payment_id  TEXT        NOT NULL,
    seq         INTEGER     NOT NULL,       -- 1, 2, 3 ... within one payment
    shard       SMALLINT    NOT NULL,       -- logical shard, 0 to 11
    kind        TEXT        NOT NULL,       -- AttemptStarted, AttemptMoved, PaymentPaid ...
    data        JSONB       NOT NULL,
    at          TIMESTAMPTZ NOT NULL,       -- from the injected clock
    PRIMARY KEY (payment_id, seq)
);
CREATE INDEX IF NOT EXISTS events_resolved ON events (at DESC)
    WHERE kind = 'AttemptMoved' AND data->>'took' IS NOT NULL;

CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT     PRIMARY KEY,       -- one payment per merchant order: pay is idempotent
    payment_id  TEXT     NOT NULL UNIQUE,
    shard       SMALLINT NOT NULL,
    open        BOOLEAN  NOT NULL           -- true until a final state; recovery reads only open ones
);
CREATE INDEX IF NOT EXISTS orders_open ON orders (shard) WHERE open;

CREATE TABLE IF NOT EXISTS shard_owners (   -- where gateways send each logical shard's traffic
    shard       SMALLINT    PRIMARY KEY,
    url         TEXT        NOT NULL,       -- e.g. http://shard-2:9000
    since       TIMESTAMPTZ NOT NULL
);
"""


def ensure_schema(conn: psycopg.Connection[Any]) -> None:
    conn.execute(SCHEMA)


def _float(v: object) -> float:
    if isinstance(v, (Decimal, int, float)):
        return float(v)
    raise TypeError(f"not a number: {v!r}")


class PostgresEventStore:
    def __init__(self, conn_for: Callable[[int], psycopg.Connection[Any]]) -> None:
        """conn_for(k) returns the connection that holds logical shard k's lock."""
        self.conn_for = conn_for

    @classmethod
    def single(cls, conn: psycopg.Connection[Any]) -> PostgresEventStore:
        """One connection for every shard: tests and tools, not a shard process."""
        return cls(lambda _k: conn)

    def load(self, payment_id: str) -> list[Event]:
        conn = self.conn_for(shard_of_payment(payment_id))
        try:
            rows = conn.execute(
                "SELECT kind, data, extract(epoch FROM at) FROM events WHERE payment_id = %s ORDER BY seq",
                (payment_id,),
            ).fetchall()
        except psycopg.Error as e:
            raise StoreFailed(str(e)) from e
        return [event_from(kind, data, _float(at)) for kind, data, at in rows]

    def append(self, payment_id: str, events: list[Event], *, expected: int, open: bool) -> None:
        """All or nothing, in one transaction on the lock's connection."""
        shard = shard_of_payment(payment_id)
        conn = self.conn_for(shard)
        rows = [
            (payment_id, expected + i + 1, shard, e.kind, Jsonb(e.data()), e.at) for i, e in enumerate(events)
        ]
        created = next((e for e in events if isinstance(e, PaymentCreated)), None)
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO events (payment_id, seq, shard, kind, data, at) "
                        "VALUES (%s, %s, %s, %s, %s, to_timestamp(%s))",
                        rows,
                    )
                    if created is not None:
                        cur.execute(
                            "INSERT INTO orders (order_id, payment_id, shard, open) VALUES (%s, %s, %s, %s)",
                            (created.order_id, payment_id, shard, open),
                        )
                    else:
                        cur.execute(
                            "UPDATE orders SET open = %s WHERE payment_id = %s AND open <> %s",
                            (open, payment_id, open),
                        )
        except psycopg.errors.UniqueViolation as e:
            raise Conflict(f"{payment_id}: another writer got there first") from e
        except psycopg.Error as e:
            raise StoreFailed(str(e)) from e

    def open_payments(self, shard: int) -> list[str]:
        try:
            rows = self.conn_for(shard).execute(
                "SELECT payment_id FROM orders WHERE shard = %s AND open ORDER BY payment_id", (shard,)
            ).fetchall()
        except psycopg.Error as e:
            raise StoreFailed(str(e)) from e
        return [r[0] for r in rows]

    def payment_of(self, order_id: str) -> str | None:
        try:
            row = self.conn_for(logical_shard(order_id)).execute(
                "SELECT payment_id FROM orders WHERE order_id = %s", (order_id,)
            ).fetchone()
        except psycopg.Error as e:
            raise StoreFailed(str(e)) from e
        return row[0] if row else None

    def resolutions(self, limit: int, shard: int | None = None) -> list[tuple[str, float]]:
        """The latest resolution times (seconds from sending to the final answer) per PSP."""
        conn = self.conn_for(shard if shard is not None else 0)
        try:
            rows = conn.execute(
                "SELECT data->>'psp', (data->>'took')::float8 FROM events "
                "WHERE kind = 'AttemptMoved' AND data->>'took' IS NOT NULL ORDER BY at DESC LIMIT %s",
                (limit,),
            ).fetchall()
        except psycopg.Error as e:
            raise StoreFailed(str(e)) from e
        return [(psp, float(took)) for psp, took in rows]
