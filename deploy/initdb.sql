-- Runs once, as the postgres superuser, when the postgres container starts on an empty volume.
-- The shards also run the CREATE TABLE IF NOT EXISTS statements at startup, so this is the same
-- schema as settle/adapters/postgres_store.py.

CREATE ROLE settle LOGIN PASSWORD 'settle';
CREATE DATABASE settle OWNER settle;
\connect settle
SET ROLE settle;

-- A payment's status is never stored: it is computed by replaying its events in order.
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

-- The input behind each stored batch of events, so stored histories can be re-run through a
-- changed decide() (python -m settle.replay) to see which payments would end differently.
CREATE TABLE IF NOT EXISTS inputs (         -- the input behind each stored batch, for replay checks
    payment_id  TEXT        NOT NULL,
    step        INTEGER     NOT NULL,       -- seq of the first event the input produced
    kind        TEXT        NOT NULL,       -- PayRequested, PspAnswer, TimerFired ...
    data        JSONB       NOT NULL,
    at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (payment_id, step)
);

-- Ownership is decided by pg_advisory_lock, not by this table. A process updates its row right
-- after taking a shard's lock, so gateways learn about a new owner within a second.
CREATE TABLE IF NOT EXISTS shard_owners (   -- where gateways send each logical shard's traffic
    shard       SMALLINT    PRIMARY KEY,
    url         TEXT        NOT NULL,       -- e.g. http://shard-2:9000
    since       TIMESTAMPTZ NOT NULL
);
