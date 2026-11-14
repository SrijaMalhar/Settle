"""python -m settle.shard --logical 0-3            # owns logical shards 0 to 3
python -m settle.shard --logical 0-11 --standby  # waits on every lock; takes over whichever frees up

The same code runs both: a process blocks on pg_advisory_lock(k) for each of its logical shards
and starts serving shard k the moment it gets the lock. A standby waits a few seconds first, so
the primaries get their own shards when everything starts together.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from settle.adapters.postgres_store import PostgresEventStore, ensure_schema
from settle.config import Settings
from settle.factory import build_engine
from settle.shard.engine import PaymentEngine, ShardEngine
from settle.shard.ownership import ShardOwnership, connect
from settle.shard.server import create_app
from settle.sharding import parse_range

log = logging.getLogger("settle.shard")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="A Settle shard process")
    ap.add_argument("--logical", required=True, help="logical shards to own, e.g. 0-3")
    ap.add_argument("--standby", action="store_true", help="wait before competing for the locks")
    ap.add_argument("--name", default=os.environ.get("SETTLE_NAME") or os.environ.get("HOSTNAME") or "shard")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    shards = parse_range(args.logical)
    url = settings.my_url()

    with connect(settings.database_url, f"{args.name}-setup") as setup:
        ensure_schema(setup)
        history = PostgresEventStore.single(setup).resolutions(3000)

    ownership = ShardOwnership(settings.database_url, shards, name=args.name,
                               delay=settings.standby_delay if args.standby else 0.0)
    store = PostgresEventStore(ownership.conn)
    engine, dispatcher, stats = build_engine(settings, store=store, owned=[])
    engine.warm_planner(history)

    def die(e: BaseException) -> None:
        log.critical("lost the database or a shard lock (%s: %s); exiting, never reconnecting", type(e).__name__, e)
        logging.shutdown()
        os._exit(1)

    shard = ShardEngine(engine, on_fatal=die, heartbeat=ownership.heartbeat)
    dispatcher.bind(shard)

    def acquired(k: int) -> None:
        def take_over(e: PaymentEngine) -> None:
            n = e.recover(k)  # rebuild state and timers from events before taking traffic
            ownership.publish(k, url)
            log.info("serving logical shard %d at %s (%d open payments recovered)", k, url, n)

        shard.post(take_over)

    ownership.on_acquired = acquired
    shard.start()
    ownership.start()
    log.info("%s starting for logical shards %s%s", args.name, args.logical, " as standby" if args.standby else "")

    from waitress import serve

    app = create_app(shard, secret=settings.psp_secret,
                     extra_stats=lambda: {"name": args.name, "url": url, "stats": stats.snapshot()})
    try:
        serve(app, host="0.0.0.0", port=settings.port, threads=32, ident=None)
    finally:
        sys.exit(1)


if __name__ == "__main__":
    main()
