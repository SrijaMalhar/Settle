from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from waitress.server import create_server

from tests.services import Postgres, Redis, Toxiproxy, free_port


@pytest.fixture(scope="session")
def postgres() -> Iterator[Postgres]:
    try:
        pg = Postgres()
    except Exception as e:  # noqa: BLE001 - any reason means "no PostgreSQL here"
        pytest.skip(f"PostgreSQL not available: {e}")
    yield pg
    pg.stop()


@pytest.fixture
def pg_dsn(postgres: Postgres) -> Iterator[str]:
    """A clean schema for each test."""
    from settle.adapters.postgres_store import ensure_schema

    with psycopg.connect(postgres.dsn, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS events, orders, shard_owners")
        ensure_schema(c)
    yield postgres.dsn


@pytest.fixture(scope="session")
def redis_server() -> Iterator[Redis]:
    try:
        r = Redis()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Redis not available: {e}")
    yield r
    r.stop()


@pytest.fixture(scope="session")
def toxiproxy() -> Iterator[Toxiproxy]:
    try:
        t = Toxiproxy()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Toxiproxy not available: {e}")
    yield t
    t.stop()


class Served:
    """A WSGI app served by waitress on a free port, in a background thread."""

    def __init__(self, app: Any, threads: int = 16) -> None:
        self.port = free_port()
        self.server = create_server(app, host="127.0.0.1", port=self.port, threads=threads)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.port}"

    def close(self) -> None:
        self.server.close()


@pytest.fixture
def serve() -> Iterator[Any]:
    started: list[Served] = []

    def start(app: Any, threads: int = 16) -> Served:
        s = Served(app, threads)
        started.append(s)
        return s

    yield start
    for s in started:
        s.close()
