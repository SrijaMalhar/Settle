"""Shared PSP health on a real Redis server."""

from __future__ import annotations

import time

import pytest
import redis

from settle.adapters.redis_health import RedisHealth
from settle.policies.breaker import CircuitBreaker
from tests.services import Redis

pytestmark = pytest.mark.redis


@pytest.fixture
def two_shards(redis_server: Redis) -> tuple[RedisHealth, RedisHealth]:
    redis.Redis.from_url(redis_server.url).flushdb()
    return RedisHealth.from_url(redis_server.url), RedisHealth.from_url(redis_server.url)


def test_one_shards_outcomes_are_seen_by_every_shard(two_shards: tuple[RedisHealth, RedisHealth]) -> None:
    a, b = two_shards
    now = time.time()
    for _ in range(4):
        a.record("axis", 0.0, now)
    a.record("axis", 1.0, now - 25)  # an older bucket, still inside the last minute
    b.record("axis", 0.5, now)
    assert b.window("axis", now) == (1.5, 6)
    assert a.window("axis", now + 70) == (0.0, 0)  # buckets older than six are not read


def test_a_breaker_opened_by_any_shard_protects_all_and_expires_by_itself(
    two_shards: tuple[RedisHealth, RedisHealth],
) -> None:
    a, b = two_shards
    breaker_a = CircuitBreaker("icici", a, threshold=5, cooldown=0.3)
    breaker_b = CircuitBreaker("icici", b, threshold=5, cooldown=0.3)
    for _ in range(5):
        breaker_a.on_failure("U28", time.time())
    assert not breaker_b.available(time.time())  # shard B never saw a failure itself
    time.sleep(0.4)
    assert breaker_b.available(time.time())  # the key expired: B's next attempt is the trial


def test_if_redis_is_down_shards_use_their_own_numbers(redis_server: Redis) -> None:
    if redis_server.proc is None:
        pytest.skip("cannot stop a Redis this test did not start")
    h = RedisHealth.from_url(redis_server.url)
    now = time.time()
    h.record("yes", 1.0, now)
    h.record("yes", 0.0, now)
    redis_server.stop()
    try:
        start = time.perf_counter()
        assert h.window("yes", now) == (1.0, 2)  # the local buckets were kept warm all along
        h.record("yes", 1.0, now)
        assert h.window("yes", now) == (2.0, 3)
        assert not h.breaker_open("yes", now)
        assert time.perf_counter() - start < 0.5
        assert not h.using_redis
    finally:
        redis_server.start()
