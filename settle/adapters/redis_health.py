"""PSP health shared by every shard through Redis.

Every shard adds its PSP outcomes to 10-second buckets in Redis and reads the last minute, so
one shard's bad experience protects all shards at once. A circuit breaker opened by any shard
is a key that expires by itself. If Redis is down, shards fall back to their own numbers: the
same buckets, kept locally all along so they are warm when needed.

Redis holds hints, not facts about money, so losing it is harmless.
"""

from __future__ import annotations

import time
from typing import Any

import redis

from settle.adapters.local_health import LocalHealth


class RedisHealth:
    def __init__(self, r: redis.Redis, bucket: int = 10, keep: int = 6, fallback: LocalHealth | None = None,
                 retry_after: float = 1.0) -> None:
        self.r, self.bucket, self.keep = r, bucket, keep
        self.local = fallback or LocalHealth(bucket, keep)
        self.retry_after = retry_after
        self.down_until = 0.0  # after an error, skip Redis for a moment instead of paying a timeout per call
        self.errors = 0

    @classmethod
    def from_url(cls, url: str, **kw: Any) -> RedisHealth:
        r = redis.Redis.from_url(url, socket_connect_timeout=0.2, socket_timeout=0.2)
        return cls(r, **kw)

    @property
    def using_redis(self) -> bool:
        return time.monotonic() >= self.down_until

    def _failed(self) -> None:
        self.errors += 1
        self.down_until = time.monotonic() + self.retry_after

    def record(self, psp: str, value: float, now: float) -> None:
        self.local.record(psp, value, now)
        if not self.using_redis:
            return
        key = f"health:{psp}:{int(now // self.bucket)}"
        try:
            pipe = self.r.pipeline()
            pipe.hincrbyfloat(key, "good", value)
            pipe.hincrby(key, "n", 1)
            pipe.expire(key, self.bucket * (self.keep + 1))
            pipe.execute()
        except redis.RedisError:
            self._failed()

    def window(self, psp: str, now: float) -> tuple[float, int]:
        if not self.using_redis:
            return self.local.window(psp, now)
        b = int(now // self.bucket)
        try:
            pipe = self.r.pipeline()
            for i in range(self.keep):
                pipe.hmget(f"health:{psp}:{b - i}", ["good", "n"])
            good = n = 0.0
            for g, c in pipe.execute():
                good += float(g or 0)
                n += int(c or 0)
            return good, int(n)
        except redis.RedisError:
            self._failed()
            return self.local.window(psp, now)

    def open_breaker(self, psp: str, seconds: float, now: float) -> None:
        self.local.open_breaker(psp, seconds, now)
        if not self.using_redis:
            return
        try:
            self.r.set(f"breaker:{psp}", "open", px=int(seconds * 1000), nx=True)
        except redis.RedisError:
            self._failed()

    def breaker_open(self, psp: str, now: float) -> bool:
        if not self.using_redis:
            return self.local.breaker_open(psp, now)
        try:
            return bool(self.r.exists(f"breaker:{psp}"))
        except redis.RedisError:
            self._failed()
            return self.local.breaker_open(psp, now)
