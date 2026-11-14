"""PSP health kept in this process: the same 10-second buckets as Redis, and breaker deadlines.

Used on its own by the simulator, and as the fallback every shard keeps warm in case Redis is
unreachable.
"""

from __future__ import annotations


class LocalHealth:
    def __init__(self, bucket: int = 10, keep: int = 6) -> None:
        self.bucket, self.keep = bucket, keep
        self.buckets: dict[str, dict[int, list[float]]] = {}
        self.open_until: dict[str, float] = {}

    def record(self, psp: str, value: float, now: float) -> None:
        b = int(now // self.bucket)
        per = self.buckets.setdefault(psp, {})
        cell = per.setdefault(b, [0.0, 0.0])
        cell[0] += value
        cell[1] += 1
        for old in [k for k in per if k <= b - self.keep]:
            del per[old]

    def window(self, psp: str, now: float) -> tuple[float, int]:
        b = int(now // self.bucket)
        per = self.buckets.get(psp, {})
        good = n = 0.0
        for i in range(self.keep):
            cell = per.get(b - i)
            if cell:
                good += cell[0]
                n += cell[1]
        return good, int(n)

    def open_breaker(self, psp: str, seconds: float, now: float) -> None:
        if not self.breaker_open(psp, now):
            self.open_until[psp] = now + seconds

    def breaker_open(self, psp: str, now: float) -> bool:
        return now < self.open_until.get(psp, 0.0)
