"""Clocks. Times are rounded to the microsecond so they survive a trip through TIMESTAMPTZ."""

from __future__ import annotations

import time


class SystemClock:
    def now(self) -> float:
        return round(time.time(), 6)


class FakeClock:
    """Time that moves only when told to. The simulator and the tests use it."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> float:
        self.t = round(self.t + seconds, 6)
        return self.t

    def set(self, t: float) -> None:
        if t < self.t:
            raise ValueError("time does not go backwards")
        self.t = round(t, 6)
