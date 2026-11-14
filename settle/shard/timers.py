"""Due actions (status checks, hold expiry, refund retries) in a min-heap.

The writer waits on its inbox only until the next timer is due. A timer that turns out to be
stale (its check was answered by a callback first) is harmless: decide() ignores it.
"""

from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass, field


@dataclass(order=True, frozen=True)
class Timer:
    due: float
    seq: int  # breaks ties in insertion order, so the run is deterministic
    payment_id: str = field(compare=False)
    kind: str = field(compare=False)
    ref: str | None = field(default=None, compare=False)
    n: int = field(default=0, compare=False)


class TimerQueue:
    def __init__(self) -> None:
        self._heap: list[Timer] = []
        self._seq = itertools.count()

    def push(self, due: float, payment_id: str, kind: str, ref: str | None = None, n: int = 0) -> None:
        heapq.heappush(self._heap, Timer(due, next(self._seq), payment_id, kind, ref, n))

    def next_due(self) -> float | None:
        return self._heap[0].due if self._heap else None

    def pop_due(self, now: float) -> list[Timer]:
        out: list[Timer] = []
        while self._heap and self._heap[0].due <= now:
            out.append(heapq.heappop(self._heap))
        return out

    def clear(self) -> None:
        self._heap.clear()

    def __len__(self) -> int:
        return len(self._heap)
