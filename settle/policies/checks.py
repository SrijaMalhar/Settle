"""Status checks under NPCI's limits.

NPCI's 2025 rules (UPI-OC-215): the first status check only 90 seconds after the transaction,
at most three checks within two hours, at least 90 seconds apart, and none after a transport
error. A check only helps if the answer is ready, so the percentile planner checks when this
PSP's answers are usually ready, then moves each time later if the rules need it.

The simulator found the one change from the spec's planner: with the last check at the 95th
percentile, a PSP with a long tail (median 60 s, but some answers take many minutes) had its
last check at about 5 minutes, earlier than the fixed plan's 9, and four times as many of its
lost callbacks went to ops. The last check now never comes earlier than the fixed plan's.
"""

from __future__ import annotations

from collections import deque
from typing import Protocol

FIRST, GAP, MAX_CHECKS, WINDOW = 90, 90, 3, 7200  # NPCI: first after 90 s, 90 s apart, 3 in 2 hours
FIXED: tuple[int, ...] = (90, 270, 540)  # the common fixed backoff: the baseline


def fit_to_rules(wanted: list[float] | tuple[float, ...], first: int = FIRST) -> tuple[int, ...]:
    """Move each wanted check later until it obeys NPCI's rules; drop any past 2 hours."""
    times: list[int] = []
    for t in sorted(wanted)[:MAX_CHECKS]:
        earliest = first if not times else times[-1] + GAP
        t = max(round(t), earliest)
        if t <= WINDOW:
            times.append(t)
    return tuple(times)


def percentile(values: list[float], p: float) -> float:
    """Linear interpolation between the two nearest ranks. values must be sorted."""
    k = (len(values) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def plan_checks(resolution_times: list[float], min_samples: int = 50) -> tuple[int, ...]:
    """Check when this PSP's answers are usually ready: at its 50th and 80th percentile time from
    sending to the final answer, and the last check at its 95th percentile but never earlier than
    the fixed plan's last check. The last check is the last chance before ops, so on a PSP with a
    long tail it is worth waiting for. Too little history: use the fixed plan."""
    if len(resolution_times) < min_samples:
        return FIXED
    s = sorted(resolution_times)
    return fit_to_rules([percentile(s, 0.50), percentile(s, 0.80), max(percentile(s, 0.95), FIXED[-1])])


class CheckPlanner(Protocol):
    def plan(self, psp: str) -> tuple[int, ...]: ...

    def observe(self, psp: str, took: float) -> None: ...


class FixedPlanner:
    """90, 270 and 540 seconds, whatever the PSP. The baseline."""

    def plan(self, psp: str) -> tuple[int, ...]:
        return FIXED

    def observe(self, psp: str, took: float) -> None:
        pass


class PercentilePlanner:
    """The last `keep` resolution times per PSP; checks at their 50th, 80th and 95th percentiles."""

    def __init__(self, keep: int = 500, min_samples: int = 50) -> None:
        self.keep, self.min_samples = keep, min_samples
        self.history: dict[str, deque[float]] = {}
        self._cache: dict[str, tuple[int, ...]] = {}

    def observe(self, psp: str, took: float) -> None:
        self.history.setdefault(psp, deque(maxlen=self.keep)).append(took)
        self._cache.pop(psp, None)

    def plan(self, psp: str) -> tuple[int, ...]:
        if psp not in self._cache:
            self._cache[psp] = plan_checks(list(self.history.get(psp, ())), self.min_samples)
        return self._cache[psp]
