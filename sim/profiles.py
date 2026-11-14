"""PSP profiles and fault switches for the simulator. Times are seconds of simulated time.

These are made-up distributions chosen to look like the stories in the spec: a fast PSP whose
customers approve within a minute, a slow one whose answers take about 5.5 minutes, and a flaky
one that loses callbacks and has its own failures. They are not measurements of any real PSP.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from settle.mockpsp import Profile

SIM_PROFILES: dict[str, Profile] = {
    #                          reply latency  median sigma decl  bank  psp   cust  deemed loss  dup   cb delay
    "fast": Profile("fast", (0.05, 0.40), 20.0, 0.8, 0.05, 0.01, 0.01, 0.03, 0.00, 0.02, 0.02, (0.2, 2.0)),
    "slow": Profile("slow", (0.30, 2.60), 330.0, 0.6, 0.05, 0.02, 0.02, 0.03, 0.04, 0.03, 0.02, (0.5, 5.0)),
    "flaky": Profile("flaky", (0.10, 1.60), 60.0, 1.0, 0.05, 0.02, 0.08, 0.03, 0.06, 0.10, 0.08, (0.5, 20.0)),
}

# For the router experiment: axis ends in success most often but sends ~9% of payments through a
# timeout first; icici succeeds a little less in the end but almost never leaves anyone waiting.
ROUTER_PROFILES: dict[str, Profile] = {
    "axis": Profile("axis", (0.05, 2.18), 15.0, 0.6, 0.04, 0.01, 0.005, 0.02, 0.00, 0.01, 0.0, (0.2, 1.0)),
    "icici": Profile("icici", (0.05, 0.50), 15.0, 0.6, 0.04, 0.01, 0.025, 0.02, 0.00, 0.01, 0.0, (0.2, 1.0)),
    "yes": Profile("yes", (0.05, 1.20), 25.0, 0.8, 0.04, 0.02, 0.06, 0.02, 0.03, 0.03, 0.0, (0.2, 2.0)),
}


@dataclass(frozen=True)
class Faults:
    """Switches for what goes wrong, on top of what each profile does by itself."""

    callback_loss: float = 0.0  # extra lost callbacks
    callback_dup: float = 0.0  # extra repeated callbacks
    callback_delay: float = 0.0  # extra delay up to this many seconds (callbacks arrive out of order)
    refused: float = 0.0  # connection refused: the PSP never sees the request
    reset_after_send: float = 0.0  # the PSP got it, the reply never came back
    black_hole: float = 0.0  # the PSP never got it, and Settle can't tell
    check_transport_error: float = 0.0  # a status check fails at the transport level
    refund_lost_reply: float = 0.0  # the refund went through; its reply was lost
    refund_refused: float = 0.0  # the refund never reached the PSP
    outages: tuple[tuple[str, float, float], ...] = ()  # (psp, from, to): refuses every connection
    crash: float = 0.0  # chance per step that Settle crashes between storing and acting
    contradiction: float = 0.0  # chance a PSP later contradicts its own answer

    @classmethod
    def random(cls, rng: random.Random, psps: list[str], horizon: float) -> Faults:
        """A different mix for every seed, from calm to hostile."""
        heat = rng.choice([0.0, 0.5, 1.0, 2.0])
        outages = []
        if rng.random() < 0.3 * heat:
            start = rng.uniform(0, horizon)
            outages.append((rng.choice(psps), start, start + rng.uniform(30, 400)))
        return cls(
            callback_loss=0.15 * heat * rng.random(),
            callback_dup=0.10 * heat * rng.random(),
            callback_delay=60.0 * heat * rng.random(),
            refused=0.05 * heat * rng.random(),
            reset_after_send=0.10 * heat * rng.random(),
            black_hole=0.03 * heat * rng.random(),
            check_transport_error=0.10 * heat * rng.random(),
            refund_lost_reply=0.30 * heat * rng.random(),
            refund_refused=0.10 * heat * rng.random(),
            outages=tuple(outages),
            crash=0.01 * heat * rng.random(),
            contradiction=0.01 * heat * rng.random() if rng.random() < 0.2 else 0.0,
        )

    @staticmethod
    def calm() -> Faults:
        """Faults stop: what the end of a run looks like."""
        return Faults()


@dataclass(frozen=True)
class Customers:
    orders: int = 40
    rate: float = 0.2  # orders per second
    double_tap: float = 0.05  # taps pay twice at the same instant
    retry_after_failure: float = 0.6  # pays again after a definite failure
    cancel: float = 0.08  # the merchant gives up the order early
    hold_seconds: float = 600.0
    amounts: tuple[int, int] = (1_000, 2_00_000)  # paise


@dataclass(frozen=True)
class SimConfig:
    profiles: dict[str, Profile] = field(default_factory=lambda: dict(SIM_PROFILES))
    faults: Faults | None = None  # None: drawn from the seed
    customers: Customers = Customers()
    router: str = "clean"
    weight: float = 0.5
    planner: str = "percentile"
    read_timeout: float = 2.0
    bugs: frozenset[str] = frozenset()
    keep_trace: bool = False
