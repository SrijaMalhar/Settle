"""The simulator itself: invariants hold, runs repeat exactly, and planted bugs are caught."""

from __future__ import annotations

import pytest

from sim.profiles import SimConfig
from sim.run import PLANTED
from sim.world import World, run


def test_many_seeds_with_random_faults_break_no_invariant() -> None:
    for seed in range(60):
        r = run(seed)
        assert r.ok, f"seed {seed}: {[str(v) for v in r.violations]}"


def test_the_same_seed_gives_the_same_trace_hash() -> None:
    assert run(7).hash == run(7).hash
    assert run(7).hash != run(8).hash


def test_crashes_happen_and_are_survived() -> None:
    from sim.profiles import Faults

    w = World(3, SimConfig(faults=Faults(crash=0.05, reset_after_send=0.2, callback_loss=0.3)))
    r = w.run()
    assert w.crashes > 5
    assert r.ok, [str(v) for v in r.violations]


@pytest.mark.parametrize("bug", sorted(PLANTED))
def test_every_planted_bug_is_caught(bug: str) -> None:
    faults, customers, expected = PLANTED[bug]
    cfg = SimConfig(faults=faults, customers=customers, bugs=frozenset({bug}))
    caught = set()
    for seed in range(30):
        caught |= {v.invariant for v in run(seed, cfg).violations}
        if caught:
            break
    assert caught, f"{bug} was never caught"
    assert caught & {x.strip() for x in expected.split(",")}, f"{bug} caught by {caught}, expected {expected}"
