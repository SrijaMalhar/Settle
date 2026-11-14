from __future__ import annotations

import random

from settle.adapters.local_health import LocalHealth
from settle.policies.breaker import BreakerState, CircuitBreaker
from settle.policies.checks import FIXED, FixedPlanner, PercentilePlanner, fit_to_rules, percentile, plan_checks
from settle.policies.routing import CleanSuccessRouter, FixedOrderRouter, blame
from settle.shard.timers import TimerQueue
from settle.sharding import logical_shard, parse_range, payment_of_attempt, shard_of_payment

T0 = 1_700_000_000.0


def test_blame_from_npci_codes() -> None:
    assert blame("ZM") == "customer"  # wrong PIN
    assert blame("Z9") == "customer"  # insufficient funds
    assert blame("U69") == "customer"  # collect request expired
    assert blame("U30") == "payer_bank"
    assert blame("UT") == "payer_bank"
    assert blame("BT") == "psp"
    assert blame("U28") == "psp"
    assert blame("RB") == "psp"  # deemed
    assert blame(None) == "psp"  # Settle's own timeouts and refused connections


def test_customer_failures_never_count_against_a_psp() -> None:
    health = LocalHealth()
    router = CleanSuccessRouter(health, w=0.5)
    for _ in range(20):
        router.record("axis", "failed", "ZM", T0)
    assert health.window("axis", T0) == (0.0, 0)
    router.record("axis", "failed", "BT", T0)
    assert health.window("axis", T0) == (0.0, 1)


def test_a_pending_success_is_worth_w() -> None:
    health = LocalHealth()
    CleanSuccessRouter(health, w=0.5).record("axis", "pending_success", None, T0)
    CleanSuccessRouter(health, w=0.5).record("axis", "clean", None, T0)
    assert health.window("axis", T0) == (1.5, 2)


def test_thompson_sampling_prefers_the_cleaner_psp_but_still_explores() -> None:
    health = LocalHealth()
    router = CleanSuccessRouter(health, w=0.5, seed=7)
    for i in range(60):
        router.record("icici", "clean", None, T0)
        router.record("axis", "pending_success" if i % 3 else "failed", "BT", T0)
    picks = [router.choose(["axis", "icici", "yes"], T0) for _ in range(500)]
    assert picks.count("icici") > 400
    assert picks.count("yes") > 0  # no data: wide draws, so it gets explored


def test_fixed_order_router_is_the_baseline() -> None:
    r = FixedOrderRouter(["axis", "icici"])
    assert r.choose(["icici", "axis"], T0) == "axis"
    assert r.choose(["icici"], T0) == "icici"
    assert r.choose([], T0) is None


def test_breaker_opens_after_five_psp_failures_and_trials_after_cooldown() -> None:
    health = LocalHealth()
    b = CircuitBreaker("axis", health, threshold=5, cooldown=30)
    for _ in range(4):
        b.on_failure("BT", T0)
    assert b.available(T0)
    b.on_failure(None, T0)  # a timeout
    assert not b.available(T0 + 1)
    assert b.state is BreakerState.OPEN
    assert b.available(T0 + 31)  # half-open: one trial
    b.on_attempt(T0 + 31)
    assert not b.available(T0 + 31.5)  # only one trial at a time
    b.on_failure("U28", T0 + 32)
    assert not b.available(T0 + 33)  # the trial failed: open again
    assert b.available(T0 + 63)
    b.on_attempt(T0 + 63)
    b.on_success(T0 + 64)
    assert b.state is BreakerState.CLOSED and b.available(T0 + 64)


def test_wrong_pins_never_open_the_breaker() -> None:
    b = CircuitBreaker("axis", LocalHealth())
    for _ in range(50):
        b.on_failure("ZM", T0)
        b.on_failure("U30", T0)
    assert b.available(T0)


def test_a_breaker_opened_by_another_shard_is_seen_here() -> None:
    shared = LocalHealth()
    mine, theirs = CircuitBreaker("axis", shared), CircuitBreaker("axis", shared)
    for _ in range(5):
        theirs.on_failure("BT", T0)
    assert not mine.available(T0 + 1)
    assert mine.available(T0 + 31)


def test_fit_to_rules_obeys_npci() -> None:
    assert fit_to_rules([10, 20, 30]) == (90, 180, 270)
    assert fit_to_rules([500, 100, 9000]) == (100, 500)  # past 2 hours: dropped
    assert fit_to_rules([1, 2, 3, 4, 5]) == (90, 180, 270)  # never more than three


def test_percentile_interpolates() -> None:
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.5) == 2.5
    assert percentile([5.0], 0.95) == 5.0


def test_too_little_history_uses_the_fixed_plan() -> None:
    assert plan_checks([30.0] * 49) == FIXED
    assert FixedPlanner().plan("axis") == FIXED


def test_planner_checks_late_for_a_slow_psp_and_early_for_a_fast_one() -> None:
    rng = random.Random(1)
    planner = PercentilePlanner()
    for _ in range(500):
        planner.observe("slow", rng.lognormvariate(5.8, 0.6))  # median about 5.5 minutes
        planner.observe("fast", rng.uniform(2, 40))
    slow = planner.plan("slow")
    assert slow[0] > 270 and len(slow) == 3
    # the earliest legal times, except the last check: never earlier than the fixed plan's
    assert planner.plan("fast") == (90, 180, 540)


def test_timer_queue_is_a_min_heap_with_stable_ties() -> None:
    q = TimerQueue()
    q.push(T0 + 5, "p1", "hold")
    q.push(T0 + 1, "p2", "check", "p2.1", 1)
    q.push(T0 + 1, "p3", "check", "p3.1", 1)
    assert q.next_due() == T0 + 1
    assert [t.payment_id for t in q.pop_due(T0 + 1)] == ["p2", "p3"]
    assert len(q) == 1 and q.pop_due(T0 + 4) == []


def test_logical_shards_are_stable_across_processes() -> None:
    # pinned: crc32 gives every process, every Python version and every restart the same answer
    assert logical_shard("ord-1") == 3
    assert logical_shard("ord-42") == 3
    assert parse_range("0-3") == [0, 1, 2, 3]
    assert parse_range("8-11,0") == [0, 8, 9, 10, 11]
    assert shard_of_payment("p07abc") == 7
    assert payment_of_attempt("p07abc.2") == "p07abc"
    assert payment_of_attempt("rf-p07abc.2") == "p07abc"
