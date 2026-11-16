"""The live world behind the browser demo: steered by hand, checked continuously."""

from __future__ import annotations

import json
from typing import Any

import pytest

from sim.live import BUGS, Demo


def ctl(d: Demo, **c: Any) -> dict[str, Any]:
    out: dict[str, Any] = json.loads(d.control(json.dumps(c)))
    return out


def violations(d: Demo) -> dict[str, int]:
    return {k: v["violations"] for k, v in json.loads(d.snapshot())["invariants"].items()}


def test_every_fault_at_once_breaks_no_invariant() -> None:
    d = Demo(seed=5)
    for what in ("chaos", "crashing", "lying"):
        ctl(d, what=what, on=True)
    ctl(d, what="rate", rate=1.5)
    for i in range(1, 181):
        if i % 30 == 0:
            psp = ["alpha", "bravo", "charlie"][(i // 30) % 3]
            fault = ["down", "slow", "dropping"][(i // 60) % 3]
            ctl(d, what="psp", psp=psp, fault=fault, on=(i // 30) % 2 == 1)
        if i % 47 == 0:
            ctl(d, what="crash")
        d.advance(10)
    snap = json.loads(d.snapshot())
    assert snap["counts"]["orders"] > 2000 and snap["crashes"] > 3
    assert violations(d) == dict.fromkeys(violations(d), 0), snap["violations"]


@pytest.mark.parametrize("bug", sorted(BUGS))
def test_each_planted_bug_is_caught_within_minutes(bug: str) -> None:
    d = Demo(seed=11)
    ctl(d, what="bug", bug=bug)
    for _ in range(60):
        d.advance(10)
        if any(violations(d).values()):
            return
    pytest.fail(f"{bug} ran 10 simulated minutes without breaking an invariant")


def test_a_crash_by_hand_recovers_every_open_payment() -> None:
    d = Demo(seed=2)
    d.advance(300)
    open_before = sum(1 for p in d.w.checker.state.values() if not p.final)
    snap = ctl(d, what="crash")
    assert snap["recovery"]["open"] == open_before > 0
    d.advance(900)
    assert not any(violations(d).values())


def test_a_slow_psp_loses_its_traffic_and_its_timeouts_are_checked_not_resent() -> None:
    d = Demo(seed=4)
    d.advance(120)
    ctl(d, what="psp", psp="alpha", fault="slow", on=True)
    d.advance(600)
    snap = json.loads(d.snapshot())
    assert snap["psps"]["alpha"]["share"] < 0.2
    assert snap["counts"]["unknown"] > 0 and snap["counts"]["checks"] > 0
    assert not any(violations(d).values())


def test_detail_and_replay_of_every_payment() -> None:
    d = Demo(seed=9)
    ctl(d, what="chaos", on=True)
    d.advance(900)
    for pid in list(d.w.checker.state)[:60]:
        detail = json.loads(d.detail(pid))
        assert detail["events"] and detail["path"][0] == "AWAITING"
        assert detail["path"][-1] == detail["status"]
        r = json.loads(d.replay(pid))
        assert r["identical"], r
    assert json.loads(d.detail("p00nope")) is None


def test_replay_finds_where_a_bug_changed_history() -> None:
    d = Demo(seed=11)
    ctl(d, what="bug", bug="timeout_is_failure")  # stores "failed" where production stores "pending"
    d.advance(300)
    flagged = list(d.w.flagged)
    assert flagged
    assert not json.loads(d.replay(flagged[0]))["identical"]


def test_views_and_catalogue_are_plain_json() -> None:
    d = Demo(seed=1)
    d.advance(200)
    for view in ("all", "doubt", "refunds", "ops", "flagged"):
        snap = json.loads(d.snapshot(0, view))
        assert set(snap["views"]) == {"all", "doubt", "refunds", "ops", "flagged"}
    cat = json.loads(Demo.catalogue())
    assert set(cat["bugs"]) == set(BUGS) and len(cat["invariants"]) == 6
    first = json.loads(d.snapshot())["feed"]
    assert json.loads(d.snapshot(first[-1]["id"]))["feed"] == []
