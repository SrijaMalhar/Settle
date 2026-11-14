"""Run the simulator.

    python -m sim.run seeds --count 10000          # every invariant, a different fault mix per seed
    python -m sim.run determinism --count 200      # I7: each seed twice, same trace hash
    python -m sim.run bugs                         # each planted bug must be caught
    python -m sim.run planner                      # percentile vs fixed status-check plans
    python -m sim.run router                       # what a pending success is worth: w = 1, 0.5, 0
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

from sim.profiles import Customers, Faults, SimConfig
from sim.world import World

# Each planted bug, the fault mix that gives it a chance to bite, and what should catch it.
PLANTED: dict[str, tuple[Faults, Customers, str]] = {
    "timeout_is_failure": (Faults(reset_after_send=0.3), Customers(retry_after_failure=1.0), "I1, I5"),
    "resend_on_timeout": (Faults(reset_after_send=0.3), Customers(), "I1"),
    "duplicate_twice": (Faults(callback_dup=0.5), Customers(cancel=0.3), "I1, I4"),
    "early_check": (Faults(callback_loss=0.5), Customers(), "I3"),
    "no_refund_after_expiry": (Faults(), Customers(cancel=0.5), "I2"),
    "send_before_store": (Faults(crash=0.05), Customers(), "I6"),
    "no_live_check": (Faults(reset_after_send=0.2), Customers(double_tap=0.5), "I5"),
}


def _one(args: tuple[int, SimConfig]) -> dict[str, Any]:
    seed, cfg = args
    r = World(seed, cfg).run()
    return {"seed": seed, "hash": r.hash, "violations": [str(v) for v in r.violations],
            "invariants": sorted({v.invariant for v in r.violations}), "metrics": r.metrics}


def _pool(workers: int) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(max_workers=workers)


def seeds(count: int, start: int, workers: int) -> dict[str, Any]:
    cfg = SimConfig()
    t = time.time()
    failing: list[dict[str, Any]] = []
    totals: Counter[str] = Counter()
    with _pool(workers) as pool:
        for r in pool.map(_one, [(s, cfg) for s in range(start, start + count)], chunksize=50):
            m = r["metrics"]
            totals.update({"orders": m["orders"], "attempts": m["attempts"], "checks": m["checks_spent"],
                           "crashes": m["crashes"], "went_to_ops": m["status"].get("NEEDS_OPS", 0),
                           "pinned_in_ops_by_a_contradiction": m["final_status"].get("NEEDS_OPS", 0)})
            if r["violations"]:
                failing.append(r)
    return {"seeds": count, "first_seed": start, "failing": len(failing), "seconds": round(time.time() - t, 1),
            "totals": dict(totals), "failures": [{"seed": f["seed"], "violations": f["violations"][:3]} for f in failing[:20]]}


def determinism(count: int, workers: int) -> dict[str, Any]:
    cfg = SimConfig()
    with _pool(workers) as pool:
        a = list(pool.map(_one, [(s, cfg) for s in range(count)]))
        b = list(pool.map(_one, [(s, cfg) for s in range(count)]))
    differ = [x["seed"] for x, y in zip(a, b) if x["hash"] != y["hash"]]
    distinct = len({x["hash"] for x in a})
    return {"seeds": count, "same_hash_twice": count - len(differ), "differ": differ, "distinct_hashes": distinct}


def bugs(max_seeds: int, workers: int) -> dict[str, Any]:
    out = {}
    for bug, (faults, customers, expected) in PLANTED.items():
        cfg = SimConfig(faults=faults, customers=customers, bugs=frozenset({bug}))
        caught_by: Counter[str] = Counter()
        first_seed, first = None, ""
        tried = 0
        with _pool(workers) as pool:
            for r in pool.map(_one, [(s, cfg) for s in range(max_seeds)]):
                tried += 1
                if r["violations"]:
                    caught_by.update(r["invariants"])
                    if first_seed is None:
                        first_seed, first = r["seed"], r["violations"][0]
        out[bug] = {"expected": expected, "caught": first_seed is not None, "caught_by": dict(caught_by),
                    "first_seed": first_seed, "example": first, "seeds_run": tried}
    return out


def planner_experiment(seeds_per_profile: int, orders: int, workers: int) -> dict[str, Any]:
    """Of attempts whose callback was lost: how many a status check resolves (vs ops), and how
    long until Settle knows the outcome. Fixed vs percentile plans, on a fast, slow and flaky PSP.
    Each world runs long enough (`orders` at 0.5 a second) that the planner's warm-up, before it
    has 50 answer times for the PSP, is a small part of the run."""
    from sim.profiles import SIM_PROFILES

    out: dict[str, Any] = {}
    for name, profile in SIM_PROFILES.items():
        lossy = replace(profile, callback_loss=0.3)
        for planner in ("fixed", "percentile"):
            cfg = SimConfig(profiles={name: lossy}, faults=Faults(), planner=planner, router="fixed",
                            customers=Customers(orders=orders, rate=0.5, cancel=0.0, double_tap=0.0))
            with _pool(workers) as pool:
                rs = list(pool.map(_one, [(s, cfg) for s in range(seeds_per_profile)]))
            lost = sum(r["metrics"]["lost_callbacks"] for r in rs)
            by_check = sum(r["metrics"]["lost_resolved_by_check"] for r in rs)
            ttc = [r["metrics"]["mean_time_to_certainty"] for r in rs if r["metrics"]["mean_time_to_certainty"]]
            checks = sum(r["metrics"]["checks_spent"] for r in rs)
            out[f"{name}/{planner}"] = {
                "lost_callbacks": lost,
                "resolved_by_a_check": by_check,
                "share_resolved_by_check": round(by_check / lost, 3) if lost else None,
                "sent_to_ops": lost - by_check,
                "mean_time_to_certainty_s": round(sum(ttc) / len(ttc), 1) if ttc else None,
                "checks_spent": checks,
                "violations": sum(len(r["violations"]) for r in rs),
            }
    return out


def router_experiment(payments: int, workers: int) -> dict[str, Any]:
    """Weight w for a success that went pending: 1 is plain success-rate routing, 0 counts only
    clean successes. One world per weight: `payments` payments at 50 a second on the made-up
    router profiles, each customer paying once."""
    from sim.profiles import ROUTER_PROFILES

    weights = (1.0, 0.5, 0.0)
    cfgs = [
        SimConfig(profiles=dict(ROUTER_PROFILES), faults=Faults(), weight=w,
                  customers=Customers(orders=payments, rate=50.0, cancel=0.0, double_tap=0.0,
                                      retry_after_failure=0.0))
        for w in weights
    ]
    with _pool(min(workers, len(weights))) as pool:
        rs = list(pool.map(_one, [(1000, cfg) for cfg in cfgs]))
    out: dict[str, Any] = {}
    for w, r in zip(weights, rs):
        m = r["metrics"]
        out[f"w={w}"] = {
            "payments": m["orders"],
            "went_pending": m["went_pending"],
            "eventual_successes": m["eventual_successes"],
            "completed_if_30pct_abandon_pending": round(m["completed_if_30pct_abandon"]),
            "checks_spent": m["checks_spent"],
            "violations": len(r["violations"]),
        }
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("what", choices=["seeds", "determinism", "bugs", "planner", "router"])
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 2)
    ap.add_argument("--json", type=Path, help="also write the result here")
    args = ap.parse_args(argv)
    if args.what == "seeds":
        result = seeds(args.count, args.start, args.workers)
    elif args.what == "determinism":
        result = determinism(args.count, args.workers)
    elif args.what == "bugs":
        result = bugs(args.count, args.workers)
    elif args.what == "planner":
        result = planner_experiment(args.count, 2000, args.workers)
    else:
        result = router_experiment(args.count, args.workers)
    text = json.dumps(result, indent=2)
    print(text)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(text + "\n")
    if args.what == "seeds" and result["failing"]:
        sys.exit(1)
    if args.what == "bugs" and not all(v["caught"] for v in result.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
