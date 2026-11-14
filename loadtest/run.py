"""Load test at 1, 2 and 3 shard processes, with Locust.

    python -m loadtest.run                                   # official images
    python -m loadtest.run --base -f docker-compose.yml -f deploy/local/compose.local.yml   # offline machine

For each layout: start the stack fresh, wait for all 12 logical shards to have an owner, warm
up, then run Locust headless for --seconds with --users users split over both gateways, and
record payments per second and the p95 latency of POST /pay.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]


def compose(files: list[str], *args: str, env: dict[str, str] | None = None) -> str:
    cmd = ["docker", "compose", *[x for f in files for x in ("-f", f)], *args]
    return subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True, env=env).stdout


def wait_ready(files: list[str], gateway: str, timeout: float = 120) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            owners = compose(files, "exec", "-T", "postgres", "psql", "-U", "settle", "-d", "settle", "-At",
                             "-c", "SELECT count(*) FROM shard_owners")
            if owners.strip() == "12" and requests.get(f"{gateway}/health", timeout=2,
                                                      proxies={"http": "", "https": ""}).ok:
                return
        except (subprocess.CalledProcessError, requests.RequestException):
            pass
        time.sleep(1)
    raise SystemExit("stack never became ready")


def locust(gateways: list[str], users: int, seconds: int, prefix: Path) -> dict[str, Any]:
    env = dict(os.environ, SETTLE_GATEWAYS=",".join(gateways))
    subprocess.run(
        [sys.executable, "-m", "locust", "-f", str(ROOT / "loadtest" / "locustfile.py"), "--headless", "-u", str(users), "-r", str(users),
         "-t", f"{seconds}s", "--host", gateways[0], "--csv", str(prefix), "--only-summary"],
        check=False, capture_output=True, text=True, env=env,
    )
    with open(f"{prefix}_stats.csv") as f:
        row = next(r for r in csv.DictReader(f) if r["Name"] == "POST /orders/[id]/pay")
    return {
        "requests": int(row["Request Count"]),
        "failures": int(row["Failure Count"]),
        "payments_per_s": round(float(row["Requests/s"]), 1),
        "p50_ms": float(row["50%"]),
        "p95_ms": float(row["95%"]),
        "p99_ms": float(row["99%"]),
    }


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-f", "--file", action="append", dest="files", help="base compose files")
    ap.add_argument("--users", type=int, default=64)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--shards", default="1,2,3")
    ap.add_argument("--gateways", default="http://localhost:8000,http://localhost:8001")
    ap.add_argument("--json", type=Path, default=ROOT / "results" / "load.json")
    args = ap.parse_args(argv)
    base = args.files or ["docker-compose.yml"]
    gateways = args.gateways.split(",")
    out: dict[str, Any] = {"users": args.users, "seconds": args.seconds, "cpus": os.cpu_count(), "layouts": {}}
    tmp = ROOT / "results" / "raw"
    tmp.mkdir(parents=True, exist_ok=True)
    for n in [int(x) for x in args.shards.split(",")]:
        files = [*base, f"loadtest/shards-{n}.yml"]
        compose(files, "down", "-v", "--remove-orphans")
        compose(files, "up", "-d", "--no-build" if "deploy/local/compose.local.yml" in base else "--build")
        wait_ready(files, gateways[0])
        locust(gateways, args.users, 10, tmp / f"warmup-{n}")
        result = locust(gateways, args.users, args.seconds, tmp / f"shards-{n}")
        out["layouts"][f"{n} shard process{'es' if n > 1 else ''}"] = result
        print(n, json.dumps(result), flush=True)
    compose([*base, "loadtest/shards-3.yml"], "down", "-v", "--remove-orphans")
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
