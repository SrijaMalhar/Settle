"""Smoke test for a running deployment: a payment end to end through both gateways, every
Prometheus target up, and the Grafana dashboard provisioned.

    python scripts/smoke.py
"""

from __future__ import annotations

import sys
import time
import uuid

import requests

http = requests.Session()
http.trust_env = False


def wait(what: str, check: object, timeout: float = 180) -> None:
    end = time.time() + timeout
    last: object = None
    while time.time() < end:
        try:
            if check():  # type: ignore[operator]
                print(f"ok: {what}")
                return
        except requests.RequestException as e:
            last = e
        time.sleep(2)
    sys.exit(f"FAILED: {what} ({last})")


def main() -> None:
    wait("both gateways see all 12 logical shards",
         lambda: all(http.get(f"http://localhost:{p}/health", timeout=3).json()["owners"] == 12 for p in (8000, 8001)))
    # Mock PSPs decline about one payment in ten and some answer slowly, so pay several orders and
    # expect most of them, not every one, to be paid within the wait.
    orders = [f"smoke-{uuid.uuid4().hex[:8]}" for _ in range(8)]
    for order in orders:
        r = http.post(f"http://localhost:8000/orders/{order}/pay", json={"amount": 49900}, timeout=10)
        assert r.status_code == 202, r.text
    print(f"ok: {len(orders)} payments accepted through gateway 8000")

    def paid() -> bool:
        statuses = [http.get(f"http://localhost:8001/orders/{o}", timeout=5).json()["status"] for o in orders]
        print(f"   read through gateway 8001: {statuses}")
        return statuses.count("PAID") >= 6

    wait("most payments are PAID, read through the other gateway", paid, 180)

    def targets_up() -> bool:
        t = http.get("http://localhost:9090/api/v1/targets", timeout=5).json()["data"]["activeTargets"]
        up = [x for x in t if x["health"] == "up"]
        print(f"   prometheus targets up: {len(up)} of {len(t)}")
        return len(up) >= 6 and len(up) == len(t)

    wait("Prometheus scrapes 4 shard processes and 2 gateways", targets_up)
    wait("settle metrics are in Prometheus", lambda: http.get(
        "http://localhost:9090/api/v1/query", params={"query": "sum(settle_attempts_total)"}, timeout=5
    ).json()["data"]["result"])
    wait("the Grafana dashboard is provisioned", lambda: len(
        http.get("http://localhost:3000/api/dashboards/uid/settle", timeout=5).json()["dashboard"]["panels"]) == 17)


if __name__ == "__main__":
    main()
