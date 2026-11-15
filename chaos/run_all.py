"""The six chaos runs, driven through Toxiproxy's API and Docker while load runs, with the
simulator's checks (I1 to I6) over the event log after each one.

    SETTLE_HOLD_SECONDS=60 docker compose up -d --build
    python -m chaos.run_all
    python -m chaos.run_all -f docker-compose.yml -f deploy/local/compose.local.yml   # offline machine

Each run injects one fault for about 20 seconds while payments keep arriving at a steady rate,
then checks the invariants over everything stored so far. At the end the load stops, every PSP
replays its final statuses (a settlement file), and once nothing is left in flight I2 is checked:
every order is charged exactly when it was paid.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import subprocess
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import requests

from chaos.check import Ledgers, Report, check
from settle.sharding import logical_shard

PSPS = ("axis", "icici", "yes")
AUTH = {"Authorization": f"Bearer {os.environ.get('SETTLE_MERCHANT_KEY', 'dev-merchant-key')}"}


class Stack:
    def __init__(self, files: list[str], toxiproxy: str, gateways: list[str]) -> None:
        self.compose = ["docker", "compose"] + [x for f in files for x in ("-f", f)]
        self.toxiproxy, self.gateways = toxiproxy.rstrip("/"), gateways
        self.http = requests.Session()
        self.http.trust_env = False

    def sh(self, *args: str, input: str | None = None) -> str:
        return subprocess.run(args, check=True, capture_output=True, text=True, input=input).stdout

    def dc(self, *args: str) -> str:
        return self.sh(*self.compose, *args)

    def container(self, service: str) -> str:
        return self.dc("ps", "-a", "-q", service).split()[0]

    def docker(self, verb: str, service: str) -> None:
        self.sh("docker", verb, self.container(service))

    def running(self, service: str) -> bool:
        return self.sh("docker", "inspect", "-f", "{{.State.Running}}", self.container(service)).strip() == "true"

    def sql(self, query: str) -> list[str]:
        out = self.dc("exec", "-T", "postgres", "psql", "-U", "settle", "-d", "settle", "-At", "-c", query)
        return [line for line in out.splitlines() if line]

    def owners(self) -> dict[int, tuple[str, float]]:
        rows = self.sql("SELECT shard, url, extract(epoch FROM since) FROM shard_owners")
        return {int(k): (u, float(t)) for k, u, t in (r.split("|") for r in rows)}

    def events(self) -> list[tuple[str, int, str, dict[str, Any], float]]:
        rows = self.sql("SELECT json_build_array(payment_id, seq, kind, data, extract(epoch FROM at)) "
                        "FROM events ORDER BY payment_id, seq")
        return [tuple(json.loads(r)) for r in rows]

    def psp(self, name: str, method: str, path: str) -> Any:
        code = (f"import urllib.request as u; r = u.Request('http://127.0.0.1:9100{path}', method='{method}', data=b'' "
                f"if '{method}' == 'POST' else None); print(u.urlopen(r).read().decode())")
        return json.loads(self.dc("exec", "-T", f"psp-{name}", "python", "-c", code))

    def ledgers(self) -> Ledgers:
        return Ledgers.merge([self.psp(n, "GET", "/_ledger") for n in PSPS])

    def toxic(self, proxy: str, kind: str, **attrs: Any) -> None:
        self.http.post(f"{self.toxiproxy}/proxies/{proxy}/toxics",
                       json={"type": kind, "stream": "downstream", "attributes": attrs}).raise_for_status()

    def clear_toxics(self) -> None:
        for name, proxy in self.http.get(f"{self.toxiproxy}/proxies").json().items():
            for t in proxy.get("toxics", []):
                self.http.delete(f"{self.toxiproxy}/proxies/{name}/toxics/{t['name']}")
            if not proxy.get("enabled", True):
                self.enable(name, True)

    def enable(self, proxy: str, on: bool) -> None:
        self.http.post(f"{self.toxiproxy}/proxies/{proxy}", json={"enabled": on}).raise_for_status()

    def stats(self) -> dict[str, Any]:
        r: dict[str, Any] = self.http.get(f"{self.gateways[0]}/stats", timeout=10, headers=AUTH).json()
        return r


class Load:
    """Open-loop load: payments arrive at a fixed rate whatever happens to the replies."""

    def __init__(self, gateways: list[str], rate: float) -> None:
        self.gateways, self.rate = gateways, rate
        self.records: list[tuple[float, float, int, str]] = []  # start, seconds, status, order
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._pool = ThreadPoolExecutor(128)
        self._local = threading.local()
        self.n = 0

    def session(self) -> requests.Session:
        s: requests.Session | None = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            s.trust_env = False
            s.headers.update(AUTH)
            self._local.s = s
        return s

    def start(self) -> Load:
        threading.Thread(target=self._tick, daemon=True).start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._pool.shutdown(wait=True)

    def _tick(self) -> None:
        nxt = time.time()
        while not self._stop.is_set():
            self.n += 1
            self._pool.submit(self._pay, f"c-{uuid.uuid4().hex[:12]}", self.gateways[self.n % len(self.gateways)])
            nxt += 1.0 / self.rate
            time.sleep(max(0.0, nxt - time.time()))

    def _pay(self, order: str, gateway: str) -> None:
        t = time.time()
        try:
            r = self.session().post(f"{gateway}/orders/{order}/pay", json={"amount": random.randint(1000, 500000)},
                                    timeout=15)
            status = r.status_code
        except requests.RequestException:
            status = 599
        with self._lock:
            self.records.append((t, time.time() - t, status, order))

    def window(self, start: float, end: float, only: Callable[[str], bool] | None = None) -> dict[str, Any]:
        with self._lock:
            rs = [r for r in self.records if start <= r[0] < end and (only is None or only(r[3]))]
        ok = [r for r in rs if r[2] < 500]
        lat = sorted(r[1] for r in ok)
        return {
            "requests": len(rs),
            "ok_per_s": round(len(ok) / max(end - start, 1e-9), 1),
            "errors": len(rs) - len(ok),
            "p50_ms": round(statistics.median(lat) * 1000, 1) if lat else None,
            "p95_ms": round(lat[int(0.95 * (len(lat) - 1))] * 1000, 1) if lat else None,
        }


def attempts_by_psp(rows: list[tuple[str, int, str, dict[str, Any], float]], start: float, end: float) -> dict[str, int]:
    out = dict.fromkeys(PSPS, 0)
    for _pid, _seq, kind, data, at in rows:
        if kind == "AttemptStarted" and start <= at < end:
            out[data["psp"]] = out.get(data["psp"], 0) + 1
    return out


def signals(rows: list[tuple[str, int, str, dict[str, Any], float]], psp: str, signal: str, start: float,
            end: float) -> list[str]:
    return [d["attempt"] for _p, _s, k, d, at in rows
            if k == "AttemptMoved" and d["psp"] == psp and d["signal"] == signal and start <= at < end]


def busiest(st: Stack, seconds: float = 10.0) -> str:
    """The PSP carrying the most new attempts right now. The router moves traffic around as PSPs
    misbehave, so a fault aimed at a PSP it has already left would test nothing."""
    now = time.time()
    counts = attempts_by_psp(st.events(), now - seconds, now)
    return max(PSPS, key=lambda p: counts.get(p, 0))


# ------------------------------------------------------------------------------------------------
# The six runs
# ------------------------------------------------------------------------------------------------


def slow_psp(st: Stack, load: Load, d: float) -> dict[str, Any]:
    """A 5-second latency toxic on the busiest PSP: its payments go unknown and checks resolve
    them; the router and breaker move new traffic away."""
    psp = busiest(st)
    t0 = time.time()
    st.toxic(psp, "latency", latency=5000)
    time.sleep(d)
    st.clear_toxics()
    rows = st.events()
    unknown = signals(rows, psp, "read_timeout", t0, t0 + d)
    return {
        "psp": psp,
        "attempts_per_psp_before": attempts_by_psp(rows, t0 - d, t0),
        "attempts_per_psp_second_half": attempts_by_psp(rows, t0 + d / 2, t0 + d),
        "attempts_that_timed_out": len(unknown),
        "_unknown": unknown,
        "load": load.window(t0, t0 + d),
    }


def psp_down(st: Stack, load: Load, d: float) -> dict[str, Any]:
    """Disable the busiest PSP's proxy: refused connections fail fast, its breaker opens on every
    shard, and traffic moves."""
    psp = busiest(st)
    t0 = time.time()
    st.enable(psp, False)
    opened: float | None = None
    while time.time() < t0 + d:
        shards = st.stats().get("shards", {})
        states = [s["psps"][psp]["breaker"] for s in shards.values() if "psps" in s]
        if opened is None and states and all(x == "open" for x in states):
            opened = time.time()
        time.sleep(0.2)
    st.enable(psp, True)
    rows = st.events()
    refused = signals(rows, psp, "not_delivered", t0, t0 + d)
    after_open = attempts_by_psp(rows, (opened or t0 + d) + 1, t0 + d)[psp]
    return {
        "psp": psp,
        "breaker_open_on_every_shard_after_s": round(opened - t0, 2) if opened else None,
        "refused_attempts": len(refused),
        "attempts_to_it_after_breaker_opened": after_open,
        "attempts_per_psp_during": attempts_by_psp(rows, t0, t0 + d),
        "load": load.window(t0, t0 + d),
    }


def resets(st: Stack, load: Load, d: float) -> dict[str, Any]:
    """Reset connections to the busiest PSP after the request went out: every reset attempt is
    checked or answered by its callback, and none is sent again (the PSPs' ledgers show any
    request received twice)."""
    psp = busiest(st)
    t0 = time.time()
    st.toxic(psp, "latency", latency=300)
    st.toxic(psp, "reset_peer", timeout=50)
    time.sleep(d)
    st.clear_toxics()
    rows = st.events()
    reset = signals(rows, psp, "read_timeout", t0, t0 + d)
    return {"psp": psp, "attempts_reset": len(reset), "_unknown": reset, "load": load.window(t0, t0 + d)}


def kill_shard(st: Stack, load: Load, d: float) -> dict[str, Any]:
    """docker kill shard-2 under load: the standby takes over; nobody is charged twice."""
    before = st.owners()
    old = before[4][0]
    t0 = time.time()
    st.docker("kill", "shard-2")
    taken: float | None = None
    while time.time() < t0 + d:
        now = st.owners()
        if all(now[k][0] != old for k in range(4, 8)):
            taken = max(now[k][1] for k in range(4, 8))
            break
        time.sleep(0.05)
    time.sleep(max(0.0, t0 + d - time.time()))
    restarted = st.running("shard-2")
    if not restarted:
        st.docker("start", "shard-2")  # comes back and waits on the locks as the new standby
    def mine(order: str) -> bool:
        return 4 <= logical_shard(order) <= 7

    return {
        "standby_serving_shards_4_to_7_after_s": round(taken - t0, 3) if taken else None,
        "throughput_5s_before": load.window(t0 - 5, t0),
        "throughput_first_5s_after": load.window(t0, t0 + 5),
        "throughput_5s_to_15s_after": load.window(t0 + 5, t0 + 15),
        "shards_4_to_7_first_5s_after": load.window(t0, t0 + 5, mine),
        "docker_restarted_it": restarted,
    }


def freeze_shard(st: Stack, load: Load, d: float) -> dict[str, Any]:
    """docker pause shard-1 for 10 s: no second writer; its customers wait."""
    before = st.owners()
    t0 = time.time()
    st.docker("pause", "shard-1")
    time.sleep(10)
    during = st.owners()
    st.docker("unpause", "shard-1")
    time.sleep(max(0.0, t0 + d - time.time()))
    def mine(order: str) -> bool:
        return logical_shard(order) <= 3

    def others(order: str) -> bool:
        return not mine(order)

    return {
        "owner_of_shards_0_to_3_unchanged": all(during[k][0] == before[k][0] for k in range(4)),
        "shards_0_to_3_during_pause": load.window(t0, t0 + 10, mine),
        "other_shards_during_pause": load.window(t0, t0 + 10, others),
        "shards_0_to_3_after": load.window(t0 + 12, t0 + d, mine),
    }


def lose_redis(st: Stack, load: Load, d: float) -> dict[str, Any]:
    """docker stop redis: shards use their own health numbers; merchants see no errors."""
    t0 = time.time()
    st.dc("stop", "redis")
    time.sleep(d)
    st.dc("start", "redis")
    return {"load_while_redis_down": load.window(t0 + 1, t0 + d)}


RUNS: list[tuple[str, Callable[[Stack, Load, float], dict[str, Any]]]] = [
    ("A PSP turns slow", slow_psp),
    ("A PSP goes down", psp_down),
    ("Resets mid-request", resets),
    ("Kill a shard under load", kill_shard),
    ("Freeze a shard", freeze_shard),
    ("Lose Redis", lose_redis),
]


def wait_ready(st: Stack, timeout: float = 120) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if len(st.owners()) == 12 and st.http.get(f"{st.gateways[0]}/health", timeout=2).ok:
                return
        except (subprocess.CalledProcessError, requests.RequestException):
            pass
        time.sleep(1)
    raise SystemExit("the stack never became ready")


def summary(rep: Report) -> dict[str, Any]:
    return {"payments": rep.payments, "status": rep.status, "violations": rep.violations}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-f", "--file", action="append", dest="files")
    ap.add_argument("--toxiproxy", default="http://localhost:8474")
    ap.add_argument("--gateways", default="http://localhost:8000,http://localhost:8001")
    ap.add_argument("--rate", type=float, default=30.0, help="payments per second")
    ap.add_argument("--duration", type=float, default=20.0, help="seconds per fault")
    ap.add_argument("--json", type=Path, default=Path("results/chaos.json"))
    args = ap.parse_args(argv)
    st = Stack(args.files or ["docker-compose.yml"], args.toxiproxy, args.gateways.split(","))
    wait_ready(st)
    st.clear_toxics()
    load = Load(st.gateways, args.rate).start()
    time.sleep(10)
    results: dict[str, Any] = {"rate_per_s": args.rate, "fault_seconds": args.duration, "runs": {}}
    unknown: list[str] = []
    for i, (name, run) in enumerate(RUNS, 1):
        print(f"[{i}/6] {name} ...", flush=True)
        out = run(st, load, args.duration)
        unknown += out.pop("_unknown", [])
        time.sleep(5)
        out["checks"] = summary(check(st.events(), st.ledgers()))
        results["runs"][name] = out
        print(json.dumps(out, indent=2), flush=True)
    load.stop()
    print("load stopped; settlement replay, then waiting for every payment to finish ...", flush=True)
    time.sleep(15)
    for psp in PSPS:
        st.psp(psp, "POST", "/_settle")
    deadline = time.time() + 900
    while time.time() < deadline:
        rep = check(st.events(), st.ledgers())
        if not ({"AWAITING", "EXPIRED", "REFUNDING"} & set(rep.status)):
            break
        time.sleep(10)
    for psp in PSPS:
        st.psp(psp, "POST", "/_settle")
    time.sleep(10)
    final = check(st.events(), st.ledgers(), final=True)
    rows = st.events()
    resolved = {d["attempt"] for _p, _s, k, d, _a in rows if k == "AttemptMoved" and d["to"] in ("succeeded", "failed")}
    results["unknown_attempts_resolved"] = f"{sum(a in resolved for a in unknown)} of {len(unknown)}"
    results["final"] = summary(final) | {"waiting_for_a_person": final.needs_ops}
    results["load_total"] = {"requests": len(load.records),
                             "errors": sum(1 for r in load.records if r[2] >= 500)}
    print(json.dumps({k: results[k] for k in ("unknown_attempts_resolved", "final", "load_total")}, indent=2))
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(results, indent=2) + "\n")
    if final.violations or any(r["checks"]["violations"] for r in results["runs"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
