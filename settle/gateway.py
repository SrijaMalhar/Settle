"""A stateless gateway: hash the order id to a logical shard, forward to whoever owns it now.

Logical shard = crc32(order id) mod 12. Owners come from the shard_owners table, re-read every
half second and immediately when a forward fails. Every Settle endpoint is idempotent (pay is
idempotent per order, callbacks deduplicate), so a forward that hit a dead or moved owner is
simply sent again to the new one.
"""

from __future__ import annotations

import argparse
import hmac
import logging
import os
import threading
import time
from typing import Any

import psycopg
import requests
import requests.adapters
from flask import Flask, Response, g, jsonify, request
from flask.typing import ResponseReturnValue
from prometheus_client import CONTENT_TYPE_LATEST

from settle import LOGICAL_SHARDS
from settle.metrics import GatewayMetrics
from settle.sharding import ORDER_ID, logical_shard, payment_of_attempt, shard_of_payment

log = logging.getLogger("settle.gateway")

HOP_HEADERS = {"connection", "keep-alive", "transfer-encoding", "content-length", "content-encoding", "server", "date"}


class OwnerMap:
    def __init__(self, dsn: str, every: float = 0.5) -> None:
        self.dsn, self.every = dsn, every
        self.owners: dict[int, str] = {}
        self._conn: psycopg.Connection[Any] | None = None
        self._lock = threading.Lock()  # guards the connection only; the dict is replaced whole
        self._stop = threading.Event()
        self.refreshes = 0

    def start(self) -> OwnerMap:
        self.refresh()
        threading.Thread(target=self._poll, name="owners", daemon=True).start()
        return self

    def stop(self) -> None:
        self._stop.set()

    def _poll(self) -> None:
        while not self._stop.wait(self.every):
            self.refresh()

    def refresh(self) -> None:
        with self._lock:
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=2,
                                                 application_name="settle-gateway")
                rows = self._conn.execute("SELECT shard, url FROM shard_owners").fetchall()
            except psycopg.Error as e:
                log.warning("could not read shard owners: %s", e)
                if self._conn is not None:
                    self._conn.close()
                self._conn = None
                return
            self.owners = {int(k): str(u) for k, u in rows}
            self.refreshes += 1

    def url(self, shard: int) -> str | None:
        return self.owners.get(shard)


class Gateway:
    """Forwards each request to the owner of its logical shard.

    Two things keep one bad shard from hurting the others. Connections to shards come from one
    shared, bounded pool, so the gateways never hold more connections than a shard accepts. And
    each owner gets a bulkhead: at most `per_owner` requests in flight to it. When a shard
    freezes, its customers wait (or get a fast 503 once its bulkhead is full) while every other
    shard's customers keep getting answers from the gateway's remaining threads.
    """

    def __init__(self, owners: OwnerMap, deadline: float = 5.0, read_timeout: float = 6.0,
                 per_owner: int = 24, metrics: GatewayMetrics | None = None) -> None:
        self.owners, self.deadline, self.read_timeout, self.per_owner = owners, deadline, read_timeout, per_owner
        self.metrics = metrics or GatewayMetrics()
        self.http = requests.Session()  # urllib3's pool is thread-safe; no cookies are used
        self.http.trust_env = False
        adapter = requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=per_owner)
        self.http.mount("http://", adapter)
        self._bulkheads: dict[str, threading.BoundedSemaphore] = {}
        self._bulkhead_lock = threading.Lock()
        self.retries = 0
        self.shed = 0

    def _bulkhead(self, url: str) -> threading.BoundedSemaphore:
        with self._bulkhead_lock:
            b = self._bulkheads.get(url)
            if b is None:
                b = self._bulkheads[url] = threading.BoundedSemaphore(self.per_owner)
            return b

    def forward(self, shard: int, method: str, path: str, body: bytes, headers: dict[str, str]) -> Response:
        end = time.monotonic() + self.deadline
        last = "no owner known"
        while time.monotonic() < end:
            url = self.owners.url(shard)
            if url is None:
                self._wait()
                continue
            bulkhead = self._bulkhead(url)
            if not bulkhead.acquire(timeout=0.5):
                self.shed += 1
                self.metrics.shed.inc()
                return _json({"error": f"logical shard {shard} is not keeping up; try again"}, 503)
            try:
                r = self.http.request(method, url + path, data=body, headers=headers,
                                      timeout=(0.5, self.read_timeout))
            except requests.exceptions.ReadTimeout:
                return _json({"error": "the shard did not answer in time; the request may still be applied"}, 504)
            except requests.exceptions.ConnectionError as e:
                last = f"{url}: {type(e).__name__}"  # dead or moved: find the new owner and send again
                self._wait()
                continue
            finally:
                bulkhead.release()
            if r.status_code == 421:
                last = f"{url} no longer owns shard {shard}"
                self._wait()
                continue
            out = Response(r.content, status=r.status_code)
            for k, v in r.headers.items():
                if k.lower() not in HOP_HEADERS:
                    out.headers[k] = v
            return out
        return _json({"error": f"logical shard {shard} has no reachable owner ({last})"}, 503)

    def _wait(self) -> None:
        self.retries += 1
        self.metrics.retries.inc()
        self.owners.refresh()
        time.sleep(0.1)

    def stats(self) -> dict[str, Any]:
        urls = sorted(set(self.owners.owners.values()))
        out: dict[str, Any] = {"owners": {str(k): v for k, v in sorted(self.owners.owners.items())},
                               "missing": [k for k in range(LOGICAL_SHARDS) if k not in self.owners.owners],
                               "shards": {}}
        for u in urls:
            try:
                out["shards"][u] = self.http.get(u + "/stats", timeout=(0.5, 3)).json()
            except (requests.RequestException, ValueError) as e:
                out["shards"][u] = {"error": str(e)}
        return out


def _json(body: dict[str, Any], status: int) -> Response:
    r = jsonify(body)
    r.status_code = status
    return r


def _route() -> str:
    path = request.path
    if path.startswith("/psp/"):
        return "callback"
    if path.endswith("/pay"):
        return "pay"
    if path.endswith("/cancel"):
        return "cancel"
    return "get" if path.startswith("/orders/") else path.strip("/") or "root"


def create_app(gw: Gateway, merchant_keys: frozenset[str] = frozenset()) -> Flask:
    """merchant_keys: the API keys a merchant may send as "Authorization: Bearer <key>" on /orders
    and /stats. Empty means no check (tests and local experiments). PSP callbacks are not checked
    here: the shard verifies their HMAC signature, which proves more than a shared key would."""
    app = Flask("settle-gateway")

    def authorised() -> bool:
        if not merchant_keys:
            return True
        header = request.headers.get("Authorization", "")
        key = header[7:] if header.startswith("Bearer ") else ""
        return any(hmac.compare_digest(key, k) for k in merchant_keys)

    @app.before_request
    def start_timer() -> ResponseReturnValue | None:
        g.started = time.perf_counter()
        if (request.path.startswith("/orders/") or request.path == "/stats") and not authorised():
            return jsonify({"error": "missing or wrong API key"}), 401
        return None

    @app.after_request
    def count(response: Response) -> Response:
        route = _route()
        if route != "metrics":
            gw.metrics.requests.labels(route, str(response.status_code)).inc()
            gw.metrics.seconds.labels(route).observe(time.perf_counter() - g.started)
        return response

    def pass_on(shard: int) -> Response:
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_HEADERS | {"host"}}
        return gw.forward(shard, request.method, request.full_path.rstrip("?"), request.get_data(), headers)

    def order_shard(order_id: str) -> int | None:
        return logical_shard(order_id) if ORDER_ID.match(order_id) else None

    @app.route("/orders/<order_id>/pay", methods=["POST"])
    @app.route("/orders/<order_id>/cancel", methods=["POST"])
    @app.route("/orders/<order_id>", methods=["GET"])
    def order(order_id: str) -> ResponseReturnValue:
        shard = order_shard(order_id)
        if shard is None:
            return jsonify({"error": "bad order id"}), 400
        return pass_on(shard)

    @app.post("/psp/<psp>/callback")
    def callback(psp: str) -> ResponseReturnValue:
        body = request.get_json(silent=True) or {}
        try:
            shard = shard_of_payment(payment_of_attempt(str(body.get("attempt_id") or "")))
        except ValueError:
            return jsonify({"ok": True, "ignored": "not one of ours"}), 200
        return pass_on(shard)  # the logical shard is part of the attempt id

    @app.get("/stats")
    def stats() -> ResponseReturnValue:
        return jsonify(gw.stats())

    @app.get("/health")
    def health() -> ResponseReturnValue:
        return jsonify({"ok": True, "owners": len(gw.owners.owners), "retries": gw.retries, "shed": gw.shed})

    @app.get("/metrics")
    def prometheus() -> ResponseReturnValue:
        gw.metrics.owners.set(len(gw.owners.owners))
        return Response(gw.metrics.expose(), mimetype=CONTENT_TYPE_LATEST)

    return app


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="A stateless Settle gateway")
    ap.add_argument("--port", type=int, default=int(os.environ.get("SETTLE_GATEWAY_PORT", "8000")))
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    dsn = os.environ.get("SETTLE_DATABASE_URL", "postgresql://settle:settle@localhost:5432/settle")
    keys = frozenset(k.strip() for k in os.environ.get("SETTLE_MERCHANT_KEYS", "").split(",") if k.strip())
    if not keys:
        log.warning("SETTLE_MERCHANT_KEYS is empty: anyone who can reach this gateway can create payments")
    gw = Gateway(OwnerMap(dsn).start())
    from waitress import serve

    log.info("gateway on :%d", args.port)
    serve(create_app(gw, keys), host="0.0.0.0", port=args.port, threads=96, connection_limit=2000, ident=None)


if __name__ == "__main__":
    main()
