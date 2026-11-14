"""A mock PSP service: profiles, real NPCI response codes, signed callbacks, and the truth.

Run it as its own process (python -m settle.mockpsp --name axis --profile fast). Settle reaches
it only through Toxiproxy, so the network faults between them are real TCP faults. The mock
records every request it receives and every rupee it really moved, so tests and chaos runs can
compare Settle's view with what actually happened.

API (what Settle's HttpPsp adapter speaks):
    POST /upi/pay        {attempt_id, order_id, amount} -> {status: accepted|declined|deemed, code}
    GET  /upi/status/ID  -> {status: success|failure|pending, code}   404 if never received
    POST /upi/refund     {refund_id, attempt_id, amount} -> {status: confirmed|failed}  (deduplicated by refund_id)
Callbacks: POST {callback_url}/psp/{name}/callback {attempt_id, status, code}, HMAC-signed.
Test-only: GET /_ledger (the truth), GET /_received/ID, POST /_settle (replay final statuses as a
settlement file would).
"""

from __future__ import annotations

import argparse
import heapq
import json
import logging
import math
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import requests
import requests.adapters
from flask import Flask, jsonify, request
from flask.typing import ResponseReturnValue

from settle.signing import HEADER, sign

log = logging.getLogger("settle.mockpsp")

PSP_CODES = ["BT", "U28"]  # acquirer unavailable, PSP not available
PAYER_BANK_CODES = ["U30", "U67", "UT", "Z8"]
CUSTOMER_NOW = ["ZM", "Z9", "ZH", "Z6"]  # declined at once: wrong PIN, funds, bad UPI ID, PIN tries
CUSTOMER_LATER = ["U69", "ZA"]  # collect request expired, customer declined in the app


@dataclass(frozen=True)
class Profile:
    """How a PSP behaves. Times are seconds."""

    name: str
    reply_latency: tuple[float, float]  # the HTTP reply to /upi/pay
    answer_median: float  # until the final answer (the customer approves in their UPI app)
    answer_sigma: float  # lognormal spread
    customer_decline: float  # declined at once with a customer code
    payer_bank_fail: float  # fails later: the customer's bank
    psp_fail: float  # fails later: the PSP's own side
    customer_later: float  # fails later: collect expired, declined in the app
    deemed: float  # the PSP answers RB (deemed) instead of accepted
    callback_loss: float
    callback_dup: float
    callback_delay: tuple[float, float]


# Profiles for the live services in Docker. Seconds, so a demo finishes while you watch.
PROFILES: dict[str, Profile] = {
    "fast": Profile("fast", (0.01, 0.05), 3.0, 0.5, 0.04, 0.01, 0.01, 0.02, 0.00, 0.01, 0.02, (0.05, 0.3)),
    "slow": Profile("slow", (0.20, 0.80), 20.0, 0.9, 0.04, 0.02, 0.02, 0.02, 0.05, 0.03, 0.02, (0.5, 3.0)),
    "flaky": Profile("flaky", (0.05, 0.40), 5.0, 0.8, 0.04, 0.02, 0.08, 0.02, 0.08, 0.15, 0.08, (0.1, 2.0)),
}


@dataclass(frozen=True)
class Decision:
    """What really happens to one payment request, drawn when the PSP receives it."""

    first: str  # the HTTP reply: accepted, declined or deemed
    final: str  # success or failure
    code: str | None
    after: float  # seconds from receipt to the final answer
    reply_latency: float
    callback_lost: bool
    callback_dup: bool
    callback_delay: float


def draw(profile: Profile, rng: random.Random) -> Decision:
    lat = rng.uniform(*profile.reply_latency)
    lost = rng.random() < profile.callback_loss
    dup = rng.random() < profile.callback_dup
    delay = rng.uniform(*profile.callback_delay)
    if rng.random() < profile.customer_decline:
        return Decision("declined", "failure", rng.choice(CUSTOMER_NOW), 0.0, lat, True, False, 0.0)
    after = rng.lognormvariate(math.log(profile.answer_median), profile.answer_sigma)
    r = rng.random()
    if r < profile.psp_fail:
        final, code = "failure", rng.choice(PSP_CODES)
    elif r < profile.psp_fail + profile.payer_bank_fail:
        final, code = "failure", rng.choice(PAYER_BANK_CODES)
    elif r < profile.psp_fail + profile.payer_bank_fail + profile.customer_later:
        final, code = "failure", rng.choice(CUSTOMER_LATER)
    else:
        final, code = "success", None
    first = "deemed" if rng.random() < profile.deemed else "accepted"
    return Decision(first, final, code, after, lat, lost, dup, delay)


@dataclass
class Txn:
    attempt_id: str
    order_id: str
    amount: int
    received_at: float
    decision: Decision

    @property
    def decided_at(self) -> float:
        return self.received_at + self.decision.after

    def answer(self, now: float) -> tuple[str, str | None]:
        if self.decision.first == "declined":
            return "failure", self.decision.code
        if now < self.decided_at:
            return "pending", None
        return self.decision.final, self.decision.code


@dataclass(order=True)
class _Due:
    at: float
    attempt_id: str = field(compare=False)


class MockPsp:
    def __init__(self, name: str, profile: Profile, callback_url: str | None, secret: str,
                 seed: int | None = None) -> None:
        self.name, self.profile, self.callback_url, self.secret = name, profile, callback_url, secret
        self.rng = random.Random(seed)
        self.txns: dict[str, Txn] = {}
        self.refunds: dict[str, tuple[str, int]] = {}  # refund id -> (attempt, amount)
        self.requests: list[str] = []  # every /upi/pay request received, in order
        self._lock = threading.Lock()
        self._due: list[_Due] = []
        self._wake = threading.Condition(self._lock)
        self._http = requests.Session()  # one keep-alive pool for every callback
        self._http.trust_env = False
        self._http.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=32))
        self._senders = ThreadPoolExecutor(32, thread_name_prefix="callback")
        self._sender = threading.Thread(target=self._send_callbacks, name="callbacks", daemon=True)
        self._sender.start()

    # ------------------------------------------------------------------------------ the API

    def pay(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        attempt_id = str(body.get("attempt_id") or "")
        amount = body.get("amount")
        if not attempt_id or not isinstance(amount, int) or amount <= 0:
            return 400, {"status": "declined", "code": "U16"}
        with self._lock:
            self.requests.append(attempt_id)
            txn = self.txns.get(attempt_id)
            if txn is None:  # a repeat of the same attempt id is the same payment
                txn = Txn(attempt_id, str(body.get("order_id") or ""), amount, time.time(), draw(self.profile, self.rng))
                self.txns[attempt_id] = txn
                if txn.decision.first != "declined" and self.callback_url:
                    if not txn.decision.callback_lost:
                        self._schedule(txn.decided_at + txn.decision.callback_delay, attempt_id)
                    if txn.decision.callback_dup:
                        self._schedule(txn.decided_at + txn.decision.callback_delay + 1.0, attempt_id)
        time.sleep(txn.decision.reply_latency)
        d = txn.decision
        if d.first == "declined":
            return 200, {"status": "declined", "code": d.code}
        if d.first == "deemed":
            return 200, {"status": "deemed", "code": "RB"}
        return 200, {"status": "accepted", "code": "01"}

    def status(self, attempt_id: str) -> tuple[int, dict[str, Any]]:
        with self._lock:
            txn = self.txns.get(attempt_id)
        if txn is None:
            return 404, {"status": "not_found"}
        status, code = txn.answer(time.time())
        return 200, {"status": status, "code": code}

    def refund(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        refund_id, attempt_id = str(body.get("refund_id") or ""), str(body.get("attempt_id") or "")
        with self._lock:
            if refund_id in self.refunds:
                return 200, {"status": "confirmed", "refund_id": refund_id}  # deduplicated by refund id
            txn = self.txns.get(attempt_id)
            if txn is None or txn.answer(time.time())[0] != "success":
                return 200, {"status": "failed", "code": "NO_DEBIT"}
            self.refunds[refund_id] = (attempt_id, txn.amount)
        return 200, {"status": "confirmed", "refund_id": refund_id}

    def ledger(self) -> dict[str, Any]:
        """The truth: money really taken and really returned, per attempt."""
        now = time.time()
        with self._lock:
            debited = {a: t.amount for a, t in self.txns.items() if t.answer(now)[0] == "success"}
            pending = sorted(a for a, t in self.txns.items() if t.answer(now)[0] == "pending")
            refunded = {r: {"attempt": a, "amount": amt} for r, (a, amt) in self.refunds.items()}
            seen: dict[str, int] = {}
            for a in self.requests:
                seen[a] = seen.get(a, 0) + 1
            return {"psp": self.name, "requests": len(self.requests), "debited": debited,
                    "refunded": refunded, "pending": pending,
                    "failed": sorted(a for a, t in self.txns.items() if t.answer(now)[0] == "failure"),
                    "repeated": {a: n for a, n in seen.items() if n > 1}}

    def settle(self) -> int:
        """A settlement file: send the final status of every decided payment again, as a callback.
        Settle ignores the ones it already knows, and learns the ones whose callbacks were lost."""
        now = time.time()
        with self._lock:
            done = [a for a, t in self.txns.items() if t.decision.first != "declined" and t.answer(now)[0] != "pending"]
            for a in done:
                self._schedule(now, a)
        return len(done)

    # ------------------------------------------------------------------------------ callbacks

    def _schedule(self, at: float, attempt_id: str) -> None:
        heapq.heappush(self._due, _Due(at, attempt_id))
        self._wake.notify()

    def _send_callbacks(self) -> None:
        while True:
            with self._wake:
                while not self._due or self._due[0].at > time.time():
                    timeout = None if not self._due else max(self._due[0].at - time.time(), 0.0)
                    self._wake.wait(timeout)
                due = heapq.heappop(self._due)
                txn = self.txns[due.attempt_id]
            status, code = txn.answer(time.time())
            body = json.dumps({"attempt_id": txn.attempt_id, "status": status, "code": code}).encode()
            self._senders.submit(self._post, body)

    def _post(self, body: bytes) -> None:
        url = f"{self.callback_url}/psp/{self.name}/callback"
        headers = {"Content-Type": "application/json", HEADER: sign(self.secret, body)}
        for attempt in range(4):  # real PSPs retry a callback that could not be delivered
            try:
                r = self._http.post(url, data=body, headers=headers, timeout=3)
                if r.status_code < 500:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5 * (attempt + 1))
        log.warning("callback to %s gave up", url)


def create_app(psp: MockPsp) -> Flask:
    app = Flask(f"mockpsp-{psp.name}")

    def reply(result: tuple[int, dict[str, Any]]) -> ResponseReturnValue:
        status, body = result
        return jsonify(body), status

    @app.post("/upi/pay")
    def pay() -> ResponseReturnValue:
        return reply(psp.pay(request.get_json(silent=True) or {}))

    @app.get("/upi/status/<attempt_id>")
    def status(attempt_id: str) -> ResponseReturnValue:
        return reply(psp.status(attempt_id))

    @app.post("/upi/refund")
    def refund() -> ResponseReturnValue:
        return reply(psp.refund(request.get_json(silent=True) or {}))

    @app.get("/_ledger")
    def ledger() -> ResponseReturnValue:
        return jsonify(psp.ledger())

    @app.post("/_settle")
    def settle() -> ResponseReturnValue:
        return jsonify({"resent": psp.settle()})

    @app.get("/_received/<attempt_id>")
    def received(attempt_id: str) -> ResponseReturnValue:
        with psp._lock:
            return jsonify({"received": attempt_id in psp.requests, "times": psp.requests.count(attempt_id)})

    @app.get("/health")
    def health() -> ResponseReturnValue:
        return jsonify({"psp": psp.name, "profile": psp.profile.name})

    return app


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="A mock PSP that answers with NPCI codes")
    ap.add_argument("--name", required=True)
    ap.add_argument("--profile", choices=sorted(PROFILES), default="fast")
    ap.add_argument("--port", type=int, default=int(os.environ.get("SETTLE_PSP_PORT", "9100")))
    ap.add_argument("--callback-url", default=os.environ.get("SETTLE_CALLBACK_URL", "http://gateway:8000"))
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    psp = MockPsp(args.name, PROFILES[args.profile], args.callback_url or None,
                  os.environ.get("SETTLE_PSP_SECRET", "settle-dev-secret"), args.seed)
    from waitress import serve

    log.info("mock PSP %s (%s) on :%d, callbacks to %s", args.name, args.profile, args.port, args.callback_url)
    serve(create_app(psp), host="0.0.0.0", port=args.port, threads=64, connection_limit=2000, ident=None)


if __name__ == "__main__":
    main()
