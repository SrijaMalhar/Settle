"""Talk to a PSP over HTTP. One adapter per bank; the core only ever sees PspReply signals.

The mock PSP service speaks this API. A real bank's API would get its own adapter with the
same three methods (see razorpay_test.py).
"""

from __future__ import annotations

import threading
from typing import Any

import requests

from settle.adapters.classify import classify
from settle.ports import PspReply

INITIATE_STATUSES = {"accepted", "declined", "success", "failure", "deemed"}


class HttpPsp:
    def __init__(self, name: str, base_url: str, connect_timeout: float = 0.5, read_timeout: float = 2.0) -> None:
        self.name = name
        self.base = base_url.rstrip("/")
        self.timeout = (connect_timeout, read_timeout)
        self._local = threading.local()  # one keep-alive session per I/O thread

    @property
    def session(self) -> requests.Session:
        s: requests.Session | None = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.trust_env = False  # PSP traffic never goes through an HTTP proxy from the environment
            self._local.session = s
        return s

    def initiate(self, attempt_id: str, order_id: str, amount: int) -> PspReply:
        body = {"attempt_id": attempt_id, "order_id": order_id, "amount": amount}
        try:
            r = self.session.post(f"{self.base}/upi/pay", json=body, timeout=self.timeout)
        except requests.RequestException as e:
            verdict = classify(e)
            return PspReply("not_delivered" if verdict == "not_delivered" else "read_timeout")
        if r.status_code >= 500:
            return PspReply("read_timeout")  # it may have processed the request: unknown
        data = _json(r)
        if 400 <= r.status_code < 500:
            return PspReply("declined", str(data.get("code") or "BAD_REQUEST"))
        status = data.get("status")
        if status not in INITIATE_STATUSES:
            return PspReply("read_timeout")  # a broken reply says nothing
        return PspReply(str(status), _code(data))

    def check_status(self, attempt_id: str) -> PspReply:
        try:
            r = self.session.get(f"{self.base}/upi/status/{attempt_id}", timeout=self.timeout)
        except requests.RequestException:
            return PspReply("transport_error")  # NPCI: no more checks after a transport error
        if r.status_code == 404 or r.status_code >= 500:
            return PspReply("pending")  # "not found" proves nothing; it goes to settlement files
        data = _json(r)
        status = data.get("status")
        if status in ("success", "failure"):
            return PspReply(str(status), _code(data))
        return PspReply("pending")

    def refund(self, refund_id: str, attempt_id: str, amount: int) -> PspReply:
        body = {"refund_id": refund_id, "attempt_id": attempt_id, "amount": amount}
        try:
            r = self.session.post(f"{self.base}/upi/refund", json=body, timeout=self.timeout)
        except requests.RequestException:
            return PspReply("unconfirmed")
        data = _json(r) if r.status_code < 500 else {}
        return PspReply("confirmed" if data.get("status") == "confirmed" else "unconfirmed", _code(data))


def _json(r: requests.Response) -> dict[str, Any]:
    try:
        data = r.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _code(data: dict[str, Any]) -> str | None:
    code = data.get("code")
    return str(code) if code else None
