"""Observers and the merchant's notifications.

MerchantNotifier runs the core's Notify and AlertOps commands: it keeps the latest messages for
GET /stats and, if a webhook is configured, posts them on its own thread so the writer never
waits on the merchant. StatsCollector watches every stored event.
"""

from __future__ import annotations

import json
import logging
import sys
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import requests

from settle.core.messages import AlertOps, AttemptMoved, Event, Notify, PaymentPaid
from settle.policies.routing import blame

log = logging.getLogger("settle")


class MerchantNotifier:
    def __init__(self, webhook: str | None = None, keep: int = 200, echo: bool = False) -> None:
        self.webhook = webhook
        self.recent: deque[dict[str, Any]] = deque(maxlen=keep)
        self.alerts: deque[dict[str, Any]] = deque(maxlen=keep)
        self.echo = echo
        self._pool = ThreadPoolExecutor(2, thread_name_prefix="notify") if webhook else None

    def notify(self, order_id: str, payment_id: str, c: Notify, at: float) -> None:
        msg = {"order_id": order_id, "payment_id": payment_id, "what": c.what, "attempt": c.attempt,
               "code": c.code, "blame": c.blame, "message": c.message, "at": at}
        self._send(self.recent, msg)

    def alert(self, order_id: str, payment_id: str, c: AlertOps, at: float) -> None:
        msg = {"order_id": order_id, "payment_id": payment_id, "what": "ops", "reason": c.reason,
               "attempt": c.attempt, "at": at}
        self._send(self.alerts, msg)

    def _send(self, into: deque[dict[str, Any]], msg: dict[str, Any]) -> None:
        into.append(msg)
        if self.echo:
            print(json.dumps(msg), file=sys.stderr, flush=True)
        if self._pool is not None and self.webhook:
            self._pool.submit(self._post, msg)

    def _post(self, msg: dict[str, Any]) -> None:
        try:
            requests.post(str(self.webhook), json=msg, timeout=2)
        except requests.RequestException as e:
            log.warning("merchant webhook failed: %s", e)


class StatsCollector:
    """Counts outcomes per PSP and who was to blame for each failure."""

    def __init__(self) -> None:
        self.outcomes: dict[str, Counter[str]] = {}
        self.blame: dict[str, Counter[str]] = {}
        self.paid = 0
        self.events = 0

    def on_event(self, payment_id: str, event: Event) -> None:
        self.events += 1
        if isinstance(event, AttemptMoved) and event.outcome:
            self.outcomes.setdefault(event.psp, Counter())[event.outcome] += 1
            if event.outcome == "failed":
                self.blame.setdefault(event.psp, Counter())[blame(event.code)] += 1
        elif isinstance(event, PaymentPaid):
            self.paid += 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "paid": self.paid,
            "events": self.events,
            "outcomes": {k: dict(v) for k, v in self.outcomes.items()},
            "failures_by_blame": {k: dict(v) for k, v in self.blame.items()},
        }
