"""Locust load: merchants paying new orders as fast as Settle answers.

    locust -f loadtest/locustfile.py --host http://localhost:8000
    SETTLE_GATEWAYS=http://localhost:8000,http://localhost:8001 locust -f loadtest/locustfile.py --headless -u 64 -t 60s

Each user pays a fresh order and immediately pays the next one. Every pay also sets off the rest
of the payment (the PSP reply, its callback, the status update), so the shards are doing about
four steps for each request Locust counts.
"""

from __future__ import annotations

import os
import random
import uuid

from locust import FastHttpUser, constant, task

GATEWAYS = [g for g in os.environ.get("SETTLE_GATEWAYS", "").split(",") if g]


class Merchant(FastHttpUser):
    wait_time = constant(0)

    def on_start(self) -> None:
        self.gateway = random.choice(GATEWAYS) if GATEWAYS else ""

    @task
    def pay(self) -> None:
        order = f"lt-{uuid.uuid4().hex[:16]}"
        self.client.post(f"{self.gateway}/orders/{order}/pay", json={"amount": random.randint(1_000, 5_00_000)},
                         name="POST /orders/[id]/pay")
