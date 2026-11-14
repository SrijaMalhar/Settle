"""Which PSP gets a new attempt, and who is to blame when one fails.

Plain success-rate routing moves traffic away from a healthy PSP every time a customer types
the wrong PIN. Settle changes what is measured: every failure is attributed from its NPCI
response code first, and a success that kept the customer at "pending" counts for less than
a clean one.
"""

from __future__ import annotations

import random
from typing import Protocol

from settle.ports import HealthStore

# Codes from Zerodha's list of NPCI error codes and Juspay's UPI codes guide.
CUSTOMER = {"ZM", "Z6", "Z9", "ZA", "U69", "ZH"}  # wrong PIN, PIN tries, funds, declined, collect expired, bad UPI ID
PAYER_BANK = {"Z7", "Z8", "U30", "U67", "UT"}  # the customer's bank: its limits, debit failed, timed out, down

# Settle's own codes for failures it observed itself. Both are the PSP side.
NOT_DELIVERED = "NOT_DELIVERED"  # connection refused
TIMEOUT = "TIMEOUT"

REASONS = {
    "ZM": "Wrong UPI PIN. Ask the customer to try again with the right PIN.",
    "Z6": "Too many wrong PIN attempts. The customer must wait or reset their PIN.",
    "Z9": "Insufficient funds in the customer's account.",
    "ZA": "The customer declined the request.",
    "U69": "The collect request expired before the customer approved it.",
    "ZH": "Invalid UPI ID.",
    "Z7": "The customer's bank limit for this transaction was exceeded.",
    "Z8": "The customer's bank limit for the day was exceeded.",
    "U30": "The customer's bank could not debit the account. Try another account.",
    "U67": "The customer's bank timed out. Try another account.",
    "UT": "The customer's bank is unavailable. Try another account.",
}


def blame(code: str | None) -> str:
    """Who caused a failure, from its NPCI response code. Anything else is the PSP side."""
    if code in CUSTOMER:
        return "customer"
    if code in PAYER_BANK:
        return "payer_bank"
    return "psp"


def reason(code: str | None) -> str:
    """The message for the customer: the exact reason when it is theirs to fix."""
    if code in REASONS:
        return REASONS[code]
    return "The payment did not go through on our side. Please pay again."


class Router(Protocol):
    def record(self, psp: str, outcome: str, code: str | None, now: float) -> None: ...

    def choose(self, healthy: list[str], now: float) -> str | None: ...


class CleanSuccessRouter:
    """Pick the PSP most likely to give a clean success. A success that kept the customer at
    "pending" counts as `w`. Failures the PSP didn't cause are left out. Thompson sampling picks
    the PSP: one draw from Beta(1 + good, 1 + bad) each, and the highest draw wins."""

    def __init__(self, health: HealthStore, w: float = 0.5, seed: int = 0) -> None:
        if not 0.0 <= w <= 1.0:
            raise ValueError("w must be between 0 and 1")
        self.health, self.w, self.rng = health, w, random.Random(seed)

    def record(self, psp: str, outcome: str, code: str | None, now: float) -> None:
        """outcome: 'clean', 'pending_success' or 'failed'."""
        if outcome == "failed" and blame(code) != "psp":
            return  # the customer's or their bank's doing
        value = {"clean": 1.0, "pending_success": self.w}.get(outcome, 0.0)
        self.health.record(psp, value, now)

    def choose(self, healthy: list[str], now: float) -> str | None:
        if not healthy:
            return None

        def draw(psp: str) -> float:
            good, n = self.health.window(psp, now)
            return self.rng.betavariate(1 + good, 1 + max(n - good, 0.0))

        return max(healthy, key=draw)


class FixedOrderRouter:
    """The first healthy PSP in a fixed order. The baseline the experiments compare against."""

    def __init__(self, order: list[str]) -> None:
        self.order = order

    def record(self, psp: str, outcome: str, code: str | None, now: float) -> None:
        pass

    def choose(self, healthy: list[str], now: float) -> str | None:
        for psp in self.order:
            if psp in healthy:
                return psp
        return healthy[0] if healthy else None
