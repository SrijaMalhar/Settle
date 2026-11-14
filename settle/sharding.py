"""Logical shards. Always 12 of them, spread over however many processes run.

Logical shard = crc32(order id) mod 12. Not Python's hash(), which gives different answers in
different processes. The payment id starts with the shard ("p07..."), and every attempt id
starts with the payment id, so a PSP callback can be routed without a lookup.
"""

from __future__ import annotations

import re
import zlib

from settle import LOGICAL_SHARDS

ORDER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")


def logical_shard(order_id: str) -> int:
    return zlib.crc32(order_id.encode()) % LOGICAL_SHARDS


def shard_of_payment(payment_id: str) -> int:
    if len(payment_id) < 3 or payment_id[0] != "p" or not payment_id[1:3].isdigit():
        raise ValueError(f"not a payment id: {payment_id!r}")
    shard = int(payment_id[1:3])
    if shard >= LOGICAL_SHARDS:
        raise ValueError(f"no logical shard {shard}")
    return shard


def payment_of_attempt(attempt_id: str) -> str:
    """'p0312ab.2' -> 'p0312ab'; refund ids 'rf-p0312ab.2' work too."""
    if attempt_id.startswith("rf-"):
        attempt_id = attempt_id[3:]
    return attempt_id.split(".", 1)[0]


def parse_range(text: str) -> list[int]:
    """'0-3' -> [0, 1, 2, 3]; '0,4,8' -> [0, 4, 8]."""
    out: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            out.extend(range(int(lo), int(hi) + 1))
        elif part:
            out.append(int(part))
    if any(not 0 <= k < LOGICAL_SHARDS for k in out):
        raise ValueError(f"logical shards run from 0 to {LOGICAL_SHARDS - 1}: {text!r}")
    return sorted(set(out))
