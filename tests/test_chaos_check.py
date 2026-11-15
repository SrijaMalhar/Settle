"""The live checker under load: the PSPs' ledgers are read on both sides of the event read."""

from __future__ import annotations

from typing import Any

from chaos.check import Ledgers, check
from settle.core.messages import AttemptStarted, Event, PaymentCreated


def rows(*events: Event) -> list[tuple[str, int, str, dict[str, Any], float]]:
    return [("p1", seq, e.kind, e.data(), e.at) for seq, e in enumerate(events, 1)]


def ledgers(*debited: str) -> Ledgers:
    return Ledgers({a: 100 for a in debited}, {}, set(debited), set(), {})


STORED = rows(PaymentCreated(at=0.0, payment_id="p1", order_id="o1", shard=0, amount=100, hold_until=600.0),
              AttemptStarted(at=0.0, attempt="p1.1", psp="axis"))


def test_a_debit_taken_while_the_events_were_read_is_not_a_missing_attempt() -> None:
    # p2.1 was stored, sent and debited after the event read began: only the later ledgers have it
    assert check(STORED, ledgers("p2.1"), before=ledgers()).violations == []


def test_a_debit_seen_before_the_events_were_read_must_be_stored() -> None:
    rep = check(STORED, ledgers("p2.1"), before=ledgers("p2.1"))
    assert rep.violations == ["I6: p2.1: a PSP took money for an attempt Settle never stored"]


def test_a_quiet_system_needs_one_read() -> None:
    assert check(STORED, ledgers("p2.1")).violations == ["I6: p2.1: a PSP took money for an attempt Settle never stored"]
    assert check(STORED, ledgers("p1.1")).violations == []
