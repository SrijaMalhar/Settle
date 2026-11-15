"""Replay check: re-run stored histories through decide(), as it is now or as you are about to
change it, and list every payment whose outcome would change.

    python -m settle.replay                                   # current rules: must match exactly
    python -m settle.replay --decide mypackage.rules:decide   # a changed decide()
    python -m settle.replay --bugs no_refund_after_expiry     # or one of the planted bugs

Every batch of events is stored with the input that caused it, in the same transaction. Feeding
those inputs back through a pure decide() rebuilds the history. With today's rules the rebuilt
events must equal the stored ones exactly; with new rules, the differences are what the change
would have done to real payments.

One limit: inputs that changed nothing (a duplicate callback, a refused second tap) store no
events and so no input, so a new rule that would act on one of those cannot be judged here.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from settle.core.decide import Rejected, UnknownAttempt, apply, decide, replay
from settle.core.messages import Command, Event, Input
from settle.core.model import IllegalMove, Payment

DecideFn = Callable[[Payment | None, Input, frozenset[str]], tuple[list[Event], list[Command]]]


class History(Protocol):
    def payment_ids(self) -> list[str]: ...

    def load(self, payment_id: str) -> list[Event]: ...

    def load_inputs(self, payment_id: str) -> list[Input]: ...


@dataclass(frozen=True)
class Outcome:
    """What a payment ended as, from the customer's side: its status and the money sent back."""

    status: str
    refunded: int  # paise

    @classmethod
    def of(cls, p: Payment | None) -> Outcome:
        if p is None:
            return cls("none", 0)
        return cls(p.status.value, sum(r.amount for r in p.refunds))


@dataclass
class Report:
    payments: int = 0
    identical: int = 0
    changed: list[dict[str, Any]] = field(default_factory=list)
    refused: Counter[str] = field(default_factory=Counter)  # inputs the new rules reject

    def summary(self) -> dict[str, Any]:
        return {
            "payments": self.payments,
            "identical_histories": self.identical,
            "outcomes_that_would_change": len(self.changed),
            "inputs_the_rules_would_refuse": dict(self.refused),
            "examples": self.changed[:10],
        }


def rerun(inputs: Iterable[Input], rules: DecideFn, bugs: frozenset[str], refused: Counter[str]) -> tuple[
        Payment | None, list[Event]]:
    p: Payment | None = None
    out: list[Event] = []
    for inp in inputs:
        try:
            events, _ = rules(p, inp, bugs)
            for e in events:
                p = apply(p, e)
        except (Rejected, UnknownAttempt, IllegalMove) as err:
            refused[f"{type(inp).__name__}: {type(err).__name__}"] += 1
            continue
        out += events
    return p, out


def check(history: History, rules: DecideFn = decide, bugs: frozenset[str] = frozenset(),
          limit: int | None = None) -> Report:
    rep = Report()
    for pid in history.payment_ids()[:limit]:
        stored = history.load(pid)
        p, events = rerun(history.load_inputs(pid), rules, bugs, rep.refused)
        rep.payments += 1
        if events == stored:
            rep.identical += 1
        before, after = Outcome.of(replay(stored)), Outcome.of(p)
        if before != after:
            first = replay(stored)
            rep.changed.append({
                "payment": pid,
                "order": first.order_id if first else None,
                "before": before.__dict__,
                "after": after.__dict__,
            })
    return rep


def load_rules(spec: str | None) -> DecideFn:
    if not spec:
        return decide
    module, _, name = spec.partition(":")
    fn: DecideFn = getattr(importlib.import_module(module), name or "decide")
    return fn


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=os.environ.get("SETTLE_DATABASE_URL",
                                                    "postgresql://settle:settle@localhost:5432/settle"))
    ap.add_argument("--decide", help="module:function of a changed decide()")
    ap.add_argument("--bugs", default="", help="comma-separated planted bugs to switch on")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args(argv)
    import psycopg

    from settle.adapters.postgres_store import PostgresEventStore

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        store = PostgresEventStore.single(conn)
        bugs = frozenset(b for b in args.bugs.split(",") if b)
        rep = check(store, load_rules(args.decide), bugs, args.limit)
    print(json.dumps(rep.summary(), indent=2))


if __name__ == "__main__":
    main()
