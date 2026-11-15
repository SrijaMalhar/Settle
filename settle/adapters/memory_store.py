"""The event store as a dictionary. Same rules as PostgreSQL: append is all or nothing, and a
sequence gap or overlap means a second writer."""

from __future__ import annotations

from settle.adapters.errors import Conflict
from settle.core.messages import AttemptMoved, Event, Input, PaymentCreated


class MemoryEventStore:
    def __init__(self) -> None:
        self.events: dict[str, list[Event]] = {}
        self.orders: dict[str, str] = {}  # order id -> payment id
        self.shard: dict[str, int] = {}  # payment id -> logical shard
        self.open: dict[str, bool] = {}
        self.inputs: dict[str, list[tuple[int, Input]]] = {}  # payment id -> (first seq, cause)
        self.appends = 0

    def load(self, payment_id: str) -> list[Event]:
        return list(self.events.get(payment_id, ()))

    def append(self, payment_id: str, events: list[Event], *, expected: int, open: bool,
               cause: Input | None = None) -> None:
        stored = self.events.get(payment_id, [])
        if len(stored) != expected:
            raise Conflict(f"{payment_id}: expected {expected} events, found {len(stored)}")
        created = [e for e in events if isinstance(e, PaymentCreated)]
        if created:
            c = created[0]
            if c.order_id in self.orders:
                raise Conflict(f"order {c.order_id} already has a payment")
            self.orders[c.order_id] = payment_id
            self.shard[payment_id] = c.shard
        self.events[payment_id] = stored + list(events)
        self.open[payment_id] = open
        if cause is not None:
            self.inputs.setdefault(payment_id, []).append((expected + 1, cause))
        self.appends += 1

    def load_inputs(self, payment_id: str) -> list[Input]:
        return [inp for _, inp in self.inputs.get(payment_id, ())]

    def payment_ids(self) -> list[str]:
        return sorted(self.events)

    def open_payments(self, shard: int) -> list[str]:
        return sorted(p for p, is_open in self.open.items() if is_open and self.shard.get(p) == shard)

    def payment_of(self, order_id: str) -> str | None:
        return self.orders.get(order_id)

    def resolutions(self, limit: int) -> list[tuple[str, float]]:
        found = [
            (e.at, e.psp, e.took)
            for events in self.events.values()
            for e in events
            if isinstance(e, AttemptMoved) and e.took is not None
        ]
        found.sort(reverse=True)
        return [(psp, took) for _, psp, took in found[:limit] if took is not None]
