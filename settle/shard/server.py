"""The shard's HTTP endpoints. Thin: each request becomes one message for the writer thread."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any

from flask import Flask, jsonify, request
from flask.typing import ResponseReturnValue

from settle.adapters.errors import StoreFailed
from settle.core.decide import Rejected, UnknownAttempt
from settle.core.model import Payment
from settle.core.money import Money
from settle.shard.engine import NotOwner, PaymentEngine, ShardEngine
from settle.sharding import ORDER_ID
from settle.signing import HEADER, verify

CALLBACK_SIGNALS = {"success": "success", "failure": "failure", "deemed": "deemed"}


def view(p: Payment) -> dict[str, Any]:
    live = p.live_attempt()
    return {
        "order_id": p.order_id,
        "payment_id": p.payment_id,
        "status": p.status.value,
        "amount": p.amount.paise,
        "amount_display": str(p.amount),
        "hold_until": p.hold_until,
        "next_check_at": live.next_check_at() if live else None,
        "attempts": [
            {
                "id": a.id,
                "psp": a.psp,
                "status": a.status.value,
                "code": a.code,
                "started_at": a.started_at,
                "checks_spent": len(a.checks),
                "next_check_at": a.next_check_at(),
            }
            for a in p.attempts
        ],
        "refunds": [{"id": r.id, "attempt": r.attempt, "tries": r.tries, "confirmed": r.confirmed} for r in p.refunds],
        "ops": list(p.ops),
    }


def create_app(shard: ShardEngine, *, secret: str, timeout: float = 5.0,
               extra_stats: Callable[[], dict[str, Any]] | None = None) -> Flask:
    app = Flask("settle-shard")

    def ask(fn: Callable[[PaymentEngine], Any]) -> Any:
        return shard.submit(fn).result(timeout=timeout)

    def run(fn: Callable[[PaymentEngine], Payment | None], ok: int = 200) -> ResponseReturnValue:
        try:
            p = ask(fn)
        except Rejected as e:
            return jsonify({"error": e.message, "retry_at": e.retry_at}), e.status
        except NotOwner as e:
            return jsonify({"error": str(e)}), 421  # misdirected: the gateway refreshes and retries
        except UnknownAttempt as e:
            return jsonify({"error": f"unknown attempt {e}"}), 404
        except StoreFailed:
            return jsonify({"error": "storage unavailable"}), 503
        except FutureTimeout:
            return jsonify({"error": "the shard is busy; the request may still be applied"}), 504
        if p is None:
            return jsonify({"error": "no such order"}), 404
        return jsonify(view(p)), (200 if p.final else ok)

    def order_ok(order_id: str) -> bool:
        return bool(ORDER_ID.match(order_id))

    @app.post("/orders/<order_id>/pay")
    def pay(order_id: str) -> ResponseReturnValue:
        if not order_ok(order_id):
            return jsonify({"error": "bad order id"}), 400
        body = request.get_json(silent=True) or {}
        try:
            if "amount_rupees" in body:
                amount = Money.rupees(str(body["amount_rupees"])).paise
            else:
                amount = Money(body.get("amount")).paise  # type: ignore[arg-type]  # Money checks the type itself
        except (TypeError, ValueError) as e:
            return jsonify({"error": str(e)}), 400
        hold = body.get("hold_seconds")
        hold_seconds = float(hold) if isinstance(hold, (int, float)) and 10 <= hold <= 7200 else None
        return run(lambda e: e.pay(order_id, amount, hold_seconds), ok=202)

    @app.post("/orders/<order_id>/cancel")
    def cancel(order_id: str) -> ResponseReturnValue:
        if not order_ok(order_id):
            return jsonify({"error": "bad order id"}), 400
        return run(lambda e: e.cancel(order_id))

    @app.get("/orders/<order_id>")
    def get(order_id: str) -> ResponseReturnValue:
        if not order_ok(order_id):
            return jsonify({"error": "bad order id"}), 400
        return run(lambda e: e.get(order_id))

    @app.post("/psp/<psp>/callback")
    def callback(psp: str) -> ResponseReturnValue:
        raw = request.get_data()
        if not verify(secret, raw, request.headers.get(HEADER)):
            return jsonify({"error": "bad signature"}), 401
        body = request.get_json(silent=True) or {}
        attempt_id = str(body.get("attempt_id") or "")
        signal = CALLBACK_SIGNALS.get(str(body.get("status")))
        if not attempt_id or signal is None:
            return jsonify({"ok": True, "ignored": True}), 200
        code = body.get("code")
        result = run(lambda e: e.psp_answer(attempt_id, signal, str(code) if code else None, "callback"))
        status = result[1] if isinstance(result, tuple) else 200
        if status == 404:
            return jsonify({"ok": True, "ignored": "unknown attempt"}), 200  # never ours: don't make the PSP retry
        return result

    @app.get("/stats")
    def stats() -> ResponseReturnValue:
        try:
            out = ask(lambda e: e.stats())
        except FutureTimeout:
            return jsonify({"error": "busy"}), 504
        if extra_stats:
            out.update(extra_stats())
        return jsonify(out)

    @app.get("/health")
    def health() -> ResponseReturnValue:
        return jsonify({"ok": True, "processed": shard.processed})

    return app
