import pytest

from settle.core.model import (
    ATTEMPT_MOVES,
    FINAL,
    LIVE,
    PAYMENT_MOVES,
    AttemptStatus,
    IllegalMove,
    PaymentStatus,
    move,
    pay_move,
)
from settle.core.money import Money

S = AttemptStatus
P = PaymentStatus


def test_a_timeout_is_unknown_never_failed() -> None:
    assert move(S.SENDING, "read_timeout") is S.PENDING


def test_only_a_refused_connection_fails_without_an_answer() -> None:
    assert move(S.SENDING, "not_delivered") is S.FAILED


def test_a_callback_can_beat_the_http_reply() -> None:
    assert move(S.SENDING, "success") is S.SUCCEEDED
    # ...and the late "accepted" reply is ignored
    assert move(S.SUCCEEDED, "accepted") is S.SUCCEEDED
    assert move(S.FAILED, "read_timeout") is S.FAILED


@pytest.mark.parametrize("status,signal", [(S.SUCCEEDED, "success"), (S.FAILED, "failure"), (S.FAILED, "declined")])
def test_a_duplicate_answer_changes_nothing(status: AttemptStatus, signal: str) -> None:
    assert move(status, signal) is status


@pytest.mark.parametrize(
    "status,signal",
    [
        (S.SUCCEEDED, "failure"),
        (S.FAILED, "success"),
        (S.SUCCEEDED, "declined"),
        (S.PENDING, "not_delivered"),
        (S.SENDING, "checks_exhausted"),
    ],
)
def test_a_contradiction_raises(status: AttemptStatus, signal: str) -> None:
    with pytest.raises(IllegalMove):
        move(status, signal)


def test_a_late_answer_after_the_checks_ran_out_is_still_the_truth() -> None:
    assert move(S.UNRESOLVED, "success") is S.SUCCEEDED
    assert move(S.UNRESOLVED, "failure") is S.FAILED


def test_final_attempt_states_have_no_way_out() -> None:
    for frm, _ in ATTEMPT_MOVES:
        assert frm not in (S.SUCCEEDED, S.FAILED)


def test_live_attempts_are_the_ones_that_might_still_take_money() -> None:
    assert LIVE == {S.SENDING, S.PENDING, S.UNRESOLVED}


def test_payment_final_states_have_no_moves_except_a_contradiction() -> None:
    for frm, _ in PAYMENT_MOVES:
        assert frm not in FINAL
    for status in FINAL:
        assert pay_move(status, "contradiction") is P.NEEDS_OPS
        with pytest.raises(IllegalMove):
            pay_move(status, "expired")


def test_every_payment_transition_in_the_spec_is_in_the_table() -> None:
    assert pay_move(P.AWAITING, "paid") is P.PAID
    assert pay_move(P.AWAITING, "expired") is P.EXPIRED
    assert pay_move(P.EXPIRED, "closed") is P.CLOSED
    assert pay_move(P.EXPIRED, "refund_started") is P.REFUNDING
    assert pay_move(P.EXPIRED, "escalated") is P.NEEDS_OPS
    assert pay_move(P.NEEDS_OPS, "refund_started") is P.REFUNDING
    assert pay_move(P.NEEDS_OPS, "closed") is P.CLOSED
    assert pay_move(P.REFUNDING, "refund_confirmed") is P.REFUNDED
    assert pay_move(P.REFUNDING, "escalated") is P.NEEDS_OPS


def test_money_is_whole_paise() -> None:
    assert Money.rupees("499.50") == Money(49950)
    assert str(Money(49950)) == "₹499.50"
    with pytest.raises(ValueError):
        Money.rupees("1.005")
    with pytest.raises(ValueError):
        Money(0)
    with pytest.raises(TypeError):
        Money(1.5)  # type: ignore[arg-type]
