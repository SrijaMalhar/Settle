"""Money as whole paise. Floats never touch an amount."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

# UPI's per-transaction ceiling for most merchants is ₹1 lakh; a few categories go to ₹5 lakh.
MAX_PAISE = 5_00_000 * 100


@dataclass(frozen=True, order=True)
class Money:
    paise: int

    def __post_init__(self) -> None:
        if isinstance(self.paise, bool) or not isinstance(self.paise, int):
            raise TypeError("Money holds whole paise as an int")
        if self.paise <= 0:
            raise ValueError("an amount must be more than zero")
        if self.paise > MAX_PAISE:
            raise ValueError("an amount above ₹5,00,000 is not a UPI payment")

    @classmethod
    def rupees(cls, text: str) -> Money:
        """Parse "499" or "499.50" exactly. Rejects a third decimal place instead of rounding it."""
        try:
            value = Decimal(text)
        except InvalidOperation as e:
            raise ValueError(f"not an amount: {text!r}") from e
        paise = value * 100
        if paise != paise.to_integral_value():
            raise ValueError(f"more than two decimal places: {text!r}")
        return cls(int(paise))

    def __str__(self) -> str:
        rupees, paise = divmod(self.paise, 100)
        return f"₹{rupees}.{paise:02d}"
