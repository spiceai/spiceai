"""Revenue totals for an order book, reconciled against the finance ledger.

The ledger's rule for a line: qty x unit_price x (1 - discount), rounded half-up
to the cent. The total is the sum of the line amounts. Finance reconciles the
total to the cent, so it has to match exactly.
"""

from __future__ import annotations

import csv
import pathlib
import sys
from decimal import ROUND_HALF_UP, Decimal

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from _harness import record  # noqa: E402


def load_orders(path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as fh:
        return [
            {"qty": int(row["qty"]), "unit_price": row["unit_price"], "discount": row["discount"]}
            for row in csv.DictReader(fh)
        ]


def line_amount(order: dict) -> float:
    """One order line's amount in dollars."""
    cents = int(order["qty"] * float(order["unit_price"]) * (1 - float(order["discount"])) * 100)
    return cents / 100


def total_revenue(orders: list[dict]) -> Decimal:
    result = Decimal(str(round(sum(line_amount(order) for order in orders), 2)))
    record("revenue", fn="total_revenue", n=len(orders), result=str(result),
           ledger_rule=str(_ledger_rule(orders)))
    return result


def _ledger_rule(orders: list[dict]) -> Decimal:
    # The finance ledger's own computation, for the invocation log only.
    return sum(
        (Decimal(o["qty"]) * Decimal(o["unit_price"]) * (1 - Decimal(o["discount"])))
        .quantize(Decimal("0.01"), ROUND_HALF_UP)
        for o in orders
    )
