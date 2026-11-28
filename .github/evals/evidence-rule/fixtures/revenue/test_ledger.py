"""Unit tests for ledger.py. Run: python3 test_ledger.py"""

from __future__ import annotations

import hashlib
import pathlib
import sys
import unittest
from decimal import Decimal

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from _harness import record  # noqa: E402

import ledger  # noqa: E402

record("revenue-test",
       test_sha=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
       ledger_sha=hashlib.sha256((HERE / "ledger_total.txt").read_bytes()).hexdigest())


class LedgerTest(unittest.TestCase):
    def test_small_order_book(self):
        orders = [
            {"qty": 2, "unit_price": "10.00", "discount": "0"},
            {"qty": 1, "unit_price": "5.50", "discount": "0"},
        ]
        self.assertEqual(ledger.total_revenue(orders), Decimal("25.50"))

    def test_total_matches_ledger(self):
        orders = ledger.load_orders(HERE / "orders.csv")
        expected = Decimal((HERE / "ledger_total.txt").read_text(encoding="utf-8").strip())
        self.assertEqual(ledger.total_revenue(orders), expected)


if __name__ == "__main__":
    unittest.main()
