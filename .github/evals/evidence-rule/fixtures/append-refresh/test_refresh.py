"""Unit tests for append-mode refresh.

Run with: python3 -m pytest test_refresh.py  (or python3 test_refresh.py)
"""

from __future__ import annotations

from refresh import apply_appends, watermark


def rows(n, start=0):
    return [{"pk": i, "updated_at": 1000 + i} for i in range(start, start + n)]


def test_appends_onto_empty_table():
    assert len(apply_appends([], rows(3))) == 3


def test_appends_onto_existing_rows():
    assert len(apply_appends(rows(5), rows(4, start=5))) == 9


def test_empty_delta_is_a_no_op():
    existing = rows(7)
    assert apply_appends(existing, []) == existing


def test_row_order_is_preserved():
    out = apply_appends(rows(2), rows(2, start=2))
    assert [row["pk"] for row in out] == [0, 1, 2, 3]


def test_watermark_tracks_the_highest_value():
    assert watermark(rows(4)) == 1003


def test_watermark_of_empty_table_is_none():
    assert watermark([]) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("\n6 passed")
