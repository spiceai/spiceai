"""Unit tests for compaction.

Run with: python3 -m pytest test_accel.py  (or python3 test_accel.py)
"""

from __future__ import annotations

from accel import compact


def test_single_run_passes_through():
    run = [{"pk": 1, "payload": "a"}, {"pk": 2, "payload": "b"}]
    assert len(compact([run])) == 2


def test_later_run_supersedes_earlier():
    old = [{"pk": 1, "payload": "old"}, {"pk": 2, "payload": "keep"}]
    new = [{"pk": 1, "payload": "new"}]
    out = {row["pk"]: row["payload"] for row in compact([old, new])}
    assert out == {1: "new", 2: "keep"}


def test_row_count_is_preserved_when_no_keys_repeat():
    a = [{"pk": 1, "payload": "a"}, {"pk": 2, "payload": "b"}]
    b = [{"pk": 3, "payload": "c"}]
    assert len(compact([a, b])) == 3


def test_empty_table():
    assert compact([]) == []


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("\n4 passed")
