#!/usr/bin/env python3
"""HTTP harness for concurrent same-key autocommit UPDATE.

HTTP 200 is not success. `UPDATE … WHERE id = 50` can affect 0 or 1 rows.
A parse failure or any other count cannot be ignored: then an unchanged
table satisfies `amount == start + count1` and the harness exits 0
without any UPDATE being acknowledged.

Usage:
  30_cayenne_concurrent_update_row_loss.py THREADS UPDATES
  30_cayenne_concurrent_update_row_loss.py --replay-malformed
  30_cayenne_concurrent_update_row_loss.py --replay-conflict
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import subprocess
import sys
from typing import Callable


SQL_URL = "http://127.0.0.1:8393/v1/sql"


class UpdateReplyError(ValueError):
    """Malformed HTTP 200 UPDATE body or unexpected affected-row count."""


class _Resp:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text


def parse_update_affected_count(body: str) -> int:
    """Return 0 or 1 from an autocommit UPDATE HTTP 200 body.

    Spice returns `[{"count": N}]`. N must be 0 or 1 for a single-key
    `WHERE id = 50` UPDATE. Empty arrays, missing counts, non-integers,
    and any other N are harness failures.
    """
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as e:
        raise UpdateReplyError(f"non-JSON UPDATE reply: {body!r}") from e

    if isinstance(payload, list):
        if len(payload) != 1 or not isinstance(payload[0], dict):
            raise UpdateReplyError(
                f"UPDATE reply must be a one-row object array, got {body!r}"
            )
        row = payload[0]
    elif isinstance(payload, dict):
        row = payload
    else:
        raise UpdateReplyError(
            f"UPDATE reply must be a JSON object or array, got {body!r}"
        )

    if "count" not in row:
        raise UpdateReplyError(f"UPDATE reply missing count: {body!r}")
    raw = row["count"]
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise UpdateReplyError(
            f"UPDATE reply count must be an integer, got {raw!r} in {body!r}"
        )
    if raw not in (0, 1):
        raise UpdateReplyError(
            f"unexpected affected-row count {raw} "
            f"(expected 0 or 1 per autocommit UPDATE): {body!r}"
        )
    return raw


def http_post(sql: str) -> _Resp:
    import requests

    r = requests.post(SQL_URL, data=sql.encode(), timeout=120)
    return _Resp(r.status_code, r.text)


def run(threads: int, n: int, post: Callable[[str], _Resp]) -> int:
    before_row = post("SELECT id,amount FROM o_cayenne WHERE id=50")
    before_count = post("SELECT count(*) c FROM o_cayenne")
    print("before:", before_row.text, before_count.text)
    before_amount = json.loads(before_row.text)[0]["amount"]

    with cf.ThreadPoolExecutor(threads) as ex:
        res = list(
            ex.map(
                lambda _i: post(
                    "UPDATE o_cayenne SET amount = amount + 1 WHERE id = 50"
                ),
                range(n),
            )
        )

    codes: dict[int, int] = {}
    bodies: list[str] = []
    count1 = 0
    count0 = 0
    errors = 0
    conflicts = 0
    bad_replies = 0
    ok = True
    for r in res:
        codes[r.status_code] = codes.get(r.status_code, 0) + 1
        bodies.append(r.text.strip())
        # A retryable write conflict is not an increment and not a harness
        # failure (SKILL.md: HTTP 200, or a 409 with a retryable conflict).
        if r.status_code == 409:
            conflicts += 1
            continue
        if r.status_code != 200:
            errors += 1
            continue
        try:
            c = parse_update_affected_count(r.text)
        except UpdateReplyError as e:
            print(f"FAIL: {e}")
            bad_replies += 1
            ok = False
            continue
        if c == 1:
            count1 += 1
        else:
            count0 += 1

    print(
        f"{n} UPDATEs with {threads} threads: HTTP codes",
        codes,
        "bodies sample:",
        set(bodies[:50]),
    )
    print(
        f"count1={count1} count0={count0} non200={errors} "
        f"conflicts={conflicts} bad_replies={bad_replies}"
    )

    after_row = post("SELECT id,amount FROM o_cayenne WHERE id=50")
    after_count = post("SELECT count(*) c FROM o_cayenne")
    print(
        "after :",
        after_row.text,
        after_count.text,
        "(expected id=50 present with amount=before+successes, count=1000)",
    )

    after_text = after_row.text.strip()
    if after_row.status_code != 200 or after_text in ("", "[]"):
        print("FAIL: id=50 missing")
        ok = False
    else:
        after = json.loads(after_row.text)
        if not after:
            print("FAIL: id=50 missing")
            ok = False
        else:
            amount = after[0]["amount"]
            expected = before_amount + count1
            if abs(amount - expected) > 1e-9:
                print(
                    f"FAIL: amount {amount} != start {before_amount} + count1 {count1} = {expected}"
                )
                ok = False
            else:
                print(f"OK amount {amount} = {before_amount} + {count1}")
    total = json.loads(after_count.text)[0]["c"]
    if int(total) != 1000:
        print(f"FAIL: count(*)={total} expected 1000")
        ok = False
    else:
        print("OK count(*)=1000")
    if count0 and errors == 0:
        print(f"FAIL: {count0} silent count=0 responses with no error")
        ok = False
    if errors:
        print(f"FAIL: {errors} non-200 responses")
        ok = False
    if bad_replies:
        print(
            f"FAIL: {bad_replies} malformed or unexpected UPDATE replies "
            f"(HTTP 200 is not an acknowledgement)"
        )
        ok = False
    return 0 if ok else 2


def _stub_post(
    update_body: str,
    after_amount: float,
    update_status: int = 200,
) -> Callable[[str], _Resp]:
    selects = {"n": 0}

    def post(sql: str) -> _Resp:
        if sql.startswith("UPDATE"):
            return _Resp(update_status, update_body)
        if "count(*)" in sql:
            return _Resp(200, '[{"c":1000}]')
        selects["n"] += 1
        if selects["n"] == 1:
            return _Resp(200, '[{"id":50,"amount":28.13}]')
        return _Resp(200, f'[{{"id":50,"amount":{after_amount}}}]')

    return post


def replay_malformed() -> int:
    """Stub-transport replay of malformed and valid UPDATE replies."""
    sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    print(f"sha={sha}")
    print("command: 30_cayenne_concurrent_update_row_loss.py --replay-malformed")

    parser_cases: list[tuple[str, int | type]] = [
        ("[]", UpdateReplyError),
        ("not-json", UpdateReplyError),
        ("{}", UpdateReplyError),
        ("[{}]", UpdateReplyError),
        ('[{"count":-1}]', UpdateReplyError),
        ('[{"count":2}]', UpdateReplyError),
        ("null", UpdateReplyError),
        ('[{"count":true}]', UpdateReplyError),
        ('[{"count":1.0}]', UpdateReplyError),
        ('[{"count":1},{"count":1}]', UpdateReplyError),
        ('[{"count":1}]', 1),
        ('[{"count":0}]', 0),
        ('{"count":1}', 1),
    ]
    parser_ok = True
    print()
    print("== parse_update_affected_count ==")
    for body, expected in parser_cases:
        try:
            got = parse_update_affected_count(body)
        except UpdateReplyError as e:
            if expected is UpdateReplyError:
                print(f"OK reject {body!r}: {e}")
            else:
                print(f"FAIL {body!r}: rejected, expected {expected}: {e}")
                parser_ok = False
            continue
        if expected is UpdateReplyError:
            print(f"FAIL {body!r}: accepted as {got}, expected reject")
            parser_ok = False
        elif got != expected:
            print(f"FAIL {body!r}: got {got}, expected {expected}")
            parser_ok = False
        else:
            print(f"OK {body!r} -> {got}")

    harness_cases: list[tuple[str, str, float, int]] = [
        ("200 []", "[]", 28.13, 2),
        ("200 not-json", "not-json", 28.13, 2),
        ("200 {}", "{}", 28.13, 2),
        ("200 [{}]", "[{}]", 28.13, 2),
        ('200 [{"count":-1}]', '[{"count":-1}]', 28.13, 2),
        ('200 [{"count":2}]', '[{"count":2}]', 28.13, 2),
        ("valid count=1 + increment", '[{"count":1}]', 29.13, 0),
        ("count=0 silent", '[{"count":0}]', 28.13, 2),
        ("count=1 without increment", '[{"count":1}]', 28.13, 2),
    ]
    harness_ok = True
    print()
    print("== stub-transport harness ==")
    for name, body, after_amount, expected_rc in harness_cases:
        print("=" * 72)
        print(f"CASE {name!r} body={body!r}")
        rc = run(1, 1, _stub_post(body, after_amount))
        print(f"exit={rc} expected={expected_rc}")
        if rc != expected_rc:
            print(f"FAIL: {name} exited {rc}, expected {expected_rc}")
            harness_ok = False
        print()

    print("SUMMARY")
    print(f"  parser: {'ok' if parser_ok else 'FAIL'}")
    print(f"  harness: {'ok' if harness_ok else 'FAIL'}")
    return 0 if parser_ok and harness_ok else 2


def replay_conflict() -> int:
    """Stub-transport replay of HTTP 409 vs unexpected failures."""
    sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    print(f"sha={sha}")
    print("command: 30_cayenne_concurrent_update_row_loss.py --replay-conflict")

    cases: list[tuple[str, int, str, float, int]] = [
        ("409 conflict, row kept", 409, '{"error":"write conflict"}', 28.13, 0),
        ("500 unexpected", 500, "internal error", 28.13, 2),
        ("403 unexpected", 403, "forbidden", 28.13, 2),
        ("200 count=0 silent", 200, '[{"count":0}]', 28.13, 2),
        ("valid count=1 + increment", 200, '[{"count":1}]', 29.13, 0),
    ]
    ok = True
    print()
    print("== stub-transport conflict harness ==")
    for name, status, body, after_amount, expected_rc in cases:
        print("=" * 72)
        print(f"CASE {name!r} status={status} body={body!r}")
        rc = run(1, 1, _stub_post(body, after_amount, update_status=status))
        print(f"exit={rc} expected={expected_rc}")
        if rc != expected_rc:
            print(f"FAIL: {name} exited {rc}, expected {expected_rc}")
            ok = False
        print()

    print("SUMMARY")
    print(f"  conflict harness: {'ok' if ok else 'FAIL'}")
    return 0 if ok else 2


def main(argv: list[str]) -> int:
    if argv == ["--replay-malformed"]:
        return replay_malformed()
    if argv == ["--replay-conflict"]:
        return replay_conflict()
    if len(argv) != 2:
        print(
            "usage: 30_cayenne_concurrent_update_row_loss.py THREADS UPDATES\n"
            "       30_cayenne_concurrent_update_row_loss.py --replay-malformed\n"
            "       30_cayenne_concurrent_update_row_loss.py --replay-conflict",
            file=sys.stderr,
        )
        return 2
    return run(int(argv[0]), int(argv[1]), http_post)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
