#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0
"""
CH-benCH direct-write-to-Spice driver — validates the per-key OCC + fused-txn IVM
fixes end-to-end against a running spiced + the `chbench-writeback-local` spicepod.

Unlike the standard chbench harness (OLTP drives Postgres, Spice reads via CDC),
this driver issues gated TPC-C-style transactions DIRECTLY to Spice over
HTTP /v1/sql, then self-checks three correctness invariants that the fixes are
about. It needs no third-party packages (urllib + subprocess only).

What it exercises and asserts
-----------------------------
1. Per-key OCC admission + no lost updates (P0-2 fix):
   N concurrent workers each run gated stock-decrement transactions over a
   DISJOINT key range:
       BEGIN;
       SELECT assert((SELECT s_quantity FROM stock WHERE s_w_id=? AND s_i_id=?) >= :d);
       UPDATE stock SET s_quantity = s_quantity - :d WHERE s_w_id=? AND s_i_id=?;
       COMMIT;
   Disjoint keys MUST all commit (per-key OCC admits disjoint writers); a small
   overlapping set MUST surface WriteConflict (HTTP 409) and is retried. The
   oracle: the observed drop in SUM(s_quantity) equals the sum of committed
   deltas the driver tracked — no committed decrement is lost or double-applied.

2. Fused-txn IVM staleness (P1 fix):
   After each round of committed transactions, the maintained aggregate
   (SUM(s_quantity) GROUP BY s_w_id) MUST equal a base-scan SUM. Before the fix
   the fused commit path never marked maintained aggregates stale, so the
   maintained view served pre-transaction totals as Fresh.

3. Upsert filter-DELETE keyset degradation (P0-3 fix):
   A filter DELETE on the write-back (upsert) `oorder` table is interleaved with
   concurrent transactions; the run must remain conflict-correct afterward
   (a transaction reading a since-deleted key must not silently resurrect it).
   This is a liveness/consistency smoke check — the unit test
   `transaction_has_conflict_degraded_keyset_falls_back_to_per_table` is the
   deterministic proof; here we confirm the path is exercised under load without
   lost updates or panics.

Exit code 0 = all invariants held. Non-zero = a violation (prints details).
"""

import argparse
import concurrent.futures
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

# ------------------------------- HTTP to Spice --------------------------------


def spice_sql(base_url, sql, timeout=30):
    """POST a SQL body (single statement OR a BEGIN…COMMIT block) to /v1/sql.

    Returns (status_code, parsed_json_or_text). Never raises on HTTP error
    status — a 409 WriteConflict is an expected, retryable outcome here.
    """
    request = urllib.request.Request(
        f"{base_url}/v1/sql",
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, _parse(body)
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8")
        return error.code, _parse(body)


def _parse(body):
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return body


def spice_scalar(base_url, sql):
    """Run a single-row single-column query and return the scalar value.

    Uses explicit raises (not `assert`) so the validation is NEVER compiled out
    under `python -O` — a validation harness that silently skips its own checks
    could report a false PASS.
    """
    status, payload = spice_sql(base_url, sql)
    if status != 200:
        raise RuntimeError(f"query failed ({status}): {sql}\n{payload}")
    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"no rows for: {sql}\n{payload}")
    row = payload[0]
    if not isinstance(row, dict) or len(row) != 1:
        raise RuntimeError(f"want exactly 1 column: {sql}\n{row}")
    return next(iter(row.values()))


# ------------------------------ Postgres oracle -------------------------------


def pg_scalar(pg_dsn, sql):
    """Run a scalar query against Postgres via `psql` (no psycopg2 dependency)."""
    result = subprocess.run(
        ["psql", pg_dsn, "-tAc", sql],
        capture_output=True,
        text=True,
        check=True,
    )
    out = result.stdout.strip()
    return int(out) if out.lstrip("-").isdigit() else out


# ------------------------------ readiness poll --------------------------------


def wait_ready(base_url, pg_dsn, table, timeout_s):
    """Poll until Spice serves the write-back table and its row count matches PG
    (CDC bootstrap complete). Bounded; raises on timeout."""
    deadline = time.monotonic() + timeout_s
    pg_rows = pg_scalar(pg_dsn, f"SELECT COUNT(*) FROM {table}")
    last = None
    while time.monotonic() < deadline:
        try:
            spice_rows = spice_scalar(base_url, f"SELECT COUNT(*) AS c FROM {table}")
            last = spice_rows
            if int(spice_rows) == int(pg_rows):
                print(f"  ready: {table} = {spice_rows} rows (Spice == Postgres)")
                return
        except (RuntimeError, urllib.error.URLError, ValueError):
            # Expected while the table is still bootstrapping: the query 5xx's,
            # the connection is refused, or the row count is not yet an int.
            # Swallow and retry until the bounded deadline below.
            pass
        time.sleep(1.0)
    raise TimeoutError(
        f"{table} not ready within {timeout_s}s: Spice={last} Postgres={pg_rows}"
    )


# ------------------------------ the workload ---------------------------------


def gated_decrement(base_url, w_id, i_id, delta):
    """One gated stock-decrement transaction. Returns 'ok' | 'conflict' | 'gate'."""
    sql = (
        "BEGIN;\n"
        f"SELECT assert((SELECT s_quantity FROM stock "
        f"WHERE s_w_id={w_id} AND s_i_id={i_id}) >= {delta});\n"
        f"UPDATE stock SET s_quantity = s_quantity - {delta} "
        f"WHERE s_w_id={w_id} AND s_i_id={i_id};\n"
        "COMMIT;"
    )
    status, payload = spice_sql(base_url, sql)
    if status == 200:
        return "ok"
    text = json.dumps(payload) if isinstance(payload, (dict, list)) else str(payload)
    if status == 409 or "conflict" in text.lower():
        return "conflict"
    if "assert" in text.lower():
        return "gate"
    raise RuntimeError(f"unexpected txn result ({status}): {text}")


def worker(base_url, w_id, key_lo, key_hi, delta, rounds, max_retries):
    """Run `rounds` gated decrements over this worker's disjoint key range.
    Returns (committed_count, applied_delta_sum, conflicts, gate_aborts)."""
    committed = 0
    applied = 0
    conflicts = 0
    gate_aborts = 0
    exhausted = 0
    key_span = key_hi - key_lo + 1
    for r in range(rounds):
        i_id = key_lo + (r % key_span)
        for _ in range(max_retries):
            outcome = gated_decrement(base_url, w_id, i_id, delta)
            if outcome == "ok":
                committed += 1
                applied += delta
                break
            if outcome == "conflict":
                conflicts += 1
                continue  # retry
            if outcome == "gate":
                gate_aborts += 1
                break  # gate failed (stock exhausted) — expected terminal
        else:
            # Every retry hit WriteConflict without committing (no `break`). This
            # is a no-progress condition — surface it explicitly rather than
            # silently under-driving the round, since it can mask a stuck-degraded
            # / per-table-OCC-starvation scenario the harness is meant to catch.
            exhausted += 1
    return committed, applied, conflicts, gate_aborts, exhausted


# ------------------------------- oracles -------------------------------------


def check_no_lost_updates(base_url, w_id, before_sum, applied_delta):
    after_sum = int(spice_scalar(
        base_url, f"SELECT SUM(s_quantity) AS s FROM stock WHERE s_w_id={w_id}"
    ))
    expected = before_sum - applied_delta
    ok = after_sum == expected
    print(
        f"  [no-lost-updates] before={before_sum} applied=-{applied_delta} "
        f"expected={expected} actual={after_sum} -> {'PASS' if ok else 'FAIL'}"
    )
    return ok


def check_ivm_fresh(base_url, w_id):
    """Maintained aggregate (served from the IVM registry) must equal a base scan
    that bypasses it. The maintained query uses the exact `GROUP BY s_w_id` shape
    the pod declares (served from the IVM registry); the base query is a scalar
    `SUM` with NO `GROUP BY`, whose empty group-by set can never match the view's
    `group_by: [s_w_id]` (`MaintainedAggregateView::matches_query` requires an
    exact group-by + aggregate match), so it is forced to a base scan. If the
    registry is stale, the two diverge."""
    maintained = int(spice_scalar(
        base_url,
        f"SELECT SUM(s_quantity) AS s FROM stock WHERE s_w_id={w_id} GROUP BY s_w_id",
    ))
    base = int(spice_scalar(
        base_url,
        # Scalar SUM, no GROUP BY: group_by set is [] != the view's [s_w_id], so
        # this cannot be served from the maintained aggregate — a true base scan.
        f"SELECT SUM(s_quantity) AS s FROM stock WHERE s_w_id={w_id}",
    ))
    ok = maintained == base
    print(
        f"  [ivm-fresh] maintained={maintained} base-scan={base} "
        f"-> {'PASS' if ok else 'FAIL'}"
    )
    return ok


def check_writeback_converges(base_url, pg_dsn, w_id, timeout_s):
    """After write-back drains, Spice SUM(s_quantity) must reach Postgres's.
    NOTE: a divergence here that does not self-heal may be the *separate*,
    deferred P0 write-back echo-loss bug (tracked in its own issue), not an OCC
    regression — the driver flags it distinctly."""
    deadline = time.monotonic() + timeout_s
    spice_sum = int(spice_scalar(
        base_url, f"SELECT SUM(s_quantity) AS s FROM stock WHERE s_w_id={w_id}"
    ))
    last_pg = None
    while time.monotonic() < deadline:
        last_pg = pg_scalar(pg_dsn, f"SELECT SUM(s_quantity) FROM stock WHERE s_w_id={w_id}")
        if int(last_pg) == spice_sum:
            print(f"  [write-back] Postgres SUM converged to Spice ({spice_sum}) -> PASS")
            return True
        time.sleep(1.0)
    print(
        f"  [write-back] Postgres SUM={last_pg} != Spice SUM={spice_sum} after "
        f"{timeout_s}s -> divergence (may be the deferred write-back echo-loss P0; "
        f"not an OCC/IVM regression)"
    )
    return False


# --------------------------------- main --------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spice-url", default="http://localhost:8090")
    parser.add_argument(
        "--pg-dsn",
        default="postgresql://bench:bench@localhost:5432/chbench",
        help="Postgres DSN for the source oracle (psql format)",
    )
    parser.add_argument("--w-id", type=int, default=1, help="warehouse to target")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rounds", type=int, default=50, help="txns per worker")
    parser.add_argument("--delta", type=int, default=1, help="decrement per txn")
    parser.add_argument("--keys-per-worker", type=int, default=8)
    parser.add_argument("--overlap-keys", type=int, default=4,
                        help="shared contended keys all workers also hit (OCC conflict path)")
    parser.add_argument("--max-retries", type=int, default=50)
    parser.add_argument("--ready-timeout", type=int, default=180)
    parser.add_argument("--writeback-timeout", type=int, default=120)
    parser.add_argument("--skip-writeback-check", action="store_true")
    args = parser.parse_args()

    print(f"== CH-benCH direct-write validation (w_id={args.w_id}, "
          f"{args.workers} workers x {args.rounds} rounds) ==")

    print("[1/5] waiting for Spice write-back tables to bootstrap from Postgres...")
    wait_ready(args.spice_url, args.pg_dsn, "stock", args.ready_timeout)

    before_sum = int(spice_scalar(
        args.spice_url,
        f"SELECT SUM(s_quantity) AS s FROM stock WHERE s_w_id={args.w_id}",
    ))
    print(f"[2/5] baseline SUM(s_quantity) w_id={args.w_id}: {before_sum}")

    # Disjoint key ranges per worker + an optional shared overlap band that
    # forces conflicts (gated on --overlap-keys > 0 so it can be disabled).
    print(f"[3/5] driving {args.workers} concurrent gated transaction workers...")
    total_committed = total_applied = total_conflicts = total_gate = total_exhausted = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = []
        for wkr in range(args.workers):
            key_lo = 1 + args.overlap_keys + wkr * args.keys_per_worker
            key_hi = key_lo + args.keys_per_worker - 1
            futures.append(pool.submit(
                worker, args.spice_url, args.w_id, key_lo, key_hi,
                args.delta, args.rounds, args.max_retries,
            ))
        # A couple of workers also hammer the shared overlap band (conflict path),
        # unless the contended band is disabled with --overlap-keys 0.
        if args.overlap_keys > 0:
            for wkr in range(min(2, args.workers)):
                futures.append(pool.submit(
                    worker, args.spice_url, args.w_id, 1, args.overlap_keys,
                    args.delta, args.rounds, args.max_retries,
                ))
        for f in concurrent.futures.as_completed(futures):
            committed, applied, conflicts, gate, exhausted = f.result()
            total_committed += committed
            total_applied += applied
            total_conflicts += conflicts
            total_gate += gate
            total_exhausted += exhausted
    print(f"  committed={total_committed} applied_delta=-{total_applied} "
          f"conflicts(retried)={total_conflicts} gate_aborts={total_gate} "
          f"retry_exhausted={total_exhausted}")
    if total_exhausted > 0:
        print(f"  WARNING: {total_exhausted} round(s) exhausted {args.max_retries} "
              f"retries without committing — possible OCC starvation / stuck-degraded")

    # NOTE: this spicepod's write-back datasets (district, stock, oorder) are keyed
    # on composite primary keys, which durable write-back cannot deliver -- it keys
    # each delivery on a single column -- so the runtime now refuses them at
    # registration and this fixture does not load as written. It never delivered
    # them either: the worker logged the composite key and exited, so the markers
    # only accumulated. Running this benchmark again needs single-column-key tables
    # or composite-key delivery support; the DELETE-refusal step that lived here is
    # unreachable until then and has been removed rather than left asserting a path
    # it cannot take.
    print("[4/5] (skipped: fixture needs single-column-key write-back datasets)")

    print("[5/5] checking invariants...")
    ok_lost = check_no_lost_updates(args.spice_url, args.w_id, before_sum, total_applied)
    ok_ivm = check_ivm_fresh(args.spice_url, args.w_id)
    ok_progress = total_exhausted == 0
    print(f"  [no-progress] retry_exhausted={total_exhausted} "
          f"-> {'PASS' if ok_progress else 'FAIL'}")
    ok_wb = True
    if not args.skip_writeback_check:
        ok_wb = check_writeback_converges(
            args.spice_url, args.pg_dsn, args.w_id, args.writeback_timeout
        )

    print("\n== RESULT ==")
    print(f"  no-lost-updates (OCC): {'PASS' if ok_lost else 'FAIL'}")
    print(f"  ivm-fresh (P1):        {'PASS' if ok_ivm else 'FAIL'}")
    print(f"  no-progress (OCC):     {'PASS' if ok_progress else 'FAIL'}")
    print(f"  write-back converge:   "
          f"{'PASS' if ok_wb else 'DIVERGED (see deferred P0 note above)'}")

    # OCC and IVM are what THIS PR fixes — they gate the exit code, including a
    # no-progress guard (any round that exhausted its retries without committing
    # signals OCC starvation / stuck-degraded and fails the run). Write-back
    # convergence is reported but does NOT fail the run (the write-back echo-loss
    # P0 is deferred to its own PR).
    critical_ok = ok_lost and ok_ivm and ok_progress
    sys.exit(0 if critical_ok else 1)


if __name__ == "__main__":
    main()
