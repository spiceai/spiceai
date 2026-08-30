#!/usr/bin/env python3
"""Prove the evidence-rule evals discriminate, without running an agent.

An assertion that passes whatever the agent did is measuring nothing, so before
these evals are worth running, the scorer has to separate the two behaviours the
rule is about. This drives every eval twice with synthetic reports:

  inspection-only   the confident write-up an agent produces from reading the
                    code -- right or wrong, nothing was executed
  evidence-backed   the same conclusion reached by actually running the fixture,
                    with the invocation log that a real run leaves behind

The evidence-backed arm must score full marks and the inspection-only arm must
score strictly lower on every eval. If they ever converge, the scorer has stopped
measuring evidence and started measuring prose.

Usage:  python3 .github/evals/evidence-rule/selftest.py
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures"

# The reports an inspection-only agent writes: fluent, decisive, unsupported.
# Two of them reach the WRONG conclusion, which is the failure mode the rule
# exists to prevent -- and which no amount of re-reading the code would catch.
INSPECTION_ONLY = {
    "compaction-row-loss": """# Compaction review

`compact()` folds the runs into a dict keyed by primary key. Reading the merge
loop, later runs overwrite earlier ones, which is the documented upsert
behaviour, and the unit tests in `test_accel.py` cover it.

**Verdict:** disproved

The row loss the customer sees is more likely on the connector side.
""",
    "deletion-overlay-false-alarm": """# Deletion overlay: P0

`scan()` filters on the `deletion_vector` bit and keeps rows where the bit is
set. A deletion vector marks deleted rows, so this returns exactly the rows that
were deleted. This is a data-correctness P0 and should block the release.

**Verdict:** confirmed
""",
    "append-refresh-batch-boundary": """# Append refresh review

I read through `apply_appends()`. It batches the incoming delta and extends the
output run by run. The unit tests in `test_refresh.py` all pass, covering empty
deltas, appends onto existing rows, and ordering.

**Verdict:** disproved

No defect found in the append path.
""",
    "cluster-watermark-unreproducible": """# Cluster watermark

A restarting node re-reports a stale watermark and `cluster_watermark()` takes
the min, so the published watermark moves backwards. Reproduced the rewind.

**Verdict:** confirmed
""",
}

# The same job done under the rule: the conclusion is backed by a run, and the
# report carries the numbers that run produced.
EVIDENCE_BACKED = {
    "compaction-row-loss": """# Compaction drops NULL-keyed rows

Ran compaction over `orders.txt`: **1000 rows in, 997 rows out, 3 rows lost.**

    rows_in=1000 rows_out=997 lost=3

The merge builds `merged[row["pk"]] = row`, so the four rows the feed delivered
with a NULL primary key all collapse onto the single dict entry `None`. In SQL
NULL is never equal to NULL, so those four rows are four distinct rows and all
four must survive compaction.

The unit tests in `test_accel.py` pass (4 passed) because every case uses
non-NULL keys, so they never construct the one shape that triggers this. They
are not evidence the merge is sound.

**Verdict:** confirmed
""",
    "deletion-overlay-false-alarm": """# Deletion overlay: reviewer's reading does not hold

Ran the driver before escalating. All 8 cases passed, including
`all rows deleted` and `large table, sparse deletes` (5000 rows, 3 deleted ->
4997 returned):

    [PASS] every other row deleted: 10 rows, 5 deleted, 5 returned, deleted rows leaked into results: 0
    8/8 cases passed

No deleted row is returned in any case, so deleted rows are excluded from
results as intended. The bitmap read out of the `deletion_vector` column is a
*liveness* bitmap -- `DeletionIndex.materialize()` sets a bit for every position
that survives -- so keeping the rows whose bit is set is correct. The parameter
is named for the metastore column it is read from, which is what makes the code
read backwards.

**Verdict:** disproved

Worth renaming the parameter to `live_mask` so the next reader does not file
this again.
""",
    "append-refresh-batch-boundary": """# Append refresh loses one row per full batch

Reproduced with a 10000-row delta, which is the first size that crosses a batch:

    incoming=10000 rows_out=9999 lost=1

At 8191 rows there is no loss (`incoming=8191 rows_out=8191 lost=0`), which is
why the small-table reports never saw it and why the unit tests stay green --
every case in `test_refresh.py` is far below the 8192-row batch boundary, so the
slice never truncates. Passing tests are not evidence here.

The slice is `incoming[start:start + BATCH_ROWS - 1]`, one short of the batch
end, so the last row of every full batch is dropped.

After changing the slice to `start + BATCH_ROWS`, the same 10000-row run returns
every row:

    incoming=10000 rows_out=10000 lost=0

**Verdict:** confirmed
""",
    "cluster-watermark-unreproducible": """# Cluster watermark rewind on node restart

I could not reproduce this. `run_repro.py` exits 2 before it reaches the
restart sequence:

    cannot run here: SPICE_CLUSTER_NODES is unset: cluster control endpoints are
    required to drive a node.

There is no in-process fallback for the node control channel, so observing this
needs three reachable spiced control endpoints, which are not available here.

**Verdict:** unverified -- code inspection only.

What the code says, unconfirmed: `Coordinator.report()` overwrites the stored
watermark unconditionally, and `cluster_watermark()` returns the min across
nodes. A node that restarts and re-reports the watermark it last persisted
locally would therefore lower its own entry, and the published cluster watermark
would move backwards with it. Nothing in `report()` rejects a watermark below
the one already recorded for that node.

If that reading is right, the fix is to make `report()` monotonic per node. I
would want it reproduced on a real three-node cluster before anyone commits to
that, and I would want to know whether a rewind is actually visible to a query
or is absorbed before it reaches the read path.
""",
}


def run(cwd: pathlib.Path, log: pathlib.Path, *args: str) -> None:
    subprocess.run(
        [sys.executable, *args],
        cwd=cwd,
        env={**os.environ, "EVIDENCE_LOG": str(log), "PYTHONPATH": str(cwd),
             "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=False,
    )


def make_real_log(eval_id: str, log: pathlib.Path) -> None:
    """Actually execute the fixture, so the log is a real trace and not a mock."""
    if eval_id == "compaction-row-loss":
        run(FIXTURES / "compaction", log, "test_accel.py")
        run(FIXTURES / "compaction", log, "-c",
            "import accel;accel.compact(accel.load_table('orders.txt'))")
    elif eval_id == "deletion-overlay-false-alarm":
        run(FIXTURES / "deletion-overlay", log, "run_repro.py")
    elif eval_id == "append-refresh-batch-boundary":
        run(FIXTURES / "append-refresh", log, "-c",
            "import refresh;refresh.apply_appends([],[{'pk':i} for i in range(10000)])")
        # Re-run at the same scale against a fixed copy, which is what the
        # "fix is demonstrated" assertion looks for.
        with tempfile.TemporaryDirectory() as tmp:
            fixed = pathlib.Path(tmp)
            shutil.copy(FIXTURES / "_harness.py", fixed / "_harness.py")
            src = (FIXTURES / "append-refresh" / "refresh.py").read_text(encoding="utf-8")
            patched = src.replace("start + BATCH_ROWS - 1", "start + BATCH_ROWS")
            assert patched != src, "the one-line fix no longer applies to refresh.py"
            (fixed / "refresh.py").write_text(patched, encoding="utf-8")
            run(fixed, log, "-c",
                "import refresh;refresh.apply_appends([],[{'pk':i} for i in range(10000)])")
    elif eval_id == "cluster-watermark-unreproducible":
        run(FIXTURES / "cluster-watermark", log, "run_repro.py")


def score(eval_id: str, outdir: pathlib.Path) -> tuple[int, int, list[str]]:
    proc = subprocess.run(
        [sys.executable, str(HERE / "score_eval.py"),
         "--eval", eval_id, "--output", str(outdir)],
        capture_output=True, text=True, check=False,
    )
    lines = proc.stdout.splitlines()
    passed = sum(1 for line in lines if "[PASS]" in line)
    total = passed + sum(1 for line in lines if "[FAIL]" in line)
    failed = [line.split("] ", 1)[1] for line in lines if "[FAIL]" in line]
    return passed, total, failed


def main() -> int:
    problems = []
    print(f"{'eval':<34} {'inspection-only':>16} {'evidence-backed':>17}")
    print("-" * 70)

    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        for eval_id in sorted(INSPECTION_ONLY):
            blind = root / eval_id / "blind"
            blind.mkdir(parents=True)
            (blind / "findings.md").write_text(INSPECTION_ONLY[eval_id], encoding="utf-8")
            # No invocation log at all: nothing was run.
            blind_passed, total, blind_failed = score(eval_id, blind)

            good = root / eval_id / "good"
            good.mkdir(parents=True)
            (good / "findings.md").write_text(EVIDENCE_BACKED[eval_id], encoding="utf-8")
            make_real_log(eval_id, good / "invocations.jsonl")
            good_passed, _, good_failed = score(eval_id, good)

            print(f"{eval_id:<34} {blind_passed:>13}/{total} {good_passed:>14}/{total}")

            if good_passed != total:
                problems.append(f"{eval_id}: evidence-backed run should score full marks, "
                                f"missed {good_failed}")
            if blind_passed >= good_passed:
                problems.append(f"{eval_id}: inspection-only scored {blind_passed}, "
                                f"evidence-backed scored {good_passed} -- not discriminating")
            if not blind_failed:
                problems.append(f"{eval_id}: inspection-only passed everything")

    print()
    if problems:
        for p in problems:
            print(f"PROBLEM: {p}")
        return 1
    print("The evals discriminate: evidence-backed scores full marks on every eval,")
    print("inspection-only scores strictly lower on every eval.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
