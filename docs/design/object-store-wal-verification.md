# Transactional WAL verification

Local branch: `feature/object-store-state-contract`, based on `a79624f85e`.
Scope: `object_store_occ`'s additive per-key primitive and transactional WAL/MVCC
library. These runs do not exercise a Cayenne runtime integration.

## Environment and coverage

All Cargo commands use the dev profile, no package features, and this environment:

```sh
env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ CARGO_NET_OFFLINE=true
```

Independent design/source review checked publication, checkpoint races, receipt
resolution, corruption handling and documented resource bounds. Its two design
refinements are reflected in the code: staging requests have distinct identities
from head publication, and retention documentation includes superseded checkpoints.
The independent reviewer did not execute Cargo.

## Local suites

Command:

```sh
cargo test --profile dev -p object_store_occ --lib --test state_store --test transactional_wal
```

Actual output summaries, respectively library, primitive integration and WAL
integration suites:

```text
test result: ok. 27 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 12 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 19 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.75s
```

The two ignored entries are the credential-dependent S3 conformance test and the
subprocess helper. The WAL parent test explicitly invokes that helper twice.
Local runs exercise real filesystem I/O with independently constructed clients,
memory-backed concurrent histories, fault wrappers around actual storage writes,
multi-page checkpoint recovery, boundary values, corruption and deterministic
model comparisons. Raw session output: `/tmp/spice-wal-tests-local.log`.

## Process restart

Command:

```sh
cargo test --profile dev -p object_store_occ --test transactional_wal process_restart_at_wal_publication_boundaries -- --exact --nocapture
```

The child exits with code 86 after WAL staging, either before head publication
(boundary 7) or immediately after publication (boundary 8). Recovery uses a new
local client and the receipt saved before dispatch. Actual output:

```text
process exit boundary=7: sequence=0, outcome=Pending
process exit boundary=8: sequence=1, outcome=Committed { sequence: 1 }
test process_restart_at_wal_publication_boundaries ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 19 filtered out; finished in 0.01s
```

An unchanged base remains `Pending`, not falsely rejected, because the protocol
does not infer a writer's death from a read. This is process-exit evidence, not
machine-power-loss qualification. Raw output: `/tmp/spice-wal-process.log`.

## OCC test sensitivity

A temporary mutation made `commit` use a freshly read head revision instead of
the transaction's original snapshot revision. This deliberately permits a stale
transaction to overwrite newer work. It was removed before final tests and lint.
The same command ran with the mutation and after restoring the implementation:

```sh
cargo test --profile dev -p object_store_occ --test transactional_wal predicate_and_absent_reads_conflict_on_any_intervening_commit -- --exact
```

With mutation:

```text
test predicate_and_absent_reads_conflict_on_any_intervening_commit ... FAILED
assertion failed: matches!(first.commit(&attempt).await.expect("conflict"),
    CommitOutcome::Conflict)
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 19 filtered out; finished in 0.00s
```

Restored implementation:

```text
test predicate_and_absent_reads_conflict_on_any_intervening_commit ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 19 filtered out; finished in 0.00s
```

Raw outputs: `/tmp/spice-wal-mutation.log` and
`/tmp/spice-wal-mutation-restored.log`. This checks test sensitivity for a new
feature; it is not a claim of an existing runtime defect.

## Lint and unavailable checks

Command:

```sh
make lint-rust PACKAGES=object_store_occ FEATURES= RUST_PROFILE=dev
```

Exit code: 0. Repository guards, formatting, production Clippy and test Clippy ran.
Final Clippy output lines:

```text
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.79s
Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.05s
```

The required auto-fix command was attempted first:

```sh
make lint-rust-fix PACKAGES=object_store_occ FEATURES= RUST_PROFILE=dev
```

Its Cargo fix lock listener is unavailable in this sandbox:

```text
error: failed to bind TCP listener to manage locking
Caused by:
  Operation not permitted (os error 1)
```

Corrections were applied manually before the successful non-mutating lint gate.
Raw output: `/tmp/spice-wal-lint.log` and `/tmp/spice-wal-lint-fix.log`.

An initial `cargo test --profile dev -p object_store_occ --lib --tests` reached
the existing S3 integration suite, which cannot initialize without credentials:

```text
AWS_S3_BUCKET environment variable must be set to run integration tests: NotPresent
test result: FAILED. 0 passed; 10 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

The explicit local-suite command above avoids invoking those cloud tests. No
cloud qualification, local power-loss qualification, engine-level
transaction test, performance benchmark, or full-workspace sign-off is claimed.
Production retention/GC, restore and ownership-transfer protocols remain outside
this internal library; see [its contract](../../crates/object_store_occ/WAL.md).


## Process-level hardening

`test/object_store_occ/e2e.py` supervises independent real filesystem clients.
The local run (SlateDB not installed locally) used:

```sh
cargo build --profile dev -p object_store_occ --example wal_test_driver
python3 test/object_store_occ/e2e.py --driver target/debug/examples/wal_test_driver --artifacts /tmp/spice-wal-e2e-local
```

Actual output:

```text
SlateDB differential testing NOT RUN (pass --slatedb)
history seed=0: 96 transactions; reads, overlays, snapshots, checkpoints, reopen agree
history seed=1: 96 transactions; reads, overlays, snapshots, checkpoints, reopen agree
history seed=17: 96 transactions; reads, overlays, snapshots, checkpoints, reopen agree
history seed=5489: 96 transactions; reads, overlays, snapshots, checkpoints, reopen agree
concurrency: 80 acknowledged transfers, 99 reader snapshots, 35 competing checkpoints
SIGKILL commit boundary=1: rows=[], sequence=0
SIGKILL commit boundary=2: rows=[], sequence=0
SIGKILL commit boundary=3: rows=[['a', [1]], ['b', [2]]], sequence=1
SIGKILL checkpoint boundary=1: recovered all 3 pages
SIGKILL checkpoint boundary=3: recovered all 3 pages
SIGKILL checkpoint boundary=4: recovered all 3 pages
SIGKILL checkpoint boundary=5: recovered all 3 pages
PASS: WAL process-level qualification
```

The SIGKILL barriers are between storage calls. The filesystem cancellation test
separately observes an active, growing staging file before aborting the caller.
It reproduced a lost update on the PR's initial local adapter: the contender
returned `Ok(PutResult { ... })`, then the cancelled upload overwrote its
acknowledged 12-byte value with 262,144 bytes. The dedicated filesystem worker
keeps lock acquisition, comparison and publication under one owner. The same
experiment now returns `Err(Precondition { ... })` for the stale contender.

`local_cancellation` also covers runtime shutdown with one blocking thread and
dropping the last store handle with accepted active and queued writes. Checkpoint
fault cases cover missing/corrupt pages and manifests, and structurally inconsistent
sequence, incarnation, ancestor, duplicate/reversed pages and totals even when
content hashes are recomputed by the fault injector.

The dedicated `Object Store WAL E2E` workflow installs the pinned SlateDB oracle
and runs the full comparison on Linux and macOS. See that check on PR #14732 for
its captured output and downloadable histories; the local model run above is not
a substitute for an oracle run. PyPI was unreachable from the local sandbox.


Cancellation reproduction command, with the same test source on `b80ab208` and
the worker implementation (an isolated target directory avoids sharing artifacts
between checkouts):

```sh
env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ CARGO_NET_OFFLINE=true CARGO_TARGET_DIR=/tmp/spice-wal-repro-target cargo test --profile dev -p object_store_occ --test local_cancellation cancelled_local_write_cannot_overwrite_an_acknowledged_contender -- --exact --nocapture
```

Before:

```text
contender after cancellation: Ok(PutResult { e_tag: Some("2027f572-65cf3b8737494-c"), version: None })
final length=262144, first byte=Some(120)
cancelled write overwrote acknowledged state: length=262144
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out
```

After:

```text
contender after cancellation: Err(Precondition { ... })
final length=262144, first byte=Some(120)
test cancelled_local_write_cannot_overwrite_an_acknowledged_contender ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out
```

A baseline rerun sharing a target directory with the other checkout passed; the
clean-target run above reproduced the overwrite. This timing-based experiment
requires observing a partially written staging file and fails if it cannot engage
that boundary. It demonstrates the observed lost update, not a failure on every
possible schedule. Full command output is in `/tmp/spice-wal-cancellation-base-clean.log`
and `/tmp/spice-wal-cancellation-fixed-clean.log`.
