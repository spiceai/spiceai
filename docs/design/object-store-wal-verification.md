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
cloud qualification, local power-loss/cancellation qualification, engine-level
transaction test, performance benchmark, or full-workspace sign-off is claimed.
Production retention/GC, restore and ownership-transfer protocols remain outside
this internal library; see [its contract](../../crates/object_store_occ/WAL.md).
