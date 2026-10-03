# Transactional object-store state

`wal::WalStateStore` adds atomic K/V transactions and multi-version snapshots on
top of `store::StateStore`. It is an internal library; the runtime does not select
it automatically, and existing `ObjectState<T>` JSON formats are unchanged.

Use an exclusive namespace and a backend providing fresh reads and atomic
conditional writes. The library cannot create durability or consistency that the
backend does not supply. In-memory and local integration runs do not qualify cloud
providers or local power loss. The local adapter keeps conditional publication
on a dedicated filesystem worker with a bounded queue, retaining its advisory
lock through caller cancellation and Tokio runtime shutdown. Accepted writes
drain after the last handle is dropped; callers still resolve cancelled writes.

## API and commit contract

| Operation | Contract |
| --- | --- |
| `WalStateStore::open(state, prefix, limits)` | Conditionally initialize or open one domain; every client must use its persisted limits |
| `snapshot()` | Recover the head-selected version into an owned, immutable view |
| `begin()` / `snapshot.transaction()` | Read one snapshot plus buffered writes; whole-head validation at commit |
| `Transaction::get` / `scan_prefix` | Read your writes, including deletes and empty values |
| `put` / `delete` | Buffer a bounded mutation batch; no storage writes yet |
| `prepare()` | Freeze the batch, base revision and unique attempt; return an immutable `PreparedCommit` |
| `PreparedCommit::receipt()` | Serializable identity to persist in application recovery state **before** submission |
| `commit(&attempt)` | Stage the immutable WAL, then conditionally publish its head; no automatic rebase |
| `resolve(&receipt)` | Recover an attempt's outcome, including from an independently reopened client |
| `checkpoint(&snapshot)` | Upload immutable pages and manifest; conditionally replace only that snapshot's head |

The head update is the only transaction commit point. A WAL upload alone does not
change reads or justify acknowledgement. Each record binds its complete mutation
batch, incarnation, parent, sequence and attempt identity with a BLAKE3 digest.
Checkpoint manifests bind page contents through digests too. Recovery follows
head references, never object listings or whichever object happens to be newest.

Transactions use optimistic concurrency control (OCC) over the entire head. Any
intervening publication conflicts, including a checkpoint with unchanged logical
sequence. This includes read dependencies on absent keys and predicates; the
library never silently rebases mutations computed from an old snapshot. A conflict
requires evaluating a new transaction and obtaining a new attempt identity.

MVCC snapshots own their materialized values. A snapshot remains stable across
later overwrites, deletion/recreation, checkpointing and dropping the store handle.
Reads and writes across multiple keys are atomic within a domain. Independent
domains do not provide an atomic cross-domain snapshot or transaction.

## Retrying and resolving uncertain writes

Save a serialized `CommitReceipt` before dispatching a commit, including before
spawning a cancellable task. Receipt durability belongs to the caller; serializing
it alone does not persist it. Receipts are trusted recovery data, not authentication
tokens. They include a format version, namespace, incarnation, attempt hash and
the original physical head publication identity.

`Committed` means the backend acknowledged the head write or retained ancestry
proves it applied. `Conflict` means ancestry and the changed head prove this attempt
cannot publish. `Unknown` requires keeping the receipt and resolving it before
repeating the logical operation. A cancelled future or error cannot undo an earlier
submission of the same attempt. A lost response or SDK retry precondition is not
treated as proof of rejection.

`resolve` returns `Committed`, `Rejected`, or `Pending`. A receipt may remain
`Pending` while its base head is unchanged: a delayed conditional write could still
apply. A checkpoint also changes physical head identity and fences the old attempt,
even without advancing logical sequence. Unavailable or corrupt evidence returns
an error instead of guessing. Full WAL ancestry remains available after checkpoints
to distinguish an applied-then-overwritten attempt from one that never applied.

An exact live retry reuses the same `PreparedCommit` on the original handle or its
clone. Independent clients can resolve a saved receipt but cannot resubmit the old
handle's revision. Staging retries use fresh request identities and verify exact
immutable content before publishing; head transport retries preserve the frozen
attempt. No separate deduplication of application business-operation IDs is implied.

## Checkpoints and limits

Snapshots recover from a checkpoint plus its bounded WAL tail. A checkpoint is
split into bounded immutable pages with a manifest anchored to an exact logical
commit. Publication uses the snapshot's original head revision. If another commit
or checkpoint wins first, it cannot be overwritten by the stale checkpoint.
Checkpoints never reset sequence or delete WAL records.

Default limits, persisted in the domain head:

| Resource | Bound |
| --- | --- |
| Key | Nonempty, at most 1 KiB of UTF-8 bytes |
| Mutation batch / checkpoint page | 1,024 keys and 128 KiB of raw key/value bytes |
| Encoded record | 1 MiB, enforced during serialization and storage reads |
| Live domain state | 65,536 keys and 64 MiB of raw key/value bytes |
| Replay tail | 128 commits; checkpoint and acquire a new snapshot when full |
| Retained WAL history | 65,536 commits; exhaustion stops new commits |

`Limits` allows smaller domain bounds. A batch reaching a limit returns a typed
error before publication. Snapshot recovery rejects malformed, incompatible,
missing or hash-mismatched referenced data. CPU-heavy serialization, hashing and
materialization run on Rayon workers rather than Tokio executor threads.

There is **no garbage collection** in this implementation. Do not externally
delete or overwrite its objects, expire them with bucket lifecycle rules, restore
an old head, or reuse the namespace. These restrictions protect snapshots, delayed
writers and commit-resolution evidence. The WAL count limit does not bound orphan
uploads or superseded checkpoint manifests/pages. They remain retained, and total
storage can grow even when logical state does not. Sustainable retention,
ownership transfer, restore fencing and runtime integration require subsequent
protocol work; a checkpoint does not clear the history admission limit.

Snapshots currently materialize the complete bounded domain. The raw-byte limit
excludes map/serialization overhead and is per domain, not a process memory budget.
Applications must limit concurrent snapshots and operations. No throughput,
latency or massive-scale qualification is claimed for this initial library.

## Verification

Run the local public-API and filesystem/fault suites:

```sh
cargo test --profile dev -p object_store_occ --lib --test state_store --test transactional_wal --test local_cancellation
make lint-rust PACKAGES=object_store_occ FEATURES= RUST_PROFILE=dev
```

`transactional_wal` covers concurrent clients, multi-key commits, predicates,
stable snapshots, deterministic model histories, paged checkpoints, exact retries,
lost responses, cancellation, delayed writes, corruption and resource bounds.
Its subprocess test exits around the publication boundary and recovers from real
local files. This exercises process termination, not machine power loss.
The existing S3 suites require explicit credentials and bucket configuration.


The process-level harness starts independent writer, reader and checkpoint
processes through the public library API. It retains JSONL requests/responses,
receipts persisted before dispatch, stderr and backing stores. SIGKILL tests
cover boundaries between storage operations; separate filesystem tests cancel
an upload while its staging file is growing and shut down its Tokio runtime.
Neither test simulates power loss.

```sh
cargo build --locked --profile dev -p object_store_occ --example wal_test_driver
python3 -m venv /tmp/wal-oracle
/tmp/wal-oracle/bin/pip install --only-binary=:all: -r test/object_store_occ/requirements.txt
/tmp/wal-oracle/bin/python test/object_store_occ/e2e.py \
  --driver target/debug/examples/wal_test_driver \
  --artifacts /tmp/wal-e2e-run --slatedb
```

The artifact directory must be new for each run. `--slatedb` requires the pinned
SlateDB 0.17.0 Python package, an independent implementation outside Spice's
runtime dependencies. Equivalent committed histories compare point state, ordered
scans, transactional overlays, pinned snapshots and reopen recovery. The oracle
waits for `await_durable()` before treating a SlateDB commit as durable. It does
not compare identical abort decisions: SlateDB's finer-grained validation differs
from this library's whole-head OCC. Concurrent CAS and atomicity are checked by
separate multi-process histories.

For three independent implementations, start a local Redis service (or use an
existing one) and add `--redis-url redis://127.0.0.1:6379/0` to the harness command:

```sh
docker run --rm -d --name spice-wal-redis -p 127.0.0.1:6379:6379 redis:7.4.11-bookworm
/tmp/wal-oracle/bin/python test/object_store_occ/e2e.py \
  --driver target/debug/examples/wal_test_driver \
  --artifacts /tmp/wal-e2e-three-oracles --slatedb \
  --redis-url redis://127.0.0.1:6379/0
docker stop spice-wal-redis
```

Redis receives each batch's original mutation sequence through `MULTI`/`EXEC`.
Committed binary values, missing versus empty values, deletes, overwrites and
point reads are compared with WAL and SlateDB. Atomic `HGETALL` reads are sorted
and filtered for prefix comparisons. Three forced `WATCH` conflicts additionally
compare stale-batch rejection with WAL's whole-domain OCC for inserts, overwrites
and deletes. Checkpoints and no-op publications have no Redis counterpart.
Historical snapshots and uncommitted overlays remain WAL/SlateDB comparisons;
Redis client reconnect checks do not qualify Redis crash recovery or durability.

The Redis oracle uses pinned redis-py 6.4.0, creates a unique hash per history,
records version, requests and results in JSONL, and deletes only that hash on
exit. It never flushes the service or restarts an existing server. Passing
`--redis-url` requires Redis to be available; a missing oracle fails the run.

`.github/workflows/object_store_wal.yml` runs these suites and the mandatory
SlateDB oracle on Linux and macOS for changes to this crate and in the merge queue.
Linux also requires Redis 7.4.11 in a health-checked service container; GitHub
service containers are unavailable on macOS. Both oracles are test dependencies.
This qualifies the library path; it is not a Cayenne/runtime integration test.
