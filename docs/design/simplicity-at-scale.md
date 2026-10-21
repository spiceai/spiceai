# Simplicity at scale implementation plan

Status: direction approved, including object-free on-prem operation, Cayenne as
the sole target accelerator, and OCC/MVCC state with a conditional-write WAL.
Runtime surface, protocols, and migrations remain proposed.
Source baseline: a79624f85e, inspected 2026-10-03. This document is the local
Enhancement draft; it has not been filed on GitHub.

## Goal-State/What/Result

Spice provides SQL, search, and AI infrastructure whose routine operation does not
require customers to manage metadata databases, data placement, compaction, or
routine recovery. Cayenne is the only acceleration engine, operating in memory or
with persistent files/objects. On-prem deployments must work without object
storage. An embedded Turso metastore may remain an internal implementation of
Cayenne's file-backed path, without a separately operated metadata service.

Where object storage is available, use conditional writes for shared durable
state and make compute replaceable without transferring local disks. Support
both optimistic concurrency control (OCC) and multi-version concurrency control
(MVCC) in the object-store state layer, with a conditional-write write-ahead log
(WAL) as its commit and recovery mechanism. The existing per-key primitive is
only the foundation for that transactional layer.

Correctness remains the absolute priority. Operational simplicity may require
more sophisticated internal implementation. Success means fewer required
decisions, infrastructure dependencies, and operator interventions throughout
deployment, growth, failure, and upgrades.

## Why/Purpose

This is a product direction, not a reproduced defect or performance finding.
Existing foundations are the shared storage configuration
[RuntimeState](../../crates/spicepod/src/component/runtime.rs), conditional object
state [ObjectState](../../crates/object_store_occ/src/state.rs), and
[LocalConditionalPut](../../crates/object_store_occ/src/local_conditional_put.rs).
Their existence does not establish the full proposed durability contract.

Cayenne's [MetadataCatalog](../../crates/cayenne/src/catalog.rs) provides domain
operations over the SQL-oriented
[MetastoreBackend](../../crates/cayenne/src/metastore.rs). Its
[commit_fused](../../crates/cayenne/src/provider/transaction.rs) combines multiple
tables in a shared metastore transaction. Replacing this substrate must preserve
that transaction scope, not merely independent table pointer swaps.

The current [Engine](../../crates/runtime-acceleration/src/engine.rs) enumerates
Arrow, partitioned Arrow, DuckDB, SQLite, Turso, PostgreSQL, and Cayenne. The target
retires every accelerator except Cayenne. This is distinct from Cayenne's
metastore backend and from the source connectors for these databases.

## By When

The internal primitive and transactional WAL/MVCC library are implemented on the
working branch; see [the implemented contract](../../crates/object_store_occ/WAL.md).
Set rollout dates after the provider, transaction, recovery, and workload
gates below have artifacts. No runtime migration date is asserted here.

## Done-Done

- [ ] Principles Driven
- [ ] The Algorithm
- [ ] PM/Design Review of the exact runtime surface
- [ ] DX/UX Review of the exact runtime surface
- [ ] Release Notes / PRFAQ
- [ ] Threat Model / Security Review
- [ ] Tests, including engine and provider fault qualification
- [ ] Telemetry / Metrics / Task History
- [ ] Performance / Benchmarks
- [ ] Documentation
- [ ] Cookbook Recipes/Tutorials
- [ ] Existing deployments migrate without losing data or acknowledged checkpoints
- [ ] Cayenne covers the reviewed memory, file, and object-backed use cases
- [ ] Production on-prem operation needs no object-store or separate metadata service
- [ ] OCC conflicts, MVCC snapshots, WAL recovery, and checkpoint/GC interactions are qualified
- [ ] Other accelerators retire only after compatibility and migration gates pass

Direction approval does not tick implementation, migration, or surface-review
items. The new internal library is not activated by runtime configuration.

## The Algorithm

Remove the acceleration-engine decision: Cayenne handles memory and persistent
file/object storage. Share logical operations and correctness contracts across
storage implementations; persistence and availability differ explicitly. Prefer
automatic maintenance. Delete new generic K/V service endpoints, watches, TTL,
public metastore selection, and active-active multi-region scope from this effort.

Keep data-source connectors. Retiring an accelerator does not retire that database
as a source, Arrow as the data representation, or a differential-test oracle.
Turso may remain embedded under Cayenne even as the Turso accelerator retires.
Retain model APIs, retrieval, embeddings, and reranking; optional GPU execution
should not become a required
database deployment dependency.

## Specification

### Customer contract proposed for runtime review

For deployments with object storage, reuse the existing configuration shape:

~~~yaml
runtime:
  state:
    location: s3://example/spice/production/
~~~

The proposed extension makes this location authoritative for Cayenne's durable
state as well as shared runtime state. Credentials follow the existing object-store
configuration. Storage namespaces and maintenance are assigned internally; do not
add a metastore selector or a second mandatory storage location.

This example is not implemented by the internal state-store change. Before
runtime integration, specify and review: local default layout, precedence against
existing per-dataset paths, supported storage classes, namespace identity,
migration activation, old-binary behavior, exact status/error text, and treatment
of each removed parameter. Review exact memory and file examples alongside the
object-backed example: no object-store endpoint or credentials may be required
for the on-prem file path. Reuse existing storage intent where possible instead
of adding engine or metastore selectors. The storage categories below are design
terms, not newly accepted Spicepod enum values. Preserve existing behavior until
that review is complete. Do not silently reinterpret an existing shared-state
deployment.

The proposed object-backed distributed durability contract requires acknowledged
state to survive loss of every compute instance and local cache within the stated object-storage
failure model. Object-store loss and cross-region disaster recovery need separate
explicit recovery objectives. Object-free on-prem operation is a production
requirement, not merely a development fallback.

| Cayenne storage | Authoritative state | Required operating contract |
| --- | --- | --- |
| Memory | Process memory | Explicitly volatile; bounded use and rebuild/replay after restart; no durable acknowledgement promise |
| Persistent files | Durable files and an embedded metastore, potentially Turso | No object store or separate metadata service; specify restart, backup/restore, supported filesystems, and exclusive writer ownership |
| Persistent objects | Object data plus the conditional WAL and checkpoints | Replaceable compute; qualified conditional writes, pinned snapshots, and recovery independent of local caches |

Replication and failover for file-backed on-prem deployments require a separate,
explicit storage/ownership design. Do not infer them from a shared path, embedded
Turso, or the object-backed protocol. The final experience should keep the same
Cayenne SQL and transaction semantics wherever promised. A volatile mode must not
advance a durable source acknowledgement past recoverable state unless the
reviewed source contract provides lossless replay or rebuild.

### Internal state contract

The first implementation lives in
[object_store_occ::store](../../crates/object_store_occ/src/store.rs).
It adds no runtime parameters, endpoints, logs, or automatic format conversion.

- Fresh read returns absence, a value and revision, or a versioned tombstone.
- Conditional write requires absence or the exact revision from a read.
- Revisions bind the key and handle; clones share identity, independently
  constructed handles obtain their own revisions. Provider ETag and version are
  both retained.
- A WriteId identifies one immutable request. An exact retry keeps the key,
  expected revision, value, and ID. Rebasing requires a new ID.
- Applied means the backend acknowledged the write. Conflict means the final
  conditional request failed; backend transport retries may have applied an
  earlier attempt. Unknown preserves other submitted-write errors.
- Cancellation does not roll back a write. Read-back can identify a matching
  current ID, but a different current ID cannot establish absence from history.
  Durable commit lineage and deduplication remain application responsibilities.
- Values are limited to 1 MiB, excluding a 25-byte versioned envelope. Read limits
  apply to both metadata and streamed bytes. Incompatible records are errors.
- There is no blind overwrite, physical deletion, stale-read cache, list snapshot,
  lease, or cross-key transaction in the core trait.

The envelope is internal and not a runtime format commitment: seven magic bytes
SPICEKV, version byte 1, value-kind byte (0 tombstone, 1 value), sixteen UUID bytes,
then value bytes. A tombstone cannot have value bytes. Use an exclusive namespace;
legacy JSON records are not decoded or upgraded implicitly.

The object-store adapter delegates durability and conditional-write guarantees to
its backend. Provider conformance is required before runtime eligibility.
LocalConditionalPut is usable for local concurrency experiments; its existence
does not certify machine power-loss durability.

### OCC, MVCC, and a conditional-write WAL

The state store must provide a transactional versioned K/V layer above the
per-key primitive. OCC governs concurrent publication; MVCC supplies consistent
committed snapshots. These are complementary guarantees, not separate storage
engines or user-selected modes. `store::StateStore` remains the per-key primitive;
the additive [wal::WalStateStore](../../crates/object_store_occ/src/wal/mod.rs)
implements atomic batches, whole-head OCC, owned MVCC snapshots, paged checkpoints,
and receipt-based outcome resolution. The following describes the full target
protocol. The initial implementation retains all history, bounds committed WAL
count and replay, and exposes no GC, restore, or ownership-transfer API. It does
not yet implement the target's sustainable retention or distributed reader leases.

Use one authoritative head per existing transaction domain, with a generation
identity, writer epoch, commit sequence, WAL reference, and checkpoint reference.
Do not create one global head for all tenants or pretend independent heads form
an atomic multi-domain transaction. The internal API must support acquiring and
releasing a snapshot, reading a key at that snapshot, buffering an atomic mutation
batch, committing against a base version, and resolving an uncertain commit.
Concrete types and module boundaries belong in the protocol implementation task.

The proposed object-backed commit path is:

1. Acquire a protected base snapshot and buffer mutations, with read-your-own-writes.
   Initially, validate the whole base head at commit. Any intervening commit
   conflicts, even on unrelated keys; this conservatively covers reads of absent
   keys and predicates. Finer validation needs a separate proof and workload case.
2. Allocate an immutable commit-attempt ID. Write referenced immutable payloads
   and a bounded WAL record with create-if-absent semantics. The record contains
   the base commit, generation/epoch, next checked sequence, all mutations or their
   immutable references, and a hash covering the canonical envelope and payload
   references. References bind payload content digests, not just object paths;
   recovery validates the contents, format, and deterministic application order.
   Re-executing after a conflict requires a new attempt ID; transport
   retries preserve the entire attempt, including its expected head and bytes.
3. After all referenced data is durable under the selected storage contract,
   compare-and-exchange the domain head from the exact base revision to the new
   WAL reference. This conditional publication is the only commit point. WAL
   objects use immutable segments/records; no appendable-object API is required.
4. Acknowledge only the published durable commit. Materialize/checkpoint later.
   Before materialization, new snapshots must still see committed values by
   combining the checkpoint with the committed WAL tail. Staged or abandoned
   records have no effect, even if an object listing finds them.

A definite conflict requires reevaluating the complete transaction, including
reads, under a new snapshot. Never silently rebase computed mutations. A failed
final precondition or lost response may follow a successful transport attempt:
resolve the attempt ID and matching envelope hash in authoritative commit lineage
before treating it as rejected or executing a replacement. An unchanged head,
pruned history, or a missing current ID alone cannot prove non-commit. Keep
uncertain attempts unresolved until the protocol can prove their outcome and
fence any outstanding publication. No exactly-once claim follows from a UUID.

An MVCC snapshot binds a generation, commit sequence, checkpoint, and committed
tail. It reads one version across all keys in its domain, including tombstones,
and remains stable across later commits and checkpoints. Snapshot acquisition
must coordinate with retention before exposing that version to a reader. Reads
must either use the pinned version or fail explicitly; never substitute latest
state after a snapshot expires. Serializable transaction behavior is the target
within one domain under whole-head validation and needs history-based tests;
MVCC by itself does not establish that isolation guarantee.

Recovery follows the authoritative head and its referenced lineage, applying
only published records after the checkpoint watermark, with validation of hashes,
generation, parents, and sequence continuity. Checkpoint an exact committed
version into immutable objects, then publish its reference conditionally while
preserving the current logical commit, writer epoch, and incarnation. An ownership
transfer or restore invalidates an old checkpoint attempt; it cannot republish an
old generation. A checkpoint is never a second commit authority. Retain attempt
outcome evidence through checkpointing even when current values no longer show
the attempt's effects.
Bound replay work, buffered mutations, checkpoint lag, and history growth with
batching and admission control. An indefinitely pinned snapshot or unresolved
write cannot justify unbounded retention: define safe expiry/fencing and recovery
evidence before reclamation, and reject new work when necessary.

GC must respect live snapshots, restore points, replay/checkpoint dependencies,
and in-flight or uncertain attempts. Lease expiration alone must not let a delayed
writer publish references that GC has deleted; publication must validate/fence
the ownership and retention generation. Qualify this protocol before enabling GC.

For an embedded Turso backend, reuse native transactions behind the same domain
operations and qualify the isolation and snapshot lifetime that Cayenne requires.
Do not simulate object APIs or duplicate a database WAL merely for symmetry.
Memory mode preserves the logical operations with explicit volatile semantics.
The object-oriented revision/error types in the initial primitive are not yet a
portable backend contract; extract shared domain types when integrating the
second backend, without hiding differences in durability.

### Cayenne publication and recovery

For the object-backed path, write immutable segments and manifest nodes, then use
the conditional WAL to publish a transaction referencing them. The domain head
selects schema, deletion state, sequence watermarks, and source checkpoints for
the same transaction version; there is no independent catalog-root commit after
the WAL commit.
Use incremental manifest trees and batched commits; never one object request per
row or one cluster-wide object for every subsystem.

A per-table root alone cannot implement existing multi-table commits. Map the
conditional WAL's domain to the existing shared transaction boundary and qualify
the hottest domain under the target workload. Do not reduce transaction semantics
to meet a benchmark. The file-backed embedded path must preserve the same
transaction boundary using its own qualified commit mechanism.

Writer transfer advances an epoch enforced at publication. Readers pin one
consistent snapshot, including statistics used as exact. Define visibility
after acknowledgement and cross-instance read-your-write behavior. Source
checkpoint acknowledgement follows the mode's promised recoverability and
committed WAL visibility. Object-backed writes require remote recoverability;
file-backed writes require qualified local durability. The WAL can precede
materialization, but an unpublished staged record never justifies acknowledgement.

Commit lineage resolves uncertain writes after later commits. Reader protection,
unfinished uploads, ambiguous commits, and backup roots constrain garbage
collection. Restore and recreation establish new incarnation identities.

### Migration and cuts

Quiesce writes at the transaction-domain boundary, drain and checkpoint, export
SQL metadata and inline data, copy data to the selected file/object destination,
validate, and atomically activate a new generation. Keep the original store
read-only while rollback is possible. After the new generation accepts writes,
reopening the old store is not
rollback; returning requires a lossless migration of those writes.

Consolidate redundant metastore implementations only after choosing and qualifying
the embedded on-prem path. Retain Turso if it supplies that path; its removal is
not an acceptance criterion. Keep migration readers where required. A deployment
must not need object storage merely to leave a legacy metastore behind.

Retire every non-Cayenne accelerator: DuckDB, Arrow/partitioned Arrow, SQLite,
Turso, and PostgreSQL. Inventory their supported types, SQL semantics, refresh and
write modes, indexes/constraints, snapshots, resource controls, and existing files.
Demonstrate equivalent supported behavior or an explicitly reviewed migration
before removal; source reload is allowed only where replayability is established.
Make Cayenne's memory path a prerequisite for retiring Arrow acceleration and its
file path a prerequisite for object-free on-prem migration. Keep connectors,
Arrow buffers/kernels, and test oracles. Delete dependencies only when no retained
capability uses them. Turso as an internal Cayenne metastore is separate from the
standalone Turso accelerator being retired.

Consolidate scheduler, shared job, rate-control, cache-warming, and snapshot
coordination storage one consumer at a time, with explicit format migration.
Do not route high-frequency execution traffic through the state store.

## Security Review

Require isolated namespaces, least-privilege workload identity, encryption and
existing secret resolution, bounded reads, and rejected incompatible formats.
Specify bucket lifecycle restrictions for live objects and retained commit
history. Audit direct object-store access so no other path bypasses conditional
publication. Include local file permissions, embedded database ownership, backup
access, and WAL/checkpoint tampering in the file-backed threat model. These items
await the threat-model review.

## How/Implementation Plan

Follow [the implementation task drafts](simplicity-at-scale-tasks.md).
The primitive and conditional WAL/MVCC library are additive. Sustainable retention,
embedded backend qualification, Cayenne memory/file/object integration, and
accelerator migrations remain separate units with independent gates. Scope tests
and lint to object_store_occ for this library, with no package features, dev profile, and a consistent wrapper
environment. Do not build the whole runtime to validate this library.

## QA Plan

Primitive commands:

~~~sh
cargo test --profile dev -p object_store_occ --lib --test state_store --test transactional_wal
cargo test --profile dev -p object_store_occ --test state_store s3_state_store_conformance -- --ignored --exact
make lint-rust PACKAGES=object_store_occ FEATURES= RUST_PROFILE=dev
~~~

The S3 test requires AWS_S3_BUCKET and credentials and creates a unique test
prefix. Local tests cover competing clients, stale revisions, tombstone/recreate,
identical values with new IDs, malformed records, scope mismatch, bounded reads,
unsupported updates, lost responses, and cancellation after application.

Before runtime eligibility, add actual multi-process and cloud-provider fault
runs, then engine runs proving multi-table atomicity, CDC acknowledgement,
compaction, pinned reads, crash recovery, upgrade, and migration. Compare actual
rows, key values, NULLs, and sequence/checkpoint state before and after migration.

Use state-machine histories and crash schedules for competing OCC transactions,
predicate/absent reads, stable MVCC snapshots across overwrite/delete/recreate,
WAL response loss, recovery before materialization, partial checkpoint uploads,
checkpoint publication racing commits/ownership transfer/restore, and GC racing
readers or delayed writers.
Verify an object-free on-prem deployment with object-store access unavailable;
exercise file restart/restore and explicitly volatile memory restart. Validate
each retired accelerator's supported workloads against Cayenne, retaining useful
independent engines as test oracles.

Benchmark one hot transaction domain, many independent domains, skewed tenants,
cold-cache recovery, and concurrent ingestion/query/maintenance. Record object
requests and cost alongside latency, throughput, memory, and operator actions.
Define numeric targets from the workload envelope before accepting a design.

## Release Notes

Draft goal-state: Cayenne is Spice's single accelerator, from memory to persistent
files or object storage. Run on-prem without an object store, or use object storage
for shared durable state and replaceable compute. Spice manages the storage
internals and routine recovery, with no separate metadata service to operate.
Publish this claim only after the runtime and migration gates pass.

## Evidence and coverage

Source inspection is grounded in a79624f85e and the linked symbols. GitHub issue
discovery was attempted with gh issue list; it returned "error connecting to
api.github.com", so existing remote Enhancements were not checked and no issue
was created. The primitive test outputs belong in the branch's verification
record. No engine, cloud-provider, power-loss, or performance qualification is
claimed by this design document.
