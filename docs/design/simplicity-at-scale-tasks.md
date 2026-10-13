# Cayenne and state-store implementation task drafts

Parent: [Simplicity at scale Enhancement draft](simplicity-at-scale.md).
These are local drafts, not GitHub issues. Source baseline: a79624f85e.
The strategic direction is approved; exact runtime surface and migrations still
need their Enhancement reviews. Do not interpret unchecked acceptance criteria as
completed work.

Implemented on the working branch: the per-key primitive and the initial
[transactional WAL/MVCC library](../../crates/object_store_occ/src/wal/mod.rs),
including checkpoint recovery and saved-receipt resolution. See its
[contract and limits](../../crates/object_store_occ/WAL.md). The task gates below
also cover qualification and production lifecycle work not completed by this
implementation; no checkboxes are implied by the presence of library code.

Sequence: internal primitive -> conditional WAL/OCC -> MVCC/checkpoints and reader
protection -> object-backed engine integration. Provider qualification gates that
path. In parallel, qualify embedded persistence for object-free on-prem and build
Cayenne's memory path; they do not depend on cloud qualification. Then migrate
existing data and retire non-Cayenne accelerators. Turso may remain an internal
Cayenne metastore. Runtime consumer migration and the operating-experience review
can proceed alongside the engine work.

## Conditional state primitive

### Problem

The approved direction requires one small conditional-state primitive beneath
the transactional OCC/MVCC layer. Existing
ObjectState in crates/object_store_occ/src/state.rs combines typed JSON, caches,
and conditional writes; its format already has consumers and must not change
implicitly.

### Proposed Solution

Add StateStore and ObjectStoreState in object_store_occ::store. Use fresh reads,
explicit revisions, bounded values, retained tombstones, and request identities.
Return distinct applied, final-precondition-failed, and unknown outcomes.
Exercise the public trait against real local storage and injected transport faults.

### Scope & Non-Goals

Internal API only. No runtime activation, existing consumer conversion, cloud
durability certification, leases, cross-key transactions, MVCC, WAL, or metastore
removal. This task alone does not deliver the full state-store requirement.

### Acceptance Criteria

- [ ] Scoped check, library tests, state_store integration tests, and production/test lint succeed.
- [ ] Same-revision contenders cannot both publish in the qualified adapter.
- [ ] Lost responses and cancellation after application are not interpreted as rollback.
- [ ] Invalid formats and oversized streamed values fail without being interpreted as missing state.
- [ ] Existing ObjectState behavior and its persisted JSON format remain intact.

### Alternatives Considered

Changing ObjectState's format in place would silently affect current consumers.
A new general database service would add deployment scope without satisfying this
primitive's need better.

### Dependencies & Sequencing

First internal unit. Implemented on the working branch; attach actual command
outputs before marking its acceptance criteria complete.

### Open Questions / Risks

Backend guarantees require independent qualification. Write IDs are correlation
identities, not durable deduplication. Wrappers must forward every trait method.

## Qualify cloud and local backends

### Problem

A shared trait cannot manufacture storage consistency or durability.
LocalConditionalPut::put_opts and the cloud object_store adapters need explicit
qualification for this contract.

### Proposed Solution

Run the common suite against each intended cloud provider and independent local
processes. Add deterministic response-loss, retry, cancellation, and process-kill
schedules. Establish the exact supported local filesystem and durable-acknowledgement
contract separately from conditional-write concurrency.

### Scope & Non-Goals

No blanket S3-compatible certification, NFS support, cloud benchmarking claims,
or runtime fallback to an unqualified backend.

### Acceptance Criteria

- [ ] Persist the provider, region/storage class, client version, commands, and raw outputs.
- [ ] Concurrent create and same-revision update histories have the required single-winner behavior.
- [ ] Cancellation cannot release coordination while an outstanding publication can still invalidate its guarantees.
- [ ] Process-kill recovery distinguishes acknowledged, unknown, and rejected operations.
- [ ] A backend lacking conditional writes fails without an unconditional fallback.

### Alternatives Considered

In-memory-only tests cannot establish cloud semantics or filesystem durability.

### Dependencies & Sequencing

Depends on the primitive. Gates production use of the object-store adapter;
embedded Turso qualification is a separate path and does not require a cloud test.

### Open Questions / Risks

The filesystem cancellation experiment reproduced a lost update on `b80ab208`:
a cancelled 262,144-byte upload overwrote a contender's acknowledged 12-byte value.
With the dedicated publication worker, the same command rejects the stale
contender with `Precondition`. The worker owns lock acquisition, comparison and
publication through cancellation and Tokio shutdown. See the [command, before/after
output and timing-sensitive negative rerun](object-store-wal-verification.md#process-level-hardening).
This evidence exercises real filesystem I/O and process lifetime; local power-loss
durability and cloud-provider qualification remain unverified.

## Conditional WAL and OCC transactions

### Problem

The per-key StateStore in object_store_occ::store has no transaction or WAL API.
Cayenne's provider/transaction.rs::commit_fused commits multiple table mutations
through one MetastoreTransaction. The new state store must preserve that atomic
scope and provide a recoverable history for OCC publication and MVCC readers.

### Proposed Solution

Implement transaction-domain heads and immutable, bounded, versioned WAL records.
Stage every referenced object with create-if-absent before publishing a head CAS;
the CAS is the only commit point. Each attempt binds an immutable ID, canonical
envelope hash, parent commit, incarnation/epoch, checked sequence, and deterministic
mutation batch. Immutable dependency references include payload content digests;
recovery validates contents and application order, not merely object paths.
Transport retries preserve the entire attempt. Whole-transaction
re-execution after a conflict gets a new attempt ID and reevaluates all reads.

Initially validate the entire base head, including for absent-key and predicate
reads; do not silently rebase. Recover only authoritative committed ancestry,
never arbitrary listed records. Resolve ambiguous outcomes from attempt IDs and
hashes in protected history, accounting for requests still in flight. Map Cayenne
schema, exact statistics, deletion state, sequence high-water marks, and source
checkpoints to atomic domain mutations. Define bounded batching and backpressure.

### Scope & Non-Goals

Prototype protocol with a faithful harness before runtime activation. No new
cross-domain transaction promise, cluster-global mutable root, per-row object I/O,
unconditional append, public K/V service, or fine-grained conflict validation.

### Acceptance Criteria

- [ ] Record executable concurrent histories proving atomic batches and whole-head OCC, including absent reads and predicates.
- [ ] Kill/restart at every upload/head-publication boundary recovers only committed ancestry with exact expected rows and checkpoints.
- [ ] Response-loss traces distinguish committed, provably rejected, and unresolved attempts after later commits; no duplicate logical mutation is applied.
- [ ] Malformed/hash-mismatched records, broken parent chains, missing dependencies, and sequence overflow return structured errors.
- [ ] Delayed pre-transfer/restore attempts cannot publish; transport retry and whole-transaction re-execution use the specified distinct identities.
- [ ] Same-rig measurements cover hottest-domain contention, commit batching, log growth, object requests, and admission bounds.

### Alternatives Considered

Overwriting independent key objects does not provide a multi-key commit boundary
or historical reads. A mutable append object requires provider-specific semantics.
Use immutable records plus conditional publication; do not narrow atomicity to fit
the chosen domain design.

### Dependencies & Sequencing

Depends on the primitive; provider qualification gates production. Precedes
MVCC/checkpoints, Cayenne's object-backed integration, and migration.

### Open Questions / Risks

The numeric workload envelope and acceptable commit cost are unset. Domain
identity must map existing metastore transaction scope without a new customer
concept. Specify canonical serialization, retry-resolution retention, request
size limits, and caller-visible unknown-outcome handling before freezing the API.

## MVCC snapshots and WAL checkpoints

### Problem

Conditional writes alone do not supply stable historical reads. The object-store
state layer also needs MVCC snapshots, bounded recovery, and checkpointing without
losing commit evidence or concurrent writes.

### Proposed Solution

Add protected transaction-domain snapshot handles binding incarnation, commit
sequence, checkpoint, and committed WAL tail. Serve reads from that version with
tombstones and a transaction-local write overlay; latest reads acquire a fresh
committed snapshot. Materialize/checkpoint an exact committed ancestor and CAS its
reference into the head while preserving the current logical commit, writer epoch,
and incarnation. Transfer or restore invalidates a checkpoint built under the old
ownership/generation. Retain resolution
evidence for uncertain attempts even when current values have superseded them.

Bound replay, read amplification, buffered values, and checkpoint lag. Coordinate
admission/expiry with reader protection; fail explicitly for expired snapshots.
Expose domain operations, not provider ETags, above the backend boundary. The
initial object-specific Revision/error representation is not yet that portable
contract; extract shared types when adding the embedded implementation.

### Scope & Non-Goals

Internal API and conformance harness first. No historical SQL surface, automatic
cross-domain snapshot, universal database abstraction, or active GC before the
reader-protection task qualifies reclamation.

### Acceptance Criteria

- [ ] State-machine histories show stable multi-key snapshots through overwrite/delete/recreate, new commits, and checkpoints.
- [ ] A new snapshot sees every acknowledged commit even before materialization; an old snapshot sees none of the later mutations.
- [ ] Crash traces around checkpoint upload and CAS preserve concurrent commits and recover the exact acknowledged state; checkpoint-versus-transfer/restore histories never reinstate an old epoch or generation.
- [ ] Commit resolution still distinguishes an applied-then-overwritten attempt after checkpointing.
- [ ] Expired or unprotected snapshots fail explicitly; retention pressure triggers bounded admission behavior.
- [ ] Same-rig measurements capture recovery replay bytes/time, snapshot read amplification, checkpoint lag, memory, and retained history.

### Alternatives Considered

Snapshot isolation and serializable transactions are different contracts. Whole-head
OCC is the initial transaction validation rule; MVCC alone is not the proof.
Provider object versions alone do not define an atomic snapshot across keys.

### Dependencies & Sequencing

Depends on conditional WAL/OCC. Coordinate snapshot acquisition with the reader
protection protocol before enabling runtime use or history truncation.

### Open Questions / Risks

Choose an immutable index/checkpoint layout and numeric replay/retention bounds
from workload measurements. Specify snapshot expiry and administrative recovery
for unresolved attempts without discarding evidence that is still required.

## Embedded persistence for object-free on-prem

### Problem

Production on-prem deployments must work without object storage. Removing all
Cayenne SQL metastores would preempt the decision to retain embedded Turso for
that purpose; standalone accelerator retirement is a separate concern.

### Proposed Solution

Qualify Turso behind Cayenne's domain read/write/transaction operations with
durable local files, native transactions, recovery, checkpointing, and reader
lifetimes. Compare it with an alternative file adapter only if that removes
operating work while meeting the same requirements. Select the embedded backend
internally from the reviewed storage intent. Preserve native WAL/transactions
instead of emulating object APIs or duplicating the object WAL.

### Scope & Non-Goals

No mandatory object-store service, cloud credentials, or separately operated
metadata database. No inferred shared-filesystem safety, replicated Turso
deployment, or automatic multi-node failover. No commitment to remove Turso.

### Acceptance Criteria

- [ ] Run spiced with object-store access unavailable through install, ingestion, query, restart, upgrade, and backup/restore; save commands and returned rows.
- [ ] Fault runs prove the chosen acknowledgement boundary, atomic multi-table commits, CDC replay, snapshot consistency, and ownership fencing.
- [ ] Capture supported filesystem/storage assumptions and process-kill versus power-loss coverage separately.
- [ ] The same logical transaction histories pass against file and object backends, with explicit durability/availability differences.
- [ ] The reviewed configuration needs no metastore choice or external metadata service.

### Alternatives Considered

Requiring customers to install an S3-compatible service violates object-free
on-prem support. Keeping Turso as a separate accelerator retains an engine choice;
embedding it inside Cayenne does not.

### Dependencies & Sequencing

Can proceed independently of cloud qualification. Coordinate the shared domain
types with the WAL/MVCC work; gates the file-backed integration and any metastore
retirement that would otherwise remove this deployment path.

### Open Questions / Risks

Qualify native isolation and snapshot semantics rather than assuming equivalence
from the database name. Define the intended on-prem HA/storage failure model
separately before promising failover or survival of volume loss.

## Cayenne memory acceleration

### Problem

Cayenne must cover the in-memory use cases before retiring Arrow and every other
memory-capable accelerator. An object-store in-memory test adapter is not evidence
that the full Cayenne engine supports this mode.

### Proposed Solution

Provide Cayenne's logical catalog, ingestion, mutation, and query paths using
bounded volatile storage. Reuse the transaction and snapshot contracts while
making restart/rebuild semantics explicit. Inventory current memory-acceleration
requirements from runtime-acceleration::Engine and the registered implementations.
Keep source replay/rebuild sufficient for any external checkpoint advanced by
this mode; do not imply persistence from an in-memory WAL.

### Scope & Non-Goals

No new acceleration engine, mandatory file/object store, durable acknowledgement
promise, or silent disk spill. Any spill policy is reviewed as storage behavior.

### Acceptance Criteria

- [ ] Real engine runs ingest/query/update/delete supported workloads without persistent acceleration files or object-store access.
- [ ] Differential query rows/types, NULLs, empty sets, constraints, and supported refresh/write modes meet the reviewed compatibility matrix.
- [ ] Multi-table commit/abort and concurrent snapshot histories meet the logical contract.
- [ ] Kill/restart demonstrates the documented volatile reset and source rebuild/replay behavior without skipping acknowledged source data.
- [ ] RSS and workload metrics under explicit memory limits demonstrate bounded buffering and actionable resource exhaustion behavior.

### Alternatives Considered

Keeping Arrow as another accelerator preserves the engine decision. Keeping Arrow
buffers/kernels inside Cayenne and DataFusion is compatible with one accelerator.
A persistent engine pointed at tmpfs is not accepted without the full memory-mode
contract and resource tests.

### Dependencies & Sequencing

Can proceed alongside the persistent backends. Exact mode/default changes need
the operating-experience review; gates memory-accelerator retirement.

### Open Questions / Risks

Set rebuild/readiness, retention, supported source/write modes, and spill policies
in the reviewed surface. Inventory any existing Cayenne memory capabilities before
deciding which implementation pieces this task needs.

## Integrate Cayenne reads and writes

### Problem

MetadataCatalog and CayenneCatalog currently supply SQL-backed operations used by
table writes, staged upserts, compaction, and multi-table transactions.

### Proposed Solution

Integrate the domain catalog with the conditional WAL/MVCC path for object storage
and the qualified embedded path for files. Route actual writes, deletes, inline
data, compaction, and CDC recovery through the shared domain operations. Readers
select one transaction snapshot; bind exact statistics and schema to it. Define
and test visibility and acknowledgement per deployment before enabling the path.

### Scope & Non-Goals

No retirement of old backends yet. No per-statement SQL-to-object translation,
reduced transaction semantics, or unbounded local buffering.

### Acceptance Criteria

- [ ] Real spiced runs exercise SQL writes and multi-table commit/abort.
- [ ] CDC tests preserve source checkpoint ordering through kill/restart and replay.
- [ ] Returned rows, duplicate/NULL behavior, schema, and delete visibility match the oracle.
- [ ] Query, ingestion, and maintenance stay within explicit resource bounds.
- [ ] Object-backed cold restart recovers without another instance's local disk; file-backed restart recovers from its authoritative durable volume without object storage.
- [ ] No acknowledgement relies on a staged but unpublished object WAL record or unqualified file flush.

### Alternatives Considered

Implementing MetastoreBackend by translating arbitrary SQL into objects retains
the wrong abstraction and obscures transaction boundaries.

### Dependencies & Sequencing

Each persistent path depends on its qualified backend and transaction/snapshot
protocol. Exact activation/config surface must pass PM and DX review before
runtime wiring. The file path does not wait for cloud qualification.

### Open Questions / Risks

Identify all concrete CayenneCatalog downcasts and direct SQL transactions before
switching paths. Enumerate the committed record that justifies CDC acknowledgement
and ensure replay can serve it before asynchronous materialization finishes.

## Reader protection and reclamation

### Problem

Object lifetime must cover MVCC snapshots, remote readers, pending publication,
uncertain commits, WAL replay, and restore points; process-local snapshot ownership
is not the object-backed lifetime model.

### Proposed Solution

Define reader registration and root selection as a protocol coordinated with
garbage collection. Retain immutable history for bounded recovery and retry
windows. Reclaim unreachable artifacts only after their protection expires or is
fenced. Coordinate snapshot admission before exposing its version, and make an
expired reader stop before reclamation. Checkpointing must retain commit-outcome
evidence independently of current key values. Fence delayed publication at the
authoritative head, not only in an advisory lease. Start conservatively.

### Scope & Non-Goals

No aggressive space reclamation, arbitrary bucket lifecycle deletion, or promise
that a prefix listing is a consistent snapshot.

### Acceptance Criteria

- [ ] Engine scans keep their files while concurrent compaction and GC run.
- [ ] A reader registering during GC cannot select an unprotected snapshot.
- [ ] Delayed writers and unknown commits cannot publish already-reclaimed objects.
- [ ] Restore points survive cleanup; expired protection terminates affected work safely.
- [ ] Checkpoint and GC traces preserve both replay dependencies and resolution evidence for unknown attempts.
- [ ] Retained bytes and cleanup object requests have measured bounds.

### Alternatives Considered

A delay alone is sufficient only with an enforced upper bound on every operation
it protects; document that bound or use explicit protection.

### Dependencies & Sequencing

Depends on conditional WAL/OCC and MVCC/checkpoint design. Gates object-backed
engine rollout and restore promises; the embedded path separately qualifies
native reader lifetimes, checkpointing, and file reclamation.

### Open Questions / Risks

Choose enforceable query/retry/backup retention horizons and ownership-loss rules
before optimizing reclamation.

## Migrate Cayenne state and consolidate metastores

### Problem

The direction requires a supported file/object destination for existing Cayenne
data. Its metastore contains inline data and transaction state. Redundant backends
can retire after migration, but Turso may remain the embedded on-prem backend.

### Proposed Solution

Build an idempotent, resumable migration at the existing transaction boundary:
quiesce, drain, capture metadata and inline data, copy dependencies into the chosen
file/object destination, validate, then atomically activate a fresh generation.
Retain read-only legacy import support for a defined transition window. Preserve
an object-free path and qualify upgrades of retained embedded formats as well as
conversion to the object protocol.

### Scope & Non-Goals

No permanent dual-write authorities, mandatory migration to object storage, or
unconditional Turso removal. Accelerator retirement has its own task. Keep
database connectors and differential-test oracles.

### Acceptance Criteria

- [ ] Golden fixtures from each supported legacy format migrate.
- [ ] Interrupted migration resumes without double application or data loss.
- [ ] Engine row/value comparisons and checkpoint checks agree before and after.
- [ ] Old binaries cannot open a new generation as if it were legacy state.
- [ ] Rollback after new writes has a defined lossless procedure.
- [ ] Object-free on-prem fixtures migrate/upgrade without object-store access; retained Turso databases stay supported.
- [ ] Removed settings, old paths, release notes, and cookbook changes match the reviewed surface.

### Alternatives Considered

Permanent dual-write operation adds a second failure/reconciliation model.
Deleting and reloading is not a universal migration when sources cannot replay.

### Dependencies & Sequencing

Depends on the selected destination's engine integration, reader protection and
backend qualification, plus the reviewed exact migration/compatibility surface.

### Open Questions / Risks

Set the support window and policy for datasets with different existing metadata
locations. Select the retained embedded backend before scheduling redundant
metastore removal. Do not merge a removal before supported fixtures and failure
artifacts exist.

## Retire non-Cayenne accelerators

### Problem

runtime-acceleration/src/engine.rs::Engine exposes Arrow/partitioned Arrow, DuckDB,
SQLite, Turso, PostgreSQL, and Cayenne. The target is Cayenne as the only accelerator
for memory and persistent file/object use cases.

### Proposed Solution

Build a compatibility and migration inventory for every non-Cayenne engine,
covering types, SQL behavior, constraints/indexes, refresh/write modes, snapshots,
partitioning, resource controls, and stored files. Close required Cayenne gaps,
review exact deprecations/default changes and upgrade behavior, and retire each
engine after its fixtures migrate. Remove engine registration, engine-specific
configuration and packaging, and dependencies no retained capability uses.

### Scope & Non-Goals

All alternative accelerators are in scope, including DuckDB. Keep source
connectors, Arrow data structures/kernels, useful independent test oracles, and
Turso if used internally by Cayenne. No dependency purge by name, silent engine
substitution, or source reload where replayability has not been established.

### Acceptance Criteria

- [ ] The inventory names every engine/default/alias and supported behavior, with source symbols and runnable before/after fixtures.
- [ ] Real Cayenne runs reproduce required results and type fidelity for migrated memory, file, and object workloads.
- [ ] Run migration/recovery tests for existing files/checkpoints and interrupted upgrades, or record the reviewed compatibility policy for any excluded format.
- [ ] Object-free on-prem upgrades need no new storage service and memory upgrades need no persistent storage.
- [ ] The final reviewed runtime offers Cayenne as the sole accelerator; connectors, retained metastore code, and differential tests still work.
- [ ] Schemas, build options, CLI validation, docs, recipes, snapshots, and diagnostics match the approved deprecation/removal surface.

### Alternatives Considered

Making Cayenne merely the default retains multiple operating and support paths.
Deleting all DuckDB/Turso/Arrow code also removes capabilities outside acceleration.
Retire the accelerator interfaces while keeping justified internal and connector use.

### Dependencies & Sequencing

Inventory and surface review can start now. Removal depends on Cayenne coverage
for the engine's use cases, memory/file/object qualification as applicable, and
data migration. Split individual engine removals into reviewable changes after
the inventory establishes their boundaries.

### Open Questions / Risks

Set compatibility/support windows and ordering from the inventory. Do not assume
feature parity or improved performance without the corresponding engine and
same-rig workload artifacts. Resolve handling of aliases such as `vortex` and
file creation/update modes in the reviewed surface.

## Consolidate existing runtime state consumers

### Problem

Shared jobs, rate control, cache warming, scheduler state, and snapshot ownership
have domain-specific state paths that should share storage mechanics.

### Proposed Solution

Inventory object_store_state.rs, cluster/shared_job_state.rs,
datafusion/query/cache_warming.rs, runtime-rate-control/src/leased.rs,
runtime-cluster/src/cluster_state.rs, and the snapshot writer lease. Migrate one
consumer per reviewable change, preserving domain-specific consistency and retry
semantics. Use the per-key primitive or transactional WAL/MVCC layer according to
the required atomicity and historical-read contract. Give each a versioned format
transition and an explicit deployment/backend scope.

### Scope & Non-Goals

No bulk rewrite of all consumers, silent JSON conversion, or routing per-request
execution messages/counters through object storage.

### Acceptance Criteria

- [ ] Every conversion includes old-format and mixed-version behavior.
- [ ] Concurrent-instance integration tests exercise the actual consumer.
- [ ] Ownership-sensitive writes use read-bound revisions.
- [ ] Cancellation/unknown outcomes have consumer-specific resolution.
- [ ] Existing external configuration and behavior remain unchanged unless separately reviewed.

### Alternatives Considered

A universal lease/job abstraction would combine different domain contracts.
Consolidate storage and reuse proven algorithms only where their semantics match.

### Dependencies & Sequencing

Primitive and backend qualification first; transactional consumers also require
the WAL/MVCC protocol. Independent of Cayenne integration.

### Open Questions / Risks

Some data is advisory and some authoritative. Classify each consumer before
selecting caching, retry, and garbage-collection behavior.

## Review the operating experience and removal surface

### Problem

The product direction needs a concrete user experience through install, growth,
failure, restore, and upgrade, not just an internal storage change.

### Proposed Solution

Draft exact configuration precedence, minimal deployment examples, migration
transcripts, live status/error messages, and old-version behavior. Quantify user
actions and required concepts for each lifecycle. Propose one production
engine, Cayenne, with memory and persistent file/object storage and automatic
maintenance. Preserve necessary resource limits and truthful diagnostics. Include
an object-free production on-prem flow and keep any embedded Turso choice internal.

### Scope & Non-Goals

Design review first. Do not remove accelerator engines, settings, APIs, or logs
until their individual compatibility and migration requirements are specified.
Defer public K/V APIs, active-active multi-region operation, and general MLOps.

### Acceptance Criteria

- [ ] Enhancement Specification contains exact YAML, API/CLI/log changes, and deprecations.
- [ ] PM/Design and DX/UX reviews assess the proposed final experience.
- [ ] Each removal names the customer work eliminated and migration path.
- [ ] Memory, file, and object examples express storage intent without exposing engine, metastore, OCC, MVCC, or WAL tuning choices.
- [ ] On-prem file deployment needs neither object storage nor a separate metadata service; its durability/failover limits are explicit.
- [ ] Failure/upgrade exercises measure interventions, recovery, request cost, and resource limits.
- [ ] Schemas, docs, recipes, and release messaging match the signed-off surface.

### Alternatives Considered

Hiding knobs without automatic safe behavior does not remove the operating work.
A new deployment-specific configuration hierarchy adds concepts unnecessarily.

### Dependencies & Sequencing

Can be designed alongside the internal primitive; gates runtime surface changes.

### Open Questions / Risks

Confirm the workload envelope, service objectives, supported cloud/storage
classes, and product support window. No peer superiority claim is established
without comparative evidence.

## Evidence and coverage

These drafts identify existing symbols by repository path and describe proposed
acceptance gates. No GitHub issue was created: remote discovery failed because
api.github.com was unreachable. Engine, cloud-provider, crash-durability, and
performance qualification have not run in this primitive implementation pass.
