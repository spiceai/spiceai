# DR-003: Acceleration Snapshotting and Recovery

## Status

Accepted (implemented in `runtime-acceleration`)

## Context

Spice's accelerated tables hold derived state — frequently many gigabytes of materialized rows that take time to rebuild from the source on restart. OSS users running a single Spice process today either accept that an accelerated dataset is re-loaded on restart, or persist file-mode acceleration to local disk and depend on local-filesystem durability.

That posture is acceptable for OSS — single-node, application-scoped, "rebuild from source if state is lost" is a reasonable contract for a developer running on one machine.

It is not acceptable for Spice Enterprise. Customers running production fleets expect:

* **Recovery time objectives** measured in minutes, not the hours it can take to re-accelerate a multi-terabyte dataset.
* **Bootstrapping new replicas** from a known snapshot rather than re-loading from the source.
* **Durable, off-host backups** of accelerated state independent of any one node's local disk.
* **Verifiable integrity** — a restored snapshot must be byte-identical to what was captured.

Snapshotting therefore depends on durable storage outside the runtime process and is most valuable in production fleets. By [`enterprise/README.md`](../README.md) principle #3 (operational guarantees that require coordination), this places it in Enterprise.

The feature is implemented today in `runtime-acceleration/src/snapshot/` and surfaced as Enterprise-only at runtime — a build without the snapshot feature emits the message `"Acceleration Snapshots are included in the Enterprise distribution of Spice.ai"` and disables the behavior.

Related decisions:

* [OSS DR-004: Use Apache Ballista as Spice's distributed query framework](../../decisions/004-distributed-query-framework.md)
* [OSS DR-006: High Availability Distributed Query with Active/Active Schedulers](../../decisions/006-ha-distributed-query.md)

## Assumptions

1. Object stores (S3, S3 Express One Zone, S3-compatible) are the durable backing for snapshots.
2. Acceleration engines used by Spice (DuckDB, SQLite, Cayenne, Turso) can produce a consistent on-disk image via an engine-native snapshot operation.
3. Source-of-truth data lives outside Spice; snapshots cover *Spice's accelerated derived state*, not the upstream sources.
4. Snapshot creation must not stop query traffic; readers continue serving from the previous accelerated state during snapshot capture.
5. Restores happen during bootstrap or in a controlled operational window; they are not part of the hot query path.
6. Each accelerated dataset is snapshotted independently; cross-dataset consistency is not promised by this DR.

## Options

### Snapshot scope

1. **Per-dataset** — capture one accelerated dataset at a time. Operationally simple; each dataset's lifecycle is independent.
2. **Per-cluster (cross-dataset consistent)** — globally consistent point-in-time across all accelerated datasets. Stronger semantics, requires global coordination.

### Engine integration

1. **Engine-native snapshot per accelerator** — a `SnapshotEngine` trait with implementations for DuckDB, SQLite, Cayenne, and Turso, each delegating to the engine's native checkpoint/backup API (or a directory archive for directory-based engines like Cayenne).
2. **Generic file copy** — copy the on-disk file while the engine is quiesced. Simpler but engine-dependent and stalls writes.

### Storage layout

1. **Object store with metadata sidecar** — snapshots are object-store objects with associated SHA-256 checksums and metadata (timestamp, engine, row count, size).
2. **Filesystem / NFS** — viable on-prem but not portable across nodes.

### Integrity

1. **SHA-256 checksum per snapshot** captured at write and verified on read.
2. **No checksum** — trust the storage layer.

### Bootstrap-on-failure behavior

1. **Configurable behavior** when bootstrap from snapshot fails: error out, fall back to source refresh, or proceed with no data.
2. **Single hardcoded behavior**.

## First-Principles

* **Data correctness is non-negotiable**: Every snapshot carries a SHA-256 checksum; mismatch on restore aborts before the data becomes visible. There is no path that produces "almost the right" state.
* **Secure by default**: Snapshots inherit the object store's encryption-at-rest configuration; access is governed by object-store credentials managed via `runtime-secrets`.
* **Object-store native**: Snapshots are object-store objects + metadata, with no new operational dependencies. Aligns with OSS DR-006's posture.
* **Simplicity**: Per-dataset is the primitive; cross-dataset consistency is not promised, which keeps the implementation tractable.
* **First-class extensibility**: New acceleration engines opt in by implementing the `SnapshotEngine` trait.

## Decision

Spice Enterprise provides per-dataset acceleration snapshotting and recovery in `runtime-acceleration/src/snapshot/`:

1. **`SnapshotEngine` trait** with engine-native implementations for **DuckDB**, **SQLite**, and **Cayenne** (directory-based; the engine's directories are archived into a tar file at snapshot time and extracted on restore), behind feature flags. The Turso engine is currently a no-op that copies the live file as-is (with the same WAL-loss caveat the file copy fallback has elsewhere); `refresh_mode: snapshot` against Turso is disabled until [spiceai/spiceai#10657](https://github.com/spiceai/spiceai/issues/10657) routes the checkpoint pragma through a libsql-native connection.
2. **Object-store storage** with an `S3ObjectStoreBuilder`-driven backend, supporting versioned writes (`PutMode`, `UpdateVersion`) where the store supports them.
3. **SHA-256 checksums** captured at snapshot time and verified on restore. A `SnapshotInfo` record carries `snapshot_id`, `timestamp_ms`, `location`, `checksum`, `checksum_algorithm`, `size_bytes`, `engine`, optional `row_count`, `is_current`, and `status`.
4. **Public list/inspect API**: `SnapshotSummary` and `SnapshotInfo` types are exposed via HTTP endpoints so operators can list snapshots, see which is current, and inspect metadata.
5. **Bootstrap-on-failure behavior** is configurable per dataset via `BootstrapOnFailureBehavior` from the spicepod schema.
6. **Enterprise gating**: builds without the snapshot feature emit the `SNAPSHOTS_ENTERPRISE_ONLY_MESSAGE` and disable the feature with a tracing warning rather than a hard error.
7. **Per-dataset scope only**: cross-dataset consistent snapshots are *not* part of this DR.

### Why

* Engine-native snapshot APIs (DuckDB `EXPORT`, SQLite backup API, Cayenne directory archive, Turso) produce consistent images without stalling writes for long.
* SHA-256 verification on restore enforces the data-correctness invariant: there is no codepath where a corrupted snapshot becomes visible to readers.
* Object-store backing matches the rest of Spice's posture (OSS DR-006) and avoids introducing a new operational dependency.
* Configurable bootstrap-on-failure lets operators decide whether snapshot-load failure should fail-fast or fall back to source refresh, matching the principle of "let the operator make the tradeoff" where the right answer depends on the workload.
* Per-dataset scope is honest about what the implementation provides today; over-claiming cluster-wide consistency would invite reliance on guarantees that don't yet exist.

## Consequences

* Each acceleration engine that wants to participate in snapshots must implement `SnapshotEngine`. Engines that do not (e.g., pure in-memory accelerators) are excluded from this feature.
* Cross-dataset consistent snapshots are *not* available; operators who need them today take per-dataset snapshots in a coordinated window and accept best-effort consistency.
* Source-of-truth data is out of scope. A snapshot rolls back Spice's derived state; it does not — and must not — attempt to roll back upstream databases.
* The HTTP list/inspect API is a stability commitment; the `SnapshotSummary` and `SnapshotInfo` shapes evolve additively.
