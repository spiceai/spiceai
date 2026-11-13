# Cayenne object-native metastore

Status: **draft design for discussion.** This document states the direction:
the product problem, the milestones, the key decisions with their
alternatives, and the order of the work. It is not a specification. The
[technical design](object-native-metastore-technical.md) holds the components,
code changes, protocol details and the roadmap's exit proofs. The
[protocol model](../../crates/cayenne-metastore-model/README.md) holds the
model checks, cited here by run name.
[Enhancement #16245](https://github.com/spiceai/spiceai/issues/16245) holds the
user-facing surface. All of them evolve together as implementation proceeds and unknowns are worked out.

---

# What and why

## The product problem

Every Cayenne acceleration depends on a metastore. Today that is an embedded
SQLite database holding every table's snapshot pointers, file manifests,
deletion state, statistics and the small-write tier. That choice shapes what
Spice can offer:

- **State is tied to one machine.** The catalog lives on an instance's disk.
  A replacement instance cannot pick up where the old one left off, and nodes
  cannot share an acceleration without exporting and importing metastore
  slices.
- **One writer.** SQLite serializes every write in a process, and nothing lets
  a second instance write the same table. Ingest from many instances,
  per-partition CDC and distributed maintenance are out of reach.
- **More to choose and operate.** Users meet modes, metastore backends and
  SQLite failure modes (busy timeouts, WAL growth, freelist vacuum), none of
  which describe what they want.

## What we are building

**One metastore for Spice and Cayenne that is native to object storage** and
runs unchanged on a local directory.

- **One storage decision.** The user chooses where durable state lives (`runtime.state.location`): a bucket, a local directory, or
  `mem://` for tests. There is no metastore choice, no tuning knob, and no
  external coordination service or database.
- **Durability is the destination's.** With an object store, acknowledged
  state survives losing any instance and its disk, and any instance can
  recover or read it. A local directory is the local and on-prem path: it
  needs no object store or credentials and runs the same code path as a
  bucket. Its state survives a process crash or kill, not the loss of the
  disk; whether it survives a power loss is an
  [open decision](object-native-metastore-technical.md#open-decisions).
- **The data moves too.** Today a file-mode dataset keeps its Vortex and
  deletion files on the instance's disk, or in S3 Express where configured.
  Here every file a commit references is in the destination before the
  commit, and local disk becomes a cache. A commit that referenced one
  instance's disk could not be recovered without that disk. And because a file
  is invisible until a record references it, one conditional write of the
  record commits both; no commit spans local disk and the destination.
  Decisions 1.11 to 1.14 say how data files are written and found, and
  decision 1.10 reclaims them like every other object.
- **Every existing guarantee holds.** Guarded commits are atomic, and so are a
  write across a dataset's partitions and a transaction across tables.
- **One new guarantee.** A CDC source position commits in the same record as
  the state it covers. Today the two are written separately.

**Scope.** This document covers data files only where the metastore depends on
them: a file is durable before a commit references it, the metastore records
where each file lives and which files are live, and files leave storage only by
rules the metastore can enforce (decisions 1.10 to 1.14).

### Consistency

- **Commits to one dataset are serializable.** One compare-and-swap (CAS) of
  the dataset's head orders every commit, and each commit's guards are checked
  against the exact state it follows.
- **Every read sees a consistent snapshot** of committed state, and a node's
  view only moves forward.
- **Across nodes, staleness is bounded.** A node sees another's commit once it
  refreshes its cached head, which a query does when the copy is older than
  the staleness bound S. While the destination is unreachable, a node keeps
  answering from the last state it read, which can be older than S. Such a
  query has no retention protection: GC may already have deleted objects it
  needs, and then it fails with an error, never other rows (model:
  `dataset-query-expiry*`). With several writers (Milestones 2 and 3),
  nodes can briefly see different recent states.
- **Read-your-writes holds on the node that wrote.** There, buffered writes are
  queryable before they commit, as today, so a read can see a value a crash,
  a fence or an unresolved commit then drops. That value was never
  acknowledged, and other nodes see only committed state. Publishing at commit
  (decision 1.2) would remove this.

**Not goals of this work:**

- **Linearizable reads, or read-your-writes across nodes.** See
  [Future work](#future-work-not-planned).
- **Atomic reads across datasets.** Writes across datasets are atomic
  (decision 1.3), but a query sees each dataset at its own snapshot, as today,
  so it can see one dataset's side of a transaction before the other's. Decision
  1.3 keeps a consistent read possible later.
- **Highly contended writes.** Writes are assumed mostly non-conflicting;
  heavy contention on shared keys needs a coordinated commit layer (Future
  work).
- **One atomic commit across regions**, and **source-version ordering** for
  overlapping keys (decision 3.2).
- **An interchange table format.** This is Cayenne's internal format.

## How it works

- **The head and its CAS.** Each dataset has one small mutable object, the
  head, carrying its newest commit records. A commit is a CAS of the head,
  which succeeds only if no one replaced it first. Success is acceptance, and
  only an accepted commit is acknowledged. Older records leave the head in
  immutable batches that the head lists.
- **The lease and the generation.** In Milestone 1, one instance at a time is
  a dataset's writer and holds its writer lease. Taking the lease raises the
  lease generation, which the head records, so a former writer's next commit
  fails. The lease decides who writes; the generation in the head makes a stale
  writer harmless. Milestone 2 leases maintenance units and Milestone 3 source
  partitions the same way, each fenced by a generation in the head.
- **Actions and immutable objects.** Records carry typed, file-level actions
  ("add these files", "replace these files with their compaction"). Data files
  and bulk content, such as file manifests and the primary-key (PK) index, are
  immutable objects that actions reference.
- **Acknowledgement points.** Writers buffer changes and commit only when
  something must be acknowledged: a CDC source position every ~2 s, a SQL
  `COMMIT`, or a freshness bound. Remote writes scale with acknowledgements,
  not ingest volume.
- **Readers and checkpoints.** A reader learns of new commits, and reads them,
  with one conditional read of the head. Incremental checkpoints bound the
  cost of opening a dataset and of recovery.

## Milestones

The [technical roadmap](object-native-metastore-technical.md#implementation-roadmap)
breaks Milestone 1 into steps. Each milestone removes the main limit the one before it leaves. Milestone 1 is  
firm. Milestones 2 and 3 are settled in  direction; what is open waits on  
a measurement or a model check, not a new design  
([Open decisions](#open-decisions)).

### Milestone 1: one object-native metastore

Enhancement [#16245](https://github.com/spiceai/spiceai/issues/16245) rolls the work out to users behind a developer flag that
its last milestone removes; its milestones are not this document's.

**What:** every accelerated dataset's metastore and data files live in the
destination, for every deployment: a laptop, an on-prem server with a local
directory, a node on S3, GCS or Azure. One writer per dataset holds a lease;
any number of instances read.

**Why:** the metastore is what ties acceleration state to one machine. One
object-native implementation for every destination is the prerequisite for
everything else.

**What the user sees:** nothing new to configure. A restarted or replacement
instance pointed at the same bucket recovers the acceleration without a full
reload.

**What it removes:**

- SQLite as a metastore, with the metastore choice and its knobs, once the new
  path has proven its durability, re-bootstrap and operating behavior. The Turso
  metastore goes earlier, with the accelerator deprecation.
- A database inside every instance, with its locking, tuning and upkeep (busy
  timeouts, write queueing, checkpointing, vacuum).
- Copying acceleration state between instances: a replacement reads the
  destination directly.
- A metastore round trip on every write: writes commit once per
  acknowledgement point.
- The small-write tier inside the database, once its replacement is measured
  (decision 1.5).
- The staging directory, its write-ahead file and the copy of every written
  file (decision 1.11), and every listing of a dataset's files on writes,
  scans and maintenance (decision 1.12).
- A second code path for local data: local directories and buckets use one
  store interface (decision 1.13).

**Delivers, beyond the metastore itself:**

- **Standbys behind a lease.** Instances run the same Spicepod on one
  destination. The holder consumes the source and writes; the others
  serve queries from shared state and take over when the lease lapses.
  Fencing, durable resume (decision 1.9) and shared reads make this possible.
  It is required before the object-native metastore replaces SQLite, so two
  instances started on one destination by accident never both consume the
  source. Each source consumes only while holding the lease, acknowledges
  after the commit, and replays after takeover
  ([step 7](object-native-metastore-technical.md#step-7-standbys)).

**Limits it leaves:**

- **One writer per dataset** caps a dataset's ingest at what one node can
  decode, encode and commit.
- **Maintenance shares the writer's node.** Compaction and the bake (merging
  file groups) compete with ingest for its CPU and I/O.
- **Failover waits for the lease.** Safety never depends on lease timing,
  because the generation in the head fences the old writer. A standby waits for
  the holder's lease to lapse, so ingestion pauses for up to the lease duration
  while reads continue.

### Milestone 2: maintenance on any node

**What:** one ingest writer per dataset, while compaction, the bake and cold
promotion run on any node.

**Why:** maintenance competing with ingest is Milestone 1's sharpest
operational limit, and it needs much less than many writers do. Maintenance records only rewrite files and draw no
new sequence numbers (decision 1.8), so with one ingest writer they need no
key footprints and no answer to decision 3.4 (model: `remote-maintenance*`).

**Delivers:**

- **Maintenance on any node,** under leases over file sets, racing the writer
  on the head with guards (decisions 2.1, 2.2). A writer that loses to a
  compaction of other files retries on the new head.
**Limits it leaves:** one ingest writer per dataset, and read-dependent writes
stay on the writer.

### Milestone 3: concurrent writers

**What:** many writers commit to one dataset wherever their data is
independent, without the user declaring a layout.

**Delivers:** many appenders to one table (so SQS replicas share the work
instead of standing by); writers over disjoint key sets; one ordered CDC
writer per source partition; distributed query writes that commit once per
job; writers in several regions over disjoint keys, against one bucket; and,
last, `UPDATE`/`DELETE … WHERE`, `INSERT … SELECT` and transactions from any
writer, serializable in commit order (decision 3.5). Until that last item, a
read-dependent write re-plans whenever another commit wrote a table it reads:
safe, but it can report a conflict under heavy ingestion.

**Limits it leaves:**

- **One log per dataset caps its commit rate** at about `1 / (conditional-write
  latency)`: on the order of 20 per second on S3 Standard, hundreds on S3
  Express or a local disk. At a 2 s cadence, tens of writers per dataset
  approach it, and each lost race adds a read and a write.
- **Cross-region writers pay cross-region latency per commit** and depend on
  the bucket's home region.
- **Overlapping keys still need a resolution:** abort and retry, or key
  ownership.

### Future work (not planned)

None of these is scheduled. They are listed so the planned milestones keep
them possible.

- **Several logs per dataset,** if one log's commit rate saturates or each
  region should commit to its own. A snapshot becomes a position in each log.
  Kept possible by opaque snapshot positions and the decision-object protocol
  (decision 1.3) and by footprints (3.1). Placement and atomicity across logs
  need their own design; a cross-log commit costs an extra round trip.
- **Coordinated commits for contended keys:** a lease-elected committer
  batches writers' changes into single records, with no external service
  (Delta Lake's coordinated commits use a separate coordinator). Kept possible
  by decision 2.2.
- **Atomic reads across datasets** through a catalog frontier of mutually
  consistent `(dataset, commit)` vectors. Kept possible by decision 1.3.
- **Cross-node consistency.** Commit numbers returned with acknowledgements are
  read-your-writes tokens (decision 1.8): a read that waits for the head to
  reach a token sees that write, but may miss later commits that completed
  before it started. A linearizable read sends a head GET after it starts and
  serves at least the commit that GET returns (decision 1.1; model:
  `reads-linearizable`, `reads-token-wait`; serving a GET sent before the query
  started fails: `reads-tick-only`). Either kind of
  strict read excludes the buffer, or the dataset publishes at commit (1.2).
- **A lookup of a transaction's outcome by id.** When a transaction across
  datasets ends with *outcome unknown*, the client checks its own data, which
  works whenever the transaction wrote something it can find. A blind update
  such as `balance = balance - 30` leaves nothing to check, and a retry may
  apply it twice. A lookup would answer from the decision object, which
  would then be kept for a retention window after the transaction's other
  records are removed. Whether it is a system table or an API is a surface
  decision for its Enhancement.
- **Tiered storage beneath the destination:** ephemeral disk, then persistent
  disk, then S3, acknowledged once data reaches the first object store tier.
  Kept possible by acknowledgement points (decision 1.2).

---

# Key decisions

## Milestone 1

**1.1 One conditional write commits: a CAS of the dataset's head.** The head
carries the newest commit records, so a CAS on its version both orders and
accepts a commit, and a stale writer's CAS fails. Guards are checked locally
against the exact version the CAS replaces, and acceptance is all-or-nothing.
A lost response resolves to *accepted*, *rejected* or *unresolved* by reading
the head, or by resending the identical CAS while the head is still at the
version it replaces; a failed resend is resolved again from the head. While
the head is unreadable, the writer retries the read, never the write. Older records move into immutable
batches the head lists. This extends the commit point of the existing
`object_store_occ` WAL so that a poll is one request and a cold open is two
dependent steps: the head, then the checkpoint root and batches in parallel.
Apache Iceberg commits the same way, and S3's conditional writes (2024) let
the object store itself coordinate.
*Details:* [The write path](object-native-metastore-technical.md#the-write-path).

- *Considered: a numbered log* (`log/<pos>` per commit, as in Delta Lake and
  SlateDB). Prototyped and timed. A create orders a commit
  without accepting it, so it needs a GC boundary checked after every create,
  no-op fill and fence records, about twice the reader requests per poll and
  slower cold opens. Rejected.
- *Considered: append to a log object.* Supported on S3 Express, Azure and GCS
  zonal buckets but not S3 Standard or most S3-compatible stores, and billed
  as a write everywhere. Rejected.

**1.2 Commit when something must be acknowledged, not on every write.** Writes
accumulate in the mem-tier, Cayenne's existing in-memory write buffer, and
commit as one record at an *acknowledgement point*: a CDC position to
acknowledge (~2 s), a SQL `COMMIT`, a freshness bound, or a buffer size or age
bound. The acknowledgement is sent only after the commit succeeds; losing the
buffer loses only unacknowledged work, which the source replays. On a local
directory each write is flushed before it returns. Committing every CDC burst
would cost a dataset about 20 head writes per second; a 2 s cadence costs 0.5,
and 60 s one a minute. Milestone 1 keeps today's local visibility:
buffered writes are queryable on the writing node once applied.

- *Option: publish at commit.* No read sees an uncommitted value, at the cost
  of up to one commit interval plus the CAS of freshness on the ingesting node
  (about 2 s plus 30–100 ms on S3 Standard). Independent of decision 3.4.
- *Alternative: a persistent disk-backed buffer.* Shorter replay after a
  crash, but not needed for correctness, and a second log format. Not in
  Milestone 1; its general form is tiered storage (Future work).

**1.3 A transaction domain per dataset.** One log per accelerated dataset, so a
commit across its partitions is one record. A small catalog log holds the
name registry. The rare transaction across datasets uses a create-if-absent
decision object, and its outcome becomes visible only through a `Resolve`
record in each dataset's log. Snapshot tokens are `(dataset, commit)` vectors,
so a consistent cross-dataset read stays possible. This gives the most
concurrency Milestones 2 and 3 can use.
*Details:* [Multi-table transactions across datasets](object-native-metastore-technical.md#multi-table-transactions-across-datasets).

- *Alternative: one head per catalog.* Cross-dataset transactions and reads
  become atomic, but one head takes about 20 commits per second on S3 Standard
  for the whole catalog (100 datasets at 2 s need 50), any second writer races
  on every commit, every poll downloads every dataset's records, and one lease
  owns the catalog. Rejected; adopting it later would be a migration.
- *Alternative: one log per transaction group.* Rejected: the groups need
  configuration or inference.
- *Alternative: one log per partition.* Rejected: fan-out writes and
  partitioned overwrites would need distributed transactions, table-wide state
  would have no home, readers would merge logs from day one, and concurrency
  would depend on declared layout.

**1.4 Data out of the log; checkpoints chunked and incremental.** Records carry
small typed actions. Manifests, deletion-vector lists, table statistics and
cold-tier manifests are immutable objects with fresh names; each reference
carries the object's BLAKE3 digest, which checks its bytes. Per-file statistics
are part of each file's entry, not objects of their own. A checkpoint is a small root
plus sorted, size-bounded pages and rewrites only the pages that changed. The
PK index is one of its collections, so it never needs history GC has removed.
Opening a dataset reads the latest checkpoint plus the records after it.
*Details:* [What the state holds](object-native-metastore-technical.md#what-the-state-holds).

**1.5 Small writes are buffered; the inline tier is retired once its
replacement is measured.** Today small writes are Arrow IPC rows inside
SQLite. Here they accumulate in the mem-tier and are written at the next
commit, so file count follows commit cadence, not write rate. A small commit
(a one-row `INSERT; COMMIT`, a CDC batch of a few KiB) carries its rows inline
in its record until the next checkpoint writes them as a level-0 file, so it
is one round trip. The SQL inline tier goes only once this meets the bar
measured against it, including a caching workload that depends on it today.
*Details:* [Small writes](object-native-metastore-technical.md#small-writes).

**1.6 File-level actions from day one.** Even with one writer, records use
`AddFiles`, `RemoveFiles`, `ReplaceFiles` and `AddDeleteFiles`, and the
current snapshot is derived state. It costs little now and lets Milestone 2
run compaction beside other writers.

**1.7 Not SlateDB.** SlateDB is a well-built LSM on object storage with one
writer per database ("SlateDB ensures a single writer through a formally
verified manifest fencing protocol, but supports multiple readers",
[slatedb.io](https://slatedb.io/), read 2026-10-26), no published plan for
multiple writers, and no atomic commit across databases. Milestone 3's writers
would each need a database, breaking cross-partition commits and remote
maintenance, or a funnel process, an added service. It also does not fsync on
a local directory, and its compaction of a large mutable keyspace solves a problem
decision 1.4 avoids. Its fencing and GC primitives (`slatedb-txn-obj`) informed
the GC design.

**1.8 Two numbers: commit numbers name states, `seq`s order mutations.** Every
accepted record, data or maintenance, gets the next commit number, which names
the state after it: snapshot tokens and checkpoints use it.
Cayenne's sequence numbers (`seq`s) stay as they are, drawn per write per
table while buffering. Within each table, every `seq` in a record is above
every `seq` in earlier records, so `seq` order is commit order and Milestone 1
changes no allocation or stamping code. Iceberg splits snapshot and data
sequence numbers the same way.
*Details:* [Commit numbers and sequence numbers](object-native-metastore-technical.md#commit-numbers-and-sequence-numbers).

- *Alternative: name states by `seq`.* Compaction and schema changes draw no
  `seq`, so two states and their checkpoints would share a name (the model
  finds both). Rejected.
- *Alternative: one `seq` rule per dataset.* It refuses correctly allocated
  records that span partitions, which have their own allocators (the model
  finds it); one allocator per dataset changes code per-table transactions
  depend on. Rejected.
- *Alternative: `seq` blocks reserved per writer.* `seq` order would diverge
  from commit order, and every cross-key comparison would be rewritten.
  Rejected.

**1.9 Source positions commit with the state they cover.** CDC positions live
today in `spice_sys_*` sidecar tables, written separately from data. Here an
`AdvanceSourcePosition` action rides in the same record as its state, so a
replacement resumes without the old instance's disk. A backfill beside live
CDC adds one rule: a live change after the backfill's snapshot position wins.
*Details:* [Source positions and backfill](object-native-metastore-technical.md#source-positions-and-backfill).

**1.10 GC deletes removed objects, data files and deletion vectors included,
after a fixed retention, collects uploads that never committed by age on the
store's clock, and readers never write.**
A read returns the object it references or fails the query, so GC's timing
decides only which long queries can fail, never which rows a query returns.

- *Removed objects* are deleted a fixed retention T (about an hour) after GC
  observes the removal. The log says what left, so no LIST is needed, as in
  Delta Lake and Iceberg.
- *Pending removals are durable:* checkpoint roots carry them until the
  delete, and a takeover restarts their timers.
- *Readers write nothing.* A query starts from a head confirmed within S of
  when its GET was sent, so every object it needs survives at least T − S.
  One that outlives that may fail, with an error saying to rerun it; it never
  gets other rows. A read-only node needs only read access.
- *Uploads that never committed* are collected by age on the store's clock.
  Every object has a fresh name (its identity plus its owner: the leased unit
  that created it and that unit's generation). GC deletes an unreferenced upload once the head's
  `LastModified` is T_O (about a day) past the upload's. A writer references
  only objects live in its CAS base or its own never-published uploads, and
  refuses an upload once its base's store time is T_O minus a margin past the
  upload's, uploading again instead. So a commit landing after GC read the
  head would have refused the upload. No head move or recheck is needed for
  safety; GC's empty commit on an idle head only lets ages advance.
- *No clock is compared across machines.* Ages are differences of store
  timestamps; GC times removals on its own clock.
- *Versioned buckets and one-way replication* are supported.
  Data replicates in any direction. The state has one writable copy, because a
  conditional write is atomic only within one bucket; it may replicate one way,
  to a copy nothing writes, with no failover to it. On a versioned bucket, GC
  deletes without a version id so the delete marker replicates, then removes
  noncurrent versions.
- T and T_O are internal constants. An upload that cannot commit within T_O is
  split across commits; if that binds, a `PREPARE_UPLOAD` record can protect it
  by reference until it commits or a `RELEASE_UPLOAD` drops it.

Model: `retention*`; with any rule removed, a committed state references a
deleted object or a query within the retention fails.
*Details:* [Garbage collection](object-native-metastore-technical.md#garbage-collection).

- *Alternative: leased reader pins,* as SlateDB's readers keep checkpoints
  alive. Rejected: a paused reader fails past its lease anyway, and pins cost
  every reader write access, renewals and a recheck. Delta Lake, Iceberg and
  Hudi protect readers with a retention bound, and none has readers register.
- *Alternative: object generations* that GC lists only once no live writer uses
  them. Rejected: it avoids listing the whole prefix, but needs generation closes across nodes, an interlock, an
  admission guard for reused addresses and repeated sweeps.
- *Alternative: the writer ages uploads on its own clock, and GC waits for the
  head to move and rechecks.* Rejected: a store clock running ahead lets GC
  delete an upload the writer thinks young.
- *Alternative: GC times candidates from the first LIST that saw them.*
  Rejected: GC would remember listings, and a restart restarts every wait.

**Data files and deletion vectors follow the same rule.**

- *A record that removes a file retires it.* `RemoveFiles`, `ReplaceFiles`,
  `RemoveDeleteFiles` and `ClearDeleteFiles` name exactly the files that leave
  the live set, and checkpoint roots carry them as retired until they are
  deleted. A file that a compaction carries is never removed, so files shared
  between snapshots need no extra rule, as in Delta Lake, DuckLake and Iceberg.
- *A retired file is deleted T after the sweep observes its removal,* by name,
  never by a listing or a prefix, and one record then forgets it. Its own
  retired entry does not block the delete; a live or prepared entry that names
  the key does. The timer is in memory: a restart restarts it from the retired
  set, which only delays a delete.
- *Every instance polls its heads,* so a query on any instance, the writer's or
  another's, reads its files for at least T − S.
- *Orphans* follow decision 1.12, not T_O.
- *Why about an hour.* Delta Lake keeps removed files for 7 days and Iceberg
  keeps snapshots for 5 days by default, because both serve time travel. Here
  no query reads an old snapshot on purpose, and a restore point holds the
  deletes it needs. The cost is about T × the bytes retired per hour: a 10 GB
  table refreshed in full every 5 minutes keeps about 120 GB more (arithmetic,
  not measured). T for data files is an
  [open decision](object-native-metastore-technical.md#open-decisions).
- *Alternative: local pins and a short grace only.* Rejected: it protects only
  scans on the writing instance, so a query on another instance, such as the
  old one during a rolling restart, could lose its files at once.

*Details:* [Data files: retirement and orphans](object-native-metastore-technical.md#data-files-retirement-and-orphans).

**1.11 Staged writes are removed: data files are written once, at their final
keys, and the commit names them, in one record.** Today an append or a keyed
write puts its files in a staging folder with a write-ahead file, then copies
them into the snapshot folder. This decision removes the staging folder, the
write-ahead file, the copy and the recovery code that finishes an interrupted
move. Every write puts each data file and deletion vector at a unique key in
its table's folder, flat, not in a folder per snapshot. The record that names
the file is its commit, and a keyed write, such as an upsert or a CDC burst,
commits its files, the deletes that hide the rows it replaces, and its source
position in that one record. A crash leaves the whole write or none of it, so
recovery has nothing to finish. A retry after an unknown outcome writes new
keys, so no key is written twice. A compaction names the files it carries
unchanged, so snapshots share files. Delta Lake, Iceberg, Hudi, Lance, DuckLake
and SlateDB all write data files this way.
*Details:* [Writing at final keys](object-native-metastore-technical.md#writing-at-final-keys).

- *Alternative: a folder per snapshot,* as Cayenne keeps today. Rejected: it
  makes each compaction copy every file it does not rewrite, about one copy per
  file in the table, and the copies run one request at a time (a 128 MB copy
  measured P99 2.2 s on S3 Standard). Copied files lose their caches, and
  position deletes do not survive a copy. None of Iceberg, Delta Lake,
  DuckLake, Lance, Hudi or Paimon keeps a folder per version.
- *Alternative: keep staging and copying.* Correct, but each staged file adds a
  write-ahead file, a copy, two deletes and about eight sequential round trips,
  and keyed CDC waits for them before it acknowledges the source. Rejected.
- *Alternative: final keys with two commits for a keyed write.* A crash between
  the two leaves the deletes without their replacement rows, as a
  fault-injection test showed. Rejected.
- *Alternative: declare each key in the log before its upload,* as Hudi's
  markers do. It adds a head write to every write. Rejected: decision 1.12
  protects writes in flight without it.
- *Alternative: S3 Express `RenameObject`.* It moves no data, but only
  directory buckets have it, and nothing needs a rename once files are at
  their final keys. Rejected.
- *Alternative: staging on local disk only.* Two protocols to prove. Rejected.
- *Alternative: the writer's lease generation in each key,* so a sweep can
  recognize a fenced writer's upload without a clock. Not adopted: the lease
  already keeps such an upload out of every commit, and the orphan sweep
  reclaims it (decision 1.12).

**1.12 The metastore is the only record of a dataset's live files; a listing
only finds garbage.** Scans, compaction's choice of inputs, sizing, and every
file list a write records take their files from committed state or from the
files the writer itself wrote. A manifest that cannot be read is an error,
never a reason to list. A listing treats "under the prefix" as "committed": with
files at their final keys, any path that lists would adopt a crashed or fenced
upload into committed state. Listing also costs requests on every write.
*Details:* [The live set is committed state](object-native-metastore-technical.md#the-live-set-is-committed-state).

- *Orphans.* An upload that no commit names is an orphan: it never becomes
  visible. One periodic sweep lists the dataset's data path and deletes an
  object only when no committed state names it, no write of this process holds
  it, and it is older than a floor of one hour on the store's clock: the
  head's `LastModified` against the object's own.
- *What protects a write in flight is the writer's record, not the floor.* The
  writer keeps, in its process, the keys it has written and not committed, and
  only the lease holder sweeps. The floor is a backstop.
- *Alternative: the manifest as a mirror of a listing,* rebuilt after each
  write. Rejected: the listing decides what is live.
- *Alternative: a floor alone,* as Delta Lake, Iceberg and Lance use. Rejected:
  an upload that stalls longer than any floor is deleted before its commit, the
  failure SlateDB documents.
- *Alternative: intent records in the log.* Rejected in decision 1.11.

**1.13 One object-store interface for every data operation, with a store per
dataset.** Every read, write, copy, rename, delete and list of a data file goes
through an `ObjectStore`, for a local directory as for a bucket. Each dataset
has its own store, scoped to its data path and registered under a URL of its
own. A local directory is also given to the few host features that need a
filesystem path. Local data files are flushed as the metastore's local writes
are. One path removes a class of defects in which the object-store branch is
the one less tested, and the interface itself adds no measurable cost.
*Details:* [Storage: one store per dataset](object-native-metastore-technical.md#storage-one-store-per-dataset).

- *Alternative: the filesystem on local disk, a store on S3.* Two
  implementations of every write and lifecycle step. Rejected.
- *Alternative: one process-wide store router keyed by bucket or by
  `file://`.* A dataset in the same bucket as a source would share or replace
  the source's store, and replacing the session's `file://` store changes it
  for every other reader of local files. Rejected.
- *Alternative: no local directory on the handle.* A host feature would have
  to ask a store for free space or a folder to archive, which a bucket does not
  have. Rejected.

**1.14 The table path in the metastore is the placement record, stored
relative to the location.** A dataset in the location records
`datasets/<folder>/data`, which is resolved against `runtime.state.location`
when it opens, so a copied or restored location stays self-contained. A data
path outside the location is recorded as given and keeps working where it is;
it is never moved. Every table entry already holds its path, and a second field
could disagree with it.

- *Alternative: a location id on each file reference.* Needed only for several
  data locations, which are out of scope. Rejected for Milestone 1.
- *Alternative: absolute URLs.* A copy of the location at another URL would
  read the original's files. Rejected.

## Milestones 2 and 3

Decisions 2.1 and 2.2 are what Milestone 2 needs; 3.1 to 3.5 come with
Milestone 3.
*Details:* [Milestones 2 and 3](object-native-metastore-technical.md#milestones-2-and-3-concurrent-writers).

**2.1 A writer that loses a race retries if the winners did not touch what it
depends on.** It checks its guards and footprint against the winners, with
four classes: compatible (retry unchanged), rebasable (transform the actions,
such as remapping a position delete onto a compaction's outputs), retryable
(re-plan) or incompatible (return the conflict). This is safe because actions
never carry values computed from what was read.

**2.2 Leases coordinate work; guards guarantee correctness.** CDC writers hold
one lease per source partition, and maintenance nodes lease a deterministic
unit. A lost lease or a racing node fails its guard and wastes work; it cannot
corrupt state. A lease turnover is a head commit, because the unit's
generation is in the dataset's state, so a plan from an older generation loses
its CAS. A retry checks the unit's generation in the head it lost to before
rebasing, and an expired unit re-plans. Model: `remote-maintenance*`.

**3.1 Commits publish key footprints,** checked cheapest first: PK hash buckets
and per-file min/max, then the keys as sorted 64-bit digests compared by one
linear merge. A digest collision can only report a false conflict. Bloom
filters are not used: at 1% per key, a 1,000-key commit almost always matches,
so they decide only small commits and the digests are needed anyway.

**3.2 Overlapping keys are resolved by the writers:** abort-and-re-plan or
key-range ownership. Disjoint writers need neither. Source-version ordering is
deferred until a concrete use case justifies its semantics and surface.

**3.3 Distributed execution writes by source partition, not table partition.**
A distributed query commits once with every successful task's output;
distributed ingest gives each executor source partitions, as an independent
writer.

**3.4 Open: how a writer that loses a race keeps `seq` order.** Its buffered
`seq`s are below the winner's, and decision 1.8's per-table rule must hold.
*(a) Shift the buffer* above the winner's in each shared table: no stamping
change, but it re-stamps the window's keys in the in-memory indexes, bounded by one commit
interval of the writer's own work. *(b)
Coalesce eagerly and stamp at commit:* the buffer tracks affected keys and
inserted rows, local reads treat it as newest, and `seq`s are stamped when the
commit succeeds; it changes the mem-tier's apply path. (a) unless measuring
the shift against a retry's round trips shows it is expensive. Independent of
publishing at commit.

**3.5 Read-dependent writes validate what they read.** An `UPDATE … WHERE`,
`DELETE … WHERE`, `INSERT … SELECT` or transaction records the keys it looked
up and the predicates it scanned. On a lost race it conflicts only with
winners that wrote one of those keys or changed a row matching one of those
predicates, before or after the change, and re-runs on the new head. Every
commit's state then equals running the accepted writes one at a time in
commit order. Checking written keys alone is not enough: with rows
`r(a=0, b=1)` and `s(a=1, b=0)`, `UPDATE t SET b = b + 1 WHERE a = 1` writes
only `s` and `UPDATE t SET a = a + 1 WHERE b = 1` writes only `r`. Their written
keys are disjoint, yet both commit to `r(1, 1), s(1, 1)`, where either serial
order gives `s(2, 1)` or `r(1, 2)`. Until this lands, any commit to a table the write read is a
conflict.

- *Alternative: one read-dependent write at a time,* with blind writes checked
  only by footprints. Rejected: a blind upsert can turn a row into one the
  predicate matches, so validation against blind writers is needed anyway.

---

# Related storage

## The datalake tier

Today's datalake tier is about where data lives; that belongs to the
object-native Enhancement ([#16245](https://github.com/spiceai/spiceai/issues/16245)). This design is about object-native
metadata. Milestone 1 maps the tier as it is: a promotion is one record, the
new cold manifest plus the removal of the promoted warm files, and its GC roots
in the destination. Existing datalake locations keep working.

## Multi-region

Not in any milestone. Replicating data, and the state one way, is Milestone 1
(decision 1.10). These rules keep the rest possible:

- **Write in one region, read in others.** Regional read-only replicas are a
  separate Enhancement. Writers, the lease and GC use the source. Regional
  readers wait for replication by default: a reader adopts a head only once
  every object it newly references is in its replica, so no query reads across
  regions.
- **Writers in several regions** over disjoint keys use one bucket; overlapping
  keys need key ownership; no commit is atomic across regions.

---

## Open decisions

The open decisions, with what decides each and the step that needs it, are one
list in the
[technical design](object-native-metastore-technical.md#open-decisions).

## User-facing surface

The surface (the flag, the folder layout, defaults, deprecations, the upgrade
import, errors, logs and metrics) is in
[Enhancement #16245](https://github.com/spiceai/spiceai/issues/16245). Nothing
in this design adds a user decision beyond `runtime.state.location`.
