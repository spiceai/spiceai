# Cayenne object-native metastore

Status: **draft design for discussion.** This document states the direction: the
goals, the milestones, the key decisions and the alternatives behind them. The
[technical design](object-native-metastore-technical.md) holds the invariants,
how Cayenne works today where this changes it, and the details, with the
measurements and model checks behind them.

These documents and the protocol model capture the current direction: complete
enough to build toward, and expected to change. Implementation will update the
design and the model together.

---

# What and why

## The product problem

Every Cayenne acceleration depends on a metastore. Today that is an embedded
SQLite database holding every table's snapshot pointers, file manifests,
deletion state, statistics and its small-write tier. That choice shapes what
Spice can offer:

- **State is tied to one machine.** The catalog lives on an instance's disk, so
a replacement instance cannot pick up where the old one left off. Nodes cannot
share an acceleration without exporting and importing metastore slices.
- **One writer.** SQLite serializes every write in a process, and nothing lets
a second instance write the same table. Log, event and metric ingest from many
instances, per-partition CDC, and distributed maintenance are all out of reach.
- **More to choose and operate.** Users and operators meet modes, metastore
backends, and SQLite-specific failure modes (busy timeouts, WAL growth,
freelist vacuum), none of which they asked for.

## What we are building

**One metastore for Spice and Cayenne that is native to object storage** and
runs unchanged on a local directory:

- The user chooses **where durable data lives**: a bucket, a local directory,
or `mem://` for tests. That is the only storage decision. There is no
metastore choice, no tuning knob, and no external coordination service or
database.
- Durability is the destination's. With an object store, acknowledged state
survives losing any instance and its disk. With a local directory, it is as
durable as the disk the user chose.
- **The data moves too, not only the metastore.** Today a file-mode dataset
keeps its Vortex and deletion files on the instance's disk, or in S3 Express
where configured, and only the instance's metastore says which are live.
Here, every file a commit references is written to the destination before
the commit, and local disk becomes a cache. This makes the change larger than
swapping the metastore, and it is deliberate:
  - A commit that referenced files on one instance's disk could not be
    recovered after losing that disk, or read by any other instance.
  - A file is invisible until a record references it, so writing the files
    first and then the record makes the record's one conditional write commit
    both. No commit has to span the instance's disk and the destination.
- **Every existing guarantee holds.** Guarded commits are atomic. A write
across a dataset's partitions commits atomically, and so does a transaction
across tables, as both do in the SQL metastore today.
- **New guarantees.** With an object-store destination, acknowledged state
survives losing any instance and its disk, and any instance can recover or
read it. A CDC source position becomes durable in the same commit as the state
it covers; today the two are written separately.
- **Then, maintenance and standbys on any node.** In a second milestone,
compaction and the bake run on any node, and standby replicas take over
ingestion when the writer fails.
- **Later, many writers per table.** In a third milestone, **many writers can
write one table** wherever their data is independent, without the user
declaring a layout to make that safe.

**Scope: metadata, with data only where the metadata needs it.** How Cayenne
stores its data (Vortex files and deletions) is a separate design. Its
direction is to stop treating local disk as durable storage with an optional
cold tier behind it, and instead treat local disk as a cache in front of one or
more tiers of durable, object-store-backed storage. This document covers data
only where the metastore depends on it: a file must be durable before a commit
references it, and the metastore records where each file lives.

### Consistency

What holds, in common terms:

- **Commits to one dataset are serializable.** One compare-and-swap of the
  dataset's head orders every commit, and each commit's guards are checked
  against the exact state it follows.
- **Every read sees a consistent snapshot** of committed state, and a node's
  view only moves forward (monotonic reads).
- **Across nodes, reads are eventually consistent, with bounded staleness.**
  Another node sees a commit once it refreshes its cached copy of the head,
  which a query does when the copy is older than the staleness bound. So the
  delay is the commit cadence plus that bound, while the destination is
  reachable; when it is not, a node keeps answering from the last state it
  read, which can be older. With several writers (Milestones 2 and 3), different
  nodes can briefly observe different recent states.
- **Read-your-writes holds on the node that wrote,** not across nodes.
- **On the writing node, buffered writes are queryable before they commit,** as
  they are today. A read there can therefore see a value that is never
  committed: a crash, a fence, or an unknown commit outcome drops the buffer,
  which was never acknowledged. Other nodes only ever see committed state.
  Publishing at commit (decision 1.2) would remove this.

What does not hold: linearizable reads, read-your-writes across nodes, and
atomic reads across datasets (non-goals below).

## Non-goals for this work

- **Linearizable reads, and read-your-writes across nodes.** A node answers
  from the state it last read, so a client that writes through one node and
  reads through another may not see its own write until the second node polls
  (see [Consistency](#consistency) for what does hold). Routing a client's
  reads to the writer, or a read that waits for the client's commit number,
  would add it later (see
  [possible future improvements](#milestone-n-possible-future-improvements)).
- **Highly contended writes.** The design assumes writes are mostly
  non-conflicting: writers of partitioned CDC, appenders, and owners of disjoint
  keys rarely touch the same keys. Heavy contention on shared keys would need a
  coordinated commit layer on top of this one (see
  [possible future improvements](#milestone-n-possible-future-improvements)).
- **Atomic reads across datasets.** A query that reads several datasets sees
  each one at its own snapshot, as it does today. Snapshot tokens and
  transaction outcomes are designed so a consistent cross-dataset read can be
  added later (decision 1.3).
- **One atomic commit across regions**, and **source-version ordering** for
  overlapping keys: deferred, as described in Milestone 3.
- **An interchange table format.** This is Cayenne's internal format, as its
  on-disk layout is today.

## How it works, in one paragraph

Each accelerated dataset has one small mutable object in the destination,
the **head**, which carries the dataset's most recent commit records. A commit
is **one conditional write**: a compare-and-swap of the head, which succeeds
only if no one else replaced it first. Success is acceptance, and only an
accepted commit is acknowledged. One instance at a time is a dataset's
writer: it holds the dataset's **writer lease**, a small object in the
destination that it renews. Taking the lease over raises the dataset's **lease
generation**, which the head records, so a former writer's next commit fails.
The lease decides who writes; the generation in the head is what makes a
stale writer harmless. Older records leave the head in immutable
batches, and the head lists them. Records carry typed, file-level actions
("add these files", "add these deletion vectors", "replace these files with
their compaction"). Data files and bulk content such as file manifests and the
primary-key index are immutable objects that the actions reference. Writers
buffer changes and commit only when something must be acknowledged — a CDC
source position every ~2 s, a SQL `COMMIT`, or a freshness bound — so remote
writes scale with acknowledgements, not with ingest volume. A reader learns of
new commits, and reads them, with one conditional read of the head. Periodic,
incremental checkpoints keep open and recovery cost bounded. From Milestone 2, a
writer that loses the race for the head checks whether the winning commits
touched anything its commit depends on. If not (a compaction of other files,
and in Milestone 3 appends, disjoint keys or disjoint source partitions), it
simply retries on the new head.

## Relationship to the datalake tier

Cayenne already puts data in object storage through its optional **datalake
tier**, enabled by `cayenne_datalake_location`. The datalake tier is about
data; this work is about metadata, and touches data only as the metadata
references it. The two answer different questions:

- **The datalake tier decides where cold rows live.** A background promoter
  moves rows from local disk to S3, once local disk passes a threshold, as
  large clustered Vortex files. The list of live cold files, with their
  statistics and key bloom filters, stays in the local SQLite metastore.
- **This work decides where the commits and their metadata live.** It moves
  the metastore itself into the destination, alongside the data.

**How they combine.** With this design, the datalake tier is not needed as a
separate feature. It is an early form of
[tiered storage beneath the destination](#milestone-n-possible-future-improvements):
it moves data, but not metadata, from local disk to S3.

- **With an object-store destination,** every data file is already in the
destination, so there is nothing to promote, and local disk serves as a
cache. What the tier does well becomes compaction and layout policy:
carry-forward rewrites of only the changed files, Hilbert clustering for
tight zone maps, large read-optimized files, and per-file statistics and key
bloom filters for reads.
- **With a local-directory destination,** moving cold data to S3 is the
tiered-storage item itself. That item would put commits and checkpoints in
the lower tier too, so the S3 tier is recoverable on its own.
- Either way, its separate location, thresholds and GC are no longer needed.
- **Milestone 1 maps the tier as it is.** A promotion becomes one record: the new
cold manifest plus the removal of the promoted warm files
([mapping, and a comparison with the tier today](object-native-metastore-technical.md#cayenne_cold_tier_file)).
Its GC then roots in the destination instead of the local metastore.
- **No existing data is stranded.** Datasets whose cold files are already in a
datalake location keep working. Folding the tier into compaction, and
retiring its settings, waits for re-bootstrap, compatibility and recovery to
be shown. Retiring the settings is a user-facing change, so it goes through the
same Enhancement review as the rest of the
[user-facing surface](#user-facing-surface-proposal-for-the-enhancement).

**One gap this closes.** The tier's GC has no reader pins, so a query still
reading cold files when GC sweeps them fails. A Cayenne integration test that
forces that interleaving reproduces it, and reader pins (decision 1.10) remove
it.

## Milestones

Three milestones are planned, and each removes the main limit left by the one
before it. All three are planned in full. Milestone 1 is firm. Milestones 2
and 3 are settled in their decisions, with the open points listed where they
arise: each waits on a measurement or a model check, and none on a new
design.
[Possible future improvements](#milestone-n-possible-future-improvements) lists
work that is not planned, kept here only so that the planned milestones do not
rule it out.

### Milestone 1: one object-native metastore

**What:** every accelerated dataset's metastore and data files live in the
user's chosen destination, written by one writer per dataset and readable by
any instance, with no SQLite or Turso.

**Why:** the metastore is the reason acceleration state is tied to one machine.
Making it native to object storage, with one implementation for every
destination, is the prerequisite for everything else.

**Delivers:**

- SQLite and Turso replaced everywhere, by one implementation for object-store
and local-directory destinations.
- A replacement instance that recovers from the bucket without a reload.
- State any instance can read directly, which Milestone 2's replicas build on.
- No metastore choice and no metastore knobs.

**Limits it leaves:**

- **One writer per dataset.** Ingest throughput for a dataset is capped by what
one node can decode, encode and commit. Log, event and metric ingest from many
instances still has to funnel through that node.
- **Maintenance shares the writer's node.** Compaction and the bake compete with
ingest for that node's CPU and I/O. A heavy dataset cannot spread maintenance
across the cluster.
- **Automatic failover waits for the lease.** Safety never depends on the
lease: a new writer fences the old one by recording its generation in the
head, and every older writer's next commit then fails. So a replacement that
is told it is the writer (a restarted pod, an orchestrator) takes over at
once. The lease only decides which of several candidate instances becomes the
writer; a candidate waits for the holder's lease to lapse, so writes pause for
up to the lease duration while reads continue.

### Milestone 2: maintenance and standbys on any node

**What:** one ingest writer per dataset, as in Milestone 1, while compaction, the
bake and cold promotion run on any node, and standby replicas take over
ingestion when the writer fails.

**Why:** Milestone 1's sharpest operational limit is maintenance competing with
ingest on one node. And while Milestone 1 fences and replaces a failed writer, a
replacement has no source handoff: nothing yet stops a standby from consuming
the source before it holds the lease. Both lift with much less than many
writers need. Maintenance
records only rewrite files: they draw no new `seq`s and raise no table's
high-water mark, so with one ingest writer they need no key footprints and
no answer to Milestone 3's `seq` decision (decision 3.4). The protocol model
checks this: one ingest writer racing maintenance on other nodes keeps every
row correct with neither.

**Delivers:**

- Maintenance on any node, coordinated by leases over file sets, racing the
  writer on the head with guards (decisions 2.1 and 2.2).
- **Replicated ingestion behind a lease.** Several replicas run the same
  Spicepod against one destination. A lease picks the replica that consumes
  the source and writes; the others answer queries from the shared state and
  take over when the lease lapses. Milestone 1 supplies what makes this safe:
  - *Fencing.* A takeover records the new writer's generation in the head, so
    a former holder that still believes it holds the lease fails its next
    commit instead of writing. Safety never depends on lease timing; only
    failover delay does, as in Milestone 1's failover limit.
  - *Durable resume.* Source positions commit with the state they cover
    (decision 1.9), so the new holder resumes from the head, not from the old
    holder's disk.
  - *Shared reads.* Any instance can already read committed state from the
    destination, so standbys answer queries from it.

  What each source needs on top:
  - *Consume only while holding the lease.* A PostgreSQL replication slot
    accepts one active consumer, so a standby must not attach until it holds
    the lease. An SQS queue delivers to any replica that polls, so a standby
    must not poll.
  - *Acknowledge the source only after the commit.* The holder confirms the
    slot's flush position, or deletes the SQS messages, only once the record
    covering them has committed: the acknowledgement points of decision 1.2.
  - *Replay after takeover.* PostgreSQL resumes from the committed position.
    SQS has no position: messages not yet deleted reappear after their
    visibility timeout, so the new holder must skip files the log already
    records.

**Limits it leaves:**

- **One ingest writer per dataset.** Ingest throughput for a dataset is still
capped by what one node can decode, encode and commit, and log, event and
metric ingest from many instances still funnels through that node.
- **Read-dependent writes on a dataset stay on the writer,** as in Milestone 1.

### Milestone 3: concurrent writers

**What:** many writers commit to one dataset at once, wherever their data is
independent.

**Why:** to remove the one-writer limit without asking users to declare a
layout.

**Delivers:**

- Many appenders to one table. Replicated ingestion from a source of
  independent appends, such as SQS, can then share the work instead of
  standing by.
- Writers over disjoint key sets.
- One ordered CDC writer per source partition.
- Distributed query writes that commit once per job.
- Writers in several regions over disjoint keys, against one bucket.
- `UPDATE`/`DELETE … WHERE`, `INSERT … SELECT` and transactions from any
  writer, serializable in commit order. They conflict only with commits that
  change rows they read, so ingestion of other rows doesn't block them. This
  lands last; until it does, a read-dependent write on a dataset with several
  writers re-plans whenever another commit wrote a table it reads, which is
  safe but can report a conflict under heavy ingestion.

**Limits it leaves:**

- **One log per dataset caps its commit rate.** Each commit is one
conditional write of the head, so a log accepts at most about `1 / (conditional-write
latency)` commits per second. That is on the order of 20 per second on S3
Standard, and hundreds on S3 Express or a local disk. Several logs per dataset
would lift this cap; that is a possible future improvement, not part of this
plan.
  - At a 2 s commit cadence per writer, races grow with writer count, and tens
  of writers per dataset approach the cap.
  - Independent writers that lose a race retry rather than abort, but each
  retry adds a read and a write.
- **Cross-region writers pay cross-region latency on every commit,** and depend
on the bucket's home region being available.
- **Writers with overlapping keys still need a resolution:** abort and retry, or
key ownership. (Source-version ordering is deferred.)

### Milestone N: Possible Future Improvements

None of the items below is part of this plan or scheduled. They are listed so the
planned milestones keep them possible.

**Several logs per dataset.** If one log's commit rate saturates, or each region
should commit to a log of its own, a dataset's state could become the merge of
several logs, each written by one group of writers:

- A snapshot would be a position in each log. Milestone 1 keeps snapshot positions
opaque, so this would need no format break.
- Milestone 3's footprints would prove writers on different logs independent.
- An atomic commit across logs would use the same decision-object protocol as
cross-dataset transactions.
- Placement (which writers use which log) and atomicity across logs would need
their own design first. Nothing here promises that sharding would be automatic.

Its own limits:

- An atomic commit across logs costs an extra round trip.
- Overlapping keys across logs still need abort-and-retry or ownership.
- One atomic commit across regions stays out of scope.

**Coordinated commits for contended workloads.** If many writers contend for
the same keys, optimistic retries waste work. A lease-elected committer could
take writers' changes and batch them into single records, which removes the
race without an external service. Delta Lake's coordinated-commits feature takes
a similar approach, though with a separate coordinator.

**Atomic reads across datasets.** A catalog transaction frontier: a record of
which `(dataset, commit)` vectors are mutually consistent, so a multi-dataset query
reads one consistent cut. Milestone 1 keeps snapshot tokens as vectors and makes
cross-dataset outcomes visible only through log records, so this needs no format
change.

**Cross-node consistency.** Read-your-writes or linearizable reads across nodes,
for example a reader that waits until the log reaches a given commit before
answering. The planned milestones keep the pieces this needs, and nothing more has to
be decided now:

- *One linearization point per dataset.* Every commit is one replace of the
  head, and object stores give strong read-after-write consistency, so a
  reader that reads the head and applies up to its commit before answering
  gives a linearizable read of that dataset.
- *Commit numbers as tokens.* Snapshot tokens are `(dataset, commit)` vectors.
  Returning the commit number with an acknowledgement is an API addition, not
  a format change, and gives clients a read-your-writes token.
- *Committed state is distinguishable from buffered writes.* Other nodes only
  ever see committed state. On the writing node, a strict read would exclude
  the buffer, or the dataset would publish at commit (decision 1.2).
- Serializable reads across datasets build on the atomic cross-dataset read
  item above.

**Tiered storage beneath the destination.** Writes could pass through a stack of
tiers: an ephemeral disk buffer, then a store on persistent disk, then S3.
The acknowledgement point happens once the data is committed to the first
object store tier. The user can then choose the first object store with the
commit latency, cost and durability they desire.

---

# Milestone 1: one object-native metastore

**Scope.** Replace SQLite and Turso as Cayenne's metastore with a head object,
its batches and immutable objects in the destination, for every deployment: a laptop, an
on-prem server with a local directory, and a node backed by S3, GCS or Azure.
One writer per dataset holds a lease; any number of readers.

**What the user sees.** Nothing new to configure. A restarted or replacement
instance pointed at the same bucket recovers the acceleration without a full
reload. Running several instances on one destination (replicas and
standbys) comes with Milestone 2, also with nothing new to configure.

**What it removes:**

- SQLite and Turso as metastores (the `turso` build feature goes earlier, with
the accelerator deprecation). Replacing them is the goal of this work, but the old storage is removed only after the new path has
proven its durability, re-bootstrap and operating behavior.
- **Running a database inside every instance:** its locking, tuning and
  upkeep, such as busy timeouts, write queueing, checkpointing and vacuum.
- **Copying acceleration state between instances.** A replacement instance
  reads the destination directly instead of importing a metastore slice, and
  so do Milestone 2's replicas.
- **A metastore round trip on every write.** Writes commit once per
  acknowledgement point.
- **A separate small-write tier inside the database,** once its replacement is
  measured (decision 1.5).

## Key decisions

**1.1 One conditional write commits: a compare-and-swap of the dataset's head.**
The head carries the newest commit records, so a CAS on its version both
orders a commit and accepts it, and a stale writer's CAS fails. Guards are
checked locally against the exact version the CAS replaces, and acceptance is
all-or-nothing for the whole record. A lost response resolves to *accepted*,
*rejected* or *unresolved* by reading the head, or by retrying the identical
CAS. Older records move into immutable batches that the head lists. This is
the commit point the existing `object_store_occ` WAL already uses, extended so
that a reader needs one request per poll and a new reader opens in two
parallel steps. Apache Iceberg commits the same way, by swapping a metadata
pointer, and S3's conditional writes (`If-None-Match` in August 2024,
`If-Match` in November 2024) are what let the object store itself be the
coordinator.
*Details in:* [Summary](object-native-metastore-technical.md#summary),
[The write path](object-native-metastore-technical.md#the-write-path).

- *Alternative: a numbered log,* creating `log/<pos>` per commit, as Delta
  Lake and SlateDB do. Prototyped, model-checked and timed against the head.
  Creating a record orders a commit without accepting it, so the log needs a
  GC boundary checked after every create, a no-op fill and fence records, and
  its readers need about twice the requests per poll and slower cold opens.
  Rejected.
- *Alternative: append to a log object.* S3 Express, Azure append blobs and GCS
  zonal buckets support a conditional append; S3 Standard and most
  S3-compatible stores do not. Every provider bills an append as a write, so
  appending would not reduce billed writes. Rejected as inconsistently
  supported and no cheaper.

**1.2 Commit when something must be acknowledged, not on every write.** Writes
accumulate in a buffer and are committed as one record when one of these
happens. Each is an *acknowledgement point*: something is waiting for an
acknowledgement. It triggers the commit, and the acknowledgement itself is sent
only after the commit succeeds.

- a CDC source position must be acknowledged (~2 s);
- a SQL `COMMIT` returns;
- a freshness bound for other readers is reached;
- the buffer hits a size or age bound.

The buffer is in memory — the existing RAM mem-tier already plays this role —
and losing it loses only unacknowledged work, which the source replays.

**Milestone 1 keeps today's local visibility.** A buffered write is queryable on
the writing node as soon as it is applied, as the mem-tier does today, and on
other nodes once its commit succeeds and they poll. So a read on the writing
node can see a value that is never committed (see [Consistency](#consistency)).

- *Option: publish at commit.* Buffered writes would become queryable, on the
  writing node too, only once their commit succeeds, so no read would ever see
  a value that is never committed. The cost is freshness on the ingesting node:
  up to one commit interval plus the conditional write, about 2 s plus
  30–100 ms on S3 Standard. It is independent of how `seq`s are stamped
  (decision 3.4), and would be chosen if reads of uncommitted values
  must be ruled out.

On a
local-directory destination, each commit is written durably (fsync) as it
happens. At S3 Standard request prices, a dataset costs roughly \$17/day if every
CDC burst commits (20/s), about \$0.50/day at a 2 s cadence, and about \$0.02/day
at 60 s.
*Details in:* [The write path](object-native-metastore-technical.md#the-write-path).

- *Alternative: a persistent disk-backed buffer.* It would shorten replay after
a crash, but it is not needed for correctness and adds a second log format.
It is not in Milestone 1. The general form, tiers of storage beneath the
destination, is listed under
[possible future improvements](#milestone-n-possible-future-improvements).

**1.3 A transaction domain per dataset.** There is one log per accelerated
dataset, so a commit across a dataset's partitions is one record. A small
catalog log holds the name registry. The rare multi-table transaction spanning
datasets uses a create-if-absent decision object. One log per dataset gives the
most concurrency Milestones 2 and 3 can use.

No Cayenne query reads several tables at one atomic snapshot today, and
per-dataset logs keep that behavior. The design keeps a consistent
cross-dataset read possible later: snapshot tokens are vectors of
`(dataset, commit)`, and a cross-dataset outcome becomes visible only through a
`Resolve` record in each dataset's log.
*Details in:* [Multi-table transactions across datasets](object-native-metastore-technical.md#multi-table-transactions-across-datasets).

- *Alternative: one head per catalog.* Every dataset's commits go through one
  head.
  - *Gains:* a transaction across datasets is one record, with no `Prepare`,
    decision or `Resolve`, and every query reads all datasets at one position,
    so cross-dataset reads are atomic.
  - *Costs:* one head is replaced at most about once per conditional-write
    round trip (about 20 per second on S3 Standard), for the whole catalog.
    One process writing every dataset can fold all of them into each
    replacement with group commit, but 100 datasets each committing every 2 s
    already need 50 commits per second from several processes, which only a
    single committer per catalog can sustain. Any second writer (a node owning
    other datasets, a maintenance node) races on every commit, so Milestone 2's
    retry machinery would be needed from the start, and contention would be the
    norm rather than the exception. Every changed poll downloads every
    dataset's recent records, a busy dataset's commits fill the window and
    checkpoints of quiet ones, and one lease would own the whole catalog.
  - Rejected. Transactions across datasets are rare, and their two-phase path
    costs about five round trips only when used; atomic cross-dataset reads can
    come later through a frontier. Adopting it later would be a migration.
- *Alternative: one log per transaction group.* Datasets that transact together
  share a log, so atomic reads and writes within a group become ordinary log
  application, at the cost of shared ordering within the group. Rejected: it
  needs configuration, or inference, to decide the groups.
- *Alternative: one log per partition.* Rejected. Writes that fan out to several
  partitions, and partitioned overwrites, commit atomically across partitions
  today, so each would need a distributed transaction. Table-wide state (schema,
  statistics, the PK index, the cold-tier manifest) would have no single home.
  Readers would have to merge logs from day one. And it ties concurrency to
  declared layout, which Milestone 3 sets out to remove.

**1.4 Data out of the log; checkpoints chunked and incremental.** Records carry
small typed actions. Manifests, deletion-vector lists, statistics and
cold-tier manifests are immutable objects named by content hash. A
checkpoint is a small root plus sorted, size-bounded parts, and it rewrites only
the parts that changed. The PK index is one of its collections, so it is
always as of its checkpoint and never needs history GC has removed. Opening a
dataset reads the latest checkpoint plus the records after it.
*Details in:* [Dataset state](object-native-metastore-technical.md#dataset-state-where-it-lives-and-how-it-changes),
[Mapping the metastore tables](object-native-metastore-technical.md#mapping-the-metastore-tables).

**1.5 Small writes are buffered locally; the inline tier is retired once its
replacement is measured.** Today small writes are stored as Arrow IPC rows
*inside* SQLite, to avoid one file per tiny batch. Here they accumulate in the
writer's RAM tier and are written out at the next commit, so the file count is
bounded by commit cadence, not write rate. A small commit, such as a one-row
`INSERT; COMMIT` or a CDC batch of a few KiB, carries its rows inline in its
record, which stays in the head until it leaves in a batch: the commit is one
round trip. The SQL inline tier is
removed only once this replacement meets the bar measured against it, including
a caching workload that depends on it today.
*Details in:* [Small writes and the inline tier](object-native-metastore-technical.md#small-writes-and-the-inline-tier).

**1.6 File-level actions from day one.** Even with one writer, records use
file-level actions (`AddFiles`, `RemoveFiles`, `ReplaceFiles`,
`AddDeleteFiles`), and "current snapshot" is derived state. That costs little
now and is what lets Milestone 2 run compaction alongside other writers.

**1.7 Not SlateDB.** SlateDB is a well-built LSM on object storage, but its model
is one writer per database: "SlateDB ensures a single writer through a formally
verified manifest fencing protocol, but supports multiple readers"
([slatedb.io](https://slatedb.io/), read 2026-10-26). We found no published
plan for multiple writers. It also has no atomic commit across databases. Milestone 3's concurrent writers would each need their own database
(breaking cross-partition commits, and Milestone 2's maintenance on other nodes) or a single
funnel process (an added service). The single-writer model is the deciding
mismatch; smaller ones are that it does not fsync on a local directory, and that
its compaction of a large mutable keyspace solves a problem decision 1.4 avoids
having. Its fencing and GC primitives (`slatedb-txn-obj`) are worth borrowing
from, and the GC design does.

**1.8 Two numbers: commit numbers name states, `seq`s order mutations.** Every
accepted record gets the next commit number, data commits and maintenance
alike, and a commit number names the dataset state after it: snapshot tokens,
checkpoints and reader pins use it. Cayenne's sequence numbers stay as they
are, drawn per write operation per table and stamped while buffering. Within
each table, every `seq` in a record is above every `seq` in earlier records,
so `seq` order is commit order, every existing `seq` comparison keeps its
meaning, and Milestone 1 changes no allocation or stamping code. Iceberg splits
its numbers the same way, between snapshot and data sequence numbers. How a
Milestone 3 writer that loses a race keeps `seq` order is decision 3.4.
*Details in:* [Commit numbers and sequence numbers](object-native-metastore-technical.md#commit-numbers-and-sequence-numbers).

- *Alternative: name states by `seq`, with no commit number.* A compaction or a
  schema change alters state without drawing a `seq`, so two states would share
  a name, and so would their checkpoints; the model finds both. Rejected.
- *Alternative: one `seq` rule per dataset.* Each partition table has its own
  allocator, so a dataset-wide rule refuses correctly allocated records that
  span partitions; the model finds that too. One allocator per dataset would
  fix it, but changes allocation code that per-table transactions depend on.
  Rejected.
- *Alternative: independent `seq` blocks reserved per writer.* `seq` order would
  differ from commit order across writers, needing re-reserve rules and every
  cross-key comparison rewritten against a separate position. Rejected.

**1.9 Source positions commit with the state they cover.** Consumer-side CDC
positions live today in `spice_sys_*` sidecar tables inside the Cayenne
metastore, written separately from data commits. Here an
`AdvanceSourcePosition` action rides in the same record as the state it covers,
so a replacement instance resumes without the old instance's disk. A backfill
running alongside live CDC needs one more rule: a live change after the
backfill's snapshot position wins over backfilled data.
*Details in:* [Source positions and backfill](object-native-metastore-technical.md#source-positions-and-backfill).

**1.10 GC deletes from the log, and reader pins are low-water marks.** GC is
one protocol with fencing, staging and reader pins, and correctness never
depends on a grace period.

- *Files that were once live* are deleted from what the records say left: a
  file leaves the state at the record that removes it, and GC deletes it once
  the floor and every reader pin are past that record. No LIST is needed, as
  Delta Lake deletes from its `remove` actions and Iceberg from expired
  snapshots.
- *Uploads that never committed* are found by listing generations: each
  upload goes into its writer's generation, which in Milestone 1 is the writer's
  lease generation, and after a takeover the new owner sweeps its
  predecessor's. A fenced writer's late upload can leak until a later sweep,
  which costs space, never correctness.
- *Each reading node keeps one pin,* a lease it renews, holding for each
  dataset the oldest commit its queries and open transactions still read. A
  new query needs no write, and a node that dies releases its pin by letting
  it lapse; SlateDB's readers keep their checkpoints alive the same way. A
  long query keeps everything removed since it started, like PostgreSQL's
  xmin horizon. Pins never block maintenance; they only delay deleting its
  inputs.
- *Milestone 2* adds explicit generation closes, because GC on one node cannot see
  another node's uploads in flight.

*Details in:* [Checkpoints and garbage collection](object-native-metastore-technical.md#checkpoints-and-garbage-collection).

- *Alternative: retention by age,* as Delta Lake's `VACUUM` and Iceberg's
  orphan cleanup use. Correctness would rest on a time bound, and the datalake
  tier's GC shows it failing. Rejected.
- *Alternative: a pin per query, or one anchored at the oldest query.* A pin
  per query costs a write each time a query starts on a new commit, and a pin
  anchored at the oldest query alone does not protect files a newer query
  reads; the model finds the loss. Rejected.

## User-facing surface (proposal for the Enhancement)

The surface change needs its own Enhancement with PM and UX/DX sign-off before
it is built. This is the proposal that Enhancement would review.

**Rollout shape.** The new path is developed behind a feature flag, off by
default, so nothing changes for users until they opt in. Once the new path is
proven, it becomes the default, and the flag is removed together with the
surface it makes obsolete: the metastore backend choice, the separate metadata
directory and the small-write inlining settings. The flag's name and shape, and
each deprecation and its timing, are defined and reviewed in the Enhancement,
not here.

**Location: reuse `runtime.state.location` rather than add
`runtime.object_store`.** Spicepods already have one root for shared runtime
state:

- `runtime.state.location` accepts `file://`, `s3://`, `abfs://`, `abfss://`
and `gs://`.
- Credentials go in `runtime.state.params`.
- Each feature writes under its own folder: `results-cache/`, `rate-control/`,
`cluster/` and `snapshots/`.

The metastore, with the data files it references, would be one more folder
in it, `cayenne/v1/`. A second root field would give users two places to
configure the same thing.

```yaml
runtime:
  state:
    location: s3://my-bucket/spice   # existing field; Cayenne uses <location>/cayenne/v1/
datasets:
  - from: postgres:public.orders
    name: orders
    acceleration:
      enabled: true
```

**Questions for the review:**

- **The default when `runtime.state` is unset.** The proposal is the local data
directory (`.spice/data`), the same default file mode uses today, so the
zero-config case still works.
- **What replaces `mode: memory`.** It is today's default. Ideally nothing
replaces it: every dataset uses the same metastore, and the runtime finds that
a table is small enough to hold in memory and serves it from there, so there
is no separate mode to choose. Whether that works depends on measurements and
details not yet available. `mem://` stays a whole-runtime test destination,
not a per-dataset choice. Deprecating `mode` can be decided and scheduled
separately from the metastore work.
- **The layout of data files.** Data files live in the destination (see
[What we are building](#what-we-are-building)), under each dataset's
`data/<generation>/` folder with their current file names
([layout](object-native-metastore-technical.md#layout)). A file keeps the
generation it was written in for life: ending a generation never copies or
moves a file, and GC only deletes files no protected state references.
- **Data locations separate from the catalog.** By default the catalog and the
data share one location. The format lets the catalog reference data in other
locations, each with its own permissions, storage class or region (see
[multiple data locations](object-native-metastore-technical.md#future-multiple-data-locations));
today's per-dataset overrides, such as S3 Express, would become one. The
surface for configuring them, and when it ships, belong to a separate
Enhancement on object-native data; this one covers the metadata. Every data
location must be an object store that every instance needing its files can
read.

---

# Milestones 2 and 3: concurrent metastore

*Note: The decisions below are settled. What is still open waits on evidence,
not on design: decision 3.4 (a measurement on Milestone 1's code), and wider
model checks that join maintenance and transactions with failures and GC.
Measurements on Milestone 1 may still adjust details.*

Several writers commit to one dataset whenever their data is independent.
Milestone 2 adds the maintenance writers; Milestone 3 adds the rest:

| Milestone | Write pattern | Example | Why writers are independent |
|---|---|---|---|
| 2 | Distributed maintenance | Compaction, bake, cold promotion on any node | Leases over file sets avoid duplicate work; guards keep it correct |
| 3 | Append | Log, event or metric ingest from several instances; no PK | Appends always commute |
| 3 | Disjoint keys | OLTP-style writers owning key sets; metric appenders each owning services, keyed `(service, metric, ts)` | Their key sets do not intersect |
| 3 | Partitioned ordered CDC | A writer per Kafka/Debezium partition, DynamoDB shard, or PostgreSQL slot | The source routes each key to one partition |
| 3 | Distributed query write | A full refresh or `INSERT … SELECT` run as many tasks | One job commit for all tasks |
| 3 | Read-dependent write | `UPDATE … WHERE` or a transaction alongside ingestion | Validation finds the commits that changed rows it read |

**The aim: no declared table layout for write safety.** A user should not have
to `partition_by` a table to let writers run concurrently. Writers prove their
independence at commit time, and partitioning and sort order become read-layout
choices that compaction can change.

**Assumption: writes are mostly non-conflicting.** Partitioned CDC writers,
appenders and owners of disjoint keys rarely or never touch the same keys. The
commit protocol is optimistic and built for that case. A workload with heavy
contention on shared keys would need a coordinated commit layer on top (see
[possible future improvements](#milestone-n-possible-future-improvements)).

**Contention.** Writers lose races only when they commit at the same moment.
Take one log, each writer flushing every 2 s, and about 50 ms per conditional
write: with 4 writers, about 10% of commits race; with 20 writers, about 40%. A
race between independent writers costs a retry, not an abort. A single log
saturates near `1 / (conditional-write latency)` commits per second, which is
the ceiling a future several-logs improvement would lift.

## Key decisions

Details for all of them are in
[Milestones 2 and 3: concurrent writers](object-native-metastore-technical.md#milestones-2-and-3-concurrent-writers).
Decisions 2.1 and 2.2 are what Milestone 2 needs; decisions 3.1 to 3.5 come with Milestone 3.

**2.1 A writer that loses a race retries if the winners did not touch what it
depends on.** The losing writer checks its guards and footprint against the
commits that won, and the outcome has four classes: compatible (retry
unchanged), rebasable (transform the actions, for example remapping a position
delete onto a compaction's outputs), retryable (re-plan from the new state), or
incompatible (return the conflict). This is safe because actions never carry
values computed from what was read.

**2.2 Leases coordinate work; guards guarantee correctness.** CDC writers hold
one lease per source partition, and maintenance nodes lease a deterministic
unit. A lost lease or a racing node fails its guard and wastes work; it cannot
corrupt state.

**3.1 Commits publish key footprints,** checked cheapest first: PK hash buckets
and per-file min/max in the record, then the commit's keys as sorted 64-bit
digests, compared by one linear merge. A digest collision can only report a
false conflict, never miss a real one. Bloom filters are not used: their false
positives compound over a commit's keys (at 1% per key, a 1,000-key commit
almost always matches), so they decide only small commits and the digests are
needed anyway.

**3.2 Overlapping keys are resolved by the writers.** Writers whose keys do not
overlap need no resolution and no coordination, which is the common case. When
keys can overlap, Milestone 3 supports abort-and-re-plan and key-range ownership.
Source-version ordering, where the highest source version of a key wins, is
deferred until a concrete use case justifies its new semantics and surface.

**3.3 Distributed execution writes by source partition, not table partition.** A
distributed query commits once with every successful task's output; distributed
ingest assigns source partitions to executors, each an independent writer.

**3.4 Open: how a writer that loses a race keeps `seq` order,** decided by
measurement. A losing writer's buffered `seq`s are below the winner's, and the
rule that, within a table, every record is above every earlier one must still
hold. Two options:

- *(a) Shift the buffer.* Move every buffered `seq` up by the same amount,
  above the winner's, in each table they share. No change to how Cayenne stamps today, but the shift
  re-stamps the window's keys in the in-memory indexes, bounded by one commit
  interval of the writer's own work.
- *(b) Coalesce eagerly and stamp at commit.* The buffer keeps a set of keys
  the window affected and a set of rows it inserted; a key affected but not
  inserted is deleted. Local reads treat the buffer as newer than committed
  data, so they need no `seq`, and the final `seq`s are stamped once, when the
  commit succeeds, so a lost race renumbers nothing. It changes the mem-tier's
  apply path.

The choice rests on measuring the shift for a realistic window against a
retry's round trips; (a) unless that is expensive. It is independent of
publishing at commit (decision 1.2).

**3.5 Read-dependent writes validate what they read.** An `UPDATE … WHERE`,
`DELETE … WHERE`, `INSERT … SELECT` or transaction records the keys it looked
up and the predicates it scanned. On a lost race it conflicts only with
winners that wrote one of those keys or changed a row matching one of those
predicates, before or after the change; a conflict re-runs the statement on
the new head. The state at every commit then equals running the accepted
writes one at a time in commit order. This lands last in Milestone 3; until then
the check is coarser, and any commit to a table the write read is a conflict.

- *Alternative: one read-dependent write at a time,* with blind writes checked
  only by key footprints. Rejected: a blind upsert can change a row into one
  the write's predicate matches, and the write then commits a stale plan. It
  needs validation against blind writers anyway.

## Multi-region

- **Write in one region, read in others:** supported. A replica exposes only a
  readable frontier whose full dependency closure it has confirmed present.
- **Writers in several regions over disjoint keys:** supported, against one
  bucket (paying cross-region latency per commit), or with a log per region (a
  possible future improvement).
- **Overlapping keys across regions:** only with key ownership.
- **One atomic commit across regions:** out of scope.

---

# Implementation roadmap

The [technical design's roadmap](object-native-metastore-technical.md#implementation-roadmap) orders the work
as discrete changes, each shipping on its own, and each with an exit proof:

1. The head log in `object_store_occ`, proven under the WAL's existing test
   suite and on S3 Standard, S3 Express, RustFS and a local directory.
2. Cayenne's catalog changes expressed as actions, with the SQL metastore
   still authoritative and a differential check that both agree.
3. A shadow log written alongside the SQL metastore, on real workloads.
4. Data files in the destination, and readers that open from the shadow in
   tests and the benchmark harness.
5. The object-native metastore as the authority for new datasets, behind the
   flag.
6. Existing datasets re-bootstrap from their sources; metastore contents are
   not migrated.
7. On by default, once a second instance on the same destination can no
   longer race the writer: either read-only support or detection that
   refuses it.
8. Removal of the SQL metastore.

The format is in real use from step 3, first as a follower; the default
changes only after re-bootstrap and recovery are shown, and the old code goes
last. Milestones 2 and 3 build on the object-native path only; removing the SQL
metastore (step 8) can proceed alongside them.

# Open decisions

Both open decisions wait on measurements, not on design.

- **What replaces per-dataset `mode: memory`.** The preferred answer is
  nothing: the same metastore for every dataset, with the runtime finding that
  a table is small enough to hold in memory. Whether that works depends on
  measurements and details not yet available. It is user-facing surface, so the
  answer goes through the Enhancement.
- **How a writer that loses a race keeps `seq` order** (decision 3.4): shift
  the buffer, or coalesce eagerly and stamp at commit, decided by measuring the
  shift on Milestone 1's code.
