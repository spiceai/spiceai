# Cayenne metastore protocol model

Manual Stateright 0.31.0 checks of the [object-native metastore design](../../docs/design/object-native-metastore-technical.md).
The design and this model capture the current direction, not a frozen
specification; both are updated as implementation teaches us more.
It is a finite protocol abstraction, not the Spice engine or an object-store implementation.
Lifecycle and coverage start with one live row per key, with two valid tier
placements preserving warm-subset and cold-file witnesses. Live-row counts are
checked independently of value selection and count identical physical rows.
No model check runs through `cargo test`.
The `seq-order-two-writers-*` runs are the concurrent-ingest scenarios of
Milestone 3.

```sh
nice -n 10 cargo run --release -p cayenne-metastore-model -- list
nice -n 10 cargo run --release -p cayenne-metastore-model -- quick
EXHAUST=1 nice -n 10 cargo run --release -p cayenne-metastore-model -- all
nice -n 10 cargo run --release -p cayenne-metastore-model -- run seq-order-two-writers-2-exhaust
EXHAUST=1 nice -n 10 cargo run --release -p cayenne-metastore-model -- run seq-order-no-shift
```

Checkers use 6 threads by default; set `MODEL_THREADS` to change it. Results do not depend on the thread count, and the binary uses mimalloc, because cloning states dominates checking. Models list candidate actions and reject disabled ones in `next_state`, so each successor is computed once; simulation runs, whose checker cannot replay a rejected pick, list only enabled actions. Controls require `EXHAUST=1`. A control stops once it has found its expected failure and every witness it must reach; its other safety properties print `NOT_FOUND`, not `HOLDS`, because the rest of its graph is not explored. A wrong outcome, missing
required control failure, invalid witness, or checker timeout makes the command fail.
A sampled run reports `NOT_FOUND`, which does not establish exhaustive safety.
New exhaustive safe graphs require their essential reachability witnesses; missing
one fails the run.

## Compositions and bounds

The shared core provides exact conditional-head identity and immutable byte-window
storage: records, stable batch cuts, create-if-absent sealing, the batch index,
checkpoint publication, aligned floors and collection. There is no publication
nonce, speculative ETag, numbered log, pipeline, or clipped-floor format.
The commit composition specializes storage to equal-size symbolic records; the
transaction and byte-storage compositions use the same variable-size `Store`.

| Run prefix | Protocol components and checked properties | Bounds |
|---|---|---|
| `remote-maintenance*` | Milestone 2: one ingest writer, remote file-set maintenance plans, guarded CAS retries without sequence shifting, warm subsets, warm-to-cold promotion, cold rewrites, coverage and PK checkpoints; optional Prepare cap; I4/I6/I8/I9 and multiplicity | 2 tables/keys, 2 drawn operations (minimal: 1), 3 accepted records (Prepare: 4), 1 or 2 maintainers; 2 plans/node, at most 2 commits with one maintainer or 1/node with two; one lease turnover/node; directed clear-retry uses 4 records |
| `read-transaction-*` | BEGIN, two explicit statements over the BEGIN snapshot plus own overlay, one publication with union footprints, whole-transaction validation/re-plan, independent transaction-program replay; I9 and multiplicity | Fixed necessity pairs; widened pairs of 28 programs per dependent writer against 15 two-table blind programs, 2 writers plus blind, exhaustive 1/2 selected transactions or blind records, 3 attempts; 3 selections sampled |
| `position-delete*` | Position deletes racing one compaction with changed output positions; either winner order, retained row mapping, exact target visibility and multiplicity; proven prior deletion completes as a no-op | 1 input/output file, 3 row identities, 2 output permutations, 1 position delete; optional prior deletion, every committed version |
| `read-widened-*` | Full modeled program-family selection per statement, two sequential statements per writer, rule 2 / stamping at commit; program replay, I9 and live-row multiplicity | Exhaustive 2/3 total selected statements, 3 planning attempts per statement, 2 tables, 3 keys and columns 0..=2; six-selection samples |
| `read-widened-delivery-*` / `read-delivery-*` | Immutable conditional requests, unknown outcomes, head-read resolution and delayed identical retries; program replay, I2/I5/I9 and multiplicity | Widened positives 1/2 selections; unknown-as-loss/exact-base controls 1, unknown-as-win 1/2; fixed single-writer comparisons; 1 explicit unknown event, 2 copies per request |
| `cold-promotion` | Atomically remove live warm inputs and add cold outputs, checkpoint plus tail recovery, delayed duplicate publication; multiplicity and I8/I9 | 2 valid initial placements, 1 staged record, 2 deliveries, 1 checkpoint; tombstone-free inputs |
| `dataset-lifecycle` | One dataset (parent and child), buffered per-table upserts/deletes, acknowledgement commits, arbitrary warm subsets, promotion-only cold rewrites, coverage, commit naming, checkpoint PK collection and tail recovery, log collection that keeps running queries' states, removal retirement, two queries within retention, takeover, a fresh-named upload, orphan collection and a late fenced upload; I4/I7/I8/I9 | Exhaustive 2 total accepted records, 2 total buffered operations, 2 keys per table, 2 initial files in each of 2 tier placements, one checkpoint, one takeover and one upload; samples 4 records, seeds 7, 42, 20261024 |
| `dataset-prepare-*` | Lifecycle file/deletion state and prepared keyed deletes/upserts, external decision and logged Resolve; I4/I6/I8/I9; cutoff cap with ordinary-write reservations | 6 accepted records, 2 buffered operations on parent table, 1 arbitrary warm subset, 1 cold promotion, 1 standalone clear, 1 checkpoint; no lease/staging actions |
| `dataset-query-expiry*` | Lifecycle maintenance/checkpoint/log collection with a query whose retention expires, delayed GET and GC selection/deletion; rows or error | 2 accepted records, 2 clock advances, 1 query, scan in key/file order |
| `dataset-prepare-query-expiry*` | Prepared actions, maintenance and checkpoint/log collection with a query that may start during a prepared transaction and whose retention expires; the `-byte-window` sibling adds byte batches, sealing and floor publication | 3 accepted records (5 with the byte window's 2-record metadata prefix), 2 clock advances, 1 query |
| `dataset-byte-window-metadata-prefix*` | Lifecycle actions in shared byte-window Store, delayed sealing, aligned floors, checkpoint PK pages and retention of every running query's state | 5 total records including 2 initial metadata records, 2 buffered operations, 2 warm rewrites, 1 promotion, 1 metadata removal, 2 checkpoint pages, 1 storage collection; early-floor controls use 3 total records |
| `source-positions` | Numbered append stream, buffered/grouped data and positions, ambiguous CAS, identical retry, acknowledgement, crash reacquisition, takeover, redelivery and skip; I10/I2 | 2 source changes, 4 attempt identities, 1 crash and 1 external takeover, up to 2 generation advances |
| `commits-restart-turnover` | Existing commit composition plus volatile buffer/handle clearing, persist-before-send receipts, restore, delayed requests and takeover; I1–I5 and existing replay checks | 1 logical writer, 2 attempt identities, original/retry delivery, 1 restart, up to 3 lease generations; no checkpoint/filler actions |
| `unknown-action-refusal` | Older reader refuses an action the serial append oracle understands; I11 | 2 records, one unsupported action kind |
| `transactions-three-participants` | Three identical participant heads; Prepare/decision/Resolve, ordinary writes, checkpoint/GC and uncertain CAS; full participant values sorted by derived `Ord` for symmetry | Exhaustive symmetry bound 0; original bound-1 samples unchanged |
| `commits-commit` | Two writers, delayed original/retry deliveries, exact-base CAS, lease takeover, folded checkpoint; I1–I5, I8 | Exhaustive bound 2: K=2, two attempts per writer, up to three leases; samples bound 4: K=3 |
| `commits-retry` | Unavailable head read, fresh-id retry, delayed deliveries and acknowledgement; exactly-once logical write, I5 | Exhaustive/control 2; one writer, two attempts |
| `commits-retry-expiry` | The retry graph plus a head store clock that advances with every head write, including takeovers, and checkpoints that drop attempt ids older than the evidence horizon; resolution consults only retained evidence, and an attempt past the horizon resolves unresolved and is reported as commit outcome unknown, never retried; I2/I5, no double apply, unknown never retried | Exhaustive/control 2; one writer, two attempts, horizon 2 store-time units, up to three leases |
| `standby` / `standby-three` | Two or three instances of one Spicepod on one location: a lease object with an expiry on a discrete clock, renewal by the holder, crash, restart, pause and resume, acquisition only after expiry, and takeover by a head commit that raises the generation; only the generation in the head fences writers; the ingester commits source changes by exact-head CAS and then acknowledges them; standbys serve head reads. No change applied twice or acknowledged without commit, no stale-generation commit accepted, one committing instance per head generation, lease exclusivity, no lease loss by a healthy holder, and an ingester again within the lease duration of a holder failure | Exhaustive: lease duration 3 ticks, 2 source changes, 1 crash, 1 pause, at most 4 lease epochs; a running holder renews and a running standby acquires or takes over before the next tick |
| `standby-early-safety` | The standby graph with acquisition allowed before expiry; checks only the safety properties, which do not depend on lease timing | Same bounds |
| `commits-etag` | Explicit store-version CAS and delayed retry across head changes; I1–I3, I5 | Exhaustive/control 1; one writer, one original/retry pair |
| `commits-storage` | Checkpoint parts/root before head publication, delayed GC, a query kept by retention, lagging reopen, late seal; I4, I7, I8 | Bound 2; samples 4 |
| `transactions-txn` | Two datasets, one transaction, competing ordinary writes, reservations, commit/abort create race, helper Resolve, uncertain CAS, checkpoint/GC; one participant writes other keys while prepared so the Prepare can reach a checkpoint; I3–I6, I8 | Exhaustive 1 (two lease generations), controls 0 except owner restriction 1; samples 2, 3 (two outstanding handles per dataset) |
| `window-storage` | Canonical inline bytes, byte/count pressure, copy rule, cold replay, a running query, checkpoint/GC; I4, I7, I8 | Exhaustive 6 records; controls 5; samples 7, 9 |
| `retention` / `retention-readers` | Readers never write and GC has no generations: removed objects deleted a fixed retention after GC observes the removal, which stays in durable state until the delete (timers restarted at takeover); never-committed uploads deleted once the head's store time is T_O past theirs; GC's empty commit on an idle head; fresh object names; a writer references only base-live objects or its own never-published uploads, while its base's store time is less than T_O minus a margin past the upload's; object timestamps that lag the writer's lower bound; deletes and commits that land arbitrarily late. `retention-readers` adds one query whose head GET responds late, and stops once the query settles. No dangling reference, rows or an error, and a query within the retention never fails | Exhaustive: 2 object names, 9 clock ticks, 6 head versions, retention 3, staleness 1, upload age 4, timestamp error 1, two sweeps, one takeover; the writer graph adds timestamp skew, the reader graph one query |
| `window-txn` | Transaction resolution composed with explicit delayed byte-batch sealing and eviction; I3–I6, I8 | Exhaustive 0; samples 2, 3 |
| `concurrent-txn-overlap`, `concurrent-txn-disjoint` | Two transaction identities, overlapping/disjoint footprints, ordinary writer, rejected and late Prepare, opposite reservations, cleanup, checkpointed pending ids; I4, I6, I8 | Exhaustive/control 0; samples 1, 2 |
| `concurrent-floor-aligned` | Published floor at batch boundaries, reader reopen, a query kept by retention and delayed deletion; I4, I7, I8 | Exhaustive 6 records; samples 7, 9 |
| `seq-order` | Per-key versions, buffered upserts/deletes, committed-prefix readers, compaction, CAS loser uniform shift and conflict re-plan; I8, I9, inherited file ranges | Two keys; one writer: 2 operations; two writers: 2 each, 2 maintenance commits; samples 3 each, seeds 7, 42, 20261024 |
| `naming` | Retained state and checkpoint names versus files, tombstones and schema; naming by sequence versus a dense commit number | Exhaustive 3 mutation/maintenance/metadata records; arbitrary checkpoint writes |
| `table-seq` | Two per-table allocators under one head, upserts/deletes, a two-table record and an atomic partition key move; refusal safety predicate, I9 values versus serial oracle, per-table ranges | Exhaustive 3 accepted records, 2 keys, initial high-water marks [4, 0] |
| `delete-coverage` | Arbitrary nonempty warm-file subsets, one cold file rewritten only by promotion, fresh upserts, coverage-aware later tombstone clearing; I9 existence versus serial oracle, including a separate cold-file predicate | Exhaustive 4 records, 2 keys, 2 initial files in each of 2 tier placements; samples 6 records, seeds 7, 42, 20261024 |
| `pk-index` | PK-existence index, checkpoint collection, record collection and reopen; recovered keys equal full state (I8) | Exhaustive 3 mutations, 2 keys; checkpoint-aligned index |

`commit-guards` and its three controls use one writer, one attempt, an original/retry
pair, three lease generations, and no checkpoint/filler actions. They isolate
CAS, lease and identical-byte obligations at exhaustive bound 1.

`commits-retry-expiry` adds a store clock to the retry graph: every head write,
including the commit that raises the lease generation at a takeover, advances
it, and each accepted position keeps the store time it landed at. A checkpoint
may at any time drop the attempt ids of positions at least the horizon older
than the head. Resolution looks up the attempt's exact position in retained
evidence: a retained id decides accepted or rejected, and a dropped one yields
unresolved even though the head has moved, so the writer reports the commit
outcome unknown and never retries. The control instead classifies a moved head
without evidence as rejection, retries a fresh id, and applies the logical write
twice.

`standby` models instances of one Spicepod on one location with relative
time: each lease holds a count of ticks left, so the clock needs no bound.
Ticks wait for urgent steps: a running holder renews before its lease runs
out, a running instance acquires an expired lease, and a new holder commits its
takeover. These are the timing assumptions availability rests on; safety needs
none of them. Acquisition is a conditional write on the lease object; the
takeover is a CAS on the head that requires a lower generation and raises it;
ingest commits are exact-head CASs, so a writer whose generation is no longer in
the head fails and steps down. An in-flight commit survives pause and step-down,
which is how a resumed writer reaches the head with a stale base. Availability
counts ticks since the latest holder failure during which some instance runs and
none ingests; it reaches the lease duration and never exceeds it. Ingest CAS
responses are never lost here; uncertain outcomes are covered by `commits-*` and
`source-positions`.

`commits-retry` and `commits-no-resolve-before-retry` extend the commit
composition with an unavailable head read: safe classification stays unresolved;
the control reports rejection and permits a fresh attempt id for the same logical
write. Both use one writer, two attempts, bound 2, three lease generations and no
checkpoint/filler actions. `commits-etag` / `commits-etag-repeat` use the guard
graph at bound 1 with explicit store tags captured at issue time. CAS compares
the tag; safe tags increase, while the control lets a later lease-head version
reuse the initial tag. No new composition is needed.

Positions in the byte model are zero-based and ranges are half-open; the
specification uses inclusive one-based positions.

The byte model uses K=3 records, batch budget B=3 symbolic bytes, maximum record
R=2 and head budget H=5. Samples visit at least 100,000 states at depth at most 200,
with seeds 7, 42 and 20261024. The transaction-resolution profile completes sealing
as part of planning and omits post-Resolve filler records; `window-txn` schedules
seals separately. Checkpoints retain symbolic prefix bytes and acceptance evidence;
they do not encode real checkpoint pages. A checkpoint names a batch end or the
record before the window, never a position inside a batch or the window. In
`window-storage` and `concurrent-*` the writer writes the root and the next
commit publishes the floor; `transactions-*` publishes it with a CAS of its own,
as a separate maintenance process does.

## Negative controls

`list` prints every individual bound, seed and expected downstream failure.

| Control suffix | Removed safeguard and required downstream failure |
|---|---|
| `remote-maintenance-minimal-lease-outside-head` | The lease turnover writes only a separate lease object, so the publication CAS cannot see it: an expired unit still publishes; I3 |
| `remote-maintenance-no-retry-lease-check` | A retry after a lost CAS rebases without checking the unit's generation in the new head: an expired unit publishes; I3 |
| `remote-maintenance-no-stable-high-water` / minimal pair | Maintenance advances high water over a buffered operation: its original stamps are refused |
| `remote-maintenance-no-committed-cutoff` / minimal pair | Apply through an allocator value that includes uncommitted deletes: later covered clearing resurrects an old row |
| `remote-maintenance-no-file-set-lease` / minimal pair | Overlapping plans with input guards retained: holds; leases avoid redundant work, not required for safety in this graph |
| `remote-maintenance-no-guard-revalidation` / minimal pair | Skip the retry's input, coverage and cap guards but keep its lease check and full file-set exclusion: holds in this graph |
| `remote-maintenance-no-file-set-lease-no-guard-revalidation` / minimal pair | Remove both exclusions: two replacements of the same input leave duplicate live rows |
| `read-transaction-*-no-union-footprint` | Validate only s2: stale s1 actions violate whole-transaction program replay |
| `read-transaction-*-no-overlay-read-keys` | Omit s2 lookup keys already written by s1: holds because the union still includes s1 written keys |
| `position-delete-*-no-position-remap*` | Silently complete without changing output positions: targeted row remains visible |
| `position-delete-*-no-row-identity-mapping` | Shift to another output position: wrong row deleted; prior-deleted target must not consume a survivor |
| `read-widened-after-image-only` / `-before-image-only` / `-no-absent-lookup` / `-no-delete-keys` / `-no-conflict-replan` / `-previous-winner-only` | The corresponding rule2 safeguard removed across nondeterministic programs and sequential statements: program replay |
| `read-widened-no-inserted-keys` | Omit inserted keys from footprints: two live physical rows |
| `read-delivery-no-resolve-before-replan` / widened counterpart | Treat unknown as rejection and re-plan: relative UPDATE applied twice |
| `read-delivery-no-acceptance-evidence` / widened counterparts | Report unknown as accepted: acknowledgement without acceptance |
| `read-delivery-no-exact-base` / widened counterpart | Ignore the original condition at duplicate delivery: two live physical rows |
| `cold-promotion-no-input-removal` / `dataset-lifecycle-no-warm-input-removal` / `-no-cold-input-removal` | Keep replaced inputs live while adding outputs: multiplicity fails despite equal maximum values |
| `dataset-prepare-no-cutoff-cap` | Omit the cutoff cap while Prepare is unresolved: I9 resurrection |
| `dataset-prepare-no-index-checkpoint` / `dataset-byte-window-metadata-prefix-no-index-checkpoint` | Keep the initial PK collection at a later checkpoint: I8 |
| `dataset-prepare-no-prepare-roots` | Collect prepared upsert objects: I4 |
| `dataset-prepare-no-write-reservation` | Admit an ordinary write on a reserved key: I9 |
| `dataset-prepare-no-reader-record` | Reader applies external decision before logged Resolve: I6 |
| `dataset-query-expiry-missing-as-empty` | Missing GET contributes no rows to the lifecycle scan: wrong snapshot result |
| `dataset-query-expiry-no-delete-selection` | Selection honors an expired query but deletion recomputes candidates without it; rows or error remains the required outcome |
| `dataset-query-expiry-no-log-expiry` | Log retention includes expired queries: safety-preserving excess retention |
| `dataset-prepare-query-expiry-missing-as-empty` | Missing GET contributes no rows while a transaction is prepared: wrong snapshot result |
| `dataset-prepare-query-expiry-byte-window-no-query-batch` | Storage collection ignores the running query's batches with prepared actions and query expiry also present: I7 byte query recovery |
| `dataset-byte-window-metadata-prefix-no-query-batch` | Drop an old batch needed by a running query's state: I7 |
| `dataset-byte-window-metadata-prefix-no-sealed-floor` | Complete checkpoint and floor cross an unsealed batch: current recovery can succeed while an intermediate protected state is lost; I7 |
| `dataset-byte-window-metadata-prefix-no-sealed-floor-without-readers` | Same early floor with no query registration: checks the separate complete-checkpoint case |
| `transactions-two-participants-symmetry-no-create-once` | Decision overwrite still finds participant disagreement with symmetry enabled |
| `dataset-lifecycle-no-coverage` | Clear without coverage in the composed graph: I9 resurrected row |
| `dataset-lifecycle-no-upload-age` | The orphan sweep takes an upload before it has aged, while the writer can still stage it: I4 |
| `dataset-lifecycle-no-table-scope` | Dataset-wide comparison of independent allocators: valid child record refused (accepted rows still satisfy I9) |
| `dataset-lifecycle-no-commit-number` | Name maintenance states by mutation high water: I8 one name has different physical states |
| `dataset-lifecycle-no-query-log-retention` | Reclaim metadata history below the floor despite an older running query: I7 reconstruction fails even with data files present |
| `dataset-lifecycle-no-index-checkpoint` | Omit PK collection refresh in checkpoint recovery: I8 |
| `retention-no-age-check` / `-no-margin` | The writer references an old upload, or checks its age without allowing for timestamp error: a committed state references a deleted object |
| `retention-writer-clock` | The writer ages its upload on its own clock while the store's clock jumps ahead: GC deletes it after the head move and the commit lands |
| `retention-list-date` | GC ages uploads by the LIST's time instead of the head's: a commit based on an older head lands after the delete |
| `retention-reref-published` | A writer references its own upload again after publishing and removing it: GC's delete of the removed object lands under the new commit |
| `retention-reuse-found` | The writer reuses an object it finds present instead of a fresh name: a committed state references a deleted object |
| `retention-readers-no-retention` / `-stale-start` | Retention zero, or a query starts from a state confirmed longer ago than the staleness bound: a query within the retention fails |
| `retention-readers-keep-timestamps` | A takeover keeps removal times from the predecessor's clock: a query within the retention fails |
| `retention-readers-volatile-removals` | Removals live only in GC's memory: after a takeover the orphan sweep deletes a recently removed object a query still reads |
| `retention-readers-sweep-removed` | The orphan sweep ignores recorded removals: it deletes a recently removed, old upload a query still reads |
| `retention-readers-response-time` | A head confirmation is timed from its response instead of its request: a late response admits a query whose objects are already deleted |
| `retention-readers-missing-as-empty` | A missing object reads as empty: wrong rows instead of an error |
| `source-ack-before-acceptance` | Acknowledge buffered changes before CAS acceptance: I2 |
| `source-position-before-data` | Separate offset record lands first: logged prefix lacks rows (I10) |
| `source-position-after-data` | Separate data record lands first, crash/redelivery appends it again: I10 |
| `source-no-redelivery-skip` | Reapply changes already covered by logged position: I10 duplicate append |
| `commits-restart-no-persist-before-send` | Send without persisted attempt identity; restart forgets it and applies the logical write again: I2 |
| `unknown-action-skip` | Ignore unsupported append: replay differs from serial oracle (I11) |
| `commit-no-copy` / `storage-no-exists` | Evict before the immutable batch exists: I4 |
| `commit-no-observed` | Seal a planned head: I8 replay equality |
| `storage-no-stable-cut` | Head-dependent cut: I4/I8 exact bytes |
| `storage-no-final` | Seal a non-final batch: I4/I8 exact bytes |
| `storage-no-range` | Index a different range from the dropped records: I4/I8 exact bytes |
| `storage-no-published` | GC uses an unpublished checkpoint: I7 |
| `storage-no-missing` | Skip missing history instead of reopening: I8 |
| `txn-no-reservation` / `txn-overlap-no-reservation` | Allow conflicting records or other transaction ids through reservations: visible replay divergence (I6/I8) |
| `txn-no-roots` | Collect unresolved Prepare objects: I4 |
| `txn-no-matching` / `txn-overlap-no-match` | Resolve without the matching pending identity: visible replay divergence (I6/I8) |
| `txn-no-create-once` | Overwrite the durable decision: I6 participant disagreement |
| `txn-no-reader-log` | Interpret a prefix using the external decision: I6/I8 |
| `commit-no-cas` | Publish against a changed base: I1 |
| `commit-no-lease` | Rebase a stale owner's condition without its lease check: I3 |
| `commit-no-identical` | Retry different bytes with the original condition: I5 |
| `commits-no-resolve-before-retry` | Treat an unreadable head as rejection and retry a fresh id: I2 acknowledged logical write exactly once |
| `commits-retry-expiry-no-horizon` | Treat no retained evidence past the horizon as rejection and retry a fresh id: logical write never applied twice (I5 also fails) |
| `standby-no-generation` | Takeover acquires the lease object and reads the head without raising its generation: a paused writer resumes and its stale-generation commit is accepted; two instances also commit under one head generation |
| `standby-early-acquire` | A standby acquires before expiry while the holder renews: two instances hold unexpired leases (and a healthy holder loses its lease). `standby-early-safety` shows the generation in the head still prevents stale commits and double application |
| `standby-no-renew` | The holder never renews: a healthy holder loses its lease; availability also fails when the standby pauses |
| `commits-etag-repeat` | Reuse an earlier store tag for a later head: I1 unique accepted position |
| `seq-order-maintenance-draw` | Maintenance draws ahead of the buffer and publishes first, then permits the lower buffered record: I9 resurrected row after a later compaction |
| `seq-order-no-shift` | Independent CAS loser retries its unchanged lower sequences: I9 resurrected row after compaction |
| `seq-order-drawn-cutoff` | Compaction clears deletes through an allocated sequence rather than its captured committed cutoff: I9 resurrected row |
| `seq-order-non-uniform` | Independent writer wins; loser assigns separate operation blocks in reverse buffer order (upserts keep delete < insert): I9 earlier value wins |
| `naming-no-commit-number` | Name distinct retained states/checkpoints by mutation `seq`: state identity and I8 |
| `table-seq-no-table-scope` | Compare independently allocated table sequences dataset-wide: valid record refused; accepted records still satisfy I9 |
| `delete-coverage-no-cold` | Omit cold files from coverage: I9 resurrection |
| `pk-index-checkpoint-no-refresh` | Retain an older PK-index reference after checkpoint/record GC: I8 recovered keys disagree |

`txn-no-owner-resolve` is a safety-preserving restriction: its required outcome is
no safety failure and no non-owner-after-turnover witness. It checks a lost helper
capability, not a temporal liveness theorem. Other `sometimes` properties document
reachable schedules, not fairness.

Different participants may expose their Resolve records at different times, as
the specification permits; these checks do not promise atomic cross-dataset reads.

I1–I9 refer to the specification's table, with descriptive suffixes narrowing each
check. Model-specific predicates include reservation diagnostics, well-formed
transaction prefixes, cleanup possibility, and visible/reader/recovery replay
comparisons against an independently filtered serial prefix. Exact-byte comparisons
also cover payload preservation. The commit model’s I2 predicate strengthens the
specification’s acknowledgement rule to exactly one accepted attempt per
acknowledged logical write. Replay checks correspond to I8; transaction prefix
independence corresponds to I6. This is not full coverage of every clause of I1–I8.

The `seq-order` composition compares sequence-based row/tombstone visibility to
an independent serial oracle that assigns each key its last logically committed
value. Upserts draw a delete sequence and a higher insert sequence; deletes draw
one. A buffer can contain repeated keys and is published as one `[seq_lo, seq_hi]`
record only after exact-base CAS acceptance. Readers cover the current state and
every retained committed prefix; buffers never enter reader state. I8 compares
incremental physical state to cold replay at every prefix (shared physical apply
helpers, separate from the logical I9 oracle).

Compaction captures committed rows and cutoff, retains each visible key's latest
version, inherits symbolic input-file ranges, then replaces only its captured
inputs and clears tombstones at or below that cutoff. Tail files survive. Its
publication abstracts atomic rebase, current-input validation and successful CAS;
stale maintenance CAS delivery is not separately scheduled. Two maintenance rounds
allow an out-of-order buffered delete to encounter a later committed cutoff.
Maintenance ranges are inherited metadata, not fresh mutation sequences; they do
not advance the head high-water mark in safe runs. The maintenance-draw control
also admits the resulting lower buffered record instead of blocking or shifting it.
The non-uniform control demonstrates that independent renumbering can lose operation
order; it does not establish that every order-preserving non-uniform mapping is unsafe.

This is bounded I9 coverage for absolute-value upserts/deletes on two keys with
buffering, maintenance and independent/conflicting writer races. It does not model
relative updates or predicate-read conflicts in that composition, real files or PK/deletion-index stamping,
sequence overflow, persistent snapshot-token reopen, or combine I9 with the separate
transaction, lease, byte-storage and GC compositions. Conflict re-planning redraws
sequences for the same absolute-value logical operations. Safety does not prove
fairness or eventual completion of every buffered window.

Identity separates commit numbering from mutation sequences;
checkpoint creation itself is an object write, not an accepted state change.
The table model checks actual per-key values, per-table record ranges and
high-water marks; it has one writer and no buffered CAS losers. A refused record
is a safety counterexample, not a fairness theorem.

Coverage tracks one symbolic row per file, with an inherited minimum sequence
and a deletes-applied-through cutoff (absent on fresh files). Warm compaction
never rewrites the cold file. Promotion applies deletes. Clearing checks all
live files whose minimum sequence is at or below the tombstone. The graph does
not model stale asynchronous rewrite publication.

The PK index refreshes as a checkpoint collection.
The graph does not model asynchronous index construction or real index bytes.

`retention` and `retention-readers` are the only reader and garbage-collection
protocol. Readers never write: a query starts from a head confirmed within the
staleness bound S of when its GET was sent, and every object it needs survives at
least T - S, because each was live at that confirmation and GC deletes a removed
object only T after observing its removal. The removal stays recorded in durable
state until the delete, so a new GC after a takeover still knows it, and the
orphan sweep skips it. Every read returns the referenced object or fails the query.
Uploads that never committed are collected by age instead of by generation, and
every age is a difference of two store timestamps: GC deletes an unreferenced
upload once the head it read is T_O past the upload's `LastModified`, and the
writer refuses its own upload once its CAS base is T_O minus a margin past a lower
bound on that upload's time. A commit landing after GC's read is based on that head
or a newer one, so the writer would have refused it. GC's empty commit lets an idle
head's time advance; it is needed for progress, not for safety. A writer references
an upload of its own only until it is published; after that, only while it is live
in the base. Every object has a fresh name, so nothing is reused by finding it
present. Removal timers are GC's local durations and restart at takeover; no
other clock is compared across machines. The graph is split in two to stay
tractable: the writer graph checks references with timestamp skew, and the reader
graph adds one query with a late GET response and stops once that query settles.
Other graphs abstract this contract: a query is protected from its start until it
expires, and the orphan sweep takes only an upload that has aged, which the writer
can no longer reference.

## Composition boundaries and liveness

`dataset-lifecycle` is one graph with shared head, committed versions, file/deletion
collections, object presence, the oldest running query and retention decisions. Both tables
share acceptance but allocate sequences independently. Buffered values enter the
serial oracle only at acknowledgement acceptance. Maintenance draws no sequences;
its one-row outputs inherit sequence metadata and apply the captured committed
cutoff. Cold inputs can only be rewritten by promotion. Checkpoints copy the full
physical collections and a PK-existence collection; recovery uses only a published
checkpoint and the contiguous surviving tail. With one new checkpoint, the initial
checkpoint and its whole tail remain until the protected low water reaches the new
checkpoint. I4 reconstructs every state at/above the floor; I7 reconstructs every
state from the oldest running query onward and compares metadata, keys and object presence
with the audit versions. Audit versions are not recovery inputs. Physical replay shares `apply`; the row oracle uses
logical buffer operations without physical sequence comparisons. A metadata page
staged from the writer's own fresh-named upload exercises staged references and
delayed retirement alongside the row files. `Age` marks an upload past the age
limit, after which the writer cannot stage it and the orphan sweep may take it; the
retention graphs check that the writer's age check and GC's head-relative age
give exactly that. A fenced predecessor may finish an upload after the orphan sweep; nothing
references it, and a later sweep collects it.

The small exhaustive bound reaches warm subset compaction, cold promotion,
standalone covered clearing, parent/child records, maintenance while buffering,
two queries, log collection/recovery, retirement deletion and late upload.
These are separate required witnesses; it does not claim every action happens
in one two-record history. In particular delete + warm rewrite + cold promotion
requires three records and is sampled in this composition; the existing focused
four-record delete-coverage graph remains exhaustive. Larger samples permit four records but still only two
buffered operations. The graph abstracts a successful local single-owner commit
and its acknowledgement as one step; it does not add asynchronous compaction
planning/CAS, real checkpoint page bytes, byte-window sealing or multi-dataset
Prepare roots. Those existing graphs remain separate. Checkpoint roots are symbolic
complete metadata objects; their byte/page encoding and asynchronous construction
are not modeled.

The expiry graph separates head validation/GET dispatch from GET completion, so
GC can delete an object while a read is outstanding. Clock steps stand for the
retention running out; a query cannot extend it. Errors discard partial results.
With immutable object identity and missing-object errors, a read after expiry
returns its snapshot's rows or an error.

The restart profile retains network requests separately from volatile handles.
`Persist` records the symbolic attempt id before `Send`; restart clears buffer and
handles, and `Restore` recovers persisted identities for resolution. The network
can finish old requests throughout. Full request bytes remain only as network requests. A restored identity scans
retained acceptance evidence; absence remains unresolved, and it cannot resend
using a lost condition or payload. This checks identity retention, not a real
receipt serialization/reconstruction format or eventual completion of unresolved
operations. New `Issue` actions denote the current owner after a lease change;
ownership election is modeled separately by `standby`. Source restarts explicitly
acquire a new generation, reconcile buffered positions with the logged prefix,
and re-read redelivery from the source acknowledgement. Stream payloads are unique
append numbers so duplicates and gaps are observable. There is no backfill, multiple
source partitions, evidence-retention expiry (see `commits-retry-expiry`) or real connector integration.

## Limits

Not covered: real HTTP/object storage; ETag collision probabilities; canonical
serialization and hashes; real file/index bytes; multipart checkpoint pages;
arbitrary transaction footprints; incarnation changes; sequence overflow;
unbounded predicate/program domains; real PK/deletion-index stamping;
source backfill concurrency; cross-dataset atomic reads; end-to-end Spice correctness;
or unbounded safety and liveness. The three-participant symmetry run exhausts bound 0. Bound 1 remains sampled only. The three-participant graph has one transaction,
not three concurrent transactions. Its bound-one samples have one outstanding
request per dataset and reach all-three commit, all-three abort, helper resolution
after turnover, and checkpointed pending state at every seed. They do not reach
Resolve batch eviction or a second outstanding handle per dataset. The original Milestone 1 graph retains its existing bounds. The new lifecycle siblings share its physical apply, rewrite, coverage and recovery helpers, and most compose one additional mechanism. Two combine them: `dataset-prepare-query-expiry` (prepared actions with queries bounded by retention, bound 3) and `dataset-prepare-query-expiry-byte-window` (prepared actions, query expiry and byte batches together, bound 5), where a query started during a prepared transaction must return its snapshot without the prepared rows. The prepared sibling abstracts the other participant and durable decision to a nondeterministic commit/abort; it does not recheck the existing multi-participant decision race. The child table stays unchanged in these siblings; parent/child allocation remains covered by the original graph. Their smaller action alphabets omit lease turnover, staging and the predecessor upload race. In the expiry sibling, terminal errors ignore late GET completions. Byte tokens stand for complete immutable action payloads: metadata-only records cost one symbolic byte and file/delete mutations two; the journal is an encoding dictionary and audit oracle, never a fallback for a missing batch. Store.root is a symbolic prefix descriptor rather than retained physical WAL; old-state recovery uses actual checkpoint pages and tail tokens. Pages include physical state and the checkpoint PK collection; recovery of protected states requires actual retained pages plus contiguous batch/window tokens.

Existing focused models retain their original bounds and controls. Their narrow
limits above still apply: in particular only the restart profile clears caller
state, and the retention graph has one writer and one GC; GC on another node and
several writers are not modelled. Evidence expiry is modeled only in the
single-writer retry graph (`commits-retry-expiry`), with one horizon value and
symbolic store times; it is not composed with checkpoint bytes, transactions or
source positions, and an unresolved attempt stays unresolved: the model does not
check how the caller recovers the logical write. `standby` has one dataset, one
source partition, at most one crash and one pause, never-lost ingest CAS
responses, a single global clock (no clock drift between instances or against
the lease object), and no retention, checkpoints or GC; its availability bound
holds only under the urgent-step timing assumptions above. Results are symbolic protocol
evidence, not observations of the Spice engine or a storage provider.

All checkers use a 540-second watchdog. Incomplete budget probes establish no
exhaustive outcome; sampled results mean no violation found, not exhaustive safety.

`dataset-prepare-cutoff-cap` caps delete application/clearing below the oldest unresolved sequence across that table. Ordinary writes reserve prepared logical keys. Abort applies no prepared mutation and changes no coverage; allocated sequence gaps remain.

The projection run mechanically checks every enabled outgoing transition of each sampled three-participant state against each two-participant projection, allowing stutters. It uses bound 0, where the global helper witness is false. This is sampled transition evidence, not an exhaustive projection theorem or a proof for arbitrary participant counts, global liveness, or atomic cross-dataset reads.


## Read-dependent programs and live-row multiplicity

The retained fixed-program profiles execute snapshot programs over two tables, three keyed slots per
table, and two non-null integer columns in 0..=3. Initial table 0 contains
r=(0,1), s=(1,0); table 1 is empty. Every writer starts at most one successful
statement; conflicts permit at most three complete planning attempts. Each full
profile has two read-dependent writers and one blind writer whose record touches
both tables. Fixed program profiles exhaust every enabled interleaving, rather than
claiming arbitrary SQL or arbitrary program combinations. The statement families are:

- `read-rule2`: the two relative UPDATEs from the specification, with a two-table
  blind upsert; `read-rule2-delete` substitutes a predicate DELETE.
- `read-rule2-select`: INSERT SELECT into another table;
  `read-rule2-select-same-table` uses `(key+1)%3` as the destination key in the
  source table, with statement errors on occupied destinations. `read-rule2-cross-table`
  also includes a lookup-dependent write in the opposite direction.
- `read-rule2-gate`: SUM(col)+1 <= 2 followed by INSERT, with same-table and
  cross-table destinations; an assertion failure is a statement error.
- `read-rule2-absent`: a lookup of an absent key gates a write in another table.
- `read-rule2-delete-keys`: UPDATE and DELETE of the same selected key.
- `read-rule2-history`: one read-dependent statement and two blind winners, so
  validation must cover a winner hidden behind a later unrelated commit.
- `read-rule2-append`: two relative UPDATEs plus an append stream assigning unique
  source keys in both tables. `read-same-key` separately races two guarded INSERTs
  of one absent key.

A statement computes actions at its snapshot and CASes its observed head. A known
CAS rejection enables validation. Compatible retries preserve actions and uniformly
shift each touched table's sequences above its new high water. Conflicts release
admission, discard actions, then re-admit and evaluate the program at a new snapshot;
exhaustion reports a conflict. `read-rule2-stamp-at-commit` instead retains logical
actions and allocates their stamps at acceptance. It checks the second specified
sequence choice on the same main graph, not local mem-tier implementation costs.

The independent logical interpreter re-executes accepted programs in commit order
and compares every committed version. Separate assertions compare the exact generated plan with the planning snapshot
and compare action/program result states at publication, compare physical
sequence visibility with recorded keyed actions (I9), enforce per-table stamp order,
and count live physical rows before choosing a value. Live-row counts include
identical rows in distinct files; no max-by-key or set conversion hides them.
The original sequence, lifecycle (including all siblings), table-sequence, coverage
and source-replay compositions also assert live-row multiplicity. Boolean-only
PK sets and symbolic object/transaction counters have no physical keyed row bag.

### Necessity and controls

Every new safeguard was first removed in a smaller profile before the full
profiles were added. All controls require their named downstream counterexample.

| Control | Hazard demonstrated |
|---|---|
| `read-rule2-select-after-image-only`, `read-rule2-select-same-table-after-image-only` | A deleted source row is copied by a stale INSERT SELECT |
| `read-rule2-minimal-blind-before-image-only` | A winner moves a previously skipped row into the predicate |
| `read-rule2-absent-no-absent-lookup` | An absent lookup becomes present but its dependent write is still accepted |
| `read-rule2-delete-keys-no-delete-keys` | A DELETE publishes no keys; a stale UPDATE recreates the row |
| `read-rule2-minimal-no-conflict-replan` | A detected predicate conflict still publishes old computed actions |
| `read-rule2-history-previous-winner-only` | A later unrelated winner hides a relevant winner since the snapshot |
| `read-same-key-no-inserted-keys` | Both INSERTs publish, leaving two live rows for the absent key |

The smaller safe comparisons `read-rule2-minimal` and `read-same-key` keep their
matching graph shapes.
Rule 2 uses exact written/lookup keys (including absence) and before/after predicate
checks against every winner since the planning snapshot. Deleted keys must be in
the winner's footprint so validation can retrieve their snapshot before images.
Unsupported or non-deterministic predicates, real digest collisions, source partition handoff,
maintenance, leases, recovery, GC, NULL/type semantics and joins are outside these
statement profiles; multi-statement transactions are checked separately below. The existing focused graphs retain their
bounds and controls for the mechanisms they model.

### Progress and model work counts

`read-progress` is a separate specialized finite fair-service abstraction; it reuses program planning but does not invoke the main model’s conflict/Retry/CAS path. It is not a progress theorem for the full concurrent graph. It runs with a three-attempt budget.
In each round, a blind stream commits one or two changes to a row matching no
predicate, then validation and the compatible rebased CAS are serviced before the
next round. Thus it assumes bounded publication service, stronger than merely
scheduling every writer: weak fairness alone cannot bound CAS losses. Predicate
validation commits in the first planning attempt.
This establishes bounded progress, not starvation freedom under
arbitrary continuous contention or an unbounded eventual-success theorem.

The progress model includes full action history to avoid DAG joins. Stateright
0.31 DFS checks `Property::eventually` on terminal paths of this finite tree;
it does not detect arbitrary lassos or supply scheduler fairness.

`MODEL_COUNTS` prints the maximum winners examined and row images evaluated per
successful rule-2 statement, including its failed plans and repeated validations.
`COMMIT_COST_PROFILES` prints all distinct (writer, commit, planning snapshot,
winners examined, row images evaluated) tuples reached at successful publication.
Counts include evaluation of nonmatching before and added-row after images and
multiple predicates, if present; they exclude initial statement execution,
key merges, I/O bytes and runtime. They are symbolic work counts, not performance
measurements. Rule 2 obtains winner additions and evaluates predicates while
allowing independent read-dependent programs to stay in flight.

### Program choice, sequential statements and uncertain delivery

`read-widened-*` retains the original fixed-program profiles and adds program
selection at each statement admission. Each read-dependent writer chooses among
28 programs: relative UPDATE, predicate DELETE, INSERT SELECT into the same or
other table, absent-key conditional write, and aggregate gate plus INSERT.
Predicates use either non-key column and each constant in 0..=2. Updates that
leave that three-value domain report a statement error. The blind writer chooses
among 15 two-table keyed upserts, keyed deletes and appends. Appends target
key 2 in both tables; an occupied slot reports an error, so this family does not
establish two distinct successful appends by that writer. Each writer selects up to two sequential statements,
with at most three planning attempts per statement. The exhaustive profiles use total bounds 2 and 3. Their
bound limits total selected statements, errors included; it is separate from
the two-statement limit per writer. Larger six-statement profiles are sampled.

The safety profiles check optimistic rule 2 and rule 2 with stamping at commit.
Each rule-2 safeguard-removal control also runs with nondeterministic choices.
Required witnesses include two statements by one writer and, at total bounds
of at least three, a commit by each distinct writer.

`read-delivery-*` schedules immutable conditional requests separately from their
original and identical-retry deliveries. Responses are lost; head reads classify
attempts using retained accepted identities and the shared resolution primitive.
A delayed original or duplicate retains its exact base, program, plan and stamps.
There is at most one explicit unknown-outcome event globally and two deliveries
per immutable attempt. Every caller otherwise still resolves through a head read.
The caller remains undecided until its head read resolves the attempt.
Widened positive profiles use total selection bounds 1 and 2. Unknown-as-loss
and exact-base controls use bound 1; unknown-as-win also completes at bound 2.
The two-selection unknown-as-loss candidate exceeded the watchdog; larger
delivery bounds remain budget probes rather than completed declarations. The oracle
executes each logical statement once and compares every committed version;
I9, physical row multiplicity, I5 and acknowledgement coverage are separate checks.
Controls remove resolution before re-planning, evidence before reporting a win,
or the exact-base condition. The relative-update control must reach a doubled
update, not merely an incorrect classification.

`cold-promotion` explicitly stages warm inputs and cold outputs, then removes
and adds them in one exact-base head record. Two deliveries share the original
condition. It checks row multiplicity and checkpoint-plus-tail recovery on tombstone-free inputs, and
requires promotion, delayed retry and checkpoint-tail witnesses. Removing input
removal leaves two physical rows despite unchanged maximum-value observations.
The lifecycle's warm- and cold-input-removal controls independently exercise its
existing rewrite helper at one accepted record.

These remain finite abstractions. Two statements are separate sequential
statements, not one multi-statement SQL transaction. Exhaustive total bounds do
not establish safety for six successful statements, arbitrary SQL, NULLs,
unbounded retries, leases, evidence reclamation, or real object-store/engine
implementations. Delivery and maintenance are covered in separate compositions.

## Measured manual suite

On 2026-10-27, `EXHAUST=1 ... all` matched 251/251 declarations in 3368 s
(about 56 minutes). Every checker uses one exploration thread.

| Representative exhaustive profile | Bound | Visits / unique states | Wall s |
|---|---:|---:|---:|
| `read-widened-rule2-3` | 3 | 18,802,544 / 10,664,682 | 20.507 |
| `read-widened-delivery-2` | 2 | 138,138,251 / 35,851,190 | 232.903 |
| `transactions-three-participants-symmetry-0-exhaust` | 0 | 235,261,889 / 35,204,308 | 311.349 |
| `remote-maintenance` | 3 | 20,332,538 / 11,941,276 | 44.043 |
| `remote-maintenance-prepare` | 4 | 4,145,606 / 2,804,816 | 9.593 |
| `remote-maintenance-no-file-set-lease` | 3 | 25,444,930 / 14,662,520 | 54.460 |
| `read-transaction-widened-2` | 2 | 24,753,751 / 18,566,591 | 30.190 |
| `read-transaction-widened-stamp-at-commit-2` | 2 | 24,753,751 / 18,566,591 | 29.009 |
| `position-delete` | 1 | 62 / 53 | 0.000 |
| `remote-maintenance-clear-retry` | 4 | 3,492 / 2,637 | 0.012 |

The longest completed checker was `transactions-three-participants-symmetry-0-exhaust` at
311.349 s. All exhaustive declarations completed below the 540-second watchdog.
The original four-selection statement, three-selection delivery and two-selection
unknown-as-loss budget probes remain incomplete evidence at their captured source;
they are not declarations silently marked safe. New transactions exhaust two
selected transactions/blind records; three-selection transaction runs are samples.
Counts and wall times describe finite model exploration, not engine performance
or a proof at larger bounds. Program replay and multiplicity correspond to I12.

## Concurrent maintenance, transactions and position identity

Remote maintenance captures its own snapshot, output objects and exact head base.
CAS loss either re-validates all input, coverage, Prepare-cap and lease guards or
releases its unit and re-plans. Maintenance retains sequence ranges and advances
no table high water. The ingest acknowledgement seals a record; compatible retries
retain every originally drawn stamp. The graph counts physical live rows and
compares every committed prefix with the logical oracle, replay and checkpoint
plus tail (including PK keys). Both tables can be acknowledged together.

Leases exclude overlapping file sets until release or turnover. A turnover
records the unit's new generation in the head, so a plan from the old generation
loses its CAS, and its retry finds the new generation and re-plans. The
publication itself reads nothing else. With live-input guards retained,
overlapping unleased plans remain safe. With full lease exclusion and the retry's
lease check, skipping the other retry guards also remains safe in the finite graph. Removing
both leaves two output copies. The two-safeguard diagnostic names both removals.
`remote-maintenance-clear-retry` isolates a delete, cold rewrite, warm rewrite and
an unrelated child-table commit. It requires the same nonempty covered clear to
survive a CAS loss and publish, with coverage checked before removing the candidate
deletes. It uses one valid initial placement, two directed ingest operations,
four records, two maintainers, and no checkpoint or lease-turnover transitions.
The publication-fence necessity pair permits one turnover even at its minimal bound.

I4 checks admission and retained reference presence: this graph has no object GC,
readers, byte windows, unavailable storage or uncertain outcomes. Checkpoints capture
an immutable named state and PK collection; a stale checkpoint recovers via the
tail and never replaces current state with its older contents.

Transactions retain one BEGIN snapshot. Each statement has its own enabled step
and updates a private overlay. External publication can occur between statements.
A final record contains the ordered mutations and the union of every statement's
written keys, lookup keys and predicates, including intermediate writes. Conflicts
discard both statements; compatible retries shift the whole buffer per table.
The stamp-at-commit profile checks the other sequence choice. A separate interpreter
replays each accepted transaction as a unit at every committed version. A failed
second statement publishes nothing. Omitting a lookup of a key already written
in the transaction does not remove the union's write conflict for that key; the
matching control is deliberately expected to hold. The broader three-selection
runs are samples, not exhaustive results. These transactions are not joined to
Prepare, delivery uncertainty, maintenance, leases or GC.

A position delete names an immutable file identity and row position. Rebase uses
the accepted replacement's row mapping, preserving the targeted row identity as
positions change. A complete mapping entry may instead prove prior deletion; the
operation then completes as a no-op. Missing/expired mappings
are not modeled and cannot be treated as proof of deletion. Retain mapping evidence
until outstanding operations can resolve or re-plan. The graph covers one rewrite,
not chains, multiple output files, keyed replacements or mapping reclamation.
