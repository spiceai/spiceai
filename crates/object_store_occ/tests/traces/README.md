# Head-log fault injection and model trace replay

Source HEAD: c52c3272215699b576d838ec7e91099080e8d661, branch
ben/metastore1. Run date: 2026-10-28 UTC. The replay implementation is an
**uncommitted worktree diff**; its source/trace identities are in
[evidence/SHA256SUMS](evidence/SHA256SUMS).

The fault-store and commit/source replays run against InMemory and
LocalConditionalPut::new_durable. **Window replay is partial**, for the
checkpoint API mismatch detailed below. Passing its checks establishes equality
for the operations actually applied, not a faithful replay of the model's
checkpoint/GC schedules.

## Files

- ../../src/sim/fault.rs: path/operation-selected hold, fail, lost-response,
  repeated identical delivery, async before-hook, ordered applied-operation log.
  Canceled caller futures do not erase captured requests.
- ../../src/sim/mod.rs: exports the fault module.
- ../fault.rs: two integration tests, each on both backends; every fault, caller
  cancellation, real before-hook I/O, forwarding and operation-log order.
- ../headlog_replay.rs: three positive replay tests, four negative-control tests,
  and listed-but-missing batch checks for cold open and lagging poll.
- *.jsonl: seven driver-generated trace sets; positive sets include walks.
- evidence/: verbatim stdout/stderr, source hashes and failed Git staging.
- ../../src/headlog/mod.rs: remove the redundant else following an unconditional
  return in Writer::commit, solely to satisfy the required Clippy gate. This
  should be committed separately from the replay work. No protocol change is
  made here. See the before-cleanup Clippy output.

This prototype is not wired into Spice or a Spicepod; these tests exercise the
real head-log API and real stores directly. The existing WAL process-kill
harness tests a different protocol, so this replay instead cancels tasks, drops
Writer handles, clears volatile receipts and deserializes only persisted bytes
for Writer::resume. This is a storage-boundary restart simulation, not an OS
process-kill test of the head log.

## Trace counts

Every positive trace runs on both backends. Positive trace lines have
initial_index=0. Walks use seed 42, count 20, max_steps 40.

| Trace set / family | Witnesses | Walks | Counterexample witnesses in file |
|---|---:|---:|---:|
| commits-restart-turnover / commits | 4 | 20 | 0 |
| window-storage / window | 3 | 20 | 0 |
| source-positions | 3 | 20 | 0 |
| commits-no-persist / commits | 6 | 0 | 2 |
| window-no-exists / window | 5 | 0 | 3 |
| source-ack-early / source-positions | 7 | 0 | 4 |
| source-no-skip / source-positions | 6 | 0 | 4 |

Control tests select FAIL witnesses for the named invariant: I2 for persistence
and early acknowledgement, I4 for batch existence, all three I10 witnesses for
source redelivery. Other control-file witnesses remain exactly as emitted by
the driver but are not replayed. Each selected trace first runs with its
safeguard enabled, then with only its matching replay-harness safeguard removed.
The test succeeds only when the former holds and the latter reports the
expected invariant failure.

## Mapping actually implemented

The serial oracle registers each prepared request's exact head bytes with its
logical payload. It derives its ordered logical-write list solely from
FaultStore's successful head CAS log. It does not use the head-log writer,
reader, batch-cut or resolution algorithms to compute expected rows. Successful
unregistered head CASes identify lease transitions; registered proposals must
have the current lease at acceptance.

After every mapped action, cold-open payloads equal this list and an incremental
reader polled at every step agrees. Published checkpoint roots contain the
serialized serial prefix. Accepted requires a logged landing; Rejected/Fenced
requires no landing and a superseded ETag. An acknowledged logical write occurs
exactly once, and no logical write occurs twice. Source rows equal the
independent numbered stream 1..=logged_position, with acknowledgements no further
ahead. An explicit deletion test requires open/poll errors for a missing listed
batch and verifies the failed-poll cursor stays unchanged.

| Family | Action | Mapping |
|---|---|---|
| commits | Initial state | Commit two distinct filler payloads, K=2; gate batch creates until Copy. Discard filler handles before model actions. |
| commits | Buffer(w) | Store volatile logical-write buffer; all traces have w=0. |
| commits | Issue(w) | Resume current owner and prepare logical-write-0. After restart, the safe control harness reuses durable exact work instead of replacing it. |
| commits | Persist(w) | Serialize PreparedAttempt to durable byte map. |
| commits | Send(w) | Spawn held commit. Safe harness persists first even if a control trace omits Persist. |
| commits | Deliver(w,i,0) | Deliver original captured request for attempt i. |
| commits | Deliver(w,i,1) | Deliver captured identical resolution retry, or re-deliver original if no separate retry exists. |
| commits | Response(w) | Release delivered response. If a resolve retry waits while an original response is available, cancel waiting handle, release original response and resolve exact saved receipt. |
| commits | Resolve(w) | Spawn resolve; capture any identical retry and leave its future pending. |
| commits | Unreadable(w) | Cancel waiting handle, fail next head GET and require resolve error. Implemented but absent from the selected restart traces; source Unknown and fault tests exercise unreadable-head faults. |
| commits | Ack(w) | Record logical acknowledgement and check exactly-once coverage. |
| commits | Restart | Abort/await tasks; clear volatile receipts, outcomes, buffers and active handle. Captured requests and serialized durable receipts survive. |
| commits | Restore | Deserialize durable bytes and construct Writer::resume for saved generation. |
| commits | Fence | Another Writer::take_over at current lease+1. Old receipts keep their generation. |
| commits | Copy(i) | Apply queued exact failed immutable create with gate temporarily open. Verify existing copies byte-for-byte. |
| commits | Observe | Collect observed batch bytes from inline sealing failures; deduplicate and sort by path/bytes. |
| commits | Poll / Recover | Incremental poll / cold open, plus all invariant checks. |
| window | Append(z) | Prepare, persist, held commit, deliver, respond with exactly sized record. Refuse drops if batch create remains gated. |
| window | Observe | Refresh pending-copy list from inline sealing attempts. |
| window | Create(j) | Apply pending immutable copy j with gate temporarily open. |
| window | Checkpoint | Call publish_checkpoint at latest published batch boundary and retain Writer through next prepare, or require refusal at inline boundary. Partial mapping, detailed below. |
| window | Gc / Pin | Skip and count; no GC or pins in head log. |
| source | Receive | Consume next numbered change, skipping logged positions unless no-skip control is active. |
| source | Issue | Filter logged positions again; prepare (rows, position), serialize receipt and spawn held commit. |
| source | Deliver(i,q) | Deliver original/identical retry for model attempt i. Safe control replays retain no-op index slots for removed redundant Issues. |
| source | Resolve | Resolve exact receipt; capture identical retries. |
| source | Unknown | Drop held response and fail next head read; require caller error. Captured request remains deliverable. |
| source | Acknowledge | Acknowledge only already-logged buffer, unless early-ack control is active. |
| source | Crash | Abort/await and drop handles; clear volatile receipt/buffer; reset cursor to acknowledged+1. Captured requests survive. |
| source | Takeover | Crash mapping, then take_over at next generation. |
| source | Acquire | take_over at next generation. |

The source model's lease-zero initial state maps to real lease 1; later advances
preserve lease order/fencing.

### Byte/count mapping

One symbolic byte maps to 10,000 original encoded Record bytes. Append(0) is
10,000 bytes and Append(1) is 20,000 bytes, including the attempt/generation and
protobuf payload envelopes (payload lengths 9,977 and 19,976).
K=3, B=30,000, R=20,000, H=55,000. H reserves 5,000 bytes for real head envelopes
and index; for these bounded traces that allowance is smaller than one symbolic
byte. Thus pressure at six symbolic bytes remains distinct from a five-byte
window, with the same count and oldest-prefix batch cuts.

### Unmapped/partial actions and limits

Model Checkpoint immediately snapshots all current records, clears the window,
and allows subsequent appends without those records. The head-log API only
checkpoints already-published immutable batch boundaries, publishing the floor
on the next accepted commit. More trace JSON cannot reconcile this semantic
difference. Positive replays insert no extra commits, premature drops, or
imaginary checkpoint publication.

Across both backend runs the window replay reports:

```text
window-storage-6-exhaust: witnesses=3 walks=20 backends=memory,durable-local skipped={"Append refused: batch not yet created": 64, "Checkpoint partially mapped: published batch prefix staged; model publishes full window immediately": 2, "Checkpoint refused: no published batch boundary": 40, "Create: checkpoint-model pending copy has no head-log equivalent": 4, "Gc": 42, "Pin": 40}
```

Counts are per action across both backends, not trace counts. All 64 positive
Append refusals occur after an unsupported/partial checkpoint; the harness
requires an error if a positive-model append is refused before that point.
Four later Create actions have no equivalent queued real copy. These remain
explicit coverage gaps, not evidence that the full window model was replayed.
Gc (42 actions) and Pin (40) are skipped as specified.

Not replayed: generations, pin-expiry, snapshot leases; the head log has no
GC/pin implementation. No claims about those invariants are made.

The driver's JSON is sufficient for the mapped fixed initial states and
actions. It lacks explicit pending-create contents, per-action head/window/floor
and logical writer/source observations; those fields would allow independent
state-alignment checks and better localization of divergence after Checkpoint.
They would not supply the missing immediate full-window checkpoint operation
in the real API. The model crate has not been edited.

## Exact trace-generation commands

These commands were run serially with the existing RUSTC_WRAPPER, no added
features. TRACE_JSON appends; use fresh output files when regenerating.
WALK replaces checking in this driver, so walks and exhaustive witnesses require
separate commands.

    EXHAUST=1 TRACE_JSON=/tmp/replay-commits.jsonl WALK=42:20:40 cargo run --release -p cayenne-metastore-model -- run commits-restart-turnover
    EXHAUST=1 TRACE_JSON=/tmp/replay-commits.jsonl cargo run --release -p cayenne-metastore-model -- run commits-restart-turnover > /tmp/replay-commits.out 2>&1
    EXHAUST=1 TRACE_JSON=/tmp/replay-window.jsonl cargo run --release -p cayenne-metastore-model -- run window-storage-6-exhaust > /tmp/replay-window.out 2>&1
    EXHAUST=1 TRACE_JSON=/tmp/replay-source.jsonl cargo run --release -p cayenne-metastore-model -- run source-positions > /tmp/replay-source.out 2>&1
    EXHAUST=1 TRACE_JSON=/tmp/replay-commits-control.jsonl cargo run --release -p cayenne-metastore-model -- run commits-restart-no-persist-before-send > /tmp/replay-commits-control.out 2>&1
    EXHAUST=1 TRACE_JSON=/tmp/replay-window-control.jsonl cargo run --release -p cayenne-metastore-model -- run window-storage-no-exists-5-exhaust > /tmp/replay-window-control.out 2>&1
    EXHAUST=1 TRACE_JSON=/tmp/replay-source-ack-control.jsonl cargo run --release -p cayenne-metastore-model -- run source-ack-before-acceptance > /tmp/replay-source-ack-control.out 2>&1
    EXHAUST=1 TRACE_JSON=/tmp/replay-source-skip-control.jsonl cargo run --release -p cayenne-metastore-model -- run source-no-redelivery-skip > /tmp/replay-source-skip-control.out 2>&1
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=/tmp/replay-window.jsonl cargo run --release -p cayenne-metastore-model -- run window-storage-6-exhaust
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=/tmp/replay-source.jsonl cargo run --release -p cayenne-metastore-model -- run source-positions

The last two commands ran via subprocess.run with captured stdout/stderr.
Verbatim walk stdout:

    WALKS commits-restart-turnover seed=42 count=20 max_steps=40 good=true
    WALKS window-storage-6-exhaust seed=42 count=20 max_steps=40 good=true
    WALKS source-positions seed=42 count=20 max_steps=40 good=true

Files were copied byte-for-byte from these generated paths:

| Generated path | Trace filename |
|---|---|
| /tmp/replay-commits.jsonl | commits-restart-turnover.jsonl |
| /tmp/replay-window.jsonl | window-storage.jsonl |
| /tmp/replay-source.jsonl | source-positions.jsonl |
| /tmp/replay-commits-control.jsonl | commits-no-persist.jsonl |
| /tmp/replay-window-control.jsonl | window-no-exists.jsonl |
| /tmp/replay-source-ack-control.jsonl | source-ack-early.jsonl |
| /tmp/replay-source-skip-control.jsonl | source-no-skip.jsonl |

Verbatim exhaustive-driver outputs, including every property and minimized
trace, are retained in evidence/model-*.txt. Every run reports matched=true,
including controls whose expected result is their downstream property failure.

## Validation commands and verbatim output

The initial default-target test command waited for another process's Cargo lock
and was interrupted before running tests. Subsequent Rust validation used the
same dedicated target directory and feature set, with RUSTC_WRAPPER unchanged.

The final command covers all three positive families, every selected control
with and without its safeguard, both fault tests, and the missing-listed-batch
test. The negative-control tests pass only when the removed safeguard produces
the expected failure. The following is its complete, unedited stdout/stderr:

    CARGO_TARGET_DIR=/tmp/object-store-replay-target cargo test -p object_store_occ --test headlog_replay --test fault -- --nocapture --test-threads=1 > /tmp/replay-final.out 2>&1

```text
   Compiling object_store_occ v0.1.0 (/Users/ben/orca/workspaces/spiceai/metastore1/crates/object_store_occ)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 1.54s
     Running tests/fault.rs (/tmp/object-store-replay-target/debug/deps/fault-4681db8befb3c8e4)

running 2 tests
test canceled_call_still_delivers_and_forwarded_operations_are_logged ... InMemory: canceled caller retains delivery; hook/put/get/list/copy/delete/multipart log order holds
LocalConditionalPut(/var/folders/vq/h3b18yld7356tgpw9zdffjb40000gn/T/.tmpKWwAjA, durable): canceled caller retains delivery; hook/put/get/list/copy/delete/multipart log order holds
ok
test faults_memory_and_durable_local ... InMemory: hold, delayed response, duplicate condition, before-hook, ambiguous landing, failed put/get, dropped held response
LocalConditionalPut(/var/folders/vq/h3b18yld7356tgpw9zdffjb40000gn/T/.tmpYOQkAf, durable): hold, delayed response, duplicate condition, before-hook, ambiguous landing, failed put/get, dropped held response
ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s

     Running tests/headlog_replay.rs (/tmp/object-store-replay-target/debug/deps/headlog_replay-cba6f1bda5916fca)

running 8 tests
test commits_restart_turnover ... commits-restart-turnover: witnesses=4 walks=20 backends=memory,durable-local skipped={}
ok
test control_ack_before_acceptance ... CONTROL backend=memory safe=holds removed=caught trace=source-ack-before-acceptance "I2 source acknowledgement covered" step=3 action=Acknowledge expected invariants hold; actual=I2 expected acknowledgement <= 0; actual 1 actions=["Receive", "Issue", "Acknowledge"]
CONTROL backend=durable-local safe=holds removed=caught trace=source-ack-before-acceptance "I2 source acknowledgement covered" step=3 action=Acknowledge expected invariants hold; actual=I2 expected acknowledgement <= 0; actual 1 actions=["Receive", "Issue", "Acknowledge"]
ok
test control_batch_exists ... CONTROL backend=memory safe=holds removed=caught trace=window-storage-no-exists-5-exhaust "I4 reader inline payload replay equality" step=3 action=Append(1) expected invariants hold; actual=I4 expected readable committed prefix; actual Head log storage error: Object at location replay/wal/00000000000000000001 not found: No data in memory found. Location: replay/wal/00000000000000000001 actions=["Append(1)", "Append(1)", "Append(1)"]
CONTROL backend=durable-local safe=holds removed=caught trace=window-storage-no-exists-5-exhaust "I4 reader inline payload replay equality" step=3 action=Append(1) expected invariants hold; actual=I4 expected readable committed prefix; actual Head log storage error: Object at location /private/var/folders/vq/h3b18yld7356tgpw9zdffjb40000gn/T/.tmp6KxHRQ/replay/wal/00000000000000000001 not found: No such file or directory (os error 2) actions=["Append(1)", "Append(1)", "Append(1)"]
ok
test control_persist_before_send ... CONTROL backend=memory safe=holds removed=caught trace=commits-restart-no-persist-before-send "I2 acknowledged logical write exactly once" step=12 action=Deliver(0, 1, 1) expected invariants hold; actual=I2 expected logical-write-0 count <= 1 (exactly 1 after acknowledgement); actual 2 actions=["Buffer(0)", "Copy(0)", "Issue(0)", "Send(0)", "Resolve(0)", "Deliver(0, 0, 1)", "Restart", "Buffer(0)", "Issue(0)", "Send(0)", "Resolve(0)", "Deliver(0, 1, 1)", "Response(0)", "Ack(0)"]
CONTROL backend=durable-local safe=holds removed=caught trace=commits-restart-no-persist-before-send "I2 acknowledged logical write exactly once" step=12 action=Deliver(0, 1, 1) expected invariants hold; actual=I2 expected logical-write-0 count <= 1 (exactly 1 after acknowledgement); actual 2 actions=["Buffer(0)", "Copy(0)", "Issue(0)", "Send(0)", "Resolve(0)", "Deliver(0, 0, 1)", "Restart", "Buffer(0)", "Issue(0)", "Send(0)", "Resolve(0)", "Deliver(0, 1, 1)", "Response(0)", "Ack(0)"]
ok
test control_redelivery_skip ... CONTROL backend=memory safe=holds removed=caught trace=source-no-redelivery-skip "I10 source append prefix exactly once" step=12 action=Deliver(2, 0) expected invariants hold; actual=I10 expected rows [1] at position 1; actual [1, 1] actions=["Receive", "Issue", "Takeover", "Receive", "Issue", "Resolve", "Deliver(1, 1)", "Crash", "Acquire", "Receive", "Issue", "Deliver(2, 0)"]
CONTROL backend=memory safe=holds removed=caught trace=source-no-redelivery-skip "I10 logged source position has its rows" step=12 action=Deliver(2, 1) expected invariants hold; actual=I10 expected rows [1] at position 1; actual [1, 1] actions=["Receive", "Issue", "Deliver(0, 0)", "Takeover", "Receive", "Issue", "Crash", "Acquire", "Receive", "Issue", "Resolve", "Deliver(2, 1)", "Resolve", "Acknowledge", "Receive", "Issue", "Deliver(3, 0)"]
CONTROL backend=memory safe=holds removed=caught trace=source-no-redelivery-skip "I10 complete source state matches logged prefix" step=12 action=Deliver(2, 0) expected invariants hold; actual=I10 expected rows [1] at position 1; actual [1, 1] actions=["Receive", "Issue", "Takeover", "Receive", "Issue", "Resolve", "Deliver(1, 1)", "Crash", "Acquire", "Receive", "Issue", "Deliver(2, 0)"]
CONTROL backend=durable-local safe=holds removed=caught trace=source-no-redelivery-skip "I10 source append prefix exactly once" step=12 action=Deliver(2, 0) expected invariants hold; actual=I10 expected rows [1] at position 1; actual [1, 1] actions=["Receive", "Issue", "Takeover", "Receive", "Issue", "Resolve", "Deliver(1, 1)", "Crash", "Acquire", "Receive", "Issue", "Deliver(2, 0)"]
CONTROL backend=durable-local safe=holds removed=caught trace=source-no-redelivery-skip "I10 logged source position has its rows" step=12 action=Deliver(2, 1) expected invariants hold; actual=I10 expected rows [1] at position 1; actual [1, 1] actions=["Receive", "Issue", "Deliver(0, 0)", "Takeover", "Receive", "Issue", "Crash", "Acquire", "Receive", "Issue", "Resolve", "Deliver(2, 1)", "Resolve", "Acknowledge", "Receive", "Issue", "Deliver(3, 0)"]
CONTROL backend=durable-local safe=holds removed=caught trace=source-no-redelivery-skip "I10 complete source state matches logged prefix" step=12 action=Deliver(2, 0) expected invariants hold; actual=I10 expected rows [1] at position 1; actual [1, 1] actions=["Receive", "Issue", "Takeover", "Receive", "Issue", "Resolve", "Deliver(1, 1)", "Crash", "Acquire", "Receive", "Issue", "Deliver(2, 0)"]
ok
test listed_missing_batch_is_error ... I4 backend=memory open=Head log storage error: Object at location replay/wal/00000000000000000001 not found: No data in memory found. Location: replay/wal/00000000000000000001; poll=Head log storage error: Object at location replay/wal/00000000000000000001 not found: No data in memory found. Location: replay/wal/00000000000000000001; cursor=0
I4 backend=durable-local open=Head log storage error: Object at location /private/var/folders/vq/h3b18yld7356tgpw9zdffjb40000gn/T/.tmpMv2Ked/replay/wal/00000000000000000001 not found: No such file or directory (os error 2); poll=Head log storage error: Object at location /private/var/folders/vq/h3b18yld7356tgpw9zdffjb40000gn/T/.tmpMv2Ked/replay/wal/00000000000000000001 not found: No such file or directory (os error 2); cursor=0
ok
test source_positions ... source-positions: witnesses=3 walks=20 backends=memory,durable-local skipped={}
ok
test window_storage ... CUTS backend=memory batch_ends=[3, 4] window_record_bytes=[20000, 20000] head_encoded_bytes=40044
CUTS backend=durable-local batch_ends=[3, 4] window_record_bytes=[20000, 20000] head_encoded_bytes=40044
window-storage-6-exhaust: witnesses=3 walks=20 backends=memory,durable-local skipped={"Append refused: batch not yet created": 64, "Checkpoint partially mapped: published batch prefix staged; model publishes full window immediately": 2, "Checkpoint refused: no published batch boundary": 40, "Create: checkpoint-model pending copy has no head-log equivalent": 4, "Gc": 42, "Pin": 40}
ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 24.62s

```

Required Clippy gate:

    CARGO_TARGET_DIR=/tmp/object-store-replay-target cargo clippy -p object_store_occ --no-deps --tests > /tmp/replay-clippy-final.out 2>&1

```text
    Checking object_store_occ v0.1.0 (/Users/ben/orca/workspaces/spiceai/metastore1/crates/object_store_occ)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.89s
```

The broader crate run used:

    cargo test -p object_store_occ --target-dir /tmp/object-store-replay-target -- --nocapture > /tmp/replay-all-tests.out 2>&1

Its complete output is in evidence/crate-tests.txt: 139 passed, 0 failed,
3 ignored across targets. That run preceded the added cancellation/forwarding
fault test and explicit byte/count-cut diagnostic assertions. Both current fault tests and all replay tests were then run by the
final command above; the subsequent Clippy output is also for the current files.

Initial replay runs failed due to harness filler indexing, pending response
selection and redundant source-attempt indexing; these mappings were corrected
without changing the head-log protocol. No head-log runtime bug is claimed.
The Clippy-before-cleanup output records a pre-existing redundant-else error
in headlog/mod.rs; the final Clippy output records its absence.

## Commit and staging status

**No new commit SHA exists.** Git staging was denied by the filesystem sandbox:
the linked worktree Git metadata is outside this session's writable roots.
The cached diff remains empty. Unrelated initial changes to Cargo.toml,
crates/cayenne/src/cayenne_catalog.rs, crates/cayenne/src/metastore_tap.rs and
the target-tapverify directory were left untouched. Cargo.lock was not changed.

Source HEAD remains c52c3272215699b576d838ec7e91099080e8d661. Intended commits:
first the isolated redundant-else lint cleanup, then the fault-store/replay,
trace files and evidence. No commit, push, rebase, amend or stash was performed.

Verbatim staging attempt/readback:

```text
$ git add crates/object_store_occ/src/headlog/mod.rs
fatal: Unable to create '/Users/ben/Code/spiceai/.git/worktrees/56594-26fcf00c-d12c-4ec7-aecb-3ae72c921784/index.lock': Operation not permitted

$ git diff --cached --stat
```
