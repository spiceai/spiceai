# Head-log fault injection and model trace replay

Traces generated from the `cayenne-metastore-model` driver at branch ben/metastore1
cbd2f28b9a (the model merged from ben/metastore-protocol-model cf5af83b40), 2026-10-29
UTC. The commit containing this file identifies the replay sources.

The fault-store, commit, source and window replays run against InMemory and
LocalConditionalPut::new_durable. The window replay maps every model action except
Gc and Query: each Checkpoint(c) writes the prototype checkpoint at commit c, and
the next accepted commit publishes the floor, as in the model.

## Files

- ../../src/sim/fault.rs: path/operation-selected hold, fail, lost-response,
  repeated identical delivery, async before-hook, ordered applied-operation log.
  Canceled caller futures do not erase captured requests.
- ../fault.rs: two integration tests, each on both backends; every fault, caller
  cancellation, real before-hook I/O, forwarding and operation-log order.
- ../headlog_replay.rs: three positive replay tests, four negative-control tests,
  and listed-but-missing batch checks for cold open and lagging poll.
- *.jsonl: seven driver-generated trace sets; positive sets include walks.
- evidence/: the driver's verbatim output per trace set (model-*.txt), the replay
  output (replay-and-controls.txt), and the checkpoint-root control
  (replay-control-checkpoint-root.txt). Temporary directories are shown as <tmp>.

This prototype is not wired into Spice or a Spicepod; these tests exercise the
real head-log API and real stores directly. The replay cancels tasks, drops
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

The window set holds 21 Checkpoint actions (Checkpoint(1) ×3, (2) ×12, (3) ×5,
(4) ×1), 17 Gc and 20 Query.

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
| window | Checkpoint(c) | publish_checkpoint(c, serialized serial prefix [..c]) on the current writer, which is kept for the next prepare so the next accepted commit publishes the floor. Model `c` is a listed batch end or the record before the window; commits number from 1, so model `c` is prototype commit `c`. A refusal fails the replay. |
| window | Gc / Query | Skip and count. The head log has no GC yet; the retention family will cover removal and running queries. |
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


### Coverage and limits

Skipped actions, counted per action across both backends:

```text
commits-restart-turnover: witnesses=4 walks=20 backends=memory,durable-local skipped={}
source-positions: witnesses=3 walks=20 backends=memory,durable-local skipped={}
window-storage-6-exhaust: witnesses=3 walks=20 backends=memory,durable-local skipped={"Gc": 34, "Query": 40}
```

A positive-trace Append that the writer refuses fails the replay, as does a
refused Checkpoint. Gc and Query are not replayed, so no claim is made about
removal or about a running query's batches. The JSON carries no per-action
head, window or floor, so the replay checks what readers see (cold open and
incremental poll against the serial reference), not internal state alignment.

**Checkpoint control.** With the root written one record short
(`serial[..c - 1]`), window_storage fails at the commit that publishes the floor:
`trace=window-storage-6-exhaust "delayed seal after GC" step=8 action=Append(0) expected invariants hold; actual=I1/I6 expected [...]`
(evidence/replay-control-checkpoint-root.txt).

## Trace-generation commands

Run serially from the repository root. TRACE_JSON appends, so each set starts from
a fresh file; WALK replaces checking, so walks run as a second command into the
same file.

    cargo build --release -p cayenne-metastore-model
    EXHAUST=1 TRACE_JSON=$R/replay-commits.jsonl target/release/cayenne-metastore-model run commits-restart-turnover
    EXHAUST=1 TRACE_JSON=$R/replay-window.jsonl target/release/cayenne-metastore-model run window-storage-6-exhaust
    EXHAUST=1 TRACE_JSON=$R/replay-source.jsonl target/release/cayenne-metastore-model run source-positions
    EXHAUST=1 TRACE_JSON=$R/replay-commits-control.jsonl target/release/cayenne-metastore-model run commits-restart-no-persist-before-send
    EXHAUST=1 TRACE_JSON=$R/replay-window-control.jsonl target/release/cayenne-metastore-model run window-storage-no-exists-5-exhaust
    EXHAUST=1 TRACE_JSON=$R/replay-source-ack-control.jsonl target/release/cayenne-metastore-model run source-ack-before-acceptance
    EXHAUST=1 TRACE_JSON=$R/replay-source-skip-control.jsonl target/release/cayenne-metastore-model run source-no-redelivery-skip
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=$R/replay-commits.jsonl target/release/cayenne-metastore-model run commits-restart-turnover
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=$R/replay-window.jsonl target/release/cayenne-metastore-model run window-storage-6-exhaust
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=$R/replay-source.jsonl target/release/cayenne-metastore-model run source-positions

Each walk run printed `WALKS <run> seed=42 count=20 max_steps=40 good=true`.

| Generated file | Trace filename |
|---|---|
| replay-commits.jsonl | commits-restart-turnover.jsonl |
| replay-window.jsonl | window-storage.jsonl |
| replay-source.jsonl | source-positions.jsonl |
| replay-commits-control.jsonl | commits-no-persist.jsonl |
| replay-window-control.jsonl | window-no-exists.jsonl |
| replay-source-ack-control.jsonl | source-ack-early.jsonl |
| replay-source-skip-control.jsonl | source-no-skip.jsonl |

## Validation

    cargo nextest run -p object_store_occ --test headlog_replay --no-capture --retries 0

`8 tests run: 8 passed`; the replay and control lines are in
evidence/replay-and-controls.txt.
