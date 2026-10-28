# Head log prototype implementation

This is an internal prototype, separate from `wal/` and `WalStateStore`.
It is not wired into Cayenne. Its integration tests exercise real memory and
fsync-enabled local object stores, and optionally S3, rather than a Spicepod:
there is currently no engine entry point through which a Spicepod can invoke it.

## Delivered scope

- Protobuf head, original encoded records, deterministic immutable batches.
- Prepared, serializable exact CAS receipts; bounded identical retries and
  resolution through head/window/batches/checkpoint evidence.
- Head-generation takeover/fencing, restart of an externally arbitrated owner.
- Conditional polling and parallel batch/checkpoint cold replay.
- Immutable generation-scoped caller uploads with BLAKE3 object references.
- Caller checkpoints, evidence retention, floor publication on the next commit.
- Conditional-write enforcement probes for creation and writer construction.

**Not implemented:** GC, pins, removal tracking, retirement interlocks,
predecessor sweeping, reference admission, reference reachability reconstruction,
or deletion of old batches/checkpoints. No immutable history is deleted. Callers
are responsible for interpreting opaque actions, applying guards, and checking
referenced data/page objects. This prototype alone does not implement those
Cayenne state-machine responsibilities.

## Public API

All operations that access storage are async.

- `HeadLog::create(Arc<dyn ObjectStore>, Path, HeadLogConfig) -> Result<Head>`.
- `Writer::take_over(store, prefix, config, new_generation) -> Result<Writer>`.
- `Writer::resume(store, prefix, config, generation) -> Result<Writer>` resumes
  an externally arbitrated generation. Recover and resolve any persisted receipt
  before preparing a new logical write. An older generation may be recovered
  for resolution only after takeover; it cannot prepare or upload. Resume is not
  lease arbitration.
- `Writer::prepare(Bytes) -> Result<PreparedAttempt>` and
  `prepare_group(Vec<Bytes>) -> Result<PreparedAttempt>`. Persist and sync the
  receipt before sending. A pending attempt blocks preparation of another one.
- `Writer::commit(&PreparedAttempt) -> Result<Outcome>` and
  `resolve(&PreparedAttempt) -> Result<Outcome>`.
- `Writer::commit_group(Vec<Bytes>, persist_callback) -> Result<Outcome>` calls
  the synchronous persistence callback before sending the single group record.
  The callback must durably save the receipt; use prepare_group for async
  persistence. Callback failure leaves the receipt pending.
- `Writer::state() -> &HeadState` includes the decoded head and CAS version.
- `Writer::sealing_error() -> Option<&Error>` exposes inline maintenance errors
  after acceptance. The next preparation retries the required copy before drop.
- `Writer::upload(name, Bytes) -> Result<ObjectRef>`; names can contain relative
  subdirectories. Existing different content is an error.
- `Writer::publish_checkpoint(at, root) -> Result<()>` stages a boundary-aligned
  root. Only the next accepted record publishes its floor.
- `Reader::open(store, prefix) -> Result<(Reader, Opened)>`, `poll() -> Result<Poll>`,
  and `state() -> &HeadState`.
- Public data types: `HeadLogConfig`, prost `Head`, `WindowRecord`, `Record`, `Group`,
  serde `PreparedAttempt` with `attempt_id()` accessor, `Outcome`, `HeadState`,
  `ObjectRef`, `RecordWithCommit`, `Opened`, `Poll`, `Error`, and `Result<T>`.

`Outcome` is Accepted { commit }, Rejected, Unresolved, or Fenced { head_lease }.
`Poll` is Unchanged, Records(Vec<RecordWithCommit>), or Reopen. Reopen does not
advance the reader: the caller must cold-open again. Errors never imply rejection.

## Protocol choices and clarifications

1. Preparation is async and fallible, unlike the illustrative synchronous
   signature. It may have to verify an existing immutable batch before it can
   return the exact proposed head to persist. Preparing never sends the head CAS.
2. Records are encoded once. B and R count original encoded record bytes,
   excluding repeated-bytes batch framing; H bounds the entire encoded head,
   including its index and envelopes. Count pressure uses K as the window limit.
3. All writers of a log must use the same config. As specified, those constants
   are not persisted in the head; configuration negotiation is not implemented.
4. Sealing runs inline after acceptance, using the now-observed head. A failed
   seal cannot undo acceptance; it is exposed through sealing_error(), and the
   next drop must retry it successfully. Batches are verified byte-for-byte on
   duplicate creates, and on other failed immutable creates that might have landed.
5. Identical CAS retries are bounded at three sends per call. Exhaustion returns
   RetryExhausted, preserving the receipt and pending state. An unreadable head
   returns an error immediately and sends no retry. A caller may resolve later.
6. The design's phrase "Never retry an attempt until it has resolved as rejected"
   is interpreted as forbidding a NEW logical attempt, not the explicitly
   permitted identical CAS retry in its resolution table.
7. Proven historical acceptance takes priority over fencing when resolving an
   already-accepted receipt. Otherwise a newer head lease returns Fenced and
   disables further preparation/uploads. Head-incarnation changes are errors,
   never acceptance/rejection inferred from the replacement log.
8. A lost takeover response returns an error. Generation arbitration is external;
   after verifying ownership, the caller may resume that generation. This avoids
   guessing that a competing same-generation candidate was this caller.
9. Receipts include prefix, incarnation, generation, base commit, attempt id,
   both version fields, and exact proposed head bytes. They are validated on
   recovery. Registering a recovered receipt as pending must happen before I/O,
   including reads that can fail.
10. Each checkpoint root is a prost envelope containing caller bytes, its
    incarnation/commit, and retained attempt ids. All accepted ids covered by
    earlier checkpoints are retained without a horizon in this prototype. A
    missing id below the floor yields Unresolved. Checkpoint 0 is not published.
11. The first batch starts at floor+1, subsequent starts follow the prior end;
    readers validate dense coverage and batch lengths. All listed reads for cold
    open run concurrently with checkpoint loading. Poll fetches only ranges it
    needs and never advances after a failed read.
12. An ETag is required for conditional polling as well as a usable CAS version.
    No version-only polling fallback is implemented. Creation/takeover/resume
    reject stores that fail the existing conditional-write enforcement probe.
13. Batch indices contain ends, as specified by both head schemas. No batch digest
    field is invented. ObjectRef includes the caller upload's BLAKE3 digest;
    callers must encode and verify their opaque payload references themselves.
14. GC and pin behavior have not been approximated. Opaque payloads and the
    requested refs/removal API do not by themselves specify a restartable state
    projection for reachability at every protected commit. That contract needs
    an explicit caller/state-machine interface before safe predecessor deletion.

## Design issue exercised by the tests

The feasibility inequality H >= B + R is necessary but is insufficient when H
bounds the full protobuf head: window wrappers, head metadata, and the batch
index also consume bytes. The exact-boundary integration experiment uses
H=300, B=200, R=100, K=64. It publishes two 100-byte records, then attempts a
third. There is no published successor to finalize the two-record batch and no
checkpoint boundary. Preparation reports a structured error rather than exceeding
H or sealing from the unpublished proposal. Choose headroom for the metadata and
checkpoint before the index consumes it. A complete budgeting contract should
reserve this overhead explicitly; this prototype does not silently reinterpret H
as a window-only budget. The test output records the actual encoded sizes.

## Validation

See `VALIDATION.md` beside this file for exact commands, outputs, date and source
identity, including the independent-review regression's before/after run.
S3 only participates when AWS_S3_BUCKET is set. The tests print its unique prefix
when running against it. Shared-worktree Cargo uses its existing RUSTC_WRAPPER
and target directory; no alternate feature flags are used.
