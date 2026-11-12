# Transactions-family model traces

Driver output of `cayenne-metastore-model` for the `transactions` family, replayed
through two real dataset logs by
`src/metastore/object_native/catalog/replay_tests.rs` (on `InMemory` and
`LocalConditionalPut::new_durable`). The module's doc comment gives the mapping
from model actions to prototype calls and the guarantees checked after every
action.

| File | Run | Content |
|---|---|---|
| transactions.jsonl | transactions-txn-1-exhaust | 9 witnesses + 20 walks (seed 42, 40 steps) |
| transactions-no-reservation.jsonl | transactions-txn-no-reservation-0-exhaust | control |
| transactions-no-matching.jsonl | transactions-txn-no-matching-0-exhaust | control |
| transactions-no-create-once.jsonl | transactions-txn-no-create-once-0-exhaust | control |
| transactions-no-fence-check.jsonl | transactions-txn-no-fence-check-0-exhaust | control |
| transactions-no-prepared-check.jsonl | transactions-txn-no-prepared-check-1-exhaust | control |
| transactions-no-coordinator-wait.jsonl | transactions-txn-no-coordinator-wait-0-exhaust | control |
| transactions-no-intent-check.jsonl | transactions-txn-no-intent-check-1-exhaust | control |

Generated serially from the repository root (TRACE_JSON appends):

    cargo build --release -p cayenne-metastore-model
    EXHAUST=1 TRACE_JSON=$R/txn.jsonl target/release/cayenne-metastore-model run transactions-txn-1-exhaust
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=$R/txn.jsonl target/release/cayenne-metastore-model run transactions-txn-1-exhaust
    EXHAUST=1 TRACE_JSON=$R/control-<c>.jsonl target/release/cayenne-metastore-model run transactions-txn-no-<c>-<bound>-exhaust

The `no-reader-log` control has no counterpart: the prototype's readers never
read the decision object. `no-owner-resolve` removes a capability rather than
breaking an invariant, and the positive set already reaches a non-owner
`Resolve` after turnover. `no-roots` removes a participant's staged objects
while it is prepared; the head log keeps all history and collects nothing, so
the replay counts it rather than drives it.

## Coverage

Every trace replays to its end on both backends: the model writes a `Prepare`
only under the coordinator's lease, as the prototype does.

A `Report` is the prototype's `confirmed_outcome`, which `commit_across` reports
by. The coordinator's retries until the deadline, the switch to an abort and the
background finish are covered by `object_native_transaction_test`, not by
replay.

The coordinator's removal is checked against the model's coordinator state,
not driven through `commit_across`: the replay refuses `Forget(0)` while the
model's coordinator has a `Prepare` in flight, which `commit_across` never does
because it removes the records only after every `Prepare` it sent has returned
(code inspection). Another process's removal is the prototype's
`remove_if_finished`, from storage alone.

## Controls

`control_*` replays each control's FAIL witnesses for the named property and
requires every guarantee to hold; where the breaking step is a record, a
decision or a removal, the prototype refuses it. With the matching safeguard
removed from the prototype, each reports the broken guarantee instead:

| Safeguard removed | Test | Reported |
|---|---|---|
| reservation guard in `DatasetState::apply` | control_reservation | `step 5 Deliver(1, 1): reservation: a write landed on a prepared table` |
| `Resolve` matching guard | control_matching | `step 4 Deliver(1, 1): matching: a Resolve landed without its Prepare` |
| decision create-if-absent (overwrite instead) | control_create_once | `step 10 Decide(1): create-once: the recorded decision changed` |
| lease check in `remove_if_finished` | control_fence_check | `step 4 Deliver(1, 0): records: participant 1 holds the transaction prepared and its records are gone` |
| prepared check in `remove_if_finished` | control_prepared_check | `step 5 Forget(1): records: participant 0 holds the transaction prepared and its records are gone` |
| intent check in `confirmed_outcome` | control_intent_check | `step 15 Report: report: the coordinator reports true where the decision is false` |

## Mem-tier provider replay

| File | Run | Content |
|---|---|---|
| mem-tier-seal-delete-only.jsonl | mem-tier-seal-delete-only | 7 witnesses + 20 walks (seed 42, 40 steps) |
| mem-tier-seal-delete-only-sqlite.jsonl | mem-tier-seal-delete-only-sqlite | 7 witnesses + 20 walks (seed 42, 40 steps) |
| mem-tier-current-3-exhaust.jsonl | mem-tier-current-3-exhaust | seal control witnesses |
| mem-tier-no-one-call.jsonl | mem-tier-no-one-call | split-commit control witnesses |

Generated serially from the repository root, starting with absent output files
(`TRACE_JSON` appends):

```sh
cargo build --release -p cayenne-metastore-model
EXHAUST=1 TRACE_JSON=crates/cayenne/tests/traces/mem-tier-seal-delete-only.jsonl target/release/cayenne-metastore-model run mem-tier-seal-delete-only
WALK=42:20:40 TRACE_JSON=crates/cayenne/tests/traces/mem-tier-seal-delete-only.jsonl target/release/cayenne-metastore-model run mem-tier-seal-delete-only
EXHAUST=1 TRACE_JSON=crates/cayenne/tests/traces/mem-tier-seal-delete-only-sqlite.jsonl target/release/cayenne-metastore-model run mem-tier-seal-delete-only-sqlite
WALK=42:20:40 TRACE_JSON=crates/cayenne/tests/traces/mem-tier-seal-delete-only-sqlite.jsonl target/release/cayenne-metastore-model run mem-tier-seal-delete-only-sqlite
EXHAUST=1 TRACE_JSON=crates/cayenne/tests/traces/mem-tier-current-3-exhaust.jsonl target/release/cayenne-metastore-model run mem-tier-current-3-exhaust
EXHAUST=1 TRACE_JSON=crates/cayenne/tests/traces/mem-tier-no-one-call.jsonl target/release/cayenne-metastore-model run mem-tier-no-one-call
```

`provider/table/tests/mem_tier_model_replay.rs` drives the real CDC apply, seal,
checkpoint, seq-prefix bake and full rewrite paths. Test-only rendezvous hooks
park maintenance tasks at each modeled boundary. Every prefix checks live row
multiplicity, source acknowledgement and recovery from a copied durable catalog
and data directory with the unacknowledged source suffix redelivered.

The control tests take the FAIL witness's schedule but require the candidate
model's rows. Split-model `*StageRest` actions are omitted only for those control
schedules because the guarded engine stages once. Positive traces refuse those
actions. No production safeguard is disabled in the checked-in replay.

The replay sets `compaction_trigger_protected_snapshots: 2`, giving the real
bake zero kept-recent inputs; the model requires at least two eligible protected
snapshots. Enabled bakes must commit and the positive suite requires actual
index pruning. No candidate gate is bypassed.

The durable comparison reads physical current/protected rows, inline rows and
stamps, every vector entry and its stamps, and standalone insert records from the
clone. Real reservation gaps are normalized to logical model sequences. On
object-native, exactly the model's durable state or that state plus ALL pending
changes is allowed, coupled with its corresponding recovered row multiset. An
observed early flush is carried into later model prefixes. SQLite uses its own
immediate-publication run. The suite checks whether all native traces are enabled
in that run and otherwise uses its generated SQLite trace set.

Original and probe reclamation are fenced while the copied directory occupies
the persisted absolute path; probe maintenance and scan-statistics tasks are
drained before paths are restored. Crash aborts and awaits the parked operation,
then drains detached maintenance without checkpointing the RAM tier. This is an
in-process approximation of process death, not a process-kill test.

`mem_tier_model_replay_early_publisher` parks a cold SQL scan immediately before
footer-statistics persistence and reads commit numbers/action names before and
after releasing it. It requires the checkpoint's delete files, protected snapshot,
insert records and table statistics to share one record. Output belongs in the
run report, not this README.

The control files retain historical FAIL schedules. The revised model includes
the PK cache and reserve-ahead handout semantics; their historical labels do not
assert a row failure under the revised model. Temporary engine mutations must
still be detected by the same full catalog/row comparisons and then reverted.
