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
