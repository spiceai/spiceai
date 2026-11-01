# Transactions-family model traces

Driver output of `cayenne-metastore-model` for the `transactions` family, replayed
through two real dataset logs by
`src/metastore/object_native/catalog/replay_tests.rs` (on `InMemory` and
`LocalConditionalPut::new_durable`). The module's doc comment gives the mapping
from model actions to prototype calls and the guarantees checked after every
action.

| File | Run | Content |
|---|---|---|
| transactions.jsonl | transactions-txn-1-exhaust | 8 witnesses + 20 walks (seed 42, 40 steps) |
| transactions-no-reservation.jsonl | transactions-txn-no-reservation-0-exhaust | control |
| transactions-no-matching.jsonl | transactions-txn-no-matching-0-exhaust | control |
| transactions-no-create-once.jsonl | transactions-txn-no-create-once-0-exhaust | control |
| transactions-no-fence-check.jsonl | transactions-txn-no-fence-check-0-exhaust | control |
| transactions-no-prepared-check.jsonl | transactions-txn-no-prepared-check-1-exhaust | control |
| transactions-no-coordinator-wait.jsonl | transactions-txn-no-coordinator-wait-0-exhaust | control |

Generated serially from the repository root (TRACE_JSON appends):

    cargo build --release -p cayenne-metastore-model
    EXHAUST=1 TRACE_JSON=$R/txn.jsonl target/release/cayenne-metastore-model run transactions-txn-1-exhaust
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=$R/txn.jsonl target/release/cayenne-metastore-model run transactions-txn-1-exhaust
    EXHAUST=1 TRACE_JSON=$R/control-<c>.jsonl target/release/cayenne-metastore-model run transactions-txn-no-<c>-<bound>-exhaust

The `no-reader-log` control has no counterpart: the prototype's readers never
read the decision object. `no-owner-resolve` removes a capability rather than
breaking an invariant, and the positive set already reaches a non-owner
`Resolve` after turnover. `no-roots` removes a participant's staged objects
while it is prepared; that is the head log's own GC, which the replay counts
rather than drives.

## Coverage

Every trace replays to its end on both backends: the model writes a `Prepare`
only under the coordinator's lease, as the prototype does. On 2026-11-01 the
positive set replayed 56 trace-backend pairs: 154 records landed, 26 fenced
deliveries refused, 92 takeovers, 48 decisions, 14 removals by the coordinator
and 18 by another process (including decisions recreated after a removal).

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

| Safeguard removed | Test | Reported | Date |
|---|---|---|---|
| reservation guard in `DatasetState::apply` | control_reservation | `step 5 Deliver(1, 1): reservation: a write landed on a prepared table` | 2026-11-01 |
| `Resolve` matching guard | control_matching | `step 4 Deliver(1, 1): matching: a Resolve landed without its Prepare` | 2026-11-01 |
| decision create-if-absent (overwrite instead) | control_create_once | `step 10 Decide(1): create-once: the recorded decision changed` | 2026-11-01 |
| lease check in `remove_if_finished` | control_fence_check | `step 4 Deliver(1, 0): records: participant 1 holds the transaction prepared and its records are gone` | 2026-11-01 |
| prepared check in `remove_if_finished` | control_prepared_check | `step 5 Forget(1): records: participant 0 holds the transaction prepared and its records are gone` | 2026-11-01 |

The first three were run against the previous trace set (commit a592a4c3f3);
the last two against this one.
