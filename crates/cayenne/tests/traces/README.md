# Transactions-family model traces

Driver output of `cayenne-metastore-model` for the `transactions` family, replayed
through two real dataset logs by
`src/metastore/object_native/catalog/replay_tests.rs` (on `InMemory` and
`LocalConditionalPut::new_durable`). The module's doc comment gives the mapping
from model actions to prototype calls and the guarantees checked after every
action.

| File | Run | Content |
|---|---|---|
| transactions.jsonl | transactions-txn-1-exhaust | 5 witnesses + 20 walks (seed 42, 40 steps) |
| transactions-no-reservation.jsonl | transactions-txn-no-reservation-0-exhaust | control |
| transactions-no-matching.jsonl | transactions-txn-no-matching-0-exhaust | control |
| transactions-no-create-once.jsonl | transactions-txn-no-create-once-0-exhaust | control |
| transactions-no-roots.jsonl | transactions-txn-no-roots-0-exhaust | control |

Generated serially from the repository root (TRACE_JSON appends):

    cargo build --release -p cayenne-metastore-model
    EXHAUST=1 TRACE_JSON=$R/txn.jsonl target/release/cayenne-metastore-model run transactions-txn-1-exhaust
    EXHAUST=1 WALK=42:20:40 TRACE_JSON=$R/txn.jsonl target/release/cayenne-metastore-model run transactions-txn-1-exhaust
    EXHAUST=1 TRACE_JSON=$R/control-<c>.jsonl target/release/cayenne-metastore-model run transactions-txn-no-<c>-0-exhaust

The `no-reader-log` control has no counterpart: the prototype's readers never
read the decision object. `no-owner-resolve` removes a capability rather than
breaking an invariant, and the positive set already reaches a non-owner
`Resolve` after turnover.

## Coverage limit

The model lets the owner that takes a log over write a `Prepare` for the
transaction. The prototype writes a `Prepare` only from the transaction's
coordinator, under the lease it held when the transaction began, so a takeover
fences it. A replay stops at a `Prepare` under a later lease and counts the
trace as truncated: 13 of the 25 traces stop there. The prototype's removal of
a transaction's records relies on that restriction (no `Prepare` can land after
the records go), and the model does not remove records, so the model does not
yet cover that removal.

## Controls

`control_*` replays each control's FAIL witnesses for the named property and
requires every guarantee to hold; where the breaking step is a record or a
decision, the prototype refuses it. With the matching safeguard removed from the
prototype, each reports the broken guarantee instead (2026-11-01):

| Safeguard removed | Test | Reported |
|---|---|---|
| reservation guard in `DatasetState::apply` | control_reservation | `step 5 Deliver(1, 1): reservation: a write landed on a prepared table` |
| `Resolve` matching guard | control_matching | `step 4 Deliver(1, 1): matching: a Resolve landed without its Prepare` |
| decision create-if-absent (overwrite instead) | control_create_once | `step 10 Decide(1): create-once: the recorded decision changed` |
| live-transaction check in record removal | control_roots | `step 10 Gc(1): records: participant 1 holds the transaction prepared and its records are gone` |

The positive replay alone does not catch the last one: only 2 `Gc` actions run
before a truncation.
