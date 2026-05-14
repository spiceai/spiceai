# MongoDB Change Streams

MongoDB datasets can use `refresh_mode: changes` to stream MongoDB Change Streams into a Spice accelerator. This keeps accelerated data in sync with source collection inserts, updates, replacements, deletes, and collection-level invalidation events.

## How it works

On startup, Spice:

1. Opens a MongoDB Change Stream on the source collection with `fullDocument=updateLookup`.
2. Emits a CDC `TRUNCATE` batch so the accelerator starts from a clean state.
3. Reads a full snapshot of the collection and applies it as CDC create/upsert rows.
4. Emits the CDC ready signal required by the runtime.
5. Processes Change Stream events in batches and applies them to the accelerator.

Opening the Change Stream before the snapshot prevents gaps between the snapshot and live stream. File-accelerated datasets persist Change Stream resume tokens and resume from the last committed token on restart; in-memory accelerators rebootstrap from a fresh collection snapshot.

## Prerequisites

- MongoDB Change Streams must be supported and enabled on the source deployment. MongoDB requires a replica set or sharded cluster for Change Streams.
- The dataset must be accelerated with an engine that supports upsert behavior. Use `duckdb`, `sqlite`, `postgres`, `turso`, or `cayenne`.
- Configure `acceleration.primary_key: _id`. MongoDB delete events only include the document key, so Spice requires `_id` to route deletes correctly.
- Configure `acceleration.on_conflict` on `_id` with `upsert` behavior so update and replacement events replace existing accelerator rows instead of appending duplicates.

## Minimal configuration

```yaml
datasets:
  - from: mongodb:users
    name: users
    params:
      mongodb_host: localhost
      mongodb_port: '27017'
      mongodb_db: my_database
      mongodb_user: my_user
      mongodb_pass: ${secrets:mongodb_pass}
    acceleration:
      enabled: true
      engine: duckdb
      refresh_mode: changes
      primary_key: _id
      on_conflict:
        _id: upsert
```

## Change Stream parameters

These optional runtime parameters live under dataset `params:`; most are not prefixed with `mongodb_`:

- `change_stream_batch_max_size` (default `1000`): Maximum number of Change Stream events to group into one CDC batch before applying it. Must be greater than 0.
- `change_stream_batch_max_duration` (default `1s`): Maximum time to wait for a Change Stream batch to fill before applying it. Accepts [fundu](https://docs.rs/fundu) duration strings and must be greater than 0.
- `change_stream_max_await_time` (default `1s`): Maximum time MongoDB waits for new Change Stream events before returning an empty server batch. Accepts [fundu](https://docs.rs/fundu) duration strings and must be greater than 0.
- `change_stream_batch_size` (default `1000`): Number of Change Stream events MongoDB should request from the server per batch. Must fit in a `u32` and be greater than 0.
- `mongodb_resume_token_invalid_behavior` (default `error`): Behavior when a persisted Change Stream resume token cannot be honored by the server (e.g. it is past the oplog retention window). `error` surfaces a clear error so the operator can decide; `rebootstrap` drops the persisted token and re-snapshots the collection.

The existing `mongodb_unnest_depth` parameter also applies to Change Stream documents, so nested BSON documents are flattened the same way as normal MongoDB reads.

## Resumability across restarts

For file-accelerated datasets (acceleration `mode: file` / `file_create` / `file_update`, or `engine: postgres`), Spice persists the most recent Change Stream resume token in a sidecar table called `spice_sys_mongodb`, stored alongside the accelerator data. The schema is one row per dataset:

| column              | type      | description                                                                                      |
| ------------------- | --------- | ------------------------------------------------------------------------------------------------ |
| `dataset_name`      | TEXT PK   | Dataset name.                                                                                    |
| `resume_token_json` | TEXT      | Serialized MongoDB resume token (the `_id` field of the most recently processed change event).   |
| `cluster_time_ts`   | INTEGER   | Optional unix-seconds cluster operation time, retained as a fallback for `startAtOperationTime`. |
| `schema_json`       | TEXT      | Optional serialized Arrow schema snapshot, used to log a warning on schema drift between runs.   |
| `created_at`        | TIMESTAMP | Row creation time.                                                                               |
| `updated_at`        | TIMESTAMP | Last commit time.                                                                                |

The token is written once after the initial collection snapshot completes (piggy-backed onto the dataset-ready signal so a crash mid-snapshot still triggers a clean re-bootstrap), then re-written after each live Change Stream batch is persisted to the accelerator. The committer fires only once the downstream accelerator write has succeeded, so the persisted token always reflects data already in the accelerator (at-least-once semantics).

On restart with a persisted token, Spice resumes the Change Stream from that token and skips the collection snapshot. If MongoDB rejects the token (typical codes `ChangeStreamHistoryLost` 286 or `ChangeStreamFatalError` 280, e.g. when the oplog window has rolled past the token's position), the behavior is governed by `mongodb_resume_token_invalid_behavior` above. Re-snapshotting a large collection is opt-in by default.

Datasets that are not file-accelerated (in-memory Arrow, etc.) do not get a sidecar row; restarts re-bootstrap from a fresh snapshot.

## Event mapping

- `insert`: create/upsert, using `fullDocument`.
- `update`: update/upsert, using `fullDocument` from `fullDocument=updateLookup`.
- `replace`: update/upsert, using `fullDocument`.
- `delete`: delete, using `documentKey`; non-key columns are null in the CDC row.
- `drop`, `rename`, `dropDatabase`, `invalidate`: truncate, because collection continuity is no longer guaranteed.

If MongoDB does not include `fullDocument` for an update or replacement event, Spice fails the stream with a clear error instead of applying a partial or incorrect row.

## Test coverage

- Unit tests cover Change Stream event conversion, truncate handling, required `fullDocument`/`documentKey` validation, primary-key/upsert validation, and Change Stream parameter parsing.
- Live runtime integration coverage is available in the MongoDB runtime tests. It starts MongoDB as a single-node replica set, loads an accelerated dataset with `refresh_mode: changes`, then verifies insert, update, and delete propagation.
- The `mongodb_change_stream` Criterion microbenchmark measures BSON Change Stream event conversion into Spice CDC batches.
- Testoperator TPCH benchmark, throughput, and load coverage is configured by `test/spicepods/tpch/sf1/accelerated/mongodb-duckdb[file]-changes.yaml` and `tools/testoperator/dispatch/tpch/sf1/accelerated/mongodb-duckdb[file]-changes.yaml`.
