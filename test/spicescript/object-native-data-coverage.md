# Object-native Cayenne data: script coverage

Which scripts cover each Cayenne feature a Spicepod can turn on, and whether
each has an object-native data variant yet (#16139). An object-native data
variant (`object-native-data-local`, `object-native-data-rustfs`,
`object-native-data-rustfs-latency` in [`variants.yaml`](variants.yaml))
keeps the dataset's files in `runtime.state.location` (#16131). It checks
three things: the files are under the location's
`cayenne/v1/datasets/<name>-*/data/`, the instance writes nothing to a local
`s3:` path (#16134), and after `wipe-instance-dir` the instance serves the
same rows with its source stopped, so nothing can be reloaded from it.

Until the data path lands, these variants fail as recorded in
`recordings/expected-failures.json` (group `cayenne`). When a recorded
failure starts passing, re-record it and drop the entry. On the prototype
metastore, `append-kill`'s small table already comes back after the wipe
with its source down (which objects in the location hold its rows is not
yet established), so only its `data/` checks fail; in `full-refresh-chaos` the iceberg
combination's entries mirror the PostgreSQL one, since its source did not
start on the machine that recorded them.

| Feature | Covered by | Object-native data variant | Issue |
| --- | --- | --- | --- |
| `refresh_mode: full` | `full-refresh-chaos.yaml`, `cayenne-transactions.yaml`, `served-while-source-down.yaml` | added (`full-refresh-chaos`, `cayenne-transactions`) | #16131 |
| `refresh_mode: append` | `append-kill.yaml`, `append-time-column.yaml` | added (`append-kill`) | #16131 |
| `refresh_mode: changes` (PostgreSQL, MySQL CDC) | `postgres-cdc-parity.yaml`, `mysql-cdc-parity.yaml`, `postgres-cdc-atomicity.yaml`, `cdc-chaos.yaml` | added (`postgres-cdc-parity`, `mysql-cdc-parity`); TODO `postgres-cdc-atomicity`, `cdc-chaos` | #16132 |
| `primary_key` | `postgres-cdc-parity.yaml`, `duplicate-keys.yaml`, `cayenne-writes.yaml` | added (`postgres-cdc-parity`, `mysql-cdc-parity`, `append-kill`) | #16132 |
| `on_conflict` (upsert, drop) | `cayenne-writes.yaml`, `conflict-batches.yaml`, `append-kill.yaml` | added (`append-kill`, upsert); TODO `cayenne-writes`, `conflict-batches` | #16132 |
| `refresh_sql` | `partitioning.yaml`, `cdc-chaos.yaml` | TODO | #16131 |
| `refresh_append_overlap` | `append-time-column.yaml`, `newest-by-time.yaml` | TODO | #16131 |
| `time_partition_column` | `append-time-partition.yaml` | TODO | #16137 |
| `retention_sql` | `retention.yaml`, `newest-by-time.yaml` | TODO | #16133 |
| SQL writes (`write_mode: acceleration`) and transactions | `cayenne-transactions.yaml`, `cayenne-writes.yaml` | added (`cayenne-transactions`); TODO `cayenne-writes` | #16132 |
| `partition_by` | `partitioning.yaml`, `layout-coverage.yaml`, `cayenne-writes.yaml` | TODO | #16137 |
| `indexes` | `index-equivalence.yaml`, `layout-coverage.yaml` | TODO | #16137 |
| `mode: file_create` | `cayenne-vnext.yaml`, `cayenne-vnext-recreate.yaml`, `settings-evolution.yaml` | TODO | #16133 |
| Snapshot bootstrap (`snapshots:`) | `snapshots/snapshots-object-store.yaml`, `snapshots/snapshots-append-restore.yaml` | TODO | #16133 |
| `mode: memory` | `cayenne-writes.yaml`, `mysql-cdc-parity.yaml` (DuckDB), `postgres-cdc-edge-values.yaml` | TODO (no data files; the variants should leave nothing in the location) | #16137 |
| A state location 20 ms away (a remote store's round trips) | the `object-native-data-rustfs-latency` variant of every script marked added | added | #16139 |
| An S3 Express state location | none (RustFS has no Express buckets) | TODO | #16135 |
| Several instances over one location | `cayenne-vnext.yaml` (metastore only) | TODO | #16136 |
| CDC kill under load | `known-failing/cayenne/postgres-cdc-kill-under-load.yaml` | TODO (the script has no state location yet) | #16141 |
