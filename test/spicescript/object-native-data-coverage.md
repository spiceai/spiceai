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

`object-store-data-path-rustfs` (also in `variants.yaml`) keeps the data at
the dataset's own `cayenne_file_path` in a RustFS bucket with
`runtime.params.cayenne_vnext` on (#16130). Its checks: no local `s3:` entry,
deletion vectors in the bucket and none on local disk, data files and
deletion vectors bounded over 180 s of CDC (`postgres-cdc-parity`), a
`file_create` restart leaving about one generation of objects
(`full-refresh-chaos`), and the staging WAL replayed after four kills during
appends with compaction never falling back to a full rewrite (`append-kill`).
That the catalog matches the objects after retention is checked by
`crates/cayenne/tests/object_store_reclaim_test.rs` and, for object-native
data with its retirement record, restarts, takeovers and kills at every
storage request of a merge and an overwrite, by
`crates/cayenne/tests/data_file_lifecycle_test.rs`; no script can reach
those states. With the flag off, an object-store data path fails to load
(`cayenne-vnext`).

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
| `refresh_mode: full` | `full-refresh-chaos.yaml`, `cayenne-transactions.yaml`, `served-while-source-down.yaml` | added (`full-refresh-chaos`, `cayenne-transactions`); data path on RustFS: `full-refresh-chaos` | #16131, #16130 |
| `refresh_mode: append` | `append-kill.yaml`, `append-time-column.yaml`, `object-native-data.yaml` | added (`append-kill`, `object-native-data`: exact count after each append, no LIST on scans, kill during an append, three directory losses, a copied location, a truncated and a deleted data file); data path on RustFS: `append-kill` | #16131, #16130 |
| `refresh_mode: changes` (PostgreSQL, MySQL CDC) | `postgres-cdc-parity.yaml`, `mysql-cdc-parity.yaml`, `postgres-cdc-atomicity.yaml`, `cdc-chaos.yaml` | added (`postgres-cdc-parity`, `mysql-cdc-parity`); data path on RustFS: `postgres-cdc-parity`, `mysql-cdc-parity`; TODO `postgres-cdc-atomicity`, `cdc-chaos` | #16132 |
| `primary_key` | `postgres-cdc-parity.yaml`, `duplicate-keys.yaml`, `cayenne-writes.yaml` | added (`postgres-cdc-parity`, `mysql-cdc-parity`, `append-kill`) | #16132 |
| `on_conflict` (upsert, drop) | `cayenne-writes.yaml`, `conflict-batches.yaml`, `append-kill.yaml` | added (`append-kill`, upsert); TODO `cayenne-writes`, `conflict-batches` | #16132 |
| `refresh_sql` | `partitioning.yaml`, `cdc-chaos.yaml` | TODO | #16131 |
| `refresh_append_overlap` | `append-time-column.yaml`, `newest-by-time.yaml` | TODO | #16131 |
| `time_partition_column` | `append-time-partition.yaml` | TODO | #16137 |
| `retention_sql` | `retention.yaml`, `newest-by-time.yaml` | TODO | #16133 |
| SQL writes (`write_mode: acceleration`) and transactions | `cayenne-transactions.yaml`, `cayenne-writes.yaml` | added (`cayenne-transactions`); TODO `cayenne-writes` | #16132 |
| `partition_by` | `partitioning.yaml`, `layout-coverage.yaml`, `cayenne-writes.yaml` | TODO | #16137 |
| `indexes` | `index-equivalence.yaml`, `layout-coverage.yaml` | TODO | #16137 |
| `mode: file_create` | `cayenne-vnext.yaml`, `cayenne-vnext-recreate.yaml`, `settings-evolution.yaml`, `object-native-data.yaml`, `cayenne-file-create-reload.yaml` | added (`object-native-data`: reload after a lost data file, the drop message names the previous folder; `cayenne-file-create-reload`: no folder marked dropped and one folder left after a reload and after a restart, a query during the reload answers whole or fails); TODO the other three | #16133 |
| Snapshot bootstrap (`snapshots:`) | `snapshots/snapshots-object-store.yaml`, `snapshots/snapshots-append-restore.yaml` | TODO | #16133 |
| `mode: memory` | `cayenne-writes.yaml`, `mysql-cdc-parity.yaml` (DuckDB), `postgres-cdc-edge-values.yaml` | TODO (no data files; the variants should leave nothing in the location) | #16137 |
| A state location 20 ms away (a remote store's round trips) | the `object-native-data-rustfs-latency` variant of every script marked added | added | #16139 |
| An S3 Express state location | none (RustFS has no Express buckets) | TODO | #16135 |
| Several instances over one location | `cayenne-vnext.yaml` (metastore only) | TODO | #16136 |
| CDC kill under load | `known-failing/cayenne/postgres-cdc-kill-under-load.yaml` | TODO (the script has no state location yet) | #16141 |
| Kill during compaction, and during a full refresh's overwrite | `known-failing/cayenne/postgres-cdc-kill-during-compaction.yaml` | added (CDC kill during compaction; a kill during a full refresh leaves one whole batch; no folder marked dropped and about one generation of the overwritten table's files after the grace) | #16133 |
| Data files bounded under sustained CDC | `postgres-cdc-parity.yaml` | added (`object-native-data-rustfs`: the dataset's objects sampled every 10 s for 180 s, bounded and not trending upward, with `sample-objects`) | #16133 |
