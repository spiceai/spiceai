# Acceleration snapshot scenarios

These scripts need `spiced` built with `snapshots` and `postgres`. From the
repository root, build the runtime and runner together, then record durations:

```sh
cargo build --profile lint -p spiced -p spicescript --no-default-features \
  --features spiced/postgres,spiced/snapshots
target/lint/spicescript test/spicescript/snapshots \
  --spiced target/lint/spiced --record-durations
```

Docker and the AWS CLI are required for the PostgreSQL and RustFS sources.
The publication-failure scenario also uses a Python container; its fault
fixture uses the standard library to sign S3 requests and changes only the
run's isolated metadata. The writer is stopped before injection, and a second
invocation restores the original metadata bytes.

## Coverage of recent snapshot changes

| Scenario | Contract exercised |
| --- | --- |
| `snapshots-index-restore` | Cold restore with the source offline; full PostgreSQL row oracle before restore and exact saved rows afterward; secondary-index lookups and deleted keys; NULLs and decimal boundaries; runtime and per-dataset locations; partition child tables and NULL partition keys; an empty snapshot superseding nonempty data. Covers the index archive and metastore-slice work (#15068, #14238). |
| `snapshots-append-restore` | Cold restore while the append source is unavailable (#15463), then append overlap refresh after recovery, updated values, NULLs, no duplicate keys, and a second cold restore. |
| `snapshots-cross-bucket-replication` | A customer-style full-refresh writer publishes through `runtime.state`; a source-free reader consumes byte-identical metadata from a second bucket. Two sorted tables share a metastore directory, with explicit primary keys, single-column and composite indexes, the `ebs` storage profile, and SQL caching (3m TTL / 1m stale-while-revalidate). Checks paused replication, archive-first and metadata-first arrival, TTL-based cache convergence, joins after both tables converge, concurrent whole-table generations, and cold restore after the source, writer, and original archives are unavailable. |
| `snapshots-source-replication` | Source-free `file_format: snapshot` reader starts before publication, discovers the engine, and follows multiple replacements including empty and repopulated tables; full PostgreSQL oracle comparisons; concurrent queries require whole generations; writes are rejected; store outage metrics and recovery; fresh reader starts with both writer and original source offline. |
| `snapshots-reader-publication-failures` | Metadata rollback, a newer snapshot with a bad checksum, and a newer snapshot whose archive is missing (#15310, #15472). Checks reason-labeled refresh errors, every loaded row remaining unchanged, and progress after repair. The missing-file case waits through the 90-second replication grace period. |
| `snapshots-writer-lease` | A single publisher, standby behavior, graceful handoff (#15256), expiry after SIGKILL, and restore of the new publisher's data. The graceful holder has a two-hour lease; publication within 20 seconds of shutdown must therefore use release rather than expiry. |
| `snapshots-kill-during-upload` | SIGKILL at different delays after refresh completion, then cold restore without the source. Requires a whole generation, the original key count and sum, a generation that could have been published, and each row's deterministic payload. |
| `snapshots-object-store` | S3-compatible runtime state and per-dataset snapshot locations, cold restore, and source recovery. |
| `snapshots-policies` | Creation triggers, `always` versus `on_change`, and create-only versus bootstrap-only behavior. |
| `snapshots-reader` | Explicit `refresh_mode: snapshot` replication and cache behavior while polls find no new snapshot. |
| `snapshots` | File-store restore and source recovery. |

The source-backed reader defect in #15547 is addressed separately in
[PR #15615](https://github.com/spiceai/spiceai/pull/15615), which moves its
reproduction from the expected-failure suite into the passing suite and adds
source-fault, policy, and engine coverage. This test expansion does not change
that reproduction or its expected-failure ledger.

These scenarios do not advance the clock to test the 28-day retention default
or exercise SQS notifications. They cover polling replication. The publication-
failure scenario keeps an archive absent until metadata repair; the cross-bucket
scenario also checks delayed successful archive arrival.

The cross-bucket scenario uses PostgreSQL instead of the customer MySQL source,
small deterministic fixtures instead of production data, and 1-second reader
polls instead of 10 seconds. It copies the current archives and unmodified
metadata at explicit barriers rather than running a continuous mirror. It does
not reproduce WAN performance, the full customer schema, CDC, or writer failover.

Snapshot refreshes invalidate plans and search results, while SQL result entries
retain their configured TTL. The cross-bucket test keeps the 3m TTL and 1m
stale-while-revalidate window, checks loaded data with `Cache-Control: no-cache`,
and allows up to 5 minutes for previously cached queries to converge. A stale
SQL result within that window does not establish a failed snapshot refresh.

The timed SIGKILL scenario checks complete generations and deterministic row
payloads. It does not establish that a kill interrupted an active upload or
that recovery selected the immediately preceding committed snapshot. The append
scenario checks restored and refreshed row contents; it does not inspect the
source query to prove watermark-based pruning.
