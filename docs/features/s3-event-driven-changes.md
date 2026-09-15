# S3 event notifications → SQS (`refresh_mode: changes`)

S3 listing datasets can use `refresh_mode: changes` driven by **S3 Event Notifications → SQS**. Spice long-polls the queue and, on `s3:ObjectCreated:*`, reads the new object and appends its rows into the accelerator.

This is the event-driven alternative to interval / `append` + `time_column: last_modified` (list-and-poll). Glue and Iceberg catalog hooks are **not** on this path.

This is **not** row-level CDC. ObjectCreated appends the object's rows. ObjectRemoved never deletes individual rows; the default is to ignore deletes, and `s3_on_object_removed: rebuild` replaces the whole accelerator from the listing prefix.

## How it works

1. If the accelerator is provably empty, Spice snapshots existing objects under the dataset `from` prefix as creates, records those object keys so the listing backfill can skip them, then marks the dataset ready. If that snapshot is interrupted, the next start sees a non-empty accelerator and takes the rebuild path below.
2. If the accelerator is **not** empty (restart after downtime, or `AccelerationContents::Unknown`), Spice emits `history_unavailable` so the accelerator is **replaced from the listing prefix**, then records the current object keys. It does **not** append every listed object on top of the existing table. SQS is not a WAL; listing is the completeness floor.
3. Spice long-polls the configured SQS queue (`WaitTimeSeconds=20`). SQS retains unconsumed notifications while Spice is offline (**4 days** by default, configurable up to **14 days**). The listing backfill covers objects whose notifications expired or were never delivered.
4. Each message is parsed as a direct S3→SQS `Records` body, an SNS-wrapped S3 notification, or an EventBridge S3 detail. `s3:TestEvent` is ignored. A missing EventBridge `detail-type` is rejected (not treated as a create). EventBridge object keys are used verbatim; only direct S3 `Records` keys are URL-decoded.
5. Object keys are matched to this dataset's bucket and prefix (`starts_with` the configured prefix). A notification that includes **any** object outside this dataset leaves the **entire** SQS message on the queue.
6. `ObjectCreated:*`: the object is read through the listing connector and applied as create (`op=c`). Hive `key=value` partition columns that an exact-object scan would drop are reconstructed from the object key so the batch matches the federated schema. The SQS message is deleted after a successful apply (at-least-once).
7. `ObjectRemoved:*` with `s3_on_object_removed: ignore` (default): the notification is acknowledged and ignored. Queries still return rows from that object.
8. `ObjectRemoved:*` with `s3_on_object_removed: rebuild`: Spice emits `history_unavailable` so the accelerator is **replaced from the listing prefix**, then replaces the in-memory applied-key set with the current listing (it does not clear the set). That is a full rebuild, not a row-level delete.
9. On `s3_changes_backfill_interval` (default `1h`), Spice lists the prefix again and applies objects whose keys are not in the in-memory applied set — including objects whose SQS notifications expired after the queue retention window.

## Minimal configuration

```yaml
datasets:
  - from: s3://my-bucket/events/
    name: events
    params:
      s3_region: us-east-1
      s3_auth: iam_role
      s3_changes_queue_url: ${secrets:s3_events_queue_url}
      file_format: parquet
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_mode: changes
      primary_key: id
      on_conflict:
        id: upsert
```

## Parameters

These live under dataset `params:` and are prefixed `s3_`:

| Param | Required | Default | Notes |
|---|---|---|---|
| `s3_changes_queue_url` | when `refresh_mode: changes` | none | SQS **queue URL**, not ARN. Secret. Queue must be exclusive to this dataset. |
| `s3_changes_region` | no | parsed from the queue URL, else `s3_region` | Registration fails if none of these resolve. |
| `s3_changes_key_prefix` | no | key prefix of `from:` | Must be equal to or nested under the dataset path. |
| `s3_changes_backfill_interval` | no | `1h` | Duration greater than 0. Periodic listing so missed or expired SQS notifications still apply. |
| `s3_on_object_removed` | no | `ignore` | `ignore` or `rebuild`. `rebuild` is a full listing-prefix replacement, **not** a row-level delete. |

Credentials reuse existing S3 auth (`s3_auth` / `s3_key` / `s3_secret` / `s3_session_token` / IAM). The same principal needs `s3:GetObject` and `s3:ListBucket` (snapshot and backfill) plus `sqs:ReceiveMessage` and `sqs:DeleteMessage`.

`s3_auth: public` is refused: a public bucket cannot receive from SQS.

## AWS setup

1. Create an SQS queue. **One queue per dataset.** Sharing a queue across datasets is not supported: a notification whose key is outside this dataset's bucket/prefix is **left on the queue** (not deleted) so it retries until visibility timeout. Fan out with **SNS → per-dataset queues**, or set a bucket notification prefix filter so this queue only receives this dataset's keys.
2. Subscribe the queue to the bucket's event notifications for `s3:ObjectCreated:*` (and `s3:ObjectRemoved:*` only if you set `s3_on_object_removed: rebuild`). Filter on the dataset prefix when possible.
3. Grant the Spice runtime role `sqs:ReceiveMessage` and `sqs:DeleteMessage` on that queue, and the existing S3 read/list permissions on the prefix.

SNS wrapping (`S3 → SNS → SQS`) and EventBridge S3 events are accepted as message bodies. There is no Glue catalog on the event path.

## Fail closed

Registration fails with an S3-specific error that names the param, says **queue URL not ARN**, and links to the S3 connector docs when:

- `refresh_mode: changes` is set without `s3_changes_queue_url`
- `s3_changes_queue_url` is set but `refresh_mode` is not `changes`
- the queue value is an ARN or empty
- `s3_on_object_removed` is not `ignore` or `rebuild`
- `s3_changes_key_prefix` is not under the dataset `from` prefix
- `s3_changes_backfill_interval` is not a duration greater than 0
- `s3_auth` is `public`
- no SQS region can be resolved

## Gaps (MVP)

- **Deletes**: default ignores `ObjectRemoved`. `s3_on_object_removed: rebuild` rebuilds the whole prefix; it is not a row-level delete of that object's rows.
- **Overwrites / at-least-once SQS**: a second `ObjectCreated` for the same key appends again. Use immutable object keys or `primary_key` + `on_conflict: upsert`.
- **Applied-key set is in-memory**: a restart with a non-empty accelerator replaces the table from the listing prefix rather than appending every listed object. SQS still delivers notifications retained on the queue (4 days default, 14 days max); objects whose notifications expired are picked up by the listing backfill.
- **Multi-dataset fan-in**: deferred. Do not share one queue across datasets.
- **Iceberg / Glue catalogs**: out of scope.
- **Custom SQS endpoint** (LocalStack): not a parameter in this MVP.
