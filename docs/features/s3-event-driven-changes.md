# S3 event notifications → SQS (`refresh_mode: changes`)

S3 listing datasets can use `refresh_mode: changes` driven by **S3 Event Notifications → SQS**. Spice long-polls the queue and, on `s3:ObjectCreated:*`, reads the new object and appends its rows into the accelerator.

This is the event-driven alternative to interval / `append` + `time_column: last_modified` (list-and-poll). Glue and Iceberg catalog hooks are **not** on this path.

This is **not** row-level CDC. ObjectCreated appends the object's rows. ObjectRemoved never deletes individual rows; the default is to ignore deletes, and `s3_on_object_removed: rebuild` replaces the whole accelerator from the listing prefix.

## How it works

1. If the accelerator is provably empty, Spice lists the dataset `from:` prefix **once**, applies those objects as creates, and records **that same listing** as in-flight keys until those envelopes commit. After a successful apply, the keys become the applied set so the completeness backfill can skip them. Snapshot and `history_unavailable` rebuild always use the `from:` prefix (the federated table), not a nested `s3_changes_key_prefix`. It does not seed applied keys from a later listing. Objects that arrive after that listing stay eligible for SQS or the next backfill. If any listed object cannot be read, the snapshot is not applied and ready waits until a later listing reads every object. If the snapshot is interrupted, the next start sees a non-empty accelerator and takes the rebuild path below.
2. If the accelerator is **not** empty (restart after downtime, or `AccelerationContents::Unknown`), Spice lists the `from:` prefix **once**, reads those objects, and emits `history_unavailable` carrying **that same listing** as both the replacement rows and the in-flight applied-key set. The consumer overwrites the accelerator from those rows; it does not scan the federated table again. Objects that appear after that listing stay eligible for the completeness backfill. If any listed object cannot be read, Spice does **not** overwrite from the subset and does not mark ready — the listing retries. It does **not** append every listed object on top of the existing table. SQS is not a WAL; listing is the completeness floor.
3. Spice long-polls the configured SQS queue (`WaitTimeSeconds=20`). SQS retains unconsumed notifications while Spice is offline (**4 days** by default, configurable up to **14 days**). The listing backfill covers objects whose notifications expired or were never delivered.
4. Each message is parsed as a direct S3→SQS `Records` body, an SNS-wrapped S3 notification, or an EventBridge S3 detail. `s3:TestEvent` is ignored. A missing EventBridge `detail-type` is rejected (not treated as a create). An EventBridge body whose `source` is not `aws.s3` is rejected (not treated as an object event). A `Records` entry whose `eventSource` is not `aws:s3` is rejected (not treated as an object event). EventBridge object keys are used verbatim; only direct S3 `Records` keys are URL-decoded.
5. Object keys are matched to this dataset's bucket and prefix (`starts_with` the configured prefix). A percent-encoded `from:` path is decoded to the object key (`s3://bucket/data%20files/` matches keys under `data files/`). Leading or trailing spaces in that decoded prefix are kept (`s3://bucket/%20events/` matches ` events/…`). A notification that includes **any** object outside this dataset leaves the **entire** SQS message on the queue. Within the prefix, only objects the dataset reads count: keys that match its `file_format` / `file_extension`, exactly as the listing table selects them. A job marker such as `_SUCCESS`, or any other object the dataset does not read, is skipped by the snapshot, rebuild, and backfill listings, and a notification that names only such objects is acknowledged without applying rows or triggering an `ObjectRemoved` rebuild.
6. `ObjectCreated:*`: if the object key is already **committed** in the applied set (snapshot, rebuild, or a prior apply), the notification is acknowledged without appending again. If **any** matching create in the message is in-flight (yielded, not yet committed), the **entire** message is left on the queue so a failed apply can still retry — a sibling create is not applied first, because that delete would drop the in-flight key's receipt. Otherwise the object is read through the listing connector and applied as create (`op=c`); a key named more than once in one notification is read and applied once. Every batch from that read (or from a listing snapshot / backfill pass that shares one commit) is one envelope and one write, so a failed apply cannot leave a partial object and then duplicate it on retry. Hive `key=value` partition columns that an exact-object scan would drop are reconstructed from the object key so the batch matches the federated schema. The SQS message is deleted after a successful apply (at-least-once).
7. `ObjectRemoved:*` with `s3_on_object_removed: ignore` (default): the notification is acknowledged and ignored. Queries still return rows from that object.
8. `ObjectRemoved:*` with `s3_on_object_removed: rebuild`: Spice lists the `from:` prefix **once**, replaces the accelerator from those objects, and records **that same listing** as the applied-key set (it does not clear the set). If any listed object cannot be read, the accelerator is not overwritten and the SQS message is left on the queue. That is a full prefix replace, not a row-level delete.
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

1. Create an SQS queue. **One queue per dataset.** Sharing a queue across datasets is not supported: a notification whose key is outside this dataset's bucket/prefix is **left on the queue** (not deleted). It becomes visible again after each visibility timeout until it is deleted or the queue retention period expires. Fan out with **SNS → per-dataset queues**, or set a bucket notification prefix filter so this queue only receives this dataset's keys.
2. Subscribe the queue to the bucket's event notifications for `s3:ObjectCreated:*` (and `s3:ObjectRemoved:*` only if you set `s3_on_object_removed: rebuild`). Filter on the dataset prefix when possible.
3. Grant the Spice runtime role `sqs:ReceiveMessage` and `sqs:DeleteMessage` on that queue, and the existing S3 read/list permissions on the prefix.

SNS wrapping (`S3 → SNS → SQS`) and EventBridge S3 events are accepted as message bodies. There is no Glue catalog on the event path.

## Fail closed

Registration fails with an S3-specific error that names the param, says **queue URL not ARN**, and links to the S3 connector docs when:

- `refresh_mode: changes` is set without `s3_changes_queue_url`
- `s3_changes_queue_url` is set but `refresh_mode` is not `changes`
- the queue value is an ARN, empty, or not an HTTPS SQS queue URL (`https://sqs.<region>.amazonaws.com/<account>/<queue>`)
- `s3_on_object_removed` is not `ignore` or `rebuild`
- `s3_changes_key_prefix` is not under the dataset `from` prefix
- `s3_changes_backfill_interval` is not a duration greater than 0
- `s3_auth` is `public`
- no SQS region can be resolved
- the dataset is unstructured text: no structured `file_format`, no structured `file_extension` (the same parser the listing table uses, including `.parquet.gz`), and no structured extension on `from`. Structured here means every listing `FileFormat` — `parquet`, `csv`, `json`/`jsonl`/`ndjson`/`ldjson`, `tsv`, `orc`, and `vortex` on non-Windows — not only parquet/csv/json.
- `from` contains a wildcard, or names a single object rather than a prefix. Every key under the prefix is the dataset, so a `from` that resolves to one object has no prefix to derive: it would snapshot an empty accelerator and treat notifications for that object as outside the dataset. S3 has no directories, so only a trailing `/` marks a prefix for certain — a `from` without one is checked against the object store, and refused when an object of that name exists. Keep single objects on `refresh_mode: full`.

## Gaps (MVP)

- **Deletes**: default ignores `ObjectRemoved`. `s3_on_object_removed: rebuild` rebuilds the whole prefix; it is not a row-level delete of that object's rows.
- **Overwrites**: `ObjectCreated` for a key already **committed** in the applied set is acknowledged without appending (covers snapshot/queue overlap after apply and SQS redelivery). An in-flight key (apply still pending) leaves the **entire** message on the queue, including sibling creates in the same notification. A later in-place overwrite of a committed key is not applied by the event path; use a new object key or `s3_on_object_removed: rebuild`.
- **Applied-key set is in-memory**: a restart with a non-empty accelerator replaces the table from one listing snapshot (replacement rows and applied keys together) rather than appending every listed object. SQS still delivers notifications retained on the queue (4 days default, 14 days max); objects whose notifications expired are picked up by the listing backfill.
- **Multi-dataset fan-in**: deferred. Do not share one queue across datasets.
- **Iceberg / Glue catalogs**: out of scope.
- **Custom SQS endpoint** (LocalStack): not a parameter in this MVP.
