# S3 Event-Driven CDC (`refresh_mode: changes`)

S3 listing datasets can use `refresh_mode: changes` driven by **S3 Event Notifications → SQS**. Spice long-polls the queue and, on `s3:ObjectCreated:*`, reads the new object and appends its rows into the accelerator as CDC creates.

This is the event-driven alternative to interval / `append` + `time_column: last_modified` (list-and-poll). Glue and Iceberg catalog hooks are **not** on this path.

## How it works

1. If the accelerator is provably empty, Spice snapshots existing objects under the dataset `from` prefix as CDC creates, then marks the dataset ready.
2. Spice long-polls the configured SQS queue (`WaitTimeSeconds=20`).
3. Each message is parsed as a direct S3→SQS `Records` body, an SNS-wrapped S3 notification, or an EventBridge S3 detail. `s3:TestEvent` is ignored.
4. Object keys are matched to this dataset's bucket and prefix.
5. `ObjectCreated:*`: the object is read through the listing connector and applied as CDC `op=c`. The SQS message is deleted after a successful apply (at-least-once).
6. `ObjectRemoved:*` is ignored by default (documented gap). With `s3_cdc_events: object_created_and_removed`, Spice emits a rebuild signal so the accelerator is replaced from the listing prefix.

## Minimal configuration

```yaml
datasets:
  - from: s3://my-bucket/events/
    name: events
    params:
      s3_region: us-east-1
      s3_auth: iam_role
      s3_cdc_queue_url: ${secrets:s3_events_queue_url}
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
| `s3_cdc_queue_url` | when `refresh_mode: changes` | none | SQS **queue URL**, not ARN. Secret. |
| `s3_cdc_region` | no | parsed from the queue URL, else `s3_region` | Registration fails if none of these resolve. |
| `s3_cdc_events` | no | `object_created` | `object_created` or `object_created_and_removed`. |
| `s3_cdc_key_prefix` | no | key prefix of `from:` | Must be equal to or nested under the dataset path. |

Credentials reuse existing S3 auth (`s3_auth` / `s3_key` / `s3_secret` / `s3_session_token` / IAM). The same principal needs `s3:GetObject` (and list, for the empty-accelerator snapshot) plus `sqs:ReceiveMessage` and `sqs:DeleteMessage`.

`s3_auth: public` is refused: a public bucket cannot receive from SQS.

## AWS setup

1. Create an SQS queue. **One queue per dataset** — sharing a queue across datasets is not supported (unmatched messages are deleted).
2. Subscribe the queue to the bucket's event notifications for `s3:ObjectCreated:*` (and `s3:ObjectRemoved:*` only if you set `object_created_and_removed`). Filter on the dataset prefix when possible.
3. Grant the Spice runtime role `sqs:ReceiveMessage` and `sqs:DeleteMessage` on that queue, and the existing S3 read permissions on the prefix.

SNS wrapping (`S3 → SNS → SQS`) and EventBridge S3 events are accepted as message bodies. There is no Glue catalog on the event path.

## Fail closed

Registration fails when:

- `refresh_mode: changes` is set without `s3_cdc_queue_url`
- `s3_cdc_queue_url` is set but `refresh_mode` is not `changes`
- the queue value is an ARN or empty
- `s3_cdc_events` is not one of the allowed values
- `s3_cdc_key_prefix` is not under the dataset `from` prefix
- `s3_auth` is `public`
- no SQS region can be resolved

## Gaps (MVP)

- **Deletes**: default ignores `ObjectRemoved`. Optional `object_created_and_removed` rebuilds the whole prefix; it is not a row-level delete.
- **Overwrites / at-least-once SQS**: a second `ObjectCreated` for the same key appends again. Use immutable object keys or `primary_key` + `on_conflict: upsert`.
- **Multi-dataset fan-in**: deferred. Do not share one queue across datasets.
- **Iceberg / Glue catalogs**: out of scope.
- **Missed notifications**: SQS is not a WAL. A non-empty accelerator that was down while objects landed will not see those objects unless they are re-notified or the accelerator is rebuilt.
- **Custom SQS endpoint** (LocalStack): not a parameter in this MVP.
