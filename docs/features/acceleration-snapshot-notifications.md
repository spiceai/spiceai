# Snapshot reloads from S3 event notifications (SQS)

A dataset with `refresh_mode: snapshot` reloads as soon as a newer acceleration snapshot is published to its S3 snapshot location. The trigger is **S3 Event Notifications → SQS**: Spice long-polls a queue subscribed to the snapshot location, and a notification that the location's `metadata.json` was rewritten reloads every snapshot-mode dataset in the process. Without a queue, a snapshot-mode dataset only checks the location every `refresh_check_interval` (default `1m`).

The same notification loads the first snapshot. A reader that starts before any snapshot exists waits in its refresh retry backoff, and a notification ends that wait, so the reader loads the first snapshot as soon as it is published.

## How it works

1. **Commit point.** A snapshot writer (a dataset with `snapshots: enabled` or `create_only`) uploads the snapshot file, then rewrites `metadata.json` at the root of the snapshot location to make the new snapshot current. S3 sends an `ObjectCreated` notification for each object.
2. **One consumer per process.** The first `refresh_mode: snapshot` dataset to register starts one SQS consumer for the queue in `snapshots.params.s3_queue_url`. Datasets registered later share it, and the consumer stops when the last snapshot-mode dataset is removed. It long-polls the queue (`WaitTimeSeconds=20`).
3. **Accepted message formats.** A message can be a direct S3 → SQS notification, an S3 → SNS → SQS envelope, or an EventBridge S3 event, the same formats as [S3 event-driven changes](s3-event-driven-changes.md). An `s3:TestEvent` is deleted.
4. **Reload on the metadata rewrite.** An `ObjectCreated` notification for the location's `metadata.json` makes the consumer read `metadata.json` once and announce each dataset's current snapshot id. The message is deleted after that read. If the read fails, the message stays on the queue and is delivered again.
   - A dataset whose announced id is newer than the snapshot it has loaded or last asked for requests a refresh. It is the same request `POST /v1/datasets/{name}/acceleration/refresh` makes. One dataset's publish does not refresh the others.
   - A dataset waiting out its retry backoff, for example because no snapshot existed when it started, retries immediately.
   - A dataset waits for the reload it asked for before asking for the next one, so a newer snapshot never restarts a download in progress. Announcements that arrive meanwhile are coalesced, and the latest is loaded once the reload finishes.
5. **What gets loaded.** The refresh loads the current snapshot only if it is strictly newer than the one loaded, with the same schema check and checksum verification as a scheduled refresh. Snapshot mode never moves back to an older snapshot.
6. **Other objects under the location.** Notifications for the snapshot files themselves and for removals are deleted without a refresh: a snapshot is not current until `metadata.json` names it.
7. **Objects outside the location.** A notification that names an object outside the snapshot location is **left on the queue** (not deleted) and logged as an error. The queue must receive only this location's notifications.
8. **Invalid messages.** A message that is not an S3 event notification is logged and deleted.
9. **Fallback to polling.** Every snapshot-mode dataset keeps checking the location on `refresh_check_interval` whether or not SQS is healthy, as it does without a queue. When SQS can't be reached (the client can't connect, or receives keep failing), datasets fall back to that check alone:
   - The outage is logged as a warning when it starts.
   - It is logged as an error if it lasts more than 5 minutes.
   - An info line is logged when notifications resume.

   The same check covers a purged queue, a changed notification configuration, and messages that expired while Spice was down.

## Minimal configuration

```yaml
snapshots:
  enabled: true
  location: s3://my-bucket/spice/snapshots/
  params:
    s3_region: us-east-1
    s3_queue_url: ${ secrets:snapshots_queue_url }

datasets:
  - from: postgres:public.orders
    name: orders
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_mode: snapshot
      snapshots: bootstrap_only
```

## Parameters

In `snapshots.params`:

| Param | Required | Default | Notes |
|---|---|---|---|
| `s3_queue_url` | no | none | SQS **queue URL**, not an ARN. Secret. The region is read from the URL. |

- **Credentials.** SQS uses the snapshot location's S3 credentials: `s3_key`/`s3_secret`/`s3_session_token` when set, the default AWS credential chain otherwise.
- **Permissions.** The principal needs `sqs:ReceiveMessage` and `sqs:DeleteMessage` on the queue.
- **Custom endpoint.** The AWS SDK's `AWS_ENDPOINT_URL_SQS` environment variable points the consumer at a custom SQS endpoint.

## AWS setup

1. **Queue.** Create an SQS queue. Every spiced process that reads snapshots needs **its own queue**: SQS delivers each message to a single consumer, so readers that share a queue each see only some of the notifications, and the rest wait for `refresh_check_interval`. For several readers, send the bucket notification to an SNS topic and subscribe one queue per reader.
2. **Notification.** Add an S3 event notification on the snapshot bucket for `s3:ObjectCreated:*`, filtered by the snapshot location's prefix (for example `spice/snapshots/`). A suffix filter of `metadata.json` is optional and cuts the messages to one per snapshot.
3. **Permissions.** Grant the reader role `sqs:ReceiveMessage` and `sqs:DeleteMessage` on its queue.

## Fail closed

A snapshot-mode dataset fails to register with an error that names `s3_queue_url` and links these docs when:

- the value is empty, an ARN, or not an HTTPS SQS queue URL (`https://sqs.<region>.amazonaws.com/<account>/<queue>`);
- `snapshots.location` is not an `s3://` location.

## Gaps

- One queue per reader process, set up by the user.
- GCS Pub/Sub and Azure Event Grid notifications are not supported.
- A rollback through `POST /v1/datasets/{name}/acceleration/snapshots/current` does not reach readers: snapshot mode never moves back to an older snapshot.
