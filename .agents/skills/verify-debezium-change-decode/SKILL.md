---
name: verify-debezium-change-decode
description: Prove that DebeziumKafka::stream_changes decodes each Kafka change event on its own, applies valid neighbors of a bad event in order, increments dataset_acceleration_cdc_decode_errors, sets a sticky dataset Error, and never logs payload values. Use when changing crates/data_components/src/debezium_kafka.rs, debezium/arrow conversion, Kafka JSON deserialize errors, or the sticky RuntimeStatus changes-decode path.
---

# Verify Debezium per-event change decode

A poll chunk that mixes valid Debezium events with one that cannot be decoded must not drop the valid events. Proof is a librdkafka mock cluster (the same harness as the other Kafka/Debezium stream tests), not a unit test of `to_change_batch` alone.

## In-process

From the repo root, with the Debezium feature (pulls `rdkafka`):

```bash
cargo test -p data_components --features debezium --lib \
  debezium_applies_valid_events_when_a_chunk_contains_a_bad_event \
  skipped_change_event_log_names_location_and_kind_without_payload \
  decoding_each_event_keeps_valid_neighbors_of_a_bad_event \
  vector_to_change_batch_fails_the_whole_chunk_when_one_event_is_bad \
  deserialize_kafka_json_rejects_invalid_payload \
  -- --nocapture
```

```bash
cargo test -p runtime-status --lib changes_decode_error_stays_error_until_cleared \
  releasing_held_ready_does_not_clear_a_decode_error -- --nocapture
```

`debezium_applies_valid_events_when_a_chunk_contains_a_bad_event` is the load-bearing check: topic `t` holds create events for `id=1`, a create whose after-image is only `secret: s3cr3t-token-value` (required `id` missing), then `id=3`. After the stream yields data, both valid ids applied in order, the decode-error counter rose by one, dataset status is `Error` with kind `missing_required_field`, and `s3cr3t-token-value` is absent from the status text and any stream error.

`vector_to_change_batch_fails_the_whole_chunk_when_one_event_is_bad` documents the HTTP ingest path, which still fails the whole vector. Do not "fix" that helper to skip; the Kafka stream is the per-event path.

## Optional live `spiced`

A local Kafka/Redpanda plus Debezium-shaped JSON is not required when the mock cluster test above is green. If you do have a broker:

1. Produce three Debezium JSON envelopes on the dataset's topic, same shape as the mock test (valid, missing required field with a secret-looking value, valid).
2. Query the accelerated table: the two valid primary keys are present.
3. `GET /v1/status` (or `SELECT * FROM runtime.task_history`) shows the dataset `Error` until a full refresh or restart.
4. Scrape `/metrics` for `dataset_acceleration_cdc_decode_errors{dataset="..."}`.
5. Confirm runtime logs for that skip name the dataset, topic, partition, offset, and kind, and do not contain the secret-looking value.

Do not add a dead-letter topic. One-off producers stay out of the repo.
