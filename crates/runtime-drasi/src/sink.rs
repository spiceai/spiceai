/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! The per-dataset forwarder: maps a CDC batch and delivers it under the
//! configured failure policy.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::config::{BOUNDED_ATTEMPTS, DrasiSinkConfig, OnDeliveryError, TransportConfig};
use crate::element::{ChangeOp, node_element, rows_to_json};
use crate::error::{Error, Result, Retryable, UnsupportedOperationSnafu};
use crate::transport::http::HttpTransport;
use crate::transport::redis_stream::RedisStreamTransport;
use crate::transport::{DrasiTransport, PreparedChange};

/// One CDC batch to forward.
///
/// The change stream's own representation is columnar and carries the primary
/// key as a per-row list of *column names*; this is the flattened view of it
/// that the mapping needs.
pub struct DrasiChangeRows<'a> {
    /// Debezium operation code per row (`c`, `u`, `d`, `r`, …).
    pub op_codes: Vec<&'a str>,
    /// Primary-key column names per row.
    ///
    /// Per-row rather than per-batch because the CDC `primary_keys` column is a
    /// nullable list: a batch can mix keyed and keyless rows (which is why
    /// `ChangeBatch::has_primary_keys` exists), so taking row 0's list for the
    /// whole batch would mis-key the rest. Names are borrowed from the batch,
    /// not copied.
    pub primary_key_columns: Vec<Vec<&'a str>>,
    /// The unwrapped `data` struct: one row per change, full table schema.
    pub data: &'a RecordBatch,
    /// Newest upstream commit timestamp in the batch, milliseconds since the
    /// Unix epoch.
    pub source_commit_ts_ms: Option<i64>,
}

/// Forwards a dataset's changes to a Drasi source.
#[derive(Debug)]
pub struct DrasiSink {
    config: DrasiSinkConfig,
    transport: Arc<dyn DrasiTransport>,
    /// Changes that were dropped under [`OnDeliveryError::Skip`].
    skipped: AtomicU64,
}

impl DrasiSink {
    /// Builds the sink described by `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be constructed from the
    /// configuration — an unusable endpoint URL, or an HTTP client that fails to
    /// build.
    pub fn try_new(config: DrasiSinkConfig) -> Result<Self> {
        let transport: Arc<dyn DrasiTransport> = match &config.transport {
            TransportConfig::Http {
                endpoint,
                request_timeout,
            } => Arc::new(HttpTransport::try_new(
                &config.dataset,
                &config.source_id,
                endpoint,
                *request_timeout,
            )?),
            TransportConfig::Redis { url, stream_key } => Arc::new(
                RedisStreamTransport::try_new(&config.dataset, &config.source_id, url, stream_key)?,
            ),
        };

        Ok(Self {
            config,
            transport,
            skipped: AtomicU64::new(0),
        })
    }

    /// Builds a sink over a caller-supplied transport. Test seam.
    #[must_use]
    pub fn with_transport(config: DrasiSinkConfig, transport: Arc<dyn DrasiTransport>) -> Self {
        Self {
            config,
            transport,
            skipped: AtomicU64::new(0),
        }
    }

    /// How many changes have been dropped undelivered under
    /// [`OnDeliveryError::Skip`].
    #[must_use]
    pub fn skipped_count(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    /// Maps `rows` onto Drasi nodes.
    fn prepare(&self, rows: &DrasiChangeRows<'_>) -> Result<Vec<PreparedChange>> {
        let rendered = rows_to_json(&self.config.dataset, rows.data)?;

        // Nanoseconds: Drasi divides by 1_000_000 to reach the millisecond
        // `effective_from` it stores, so passing milliseconds through would date
        // every element to 1970.
        let timestamp_ns = rows
            .source_commit_ts_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .and_then(|ms| ms.checked_mul(1_000_000));

        let mut prepared = Vec::with_capacity(rendered.len());
        for (index, row) in rendered.into_iter().enumerate() {
            let code = rows.op_codes.get(index).copied().unwrap_or_default();
            let op = ChangeOp::from_op_code(code).map_err(|unsupported| {
                UnsupportedOperationSnafu {
                    dataset: &self.config.dataset,
                    operation: unsupported.to_string(),
                }
                .build()
            })?;

            let primary_key_columns = rows
                .primary_key_columns
                .get(index)
                .map_or(&[][..], Vec::as_slice);
            let node = node_element(&self.config.mapping, primary_key_columns, row)?;

            prepared.push(PreparedChange {
                op,
                node,
                timestamp_ns,
            });
        }

        Ok(prepared)
    }

    /// Maps and delivers `rows` under the configured failure policy.
    ///
    /// Returns `Ok(())` when the batch was delivered, or when it was dropped
    /// under [`OnDeliveryError::Skip`].
    ///
    /// # Errors
    ///
    /// Returns an error when delivery failed and the policy is to surface it:
    /// always for a permanent failure, and for a transient one once
    /// [`OnDeliveryError::Fail`] exhausts its attempts.
    pub async fn forward(&self, rows: &DrasiChangeRows<'_>) -> Result<()> {
        if rows.data.num_rows() == 0 {
            return Ok(());
        }

        let prepared = match self.prepare(rows) {
            Ok(prepared) => prepared,
            // A mapping fault is permanent by construction, so it takes the same
            // route as a permanent delivery failure rather than being retried.
            Err(e) => return self.on_terminal_failure(e, rows.data.num_rows()),
        };

        match self.deliver_with_retry(&prepared).await {
            Ok(()) => Ok(()),
            Err(e) => self.on_terminal_failure(e, prepared.len()),
        }
    }

    async fn deliver_with_retry(&self, prepared: &[PreparedChange]) -> Result<()> {
        let max_retries = match self.config.on_delivery_error {
            // Unbounded: the contract is that a change is never lost, so the
            // stream waits for the target rather than moving past it.
            OnDeliveryError::Block => None,
            OnDeliveryError::Skip | OnDeliveryError::Fail => Some(BOUNDED_ATTEMPTS),
        };
        let strategy = FibonacciBackoffBuilder::new()
            .max_retries(max_retries)
            .build();

        let attempt = AtomicU64::new(0);
        retry(strategy, || async {
            match self.transport.deliver(prepared).await {
                Ok(()) => Ok(()),
                Err(e) => match e.retryable() {
                    Retryable::Permanent => Err(RetryError::permanent(e)),
                    Retryable::Transient => {
                        let attempts = attempt.fetch_add(1, Ordering::Relaxed) + 1;
                        // A blocking policy has no attempt ceiling, so this log
                        // line is the only signal that replication is stalled on
                        // an unreachable target.
                        tracing::warn!(
                            "Retrying Drasi delivery for dataset {} (attempt {attempts}): {e}",
                            self.config.dataset
                        );
                        Err(RetryError::transient(e))
                    }
                },
            }
        })
        .await
    }

    /// Applies the configured policy to a failure that retrying will not fix.
    fn on_terminal_failure(&self, error: Error, changes: usize) -> Result<()> {
        match self.config.on_delivery_error {
            OnDeliveryError::Skip => {
                let total = self
                    .skipped
                    .fetch_add(changes as u64, Ordering::Relaxed)
                    + changes as u64;
                tracing::warn!(
                    "Dropping {changes} undelivered change(s) for dataset {} (drasi, on_delivery_error: skip; {total} dropped so far): {error}",
                    self.config.dataset
                );
                Ok(())
            }
            OnDeliveryError::Block | OnDeliveryError::Fail => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::element::ElementMapping;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct RecordingTransport {
        outcome: Mutex<Vec<Result<()>>>,
        delivered: Mutex<Vec<Vec<PreparedChange>>>,
    }

    impl RecordingTransport {
        fn always_ok() -> Arc<Self> {
            Arc::new(Self {
                outcome: Mutex::new(Vec::new()),
                delivered: Mutex::new(Vec::new()),
            })
        }

        /// Queues per-attempt outcomes; once drained, delivery succeeds.
        fn with_outcomes(outcomes: Vec<Result<()>>) -> Arc<Self> {
            Arc::new(Self {
                outcome: Mutex::new(outcomes),
                delivered: Mutex::new(Vec::new()),
            })
        }

        fn attempts(&self) -> usize {
            self.delivered.lock().expect("not poisoned").len()
        }
    }

    #[async_trait]
    impl DrasiTransport for RecordingTransport {
        async fn deliver(&self, changes: &[PreparedChange]) -> Result<()> {
            self.delivered
                .lock()
                .expect("not poisoned")
                .push(changes.to_vec());

            let mut queued = self.outcome.lock().expect("not poisoned");
            if queued.is_empty() {
                Ok(())
            } else {
                queued.remove(0)
            }
        }
    }

    fn delivery_error(retryable: Retryable) -> Error {
        Error::Delivery {
            dataset: "orders".to_string(),
            source_id: "spice-cdc".to_string(),
            endpoint: "http://drasi:9000".to_string(),
            message: "boom".to_string(),
            retryable,
        }
    }

    fn config(on_delivery_error: OnDeliveryError) -> DrasiSinkConfig {
        DrasiSinkConfig {
            dataset: "orders".to_string(),
            source_id: "spice-cdc".to_string(),
            mapping: ElementMapping::new("orders".to_string(), vec!["public.orders".to_string()]),
            transport: TransportConfig::Http {
                endpoint: url::Url::parse("http://drasi:9000").expect("valid url"),
                request_timeout: std::time::Duration::from_secs(1),
            },
            on_delivery_error,
        }
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("new"), None])),
            ],
        )
        .expect("valid batch")
    }

    fn rows<'a>(data: &'a RecordBatch, op_codes: Vec<&'a str>) -> DrasiChangeRows<'a> {
        DrasiChangeRows {
            op_codes,
            primary_key_columns: vec![vec!["id"]; data.num_rows()],
            data,
            source_commit_ts_ms: Some(1_699_900_000_000),
        }
    }

    #[test]
    fn commit_milliseconds_are_converted_to_nanoseconds() {
        let sink = DrasiSink::with_transport(
            config(OnDeliveryError::Block),
            RecordingTransport::always_ok(),
        );
        let data = batch();
        let prepared = sink
            .prepare(&rows(&data, vec!["c", "u"]))
            .expect("maps rows");

        assert_eq!(
            prepared[0].timestamp_ns,
            Some(1_699_900_000_000_000_000),
            "Drasi divides by 1_000_000, so milliseconds here would date the element to 1970"
        );
    }

    #[test]
    fn each_row_maps_to_a_node_keyed_by_its_primary_key() {
        let sink = DrasiSink::with_transport(
            config(OnDeliveryError::Block),
            RecordingTransport::always_ok(),
        );
        let data = batch();
        let prepared = sink
            .prepare(&rows(&data, vec!["c", "d"]))
            .expect("maps rows");

        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].op, ChangeOp::Insert);
        assert_eq!(prepared[0].node.id, "public.orders:1");
        assert_eq!(prepared[1].op, ChangeOp::Delete);
        assert_eq!(prepared[1].node.id, "public.orders:2");
    }

    /// Truncate cannot be expressed as a set of deletes, and dropping it would
    /// leave Drasi holding rows the source no longer has.
    #[test]
    fn truncate_is_surfaced_not_dropped() {
        let sink = DrasiSink::with_transport(
            config(OnDeliveryError::Block),
            RecordingTransport::always_ok(),
        );
        let data = batch();
        let err = sink
            .prepare(&rows(&data, vec!["t", "t"]))
            .expect_err("truncate has no Drasi equivalent");
        assert!(matches!(err, Error::UnsupportedOperation { .. }));
    }

    #[tokio::test]
    async fn a_heartbeat_batch_is_not_delivered() {
        let transport = RecordingTransport::always_ok();
        let sink =
            DrasiSink::with_transport(config(OnDeliveryError::Block), Arc::clone(&transport) as _);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let empty = RecordBatch::new_empty(schema);

        sink.forward(&DrasiChangeRows {
            op_codes: vec![],
            primary_key_columns: vec![],
            data: &empty,
            source_commit_ts_ms: None,
        })
        .await
        .expect("an empty batch is a no-op");

        assert_eq!(transport.attempts(), 0);
    }

    #[tokio::test]
    async fn transient_failures_are_retried_then_succeed() {
        let transport = RecordingTransport::with_outcomes(vec![
            Err(delivery_error(Retryable::Transient)),
            Err(delivery_error(Retryable::Transient)),
        ]);
        let sink =
            DrasiSink::with_transport(config(OnDeliveryError::Fail), Arc::clone(&transport) as _);
        let data = batch();

        sink.forward(&rows(&data, vec!["c", "c"]))
            .await
            .expect("succeeds once the target recovers");

        assert_eq!(transport.attempts(), 3);
    }

    /// A rejected payload cannot be fixed by waiting, so it must not consume the
    /// retry budget — under `block` it would otherwise stall replication forever.
    #[tokio::test]
    async fn permanent_failures_are_not_retried_under_block() {
        let transport =
            RecordingTransport::with_outcomes(vec![Err(delivery_error(Retryable::Permanent))]);
        let sink =
            DrasiSink::with_transport(config(OnDeliveryError::Block), Arc::clone(&transport) as _);
        let data = batch();

        sink.forward(&rows(&data, vec!["c", "c"]))
            .await
            .expect_err("a permanent failure surfaces immediately");

        assert_eq!(transport.attempts(), 1, "no retry was attempted");
    }

    #[tokio::test]
    async fn skip_policy_drops_the_batch_and_counts_it() {
        let transport =
            RecordingTransport::with_outcomes(vec![Err(delivery_error(Retryable::Permanent))]);
        let sink =
            DrasiSink::with_transport(config(OnDeliveryError::Skip), Arc::clone(&transport) as _);
        let data = batch();

        sink.forward(&rows(&data, vec!["c", "c"]))
            .await
            .expect("skip lets the change through undelivered");

        assert_eq!(sink.skipped_count(), 2, "both rows are counted as dropped");
    }

    /// A mapping fault is deterministic, so `skip` must absorb it the same way
    /// it absorbs a rejected delivery — otherwise one unmappable row wedges a
    /// stream the operator explicitly asked never to block.
    #[tokio::test]
    async fn skip_policy_absorbs_a_mapping_failure() {
        let transport = RecordingTransport::always_ok();
        let sink =
            DrasiSink::with_transport(config(OnDeliveryError::Skip), Arc::clone(&transport) as _);
        let data = batch();

        sink.forward(&rows(&data, vec!["t", "t"]))
            .await
            .expect("skip absorbs an unmappable operation");

        assert_eq!(transport.attempts(), 0, "nothing reached the transport");
        assert_eq!(sink.skipped_count(), 2);
    }

    #[tokio::test]
    async fn fail_policy_surfaces_a_permanent_failure() {
        let transport =
            RecordingTransport::with_outcomes(vec![Err(delivery_error(Retryable::Permanent))]);
        let sink =
            DrasiSink::with_transport(config(OnDeliveryError::Fail), Arc::clone(&transport) as _);
        let data = batch();

        sink.forward(&rows(&data, vec!["c", "c"]))
            .await
            .expect_err("fail surfaces the error");
        assert_eq!(sink.skipped_count(), 0);
    }

    #[tokio::test]
    async fn delivery_preserves_batch_order() {
        let transport = RecordingTransport::always_ok();
        let sink =
            DrasiSink::with_transport(config(OnDeliveryError::Block), Arc::clone(&transport) as _);
        let data = batch();

        sink.forward(&rows(&data, vec!["c", "d"]))
            .await
            .expect("delivers");

        let delivered = transport.delivered.lock().expect("not poisoned");
        let batch = &delivered[0];
        assert_eq!(batch[0].node.id, "public.orders:1");
        assert_eq!(batch[1].node.id, "public.orders:2");
    }
}
