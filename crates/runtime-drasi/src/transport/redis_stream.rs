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

//! Delivery to a Drasi platform source over a Redis stream.
//!
//! Reaches a Kubernetes-deployed Drasi without building custom source
//! containers: the `CloudEvents` envelope is `XADD`ed onto the stream the
//! platform source consumes.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use snafu::prelude::*;
use tokio::sync::OnceCell;

use crate::config::redact_url;
use crate::element::ChangeOp;
use crate::error::{Error, InvalidConfigurationSnafu, Result, Retryable};
use crate::model::{CloudEventEnvelope, PlatformChange, PlatformPayload, PlatformSource};
use crate::transport::{DeliveryTarget, DrasiTransport, PreparedChange};

/// The `payload.source.table` value for a node. Drasi reads this as the element
/// kind — only `"node"` and `"rel"` are legal — not as a SQL table name.
const ELEMENT_KIND_NODE: &str = "node";

/// The Redis entry field the Drasi platform source reads first. It also accepts
/// `event`, `payload` and `message`, in that order.
const ENTRY_FIELD: &str = "data";

#[derive(Debug)]
pub struct RedisStreamTransport {
    client: redis::Client,
    /// Built lazily so a dataset registers even when Redis is down, and shared
    /// so reconnection is handled once. `ConnectionManager` reconnects itself
    /// with backoff.
    connection: OnceCell<ConnectionManager>,
    stream_key: String,
    target: DeliveryTarget,
    /// Disambiguates `CloudEvent` ids emitted within the same millisecond.
    sequence: AtomicU64,
}

impl RedisStreamTransport {
    /// # Errors
    ///
    /// Returns an error if the Redis URL is not a URL Redis understands.
    pub fn try_new(dataset: &str, source_id: &str, url: &str, stream_key: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|e| Error::InvalidConfiguration {
            dataset: dataset.to_string(),
            message: format!(
                "Parameter 'drasi_redis_url' is not a valid Redis URL: {e}. \
                Expected a value like 'redis://host:6379' (or 'rediss://' for TLS)."
            ),
        })?;

        ensure!(
            !stream_key.is_empty(),
            InvalidConfigurationSnafu {
                dataset,
                message:
                    "Parameter 'drasi_stream_key' is empty. Set it to the stream the Drasi platform source reads, e.g. 'drasi-events'."
                        .to_string(),
            }
        );

        Ok(Self {
            client,
            connection: OnceCell::new(),
            stream_key: stream_key.to_string(),
            target: DeliveryTarget {
                dataset: dataset.to_string(),
                source_id: source_id.to_string(),
                // Redacted here, at the one place the raw URL is known, so a
                // credential in the authority cannot reach a log line or the
                // `runtime.task_history` record an error is written to.
                endpoint: format!("{} (stream {stream_key})", redact_url(url)),
            },
            sequence: AtomicU64::new(0),
        })
    }

    async fn connection(&self) -> Result<ConnectionManager> {
        let manager = self
            .connection
            .get_or_try_init(|| async {
                ConnectionManager::new(self.client.clone())
                    .await
                    .map_err(|e| {
                        self.target.error(
                            format!("Could not connect to Redis: {e}."),
                            Retryable::Transient,
                        )
                    })
            })
            .await?;

        // Cloning shares the underlying multiplexed connection; it does not open
        // a second one.
        Ok(manager.clone())
    }

    /// A `CloudEvent` id unique within this process.
    fn next_event_id(&self, now_ms: i64) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!("{}-{now_ms}-{sequence}", self.target.source_id)
    }
}

/// Substitutes the current time when the source reported no commit timestamp.
///
/// The platform format requires `ts_ns`, unlike the HTTP format where it is
/// optional and Drasi stamps arrival time itself.
fn timestamp_ns(change: &PreparedChange, now: &chrono::DateTime<chrono::Utc>) -> u64 {
    change.timestamp_ns.unwrap_or_else(|| {
        u64::try_from(now.timestamp_nanos_opt().unwrap_or_default()).unwrap_or_default()
    })
}

#[async_trait]
impl DrasiTransport for RedisStreamTransport {
    async fn deliver(&self, changes: &[PreparedChange]) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now();

        // Drasi rejects an element whose label array is empty, where the HTTP
        // format tolerates it. Catching it here names the dataset and the fix
        // rather than surfacing a bare parse error from the far side.
        if let Some(change) = changes.iter().find(|c| c.node.labels.is_empty()) {
            return Err(Error::InvalidConfiguration {
                dataset: self.target.dataset.clone(),
                message: format!(
                    "Element '{}' has no labels, which the Drasi platform source rejects. \
                    Set 'drasi.labels' on this dataset.",
                    change.node.id
                ),
            });
        }

        let data = changes
            .iter()
            .map(|change| {
                let source = PlatformSource {
                    db: &self.target.source_id,
                    table: ELEMENT_KIND_NODE,
                    ts_ns: timestamp_ns(change, &now),
                };

                // `after` carries the new state for insert/update; a delete
                // instead carries `before`, and Drasi rejects one that omits it.
                let payload = match change.op {
                    ChangeOp::Insert | ChangeOp::Update => PlatformPayload {
                        after: Some(&change.node),
                        before: None,
                        source,
                    },
                    ChangeOp::Delete => PlatformPayload {
                        after: None,
                        before: Some(&change.node),
                        source,
                    },
                };

                PlatformChange {
                    op: change.op.platform_code(),
                    payload,
                }
            })
            .collect();

        let envelope = CloudEventEnvelope::new(
            &self.target.source_id,
            self.next_event_id(now.timestamp_millis()),
            now.to_rfc3339(),
            data,
        );

        let body = serde_json::to_string(&envelope).map_err(|e| Error::EncodeChange {
            dataset: self.target.dataset.clone(),
            message: e.to_string(),
        })?;

        let mut connection = self.connection().await?;

        redis::cmd("XADD")
            .arg(&self.stream_key)
            .arg("*")
            .arg(ENTRY_FIELD)
            .arg(body)
            .query_async::<()>(&mut connection)
            .await
            .map_err(|e| {
                self.target.error(
                    format!("Could not append to Redis stream '{}': {e}.", self.stream_key),
                    Retryable::Transient,
                )
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NodeElement;
    use serde_json::Map;

    fn change(op: ChangeOp, labels: Vec<String>) -> PreparedChange {
        PreparedChange {
            op,
            node: NodeElement {
                id: "public.orders:1".to_string(),
                labels: labels.into(),
                properties: Map::new(),
            },
            timestamp_ns: Some(1_699_900_000_000_000_000),
        }
    }

    fn transport() -> RedisStreamTransport {
        RedisStreamTransport::try_new("orders", "spice-cdc", "redis://127.0.0.1:6379", "drasi-events")
            .expect("builds")
    }

    #[test]
    fn invalid_redis_url_is_rejected_at_construction() {
        let err = RedisStreamTransport::try_new(
            "orders",
            "spice-cdc",
            "http://not-redis:6379",
            "drasi-events",
        )
        .expect_err("an http:// URL is not a Redis URL");
        assert!(matches!(err, Error::InvalidConfiguration { .. }));
    }

    #[test]
    fn empty_stream_key_is_rejected_at_construction() {
        let err =
            RedisStreamTransport::try_new("orders", "spice-cdc", "redis://127.0.0.1:6379", "")
                .expect_err("an empty stream key has no target");
        assert!(matches!(err, Error::InvalidConfiguration { .. }));
    }

    /// Drasi's platform parser hard-errors on an empty label array, so this must
    /// be caught before it reaches the wire — and before a connection is opened.
    #[tokio::test]
    async fn unlabelled_elements_are_rejected_before_connecting() {
        let err = transport()
            .deliver(&[change(ChangeOp::Insert, vec![])])
            .await
            .expect_err("Drasi rejects an element with no labels");
        assert!(matches!(err, Error::InvalidConfiguration { .. }));
    }

    #[tokio::test]
    async fn empty_batch_is_not_sent() {
        transport()
            .deliver(&[])
            .await
            .expect("an empty batch short-circuits without connecting");
    }

    #[test]
    fn event_ids_are_unique_within_a_millisecond() {
        let transport = transport();
        let first = transport.next_event_id(1_700_000_000_000);
        let second = transport.next_event_id(1_700_000_000_000);
        assert_ne!(first, second);
        assert!(first.starts_with("spice-cdc-"));
    }

    /// The platform format has no optional timestamp; a source that reports none
    /// gets the current time rather than 1970.
    #[test]
    fn missing_source_timestamp_falls_back_to_now() {
        let now = chrono::Utc::now();
        let mut change = change(ChangeOp::Insert, vec!["orders".to_string()]);
        change.timestamp_ns = None;

        let stamped = timestamp_ns(&change, &now);
        assert_eq!(
            stamped,
            u64::try_from(now.timestamp_nanos_opt().unwrap_or_default()).unwrap_or_default()
        );
        assert!(stamped > 1_600_000_000_000_000_000, "must not be near zero");
    }

    #[test]
    fn source_timestamp_is_preserved_when_present() {
        let now = chrono::Utc::now();
        let change = change(ChangeOp::Insert, vec!["orders".to_string()]);
        assert_eq!(timestamp_ns(&change, &now), 1_699_900_000_000_000_000);
    }
}
