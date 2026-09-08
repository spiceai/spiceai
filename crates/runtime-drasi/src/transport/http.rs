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

//! Delivery to a Drasi HTTP source.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use snafu::prelude::*;

use crate::config::redact_url;
use crate::element::ChangeOp;
use crate::error::{BuildClientSnafu, Result, Retryable};
use crate::model::{BatchEventRequest, EventResponse, HttpSourceChange};
use crate::transport::{DeliveryTarget, DrasiTransport, PreparedChange};

#[derive(Debug)]
pub struct HttpTransport {
    client: reqwest::Client,
    /// Fully-resolved `…/sources/{source_id}/events/batch`.
    batch_url: String,
    target: DeliveryTarget,
}

impl HttpTransport {
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be constructed.
    pub fn try_new(
        dataset: &str,
        source_id: &str,
        endpoint: &url::Url,
        request_timeout: Duration,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .context(BuildClientSnafu { dataset })?;

        // Built by hand rather than with `Url::join`, which would drop a path
        // prefix on the configured endpoint (`http://host/drasi` + `sources/…`
        // resolves to `http://host/sources/…`).
        let base = endpoint.as_str().trim_end_matches('/');
        let batch_url = format!("{base}/sources/{source_id}/events/batch");

        Ok(Self {
            client,
            batch_url,
            target: DeliveryTarget {
                dataset: dataset.to_string(),
                source_id: source_id.to_string(),
                // reqwest turns `user:pass@` in the authority into a working
                // `Authorization: Basic` header, so this URL can hold a secret.
                endpoint: redact_url(endpoint.as_str()),
            },
        })
    }
}

/// Whether a status code could succeed on an identical retry.
///
/// 4xx means the request itself was rejected — a malformed payload, or a source
/// id that does not match the one the target was configured with. 5xx, 408 and
/// 429 are server-side or load conditions that can clear.
fn classify(status: StatusCode) -> Retryable {
    if status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
    {
        Retryable::Transient
    } else {
        Retryable::Permanent
    }
}

#[async_trait]
impl DrasiTransport for HttpTransport {
    async fn deliver(&self, changes: &[PreparedChange]) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let events = changes
            .iter()
            .map(|change| match change.op {
                ChangeOp::Insert => HttpSourceChange::Insert {
                    element: (&change.node).into(),
                    timestamp: change.timestamp_ns,
                },
                ChangeOp::Update => HttpSourceChange::Update {
                    element: (&change.node).into(),
                    timestamp: change.timestamp_ns,
                },
                ChangeOp::Delete => HttpSourceChange::Delete {
                    id: &change.node.id,
                    labels: Some(change.node.labels.as_ref()),
                    timestamp: change.timestamp_ns,
                },
            })
            .collect();

        let response = self
            .client
            .post(&self.batch_url)
            .json(&BatchEventRequest { events })
            .send()
            .await
            .map_err(|e| {
                // A transport-level failure is a connection refusal, a DNS
                // failure or a timeout — all conditions that can clear.
                self.target.error(format!("{e}."), Retryable::Transient)
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(self
                .target
                .error(format!("Drasi returned HTTP {status}."), classify(status)));
        }

        // A 200 does not mean every event landed: when only some fail, Drasi
        // still answers 200 with `"success": true` and reports the count in
        // `message`, setting `error` to the last failure. There is no per-event
        // result, so a partial failure cannot be narrowed to the events that
        // failed — the whole batch is redelivered instead. That is safe because
        // both formats are idempotent per element id.
        let body: EventResponse = response.json().await.map_err(|e| {
            self.target.error(
                format!("Drasi returned a response that could not be read: {e}."),
                Retryable::Transient,
            )
        })?;

        // Nothing from `body` is quoted into the error below, and that is
        // deliberate. Drasi's per-event rejection text names the offending
        // element and its property *values* — which are the replicated row. This
        // error becomes a log line, and the change stream turns it into the
        // dataset's `error_message` in `GET /v1/datasets?status=true`, so
        // quoting it would republish row data to a surface weaker than the
        // permission to read the table. Only fixed vocabulary and counts are
        // reported; the per-event detail stays in the Drasi server's own log,
        // where the data already is. Same reasoning as `describe_bulk_failure`
        // in the Elasticsearch index writer.
        if body.error.is_some() {
            return Err(self.target.error(
                format!(
                    "Drasi accepted the batch of {} change(s) but reported that some of them failed. \
                    Check the Drasi source's log for the rejected events.",
                    changes.len()
                ),
                Retryable::Transient,
            ));
        }

        if !body.success {
            return Err(self.target.error(
                format!(
                    "Drasi rejected the batch of {} change(s). \
                    Check the Drasi source's log for the rejected events.",
                    changes.len()
                ),
                Retryable::Permanent,
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_errors_and_load_signals_are_retryable() {
        assert_eq!(
            classify(StatusCode::SERVICE_UNAVAILABLE),
            Retryable::Transient,
            "503 is what Drasi answers when its durability buffer is full"
        );
        assert_eq!(
            classify(StatusCode::INTERNAL_SERVER_ERROR),
            Retryable::Transient
        );
        assert_eq!(classify(StatusCode::REQUEST_TIMEOUT), Retryable::Transient);
        assert_eq!(
            classify(StatusCode::TOO_MANY_REQUESTS),
            Retryable::Transient
        );
    }

    /// A 400 is a source-id mismatch or a malformed payload; retrying it just
    /// stalls replication.
    #[test]
    fn client_errors_are_permanent() {
        assert_eq!(classify(StatusCode::BAD_REQUEST), Retryable::Permanent);
        assert_eq!(classify(StatusCode::NOT_FOUND), Retryable::Permanent);
        assert_eq!(classify(StatusCode::UNAUTHORIZED), Retryable::Permanent);
    }

    #[test]
    fn batch_url_preserves_a_path_prefix_on_the_endpoint() {
        let transport = HttpTransport::try_new(
            "orders",
            "spice-cdc",
            &url::Url::parse("http://drasi:9000/ingress/").expect("valid url"),
            Duration::from_secs(5),
        )
        .expect("builds");

        assert_eq!(
            transport.batch_url,
            "http://drasi:9000/ingress/sources/spice-cdc/events/batch"
        );
    }

    /// `DeliveryTarget.endpoint` is rendered into every delivery error, which is
    /// logged and becomes the dataset's public `error_message` — and reqwest
    /// turns HTTP userinfo into a working `Authorization: Basic` header, so this
    /// is a configuration an operator would really write.
    #[test]
    fn endpoint_credentials_never_reach_a_delivery_error() {
        let transport = HttpTransport::try_new(
            "orders",
            "spice-cdc",
            &url::Url::parse("http://spice:S3cret@drasi:9000").expect("valid url"),
            Duration::from_secs(5),
        )
        .expect("builds");

        let rendered = transport
            .target
            .error("boom".to_string(), Retryable::Transient)
            .to_string();

        assert!(!rendered.contains("S3cret"), "password leaked: {rendered}");
        assert!(!rendered.contains("spice:"), "username leaked: {rendered}");
        assert!(rendered.contains("drasi:9000"), "host lost: {rendered}");
    }

    #[test]
    fn batch_url_is_built_from_the_source_id() {
        let transport = HttpTransport::try_new(
            "orders",
            "spice-cdc",
            &url::Url::parse("http://drasi:9000").expect("valid url"),
            Duration::from_secs(5),
        )
        .expect("builds");

        assert_eq!(
            transport.batch_url,
            "http://drasi:9000/sources/spice-cdc/events/batch"
        );
    }

    #[tokio::test]
    async fn empty_batch_is_not_sent() {
        let transport = HttpTransport::try_new(
            "orders",
            "spice-cdc",
            // Port 1 is not listening; reaching the network would error.
            &url::Url::parse("http://127.0.0.1:1").expect("valid url"),
            Duration::from_millis(50),
        )
        .expect("builds");

        transport
            .deliver(&[])
            .await
            .expect("an empty batch short-circuits without a request");
    }
}
