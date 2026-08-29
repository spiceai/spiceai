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

use std::time::Duration;

use crate::element::ElementMapping;

/// Default per-request timeout when `drasi_request_timeout` is unset.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How many attempts a bounded retry makes before giving up.
///
/// Applies to [`OnDeliveryError::Skip`] and [`OnDeliveryError::Fail`];
/// [`OnDeliveryError::Block`] retries transient failures without a ceiling.
pub const BOUNDED_ATTEMPTS: usize = 8;

/// What the change stream does when a change cannot be delivered.
///
/// A *permanent* failure — a rejected payload, or a change with no Drasi
/// equivalent — never retries under any policy, because an identical retry
/// produces an identical rejection. Only the transient case is affected by the
/// choice below.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OnDeliveryError {
    /// Retry transient failures indefinitely. No change is lost; the stream
    /// stalls while Drasi is unreachable and the source's replication log grows
    /// behind the unacknowledged position.
    #[default]
    Block,
    /// Retry up to [`BOUNDED_ATTEMPTS`], then log, count, and let the change
    /// through undelivered.
    Skip,
    /// Retry up to [`BOUNDED_ATTEMPTS`], then fail the change stream.
    Fail,
}

/// Where and how a dataset's changes reach Drasi.
#[derive(Debug, Clone)]
pub enum TransportConfig {
    Http {
        /// The Drasi HTTP source's own listener — not the Drasi Server
        /// management API port.
        endpoint: url::Url,
        request_timeout: Duration,
    },
    Redis {
        url: String,
        /// The stream the Drasi platform source reads.
        stream_key: String,
    },
}

/// Everything the forwarder needs for one dataset.
#[derive(Debug, Clone)]
pub struct DrasiSinkConfig {
    /// Dataset name, used in log lines and error messages.
    pub dataset: String,
    /// The Drasi source id. Must match a source already declared on the Drasi
    /// side; the HTTP route rejects a mismatch and Drasi never auto-creates one.
    pub source_id: String,
    pub mapping: ElementMapping,
    pub transport: TransportConfig,
    pub on_delivery_error: OnDeliveryError,
}

/// Strips any inline credentials from a URL before it reaches a log line or an
/// error message.
///
/// Both `redis://user:password@host:6379` and `http://user:password@host:9000`
/// carry the secret in the authority — and reqwest turns HTTP userinfo into a
/// working `Authorization: Basic` header, so an operator fronting Drasi with an
/// authenticating proxy has every reason to configure one. Delivery errors are
/// logged and become the dataset's public `error_message`, so no transport may
/// retain the raw form.
#[must_use]
pub fn redact_url(raw: &str) -> String {
    if let Ok(mut parsed) = url::Url::parse(raw) {
        if parsed.password().is_some() {
            let _ = parsed.set_password(None);
        }
        if !parsed.username().is_empty() {
            let _ = parsed.set_username("");
        }
        return parsed.to_string();
    }

    // Unparseable, so the authority cannot be located reliably. Echo it only
    // when it cannot contain userinfo at all — which keeps a plain typo
    // diagnosable without risking a password.
    if raw.contains('@') {
        "<url>".to_string()
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Delivery errors are logged and become the dataset's public
    /// `error_message`, so neither transport may carry a credential into one.
    #[test]
    fn credentials_are_stripped_from_every_scheme() {
        for raw in [
            "redis://admin:hunter2@cache.internal:6379",
            "http://admin:hunter2@drasi.internal:9000",
            "rediss://admin:hunter2@cache.internal:6379",
        ] {
            let redacted = redact_url(raw);
            assert!(!redacted.contains("hunter2"), "password leaked from {raw}");
            assert!(!redacted.contains("admin"), "username leaked from {raw}");
            assert!(redacted.contains("internal"), "host lost from {raw}");
        }
    }

    #[test]
    fn credential_free_url_survives_intact() {
        assert_eq!(
            redact_url("redis://cache.internal:6379"),
            "redis://cache.internal:6379"
        );
    }

    /// An unparseable value that could still hold userinfo is replaced
    /// wholesale rather than truncated.
    #[test]
    fn unparseable_url_with_userinfo_is_replaced_wholesale() {
        assert_eq!(redact_url("not a url@secret"), "<url>");
    }

    /// A plain typo carries no credential, so it stays diagnosable.
    #[test]
    fn unparseable_url_without_userinfo_is_echoed() {
        assert_eq!(redact_url("htp:/drasi:9000"), "htp:/drasi:9000");
    }
}
