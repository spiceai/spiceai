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

//! AWS Secrets Manager secret store.
//!
//! This store resolves secrets by fetching a single AWS Secrets Manager secret
//! (by name or ARN) whose value is expected to be a JSON object mapping keys to
//! string values. Each key lookup is satisfied from that map.
//!
//! Design notes
//! - The AWS SDK client is created once (lazily) and reused for all lookups.
//!   Re-creating the client per-call is expensive and produces extra log noise.
//! - The resolved secret payload is cached in-process for a short TTL behind an
//!   `Arc` so that concurrent readers share the same allocation and avoid
//!   cloning the (potentially secret-laden) map on every lookup.
//! - Concurrent cache misses are coalesced behind a single async `Mutex` so
//!   that only one `GetSecretValue` call is in flight at a time per store.
//! - Transient failures (throttling, 5xx, connection errors) are retried by
//!   the AWS SDK itself using the standard retry strategy with exponential
//!   backoff and jitter (SDK default: 3 attempts).
//! - A per-operation timeout prevents a stalled AWS endpoint from hanging
//!   Spicepod loads or parameter resolution indefinitely.
//! - `ResourceNotFoundException` is treated as "secret does not exist" and
//!   cached (negative cache) so we don't hammer AWS with repeated misses.
//!   `DecryptionFailure` and malformed-request errors are surfaced with
//!   actionable guidance. All other errors are propagated.
//! - Both `SecretString` and `SecretBinary` payloads are supported. If the
//!   payload is not a JSON object, the store logs a warning once and returns
//!   `None` for every key rather than failing hard, since secret lookups
//!   fall through a precedence list of stores.
//! - Intermediate plaintext payload buffers are wrapped in [`Zeroizing`] so
//!   their backing memory is scrubbed when they are dropped.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_credential_bridge::default_aws_config;
use aws_sdk_secretsmanager::{error::SdkError, operation::get_secret_value::GetSecretValueError};
use aws_sdk_sts::operation::get_caller_identity::GetCallerIdentityError;
use secrecy::SecretString;
use secrecy::zeroize::Zeroizing;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, OnceCell};

use crate::SecretStore;

/// Prefix used to scope secret keys to Spice.
///
/// When present in the secret's JSON payload, keys prefixed with `spice_` take
/// precedence over the unprefixed key of the same name. This lets users store
/// Spice-specific values alongside other application secrets in the same AWS
/// secret without collisions.
const SPICE_KEY_PREFIX: &str = "spice_";

/// Default TTL for cached secret payloads.
///
/// Chosen to balance responsiveness to secret rotation against API call volume.
/// Secret rotation in AWS is typically on the order of hours/days, so a minute
/// of staleness is acceptable for most workloads.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);

/// Negative-cache TTL for confirmed-missing secrets.
///
/// Shorter than [`DEFAULT_CACHE_TTL`] so that newly created secrets become
/// visible quickly, but long enough to avoid hammering AWS when a Spicepod
/// references a missing key.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(10);

/// Per-attempt timeout for a single Secrets Manager API call. Bounded so a
/// stalled endpoint cannot hang Spicepod initialization indefinitely.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// Overall timeout across all retry attempts for a single operation.
///
/// Sized larger than the SDK's default retry budget (3 attempts with
/// exponential backoff + jitter) so the retry strategy gets a chance to
/// fire before the wall-clock cap.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "AWS identity verification failed, check configuration with `aws configure list` and `aws sts get-caller-identity`: {source}"
    ))]
    UnableToVerifyAwsIdentity {
        // Boxed to keep `Error` small: `SdkError` is ~350 bytes and this
        // variant is on the cold init path.
        source: Box<SdkError<GetCallerIdentityError>>,
    },

    #[snafu(display("Unable to parse AWS secret as JSON: {source}"))]
    UnableToParseJson { source: serde_json::Error },

    #[snafu(display("Invalid AWS secret value: a JSON object is expected"))]
    InvalidJsonFormat {},

    #[snafu(display(
        "Unable to get AWS secret '{secret_name}': {source}. Verify the secret exists, is in the expected region, and that your IAM principal has `secretsmanager:GetSecretValue` permission."
    ))]
    UnableToGetSecret {
        secret_name: String,
        // Boxed because `SdkError` is large (>256 bytes) and this variant is on
        // the cold error path; avoids bloating `Result` on hot lookup paths.
        source: Box<SdkError<GetSecretValueError>>,
    },

    #[snafu(display(
        "AWS Secrets Manager could not decrypt the secret '{secret_name}'. Verify the KMS key used to encrypt this secret is enabled and that your IAM principal has `kms:Decrypt` permission on it."
    ))]
    UnableToDecryptSecret { secret_name: String },

    #[snafu(display(
        "AWS Secrets Manager rejected the request for '{secret_name}' as invalid: {details}. Check the secret name/ARN and the region configuration."
    ))]
    InvalidSecretRequest {
        secret_name: String,
        details: String,
    },

    #[snafu(display(
        "AWS secret name must not be empty. Specify the secret as `from: aws_secrets_manager:<secret-name-or-arn>`."
    ))]
    EmptySecretName {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Cached view of the AWS secret payload.
///
/// The parsed map is held behind an `Arc` so that readers share a single
/// allocation and never clone the (potentially secret-bearing) map out of the
/// cache. Memory is released (and zeroizing-sensitive buffers in the caller
/// can take effect) once the final `Arc` reference is dropped.
struct CachedPayload {
    /// Parsed key/value map. Empty if the secret is missing or not a JSON object.
    data: Arc<HashMap<String, String>>,
    /// Monotonic timestamp at which this entry was captured.
    fetched_at: Instant,
    /// Effective TTL for this entry (shorter for negative results).
    ttl: Duration,
}

impl CachedPayload {
    fn is_fresh(&self) -> bool {
        self.fetched_at.elapsed() < self.ttl
    }
}

pub struct AwsSecretsManager {
    secret_name: String,
    /// Lazily-initialized SDK client, shared across all lookups.
    client: OnceCell<aws_sdk_secretsmanager::Client>,
    /// In-process cache of the parsed secret payload.
    cache: Mutex<Option<CachedPayload>>,
    cache_ttl: Duration,
    /// One-shot flag used to emit a single warning when a non-object payload
    /// is encountered. Prevents log spam on hot paths.
    warned_non_object: AtomicBool,
}

impl AwsSecretsManager {
    /// Creates a new [`AwsSecretsManager`] store bound to the given secret name or ARN.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EmptySecretName`] if `secret_name` is empty or whitespace-only.
    pub fn new(secret_name: &str) -> Result<Self> {
        let trimmed = secret_name.trim();
        if trimmed.is_empty() {
            return EmptySecretNameSnafu.fail();
        }

        Ok(Self {
            secret_name: trimmed.to_string(),
            client: OnceCell::new(),
            cache: Mutex::new(None),
            cache_ttl: DEFAULT_CACHE_TTL,
            warned_non_object: AtomicBool::new(false),
        })
    }

    /// Overrides the default cache TTL. Primarily intended for tests.
    #[cfg(test)]
    #[must_use]
    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = ttl;
        self
    }

    /// Verifies that AWS credentials can be resolved by calling
    /// `sts:GetCallerIdentity`. This is a fast, read-only check that fails
    /// early with an actionable error at Spicepod load time rather than on the
    /// first secret lookup.
    ///
    /// The STS call inherits the SDK's standard retry behavior, so transient
    /// credential-provider or endpoint errors are retried automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if the STS call fails (e.g. missing/expired credentials,
    /// unreachable STS endpoint, denied IAM policy).
    pub async fn init(&self) -> Result<()> {
        let config = build_aws_config().await;
        let sts_client = aws_sdk_sts::Client::new(&config);

        sts_client
            .get_caller_identity()
            .send()
            .await
            .map_err(|source| Error::UnableToVerifyAwsIdentity {
                source: Box::new(source),
            })?;

        Ok(())
    }

    async fn client(&self) -> &aws_sdk_secretsmanager::Client {
        self.client
            .get_or_init(|| async {
                let config = build_aws_config().await;
                aws_sdk_secretsmanager::Client::new(&config)
            })
            .await
    }

    /// Fetches the secret payload from AWS and parses it into a key/value map.
    ///
    /// Returns an empty map if the secret is not found, the payload is empty,
    /// or the payload is not a JSON object. Network/permission errors are
    /// surfaced to the caller. The AWS SDK transparently retries transient
    /// failures (throttling, 5xx, connection reset) according to the retry
    /// strategy configured on the client.
    async fn fetch_payload(
        &self,
    ) -> crate::AnyErrorResult<(Arc<HashMap<String, String>>, Duration)> {
        tracing::debug!(
            secret_name = %self.secret_name,
            "Fetching AWS secret payload"
        );

        let client = self.client().await;

        let secret_value = match client
            .get_secret_value()
            .secret_id(&self.secret_name)
            .send()
            .await
        {
            Ok(v) => v,
            Err(SdkError::ServiceError(e)) if e.err().is_resource_not_found_exception() => {
                tracing::debug!(
                    secret_name = %self.secret_name,
                    "AWS secret not found; caching negative result"
                );
                return Ok((Arc::new(HashMap::new()), NEGATIVE_CACHE_TTL));
            }
            Err(SdkError::ServiceError(e)) if e.err().is_decryption_failure() => {
                return Err(Box::new(Error::UnableToDecryptSecret {
                    secret_name: self.secret_name.clone(),
                }));
            }
            Err(SdkError::ServiceError(e))
                if e.err().is_invalid_parameter_exception()
                    || e.err().is_invalid_request_exception() =>
            {
                return Err(Box::new(Error::InvalidSecretRequest {
                    secret_name: self.secret_name.clone(),
                    details: e.err().to_string(),
                }));
            }
            Err(err) => {
                // All other errors — including throttling, 5xx, credentials,
                // and access-denied — are surfaced to the caller. Retryable
                // variants have already been retried by the SDK.
                return Err(Box::new(Error::UnableToGetSecret {
                    secret_name: self.secret_name.clone(),
                    source: Box::new(err),
                }));
            }
        };

        // Extract the plaintext payload into a `Zeroizing<String>` so the
        // backing memory is scrubbed when the buffer goes out of scope, even
        // on the error/non-JSON paths below.
        let payload: Option<Zeroizing<String>> = if let Some(s) = secret_value.secret_string() {
            Some(Zeroizing::new(s.to_string()))
        } else if let Some(blob) = secret_value.secret_binary() {
            if let Ok(s) = std::str::from_utf8(blob.as_ref()) {
                Some(Zeroizing::new(s.to_string()))
            } else {
                tracing::warn!(
                    secret_name = %self.secret_name,
                    "AWS secret binary payload is not valid UTF-8; ignoring"
                );
                None
            }
        } else {
            None
        };

        let Some(payload) = payload else {
            return Ok((Arc::new(HashMap::new()), self.cache_ttl));
        };

        if let Ok(map) = parse_json_to_hashmap(payload.as_str()) {
            Ok((Arc::new(map), self.cache_ttl))
        } else {
            // Only warn once per store instance; the secret payload format
            // does not typically change between calls.
            if !self.warned_non_object.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    secret_name = %self.secret_name,
                    "AWS secret payload is not a JSON object of string values. \
                     Spice expects the secret value to be a JSON object mapping keys \
                     to string values; this secret will resolve to no keys."
                );
            }
            Ok((Arc::new(HashMap::new()), self.cache_ttl))
        }
    }

    /// Returns a fresh secret payload, using the in-process cache when possible.
    ///
    /// Returns an `Arc`-shared map so callers do not clone the underlying
    /// (potentially secret-bearing) data out of the cache.
    async fn payload(&self) -> crate::AnyErrorResult<Arc<HashMap<String, String>>> {
        let mut guard = self.cache.lock().await;

        if let Some(entry) = guard.as_ref()
            && entry.is_fresh()
        {
            return Ok(Arc::clone(&entry.data));
        }

        // Cache is empty or stale. Fetch under the lock to coalesce concurrent
        // misses into a single AWS call. Holding an async `Mutex` across `.await`
        // is intentional here: the critical section is exactly the fetch itself,
        // and this store is only touched during parameter resolution.
        let (data, ttl) = self.fetch_payload().await?;
        *guard = Some(CachedPayload {
            data: Arc::clone(&data),
            fetched_at: Instant::now(),
            ttl,
        });
        Ok(data)
    }
}

/// Builds an AWS SDK configuration with explicit timeouts for secret retrieval.
///
/// The SDK's standard retry strategy (exponential backoff + jitter, 3
/// attempts by default) already covers the common transient failure modes
/// for Secrets Manager (throttling, 5xx, connection reset, I/O errors), so
/// we rely on the defaults. We layer on operation-level timeouts so a
/// stalled endpoint cannot hang Spicepod initialization indefinitely.
async fn build_aws_config() -> aws_config::SdkConfig {
    default_aws_config()
        .timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(ATTEMPT_TIMEOUT)
                .operation_timeout(OPERATION_TIMEOUT)
                .build(),
        )
        .load()
        .await
}

#[async_trait]
impl SecretStore for AwsSecretsManager {
    async fn get_secret(&self, key: &str) -> crate::AnyErrorResult<Option<SecretString>> {
        tracing::trace!(
            secret_name = %self.secret_name,
            key = %key,
            "Resolving secret key via AWS Secrets Manager"
        );

        let data = self.payload().await?;

        // Prefer the Spice-prefixed key so that Spice-owned values can coexist
        // with other application secrets in the same AWS secret without
        // collisions, then fall back to the unprefixed key.
        let prefixed_key = format!("{SPICE_KEY_PREFIX}{key}");
        if let Some(value) = data.get(&prefixed_key) {
            return Ok(Some(SecretString::from(value.clone())));
        }
        Ok(data.get(key).cloned().map(SecretString::from))
    }
}

/// Parses a JSON string into a `HashMap<String, String>`.
///
/// The input must be a JSON object. Primitive scalar values (strings, numbers,
/// booleans) are coerced to their string representation; objects, arrays, and
/// null values are skipped so that a partially-structured secret still yields
/// the scalar keys it does contain.
///
/// # Errors
///
/// Returns an error if the input is not valid JSON or is not a JSON object.
pub fn parse_json_to_hashmap(json_str: &str) -> Result<HashMap<String, String>> {
    let parsed: serde_json::Value =
        serde_json::from_str(json_str).context(UnableToParseJsonSnafu)?;
    let root = parsed.as_object().context(InvalidJsonFormatSnafu)?;

    let mut data = HashMap::with_capacity(root.len());
    for (key, value) in root {
        match value {
            serde_json::Value::String(s) => {
                data.insert(key.clone(), s.clone());
            }
            serde_json::Value::Number(n) => {
                data.insert(key.clone(), n.to_string());
            }
            serde_json::Value::Bool(b) => {
                data.insert(key.clone(), b.to_string());
            }
            // Skip null/object/array values – they cannot be injected as
            // strings into parameters, and silently coercing them risks
            // producing incorrect secret values.
            _ => {
                tracing::debug!(
                    key = %key,
                    "Skipping non-scalar value in AWS secret JSON payload"
                );
            }
        }
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn parses_string_number_and_bool_values() {
        let json = r#"{"a": "hello", "b": 42, "c": true, "d": null, "e": [1,2], "f": {"x": 1}}"#;
        let map = parse_json_to_hashmap(json).expect("parse succeeds");
        assert_eq!(map.get("a").map(String::as_str), Some("hello"));
        assert_eq!(map.get("b").map(String::as_str), Some("42"));
        assert_eq!(map.get("c").map(String::as_str), Some("true"));
        // Null/array/object entries are intentionally skipped.
        assert!(!map.contains_key("d"));
        assert!(!map.contains_key("e"));
        assert!(!map.contains_key("f"));
    }

    #[test]
    fn rejects_non_object_payload() {
        assert!(parse_json_to_hashmap(r#"["a","b"]"#).is_err());
        assert!(parse_json_to_hashmap(r#""just a string""#).is_err());
        assert!(parse_json_to_hashmap("not json").is_err());
    }

    #[test]
    fn rejects_empty_or_whitespace_secret_name() {
        assert!(matches!(
            AwsSecretsManager::new(""),
            Err(Error::EmptySecretName { .. })
        ));
        assert!(matches!(
            AwsSecretsManager::new("   "),
            Err(Error::EmptySecretName { .. })
        ));
    }

    #[test]
    fn trims_secret_name() {
        let s = AwsSecretsManager::new("  my-secret  ").expect("valid name");
        assert_eq!(s.secret_name, "my-secret");
    }

    /// Exercises the lookup/prefix/fallback logic without hitting AWS by
    /// seeding the cache directly.
    #[tokio::test]
    async fn prefers_spice_prefixed_keys_then_falls_back() {
        let store = AwsSecretsManager::new("test").expect("valid name");

        let mut data = HashMap::new();
        data.insert("spice_api_key".to_string(), "prefixed".to_string());
        data.insert("api_key".to_string(), "plain".to_string());
        data.insert("only_plain".to_string(), "plain-value".to_string());

        *store.cache.lock().await = Some(CachedPayload {
            data: Arc::new(data),
            fetched_at: Instant::now(),
            ttl: Duration::from_secs(60),
        });

        let v = store
            .get_secret("api_key")
            .await
            .expect("lookup ok")
            .expect("present");
        assert_eq!(v.expose_secret(), "prefixed");

        let v = store
            .get_secret("only_plain")
            .await
            .expect("lookup ok")
            .expect("present");
        assert_eq!(v.expose_secret(), "plain-value");

        assert!(
            store
                .get_secret("missing")
                .await
                .expect("lookup ok")
                .is_none()
        );
    }

    #[tokio::test]
    async fn honors_negative_cache_entries() {
        let store = AwsSecretsManager::new("test").expect("valid name");

        *store.cache.lock().await = Some(CachedPayload {
            data: Arc::new(HashMap::new()),
            fetched_at: Instant::now(),
            ttl: Duration::from_secs(60),
        });

        assert!(
            store
                .get_secret("anything")
                .await
                .expect("lookup ok")
                .is_none()
        );
    }
}
