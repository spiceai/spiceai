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
use runtime_parameter_spec::ParameterSpec;
use secrecy::zeroize::Zeroizing;
use secrecy::{ExposeSecret, SecretString};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, Notify, OnceCell, RwLock};

use crate::SecretStore;

/// Parameters accepted by the `aws_secrets_manager` secret store.
///
/// `region` and `endpoint_url` mirror the conventions used by the AWS
/// data connectors (S3, `DynamoDB`, etc.) so users can configure all AWS
/// components with a consistent vocabulary. When `region` is omitted, the
/// AWS SDK falls back to the standard credential-provider chain
/// (`AWS_REGION` / `AWS_DEFAULT_REGION` / IMDS).
pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("region")
        .description(
            "AWS region the Secrets Manager secret lives in. When omitted, the SDK \
             falls back to AWS_REGION / AWS_DEFAULT_REGION / IMDS.",
        )
        .examples(&["us-east-1", "eu-west-2"]),
    ParameterSpec::runtime("endpoint_url")
        .description(
            "Override the Secrets Manager endpoint URL. Useful for VPC endpoints, \
             FIPS endpoints, or local testing against e.g. LocalStack.",
        )
        .examples(&[
            "https://secretsmanager.us-east-1.amazonaws.com",
            "https://localhost:4566",
        ]),
    // Static credential params. Naming matches the S3 connector
    // (`key`, `secret`, `session_token`) so AWS-flavored components share
    // a vocabulary. When `key` and `secret` are both supplied they take
    // precedence over the default credential chain; when omitted, the
    // SDK's standard provider chain (env vars / shared config / web
    // identity / ECS / IMDS) is used.
    ParameterSpec::runtime("key").description(
        "AWS access key ID for static credentials. Must be set together with `secret`. \
             Typically sourced from another secret store, e.g. `${ env:AWS_ACCESS_KEY_ID }`.",
    ),
    ParameterSpec::runtime("secret").description(
        "AWS secret access key for static credentials. Must be set together with `key`. \
             Typically sourced from another secret store, e.g. `${ env:AWS_SECRET_ACCESS_KEY }`.",
    ),
    ParameterSpec::runtime("session_token").description(
        "Optional AWS session token. Only meaningful alongside `key` and `secret`, \
             e.g. for short-lived STS credentials.",
    ),
];

/// Resolved configuration for the `aws_secrets_manager` secret store.
///
/// `secret_access_key` and `session_token` are held as [`SecretString`] so
/// they carry zeroize-on-drop semantics and so the manual `Debug` impl
/// below can redact them — deriving `Debug` on plain `String` credentials
/// would surface them via any `{:?}` print (panic dumps, log calls, etc.).
#[derive(Clone)]
pub struct AwsSecretsManagerConfig {
    pub secret_name: String,
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<SecretString>,
    pub session_token: Option<SecretString>,
}

impl std::fmt::Debug for AwsSecretsManagerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsSecretsManagerConfig")
            .field("secret_name", &self.secret_name)
            .field("region", &self.region)
            .field("endpoint_url", &self.endpoint_url)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl AwsSecretsManagerConfig {
    /// Builds an [`AwsSecretsManagerConfig`] from the parsed selector and a
    /// validated parameter map.
    #[must_use]
    pub fn from_params(secret_name: String, params: &HashMap<String, String>) -> Self {
        Self {
            secret_name,
            region: params.get("region").cloned(),
            endpoint_url: params.get("endpoint_url").cloned(),
            access_key_id: params.get("key").cloned(),
            secret_access_key: params.get("secret").cloned().map(SecretString::from),
            session_token: params.get("session_token").cloned().map(SecretString::from),
        }
    }
}

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
const DEFAULT_CACHE_TTL: Duration = Duration::from_mins(1);

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
/// Sized generously above the SDK's default retry budget (3 attempts with
/// exponential backoff + jitter; worst-case ~30s of attempts plus ~20s of
/// backoff) so the retry strategy always gets a chance to fire before the
/// wall-clock cap.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(90);

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
/// cache. Each value is a [`SecretString`]: when the final `Arc` reference
/// drops (cache eviction, store shutdown), every value's backing buffer is
/// zeroized via `secrecy`'s Drop impl. Previously this was a
/// `HashMap<String, String>` whose freed allocations could linger in the
/// heap until reuse overwrote them.
struct CachedPayload {
    /// Parsed key/value map. Empty if the secret is missing or not a JSON object.
    data: Arc<HashMap<String, SecretString>>,
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
    /// Optional AWS region override sourced from the spicepod `params:` block.
    region: Option<String>,
    /// Optional Secrets Manager endpoint URL override sourced from the
    /// spicepod `params:` block.
    endpoint_url: Option<String>,
    /// Optional static credentials sourced from the spicepod `params:`
    /// block. When `access_key_id` and `secret_access_key` are both set
    /// they take precedence over the SDK's default credential chain.
    /// The secret half is held as a [`SecretString`] so its backing buffer
    /// is zeroized on drop.
    access_key_id: Option<String>,
    secret_access_key: Option<SecretString>,
    session_token: Option<SecretString>,
    /// Lazily-initialized, shared SDK configuration. Resolved exactly once per
    /// store instance and reused by both the STS pre-flight and the Secrets
    /// Manager client, so credential-provider resolution (including IMDS
    /// lookups) does not run twice.
    sdk_config: OnceCell<aws_config::SdkConfig>,
    /// Lazily-initialized SDK client, shared across all lookups.
    client: OnceCell<aws_sdk_secretsmanager::Client>,
    /// In-process cache of the parsed secret payload. Protected by an
    /// `RwLock` so cache hits never serialize against each other.
    cache: RwLock<Option<CachedPayload>>,
    /// Single-flight coordinator. The `fetch_mutex` is held only long enough
    /// to elect a winner via `fetch_inflight`; it is dropped before any
    /// `.await` on the AWS call. The winner performs the network fetch with
    /// no locks held; losers `await` on `fetch_notify` and then re-check the
    /// cache. This both coalesces concurrent misses and obeys the
    /// "don't hold locks across `.await`" guideline.
    fetch_mutex: Mutex<()>,
    fetch_inflight: AtomicBool,
    fetch_notify: Notify,
    cache_ttl: Duration,
    /// One-shot flag used to emit a single warning when a non-object payload
    /// is encountered. Prevents log spam on hot paths.
    warned_non_object: AtomicBool,
}

impl AwsSecretsManager {
    /// Creates a new [`AwsSecretsManager`] store bound to the given secret name or ARN,
    /// without any region or endpoint override.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EmptySecretName`] if `secret_name` is empty or whitespace-only.
    pub fn new(secret_name: &str) -> Result<Self> {
        Self::from_config(AwsSecretsManagerConfig {
            secret_name: secret_name.to_string(),
            region: None,
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
        })
    }

    /// Creates a new [`AwsSecretsManager`] store from a validated
    /// [`AwsSecretsManagerConfig`] (i.e. one produced by
    /// [`crate::validate_params`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::EmptySecretName`] if the configured secret name is
    /// empty or whitespace-only.
    pub fn from_config(config: AwsSecretsManagerConfig) -> Result<Self> {
        let trimmed = config.secret_name.trim();
        if trimmed.is_empty() {
            return EmptySecretNameSnafu.fail();
        }

        Ok(Self {
            secret_name: trimmed.to_string(),
            region: config.region,
            endpoint_url: config.endpoint_url,
            access_key_id: config.access_key_id,
            secret_access_key: config.secret_access_key,
            session_token: config.session_token,
            sdk_config: OnceCell::new(),
            client: OnceCell::new(),
            cache: RwLock::new(None),
            fetch_mutex: Mutex::new(()),
            fetch_inflight: AtomicBool::new(false),
            fetch_notify: Notify::new(),
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
        let config = self.sdk_config().await;
        let sts_client = aws_sdk_sts::Client::new(config);

        sts_client
            .get_caller_identity()
            .send()
            .await
            .map_err(|source| Error::UnableToVerifyAwsIdentity {
                source: Box::new(source),
            })?;

        Ok(())
    }

    /// Returns the shared [`aws_config::SdkConfig`] for this store, loading
    /// it on first use. Reused by both the STS pre-flight and the Secrets
    /// Manager client so credential-provider resolution only happens once.
    async fn sdk_config(&self) -> &aws_config::SdkConfig {
        self.sdk_config
            .get_or_init(|| {
                build_aws_config(
                    self.region.clone(),
                    self.access_key_id.clone(),
                    self.secret_access_key.clone(),
                    self.session_token.clone(),
                )
            })
            .await
    }

    async fn client(&self) -> &aws_sdk_secretsmanager::Client {
        self.client
            .get_or_init(|| async {
                let config = self.sdk_config().await;
                let mut builder = aws_sdk_secretsmanager::config::Builder::from(config);
                if let Some(endpoint) = self.endpoint_url.as_deref() {
                    builder = builder.endpoint_url(endpoint);
                }
                aws_sdk_secretsmanager::Client::from_conf(builder.build())
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
    ) -> crate::AnyErrorResult<(Arc<HashMap<String, SecretString>>, Duration)> {
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

        // Treat whitespace-only payloads as empty — they can occur as test
        // fixtures or when a secret is reset — without tripping the
        // non-object warning below.
        if payload.as_str().trim().is_empty() {
            return Ok((Arc::new(HashMap::new()), self.cache_ttl));
        }

        match parse_json_to_hashmap(payload.as_str()) {
            Ok(map) => Ok((Arc::new(map), self.cache_ttl)),
            Err(err) => {
                // Only warn once per store instance; the secret payload format
                // does not typically change between calls. The error message
                // from `serde_json` never includes the payload contents, so it
                // is safe to log.
                if !self.warned_non_object.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        secret_name = %self.secret_name,
                        error = %err,
                        "AWS secret payload is not a JSON object of string values. \
                         Spice expects the secret value to be a JSON object mapping keys \
                         to string values; this secret will resolve to no keys."
                    );
                }
                Ok((Arc::new(HashMap::new()), self.cache_ttl))
            }
        }
    }

    /// Returns a fresh secret payload, using the in-process cache when possible.
    ///
    /// Returns an `Arc`-shared map so callers do not clone the underlying
    /// (potentially secret-bearing) data out of the cache.
    ///
    /// Concurrency
    /// - Cache hits take only an `RwLock` read and never serialize against
    ///   each other or against in-flight fetches.
    /// - On a miss, one task is elected winner via the `fetch_inflight`
    ///   atomic under a brief `fetch_mutex` critical section. The mutex is
    ///   dropped before any cache `RwLock` await or AWS round-trip.
    /// - Losing tasks pre-register on `fetch_notify`, re-check the cache,
    ///   then `await` the notification if needed.
    /// - The AWS SDK call is never made while any lock is held, so a
    ///   stalled endpoint cannot stall cache readers.
    async fn payload(&self) -> crate::AnyErrorResult<Arc<HashMap<String, SecretString>>> {
        loop {
            // Fast path: fresh cache hit.
            if let Some(data) = self.try_cached().await {
                return Ok(data);
            }

            let waiter = {
                let _fetch_guard = self.fetch_mutex.lock().await;
                if self.fetch_inflight.load(Ordering::Acquire) {
                    let mut notified = Box::pin(self.fetch_notify.notified());
                    notified.as_mut().enable();
                    Some(notified)
                } else {
                    self.fetch_inflight.store(true, Ordering::Release);
                    None
                }
            };

            if let Some(notified) = waiter {
                // Loser: wait for the winner to publish a result, then loop
                // and re-check the cache. If the winner failed, the cache
                // is still empty and we will try to become the new winner.
                if let Some(data) = self.try_cached().await {
                    return Ok(data);
                }

                notified.await;
                continue;
            }

            // Another task may have refreshed the cache while we were
            // waiting to become the winner. Re-check outside `fetch_mutex`
            // so the mutex is never held across the cache `RwLock` await.
            if let Some(data) = self.try_cached().await {
                self.finish_fetch().await;
                return Ok(data);
            }

            // Winner: perform the network fetch with no locks held.
            // Always clear `fetch_inflight` and wake waiters, regardless of
            // success or failure, so they can make progress.
            let result = self.fetch_payload().await;
            return match result {
                Ok((data, ttl)) => {
                    let mut guard = self.cache.write().await;
                    *guard = Some(CachedPayload {
                        data: Arc::clone(&data),
                        fetched_at: Instant::now(),
                        ttl,
                    });
                    drop(guard);
                    self.finish_fetch().await;
                    Ok(data)
                }
                Err(err) => {
                    self.finish_fetch().await;
                    Err(err)
                }
            };
        }
    }

    async fn finish_fetch(&self) {
        let _fetch_guard = self.fetch_mutex.lock().await;
        self.fetch_inflight.store(false, Ordering::Release);
        self.fetch_notify.notify_waiters();
    }

    /// Returns the cached payload if present and still fresh.
    async fn try_cached(&self) -> Option<Arc<HashMap<String, SecretString>>> {
        let guard = self.cache.read().await;
        guard
            .as_ref()
            .filter(|e| e.is_fresh())
            .map(|e| Arc::clone(&e.data))
    }
}

/// Builds an AWS SDK configuration with explicit timeouts for secret retrieval.
///
/// The SDK's standard retry strategy (exponential backoff + jitter, 3
/// attempts by default) already covers the common transient failure modes
/// for Secrets Manager (throttling, 5xx, connection reset, I/O errors), so
/// we rely on the defaults. We layer on operation-level timeouts so a
/// stalled endpoint cannot hang Spicepod initialization indefinitely.
///
/// When `access_key_id` and `secret_access_key` are both supplied, they
/// override the SDK's default credential chain via
/// `aws_credential_types::Credentials`. Mismatched pairs (only one of the
/// two set) are ignored with a warning so the SDK can still fall back to
/// the chain rather than panicking at config-build time.
async fn build_aws_config(
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<SecretString>,
    session_token: Option<SecretString>,
) -> aws_config::SdkConfig {
    let mut builder = default_aws_config().timeout_config(
        TimeoutConfig::builder()
            .operation_attempt_timeout(ATTEMPT_TIMEOUT)
            .operation_timeout(OPERATION_TIMEOUT)
            .build(),
    );
    if let Some(region) = region {
        builder = builder.region(aws_config::Region::new(region));
    }
    match (access_key_id, secret_access_key) {
        (Some(key), Some(secret)) => {
            // `Credentials::new` takes owned `String`s. We call
            // `expose_secret().to_string()` exactly once, right at the SDK
            // boundary — the allocation then lives inside
            // `aws_credential_types::Credentials` (which has its own
            // scrubbing behavior on drop) and is never re-exposed.
            let credentials = aws_credential_types::Credentials::new(
                key,
                secret.expose_secret().to_string(),
                session_token.map(|t| t.expose_secret().to_string()),
                None,
                "SpiceAwsSecretsManagerStore",
            );
            builder = builder.credentials_provider(credentials);
        }
        (Some(_), None) | (None, Some(_)) => {
            tracing::warn!(
                "aws_secrets_manager: `key` and `secret` must both be set to use static \
                 credentials; only one was provided. Falling back to the default AWS \
                 credential chain."
            );
        }
        (None, None) => {
            if session_token.is_some() {
                tracing::warn!(
                    "aws_secrets_manager: `session_token` is only meaningful alongside \
                     `key` and `secret`; ignoring it."
                );
            }
        }
    }
    builder.load().await
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
        //
        // The cached values are already `SecretString`s, so clone the
        // zeroize-on-drop wrapper directly instead of exposing plaintext.
        let prefixed_key = format!("{SPICE_KEY_PREFIX}{key}");
        if let Some(value) = data.get(&prefixed_key) {
            return Ok(Some(value.clone()));
        }
        Ok(data.get(key).cloned())
    }
}

/// Parses a JSON string into a `HashMap<String, SecretString>`.
///
/// The input must be a JSON object. Primitive scalar values (strings, numbers,
/// booleans) are coerced to their string representation; objects, arrays, and
/// null values are skipped so that a partially-structured secret still yields
/// the scalar keys it does contain.
///
/// Every value is wrapped in [`SecretString`] so the parsed entries carry
/// zeroize-on-drop semantics into the cache. Intermediate `serde_json::Value`
/// allocations still exist briefly and are not themselves zeroized — that's
/// a property of `serde_json`, not something we can fix here without a
/// custom parser — but those allocations drop when this function returns,
/// and the cached representation handed back to callers is secret-aware.
///
/// # Errors
///
/// Returns an error if the input is not valid JSON or is not a JSON object.
pub fn parse_json_to_hashmap(json_str: &str) -> Result<HashMap<String, SecretString>> {
    let parsed: serde_json::Value =
        serde_json::from_str(json_str).context(UnableToParseJsonSnafu)?;
    let root = parsed.as_object().context(InvalidJsonFormatSnafu)?;

    let mut data = HashMap::with_capacity(root.len());
    for (key, value) in root {
        match value {
            serde_json::Value::String(s) => {
                data.insert(key.clone(), SecretString::from(s.clone()));
            }
            serde_json::Value::Number(n) => {
                data.insert(key.clone(), SecretString::from(n.to_string()));
            }
            serde_json::Value::Bool(b) => {
                data.insert(key.clone(), SecretString::from(b.to_string()));
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
        assert_eq!(map.get("a").map(ExposeSecret::expose_secret), Some("hello"));
        assert_eq!(map.get("b").map(ExposeSecret::expose_secret), Some("42"));
        assert_eq!(map.get("c").map(ExposeSecret::expose_secret), Some("true"));
        // Null/array/object entries are intentionally skipped.
        assert!(!map.contains_key("d"));
        assert!(!map.contains_key("e"));
        assert!(!map.contains_key("f"));
    }

    #[test]
    fn rejects_non_object_payload() {
        let _ = parse_json_to_hashmap(r#"["a","b"]"#).expect_err("array is not an object");
        let _ = parse_json_to_hashmap(r#""just a string""#).expect_err("string is not an object");
        let _ = parse_json_to_hashmap("not json").expect_err("invalid JSON");
    }

    /// Ensures a `{:?}` print of the config never surfaces the raw
    /// `secret_access_key` or `session_token`.
    #[test]
    fn config_debug_redacts_static_credentials() {
        let cfg = AwsSecretsManagerConfig {
            secret_name: "my-secret".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            access_key_id: Some("AKIA_PUBLIC".to_string()),
            secret_access_key: Some(SecretString::from("super-secret-value".to_string())),
            session_token: Some(SecretString::from("SESSION_TOKEN_VALUE".to_string())),
        };
        let debug = format!("{cfg:?}");
        assert!(
            !debug.contains("super-secret-value"),
            "Debug output must not include the secret_access_key; got: {debug}"
        );
        assert!(
            !debug.contains("SESSION_TOKEN_VALUE"),
            "Debug output must not include the session_token; got: {debug}"
        );
        assert!(debug.contains("<redacted>"), "got {debug}");
        // access_key_id is not a secret — should still be visible for
        // debugging.
        assert!(debug.contains("AKIA_PUBLIC"), "got {debug}");
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

        let mut data: HashMap<String, SecretString> = HashMap::new();
        data.insert(
            "spice_api_key".to_string(),
            SecretString::from("prefixed".to_string()),
        );
        data.insert(
            "api_key".to_string(),
            SecretString::from("plain".to_string()),
        );
        data.insert(
            "only_plain".to_string(),
            SecretString::from("plain-value".to_string()),
        );

        *store.cache.write().await = Some(CachedPayload {
            data: Arc::new(data),
            fetched_at: Instant::now(),
            ttl: Duration::from_mins(1),
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
    async fn returns_none_for_empty_cached_payload() {
        // Seeds the cache with an empty map (the state produced for
        // `ResourceNotFoundException`) and verifies lookups resolve to `None`
        // without any network call.
        let store = AwsSecretsManager::new("test").expect("valid name");

        *store.cache.write().await = Some(CachedPayload {
            data: Arc::new(HashMap::new()),
            fetched_at: Instant::now(),
            ttl: NEGATIVE_CACHE_TTL,
        });

        assert!(
            store
                .get_secret("anything")
                .await
                .expect("lookup ok")
                .is_none()
        );
    }

    #[tokio::test]
    async fn expired_cache_entry_is_not_served() {
        // A stale entry must not be returned by the cache fast-path. We don't
        // exercise the refresh path here (that would hit AWS); we just verify
        // that `try_cached()` discards expired entries.
        let store = AwsSecretsManager::new("test").expect("valid name");

        let mut data: HashMap<String, SecretString> = HashMap::new();
        data.insert("k".to_string(), SecretString::from("v".to_string()));

        *store.cache.write().await = Some(CachedPayload {
            data: Arc::new(data),
            fetched_at: Instant::now()
                .checked_sub(Duration::from_hours(1))
                .expect("system time has enough headroom"),
            ttl: Duration::from_mins(1),
        });

        assert!(store.try_cached().await.is_none());
    }
}
