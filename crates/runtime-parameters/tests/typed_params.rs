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

//! End-to-end tests for `#[derive(TypedParams)]` and its support runtime.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use runtime_parameters::TypedParams;
use runtime_parameters_typed::{ParamsError, TypedParams as _};
use runtime_secrets::{AnyErrorResult, SecretStore, Secrets};
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::RwLock;

/// A secret store backed by a fixed map, for exercising autoload.
struct FakeStore(HashMap<String, String>);

#[async_trait]
impl SecretStore for FakeStore {
    async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        Ok(self.0.get(key).map(|v| SecretString::from(v.clone())))
    }
}

fn secrets_with(entries: &[(&str, &str)]) -> Arc<RwLock<Secrets>> {
    let mut secrets = Secrets::new();
    secrets.register_store(
        "fake",
        Arc::new(FakeStore(
            entries
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        )),
    );
    Arc::new(RwLock::new(secrets))
}

fn empty_secrets() -> Arc<RwLock<Secrets>> {
    Arc::new(RwLock::new(Secrets::new()))
}

fn params(entries: &[(&str, &str)]) -> HashMap<String, SecretString> {
    entries
        .iter()
        .map(|(k, v)| ((*k).to_string(), SecretString::from((*v).to_string())))
        .collect()
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Tier {
    Free,
    Paid,
}

impl FromStr for Tier {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "free" => Ok(Tier::Free),
            "paid" => Ok(Tier::Paid),
            other => Err(format!("must be one of: free, paid. Found {other}")),
        }
    }
}

fn parse_percent(s: &str) -> Result<u8, String> {
    let v: u8 = s.parse().map_err(|e| format!("{e}"))?;
    if v > 100 {
        return Err("must be between 0 and 100".to_string());
    }
    Ok(v)
}

#[derive(Debug, TypedParams)]
#[params(prefix = "acme")]
struct AcmeParams {
    /// The Acme API key.
    #[param(autoload_secret)]
    api_key: Option<SecretString>,
    /// The Acme service endpoint.
    #[param(runtime, default = "https://api.acme.dev")]
    endpoint: String,
    /// The account tier.
    #[param(default = "free")]
    tier: Tier,
    /// A required identifier.
    org_id: String,
    /// Optional numeric knob.
    parallelism: Option<usize>,
    /// Renamed in the spicepod.
    #[param(rename = "project", alias = "proj")]
    project_id: Option<String>,
    /// Already carries the prefix in its name.
    acme_scoped: Option<String>,
    /// Custom-parsed percentage.
    #[param(parse_with = parse_percent)]
    sample_percent: Option<u8>,
}

#[tokio::test]
async fn parses_typical_component_params() {
    let typed = AcmeParams::try_from_params(
        "component acme_test",
        params(&[
            ("acme_api_key", "sk-123"),
            ("acme_org_id", "org-7"),
            ("acme_tier", "paid"),
            ("endpoint", "https://custom.acme.dev"),
            ("acme_parallelism", "8"),
            ("acme_project", "p1"),
            ("acme_acme_scoped", "unused"), // wrong: name is already prefixed
            ("acme_scoped", "s1"),
            ("acme_sample_percent", "50"),
        ]),
        &empty_secrets(),
    )
    .await
    .expect("params should deserialize");

    assert_eq!(
        typed
            .api_key
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret),
        Some("sk-123")
    );
    assert_eq!(typed.endpoint, "https://custom.acme.dev");
    assert_eq!(typed.tier, Tier::Paid);
    assert_eq!(typed.org_id, "org-7");
    assert_eq!(typed.parallelism, Some(8));
    assert_eq!(typed.project_id.as_deref(), Some("p1"));
    assert_eq!(typed.acme_scoped.as_deref(), Some("s1"));
    assert_eq!(typed.sample_percent, Some(50));
    assert_eq!(AcmeParams::PREFIX, "acme");
}

#[tokio::test]
async fn missing_required_names_prefixed_key_and_doc_hint() {
    let err = AcmeParams::try_from_params("component acme_test", params(&[]), &empty_secrets())
        .await
        .expect_err("org_id is required");
    let message = err.to_string();
    assert!(
        message.contains("Missing required parameter: acme_org_id."),
        "unexpected message: {message}"
    );
    assert!(
        message.contains("A required identifier."),
        "doc hint missing: {message}"
    );
}

#[tokio::test]
async fn defaults_apply_when_absent() {
    let typed = AcmeParams::try_from_params(
        "component acme_test",
        params(&[("acme_org_id", "org-7")]),
        &empty_secrets(),
    )
    .await
    .expect("params should deserialize");
    assert_eq!(typed.endpoint, "https://api.acme.dev");
    assert_eq!(typed.tier, Tier::Free);
    assert_eq!(typed.parallelism, None);
    assert!(typed.api_key.is_none());
}

#[tokio::test]
async fn invalid_value_names_prefixed_key() {
    let err = AcmeParams::try_from_params(
        "component acme_test",
        params(&[("acme_org_id", "org-7"), ("acme_parallelism", "lots")]),
        &empty_secrets(),
    )
    .await
    .expect_err("parallelism is not a number");
    let message = err.to_string();
    assert!(
        message.contains("Invalid value for parameter 'acme_parallelism'"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn enum_parse_error_reports_allowed_values() {
    let err = AcmeParams::try_from_params(
        "component acme_test",
        params(&[("acme_org_id", "org-7"), ("acme_tier", "platinum")]),
        &empty_secrets(),
    )
    .await
    .expect_err("tier is invalid");
    let message = err.to_string();
    assert!(
        message.contains("'acme_tier'") && message.contains("must be one of: free, paid"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn parse_with_failure_names_prefixed_key() {
    let err = AcmeParams::try_from_params(
        "component acme_test",
        params(&[("acme_org_id", "org-7"), ("acme_sample_percent", "200")]),
        &empty_secrets(),
    )
    .await
    .expect_err("sample_percent out of range");
    let message = err.to_string();
    assert!(
        message.contains("'acme_sample_percent'") && message.contains("between 0 and 100"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn alias_is_accepted() {
    let typed = AcmeParams::try_from_params(
        "component acme_test",
        params(&[("acme_org_id", "org-7"), ("acme_proj", "p2")]),
        &empty_secrets(),
    )
    .await
    .expect("params should deserialize");
    assert_eq!(typed.project_id.as_deref(), Some("p2"));
}

#[tokio::test]
async fn primary_key_wins_over_alias() {
    let typed = AcmeParams::try_from_params(
        "component acme_test",
        params(&[
            ("acme_org_id", "org-7"),
            ("acme_project", "primary"),
            ("acme_proj", "alias"),
        ]),
        &empty_secrets(),
    )
    .await
    .expect("params should deserialize");
    assert_eq!(typed.project_id.as_deref(), Some("primary"));
}

#[tokio::test]
async fn secret_autoload_fetches_absent_key_by_prefixed_name() {
    let typed = AcmeParams::try_from_params(
        "component acme_test",
        params(&[("acme_org_id", "org-7")]),
        &secrets_with(&[("acme_api_key", "from-store")]),
    )
    .await
    .expect("params should deserialize");
    assert_eq!(
        typed
            .api_key
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret),
        Some("from-store")
    );
}

#[tokio::test]
async fn explicit_param_beats_autoload() {
    let typed = AcmeParams::try_from_params(
        "component acme_test",
        params(&[("acme_org_id", "org-7"), ("acme_api_key", "explicit")]),
        &secrets_with(&[("acme_api_key", "from-store")]),
    )
    .await
    .expect("params should deserialize");
    assert_eq!(
        typed
            .api_key
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret),
        Some("explicit")
    );
}

#[tokio::test]
async fn non_secret_fields_are_never_autoloaded() {
    let err = AcmeParams::try_from_params(
        "component acme_test",
        params(&[]),
        &secrets_with(&[("acme_org_id", "from-store")]),
    )
    .await
    .expect_err("org_id is not #[param(autoload_secret)], so the store must not satisfy it");
    assert!(matches!(err, ParamsError::MissingRequired { .. }));
}

/// Autoload takes precedence over a field default, matching `Parameters::try_new`
/// where autoload runs before defaults are applied.
#[derive(TypedParams)]
#[params(prefix = "beta")]
struct BetaParams {
    /// Secret with a fallback default.
    #[param(autoload_secret, default = "default-token")]
    token: SecretString,
}

#[tokio::test]
async fn autoload_beats_default() {
    let typed = BetaParams::try_from_params(
        "component beta_test",
        params(&[]),
        &secrets_with(&[("beta_token", "from-store")]),
    )
    .await
    .expect("params should deserialize");
    assert_eq!(typed.token.expose_secret(), "from-store");
}

#[tokio::test]
async fn default_applies_when_autoload_misses() {
    let typed = BetaParams::try_from_params("component beta_test", params(&[]), &empty_secrets())
        .await
        .expect("params should deserialize");
    assert_eq!(typed.token.expose_secret(), "default-token");
}

/// A required secret with no default: absent everywhere → `MissingRequired`.
#[derive(Debug, TypedParams)]
#[params(prefix = "gamma")]
struct GammaParams {
    /// The Gamma API key.
    #[param(autoload_secret)]
    api_key: SecretString,
}

#[tokio::test]
async fn required_secret_satisfied_by_autoload() {
    let typed = GammaParams::try_from_params(
        "component gamma_test",
        params(&[]),
        &secrets_with(&[("gamma_api_key", "from-store")]),
    )
    .await
    .expect("autoload satisfies the required secret");
    assert_eq!(typed.api_key.expose_secret(), "from-store");
}

#[tokio::test]
async fn required_secret_missing_everywhere_errors() {
    let err = GammaParams::try_from_params("component gamma_test", params(&[]), &empty_secrets())
        .await
        .expect_err("api_key absent from params and stores");
    let message = err.to_string();
    assert!(
        message.contains("Missing required parameter: gamma_api_key."),
        "unexpected message: {message}"
    );
}

/// Deprecated params still work but warn at runtime.
#[derive(TypedParams)]
#[params(prefix = "delta")]
struct DeltaParams {
    /// Old knob.
    #[deprecated(note = "use `delta_new_knob` instead")]
    old_knob: Option<String>,
}

#[tokio::test]
async fn deprecated_field_still_deserializes() {
    #[expect(deprecated)]
    let old_knob = DeltaParams::try_from_params(
        "component delta_test",
        params(&[("delta_old_knob", "v")]),
        &empty_secrets(),
    )
    .await
    .expect("deprecated params still deserialize")
    .old_knob;
    assert_eq!(old_knob.as_deref(), Some("v"));
}

#[tokio::test]
async fn unknown_keys_do_not_fail_deserialization() {
    // Unknown and misprefixed keys warn (asserting log output is out of scope
    // here) but never error, preserving `Parameters::try_new` behavior.
    let typed = AcmeParams::try_from_params(
        "component acme_test",
        params(&[
            ("acme_org_id", "org-7"),
            ("acme_orgg_id", "typo"),
            ("acme_endpoint", "should-be-unprefixed"),
            ("tier", "should-be-prefixed"),
        ]),
        &empty_secrets(),
    )
    .await
    .expect("unknown keys are warnings, not errors");
    assert_eq!(typed.org_id, "org-7");
    // The misprefixed forms must not be consumed as if they were correct.
    assert_eq!(typed.endpoint, "https://api.acme.dev");
    assert_eq!(typed.tier, Tier::Free);
}
