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

//! Per-model rate controller construction for AI UDF concurrency control.
//!
//! Provider defaults are sourced from official documentation:
//! - `OpenAI`: <https://platform.openai.com/docs/guides/rate-limits>
//! - Anthropic: <https://docs.anthropic.com/en/api/rate-limits>
//! - Google Gemini: <https://ai.google.dev/gemini-api/docs/rate-limits>
//! - xAI: <https://docs.x.ai/docs/rate-limits>
//! - `Azure OpenAI`: <https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits>

use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use governor::Quota;
use runtime_rate_control::RateController;
use secrecy::SecretString;
use spicepod::component::model::{Model, ModelSource};

/// Resolved rate limit configuration for a model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RateLimitConfig {
    pub max_concurrency: Option<usize>,
    pub requests_per_minute: Option<u32>,
}

impl RateLimitConfig {
    fn build(self) -> Arc<RateController> {
        let mut builder = RateController::builder();

        if let Some(concurrency) = self.max_concurrency {
            builder = builder.with_max_concurrent_requests(concurrency);
        }

        if let Some(rpm) = self.requests_per_minute
            && let Some(nz_rpm) = NonZeroU32::new(rpm)
        {
            builder = builder.add_quota(Quota::per_minute(nz_rpm));
        }

        builder.build()
    }
}

/// Builds a [`RateController`] for a model based on user-configured params or provider defaults.
///
/// User params take precedence:
/// - `max_concurrency`: max concurrent requests for this model
/// - `requests_per_minute_limit`: max RPM for this model
///
/// If neither is set, provider-specific defaults are used based on the model source,
/// model ID, and any provider-specific tier params (e.g. `usage_tier` for `OpenAI`).
#[must_use]
pub fn build_model_rate_controller(
    model: &Model,
    params: &HashMap<String, SecretString>,
) -> Arc<RateController> {
    resolve_rate_limit_config(model, params).build()
}

/// Resolves the rate limit config for a model, testable without building a controller.
fn resolve_rate_limit_config(
    model: &Model,
    params: &HashMap<String, SecretString>,
) -> RateLimitConfig {
    // Start with provider-specific defaults, then apply only the explicit user overrides.
    let source = model.get_source();
    let model_id = model.get_model_id().unwrap_or_default();
    let mut config = provider_default_config(source.as_ref(), &model_id, params);

    if let Some(max_concurrency) = parse_param_u32(params, "max_concurrency") {
        config.max_concurrency = NonZeroU32::new(max_concurrency).map(|nz| nz.get() as usize);
    }

    if let Some(rpm) = parse_param_u32(params, "requests_per_minute_limit") {
        config.requests_per_minute = Some(rpm);
    }
    config
}

/// Returns provider-specific default config.
fn provider_default_config(
    source: Option<&ModelSource>,
    model_id: &str,
    params: &HashMap<String, SecretString>,
) -> RateLimitConfig {
    match source {
        Some(ModelSource::OpenAi) => openai_config(model_id, params),
        Some(ModelSource::Azure) => azure_config(model_id),
        Some(ModelSource::Anthropic) => anthropic_config(model_id, params),
        Some(ModelSource::Google) => google_config(model_id),
        Some(ModelSource::Xai) => xai_config(model_id, params),
        Some(ModelSource::Bedrock) => config(20, 800),
        Some(ModelSource::Databricks | ModelSource::SpiceAI) => config(10, 500),
        // Local models: conservative concurrency (typically 1 GPU), no RPM limit.
        // Users with multi-GPU setups should override via max_concurrency.
        Some(ModelSource::HuggingFace | ModelSource::File) => RateLimitConfig {
            max_concurrency: Some(1),
            requests_per_minute: None,
        },
        None => config(4, 500),
    }
}

// ---------------------------------------------------------------------------
// OpenAI defaults (per-model, per-tier)
// Source: https://platform.openai.com/docs/guides/rate-limits
//
// Models fall into two RPM groups:
// - "Full-size" (gpt-4o, gpt-4.1, o3): lower RPM
// - "Mini/nano" (gpt-4o-mini, gpt-4.1-mini, gpt-4.1-nano, o3-mini, o4-mini): higher RPM
// ---------------------------------------------------------------------------

fn openai_config(model_id: &str, params: &HashMap<String, SecretString>) -> RateLimitConfig {
    let tier = params
        .get("usage_tier")
        .map(secrecy::ExposeSecret::expose_secret)
        .and_then(|s| s.parse::<llms::openai::UsageTier>().ok());

    let is_mini = is_openai_mini_model(model_id);

    let rpm = match (&tier, is_mini) {
        (Some(llms::openai::UsageTier::Free), _) => 3,
        (Some(llms::openai::UsageTier::Tier1) | None, false) => 500,
        (Some(llms::openai::UsageTier::Tier1) | None, true) => 1000,
        (Some(llms::openai::UsageTier::Tier2 | llms::openai::UsageTier::Tier3), _) => 5000,
        (Some(llms::openai::UsageTier::Tier4), _)
        | (Some(llms::openai::UsageTier::Tier5), false) => 10_000,
        (Some(llms::openai::UsageTier::Tier5), true) => 30_000,
    };

    let max_concurrent = match &tier {
        Some(llms::openai::UsageTier::Free) => 1,
        Some(llms::openai::UsageTier::Tier1) | None => 50,
        Some(llms::openai::UsageTier::Tier2 | llms::openai::UsageTier::Tier3) => 100,
        Some(llms::openai::UsageTier::Tier4 | llms::openai::UsageTier::Tier5) => 200,
    };

    config(max_concurrent, rpm)
}

fn is_openai_mini_model(model_id: &str) -> bool {
    let id = model_id.to_lowercase();
    id.contains("mini") || id.contains("nano")
}

// ---------------------------------------------------------------------------
// Anthropic defaults (per-tier)
// Source: https://docs.anthropic.com/en/api/rate-limits
//
// Tiers 1-4. All Claude 4.x models (Opus, Sonnet) share the same RPM per tier.
// Haiku has higher limits but we use the lower Opus/Sonnet limits as default.
// ---------------------------------------------------------------------------

fn anthropic_config(_model_id: &str, params: &HashMap<String, SecretString>) -> RateLimitConfig {
    let tier = parse_param_u32(params, "anthropic_usage_tier")
        .or_else(|| parse_param_u32(params, "usage_tier"));

    match tier {
        Some(1) => config(10, 50),
        Some(3) => config(100, 2000),
        Some(4) => config(200, 4000),
        _ => config(50, 1000), // Default: Tier 2
    }
}

// ---------------------------------------------------------------------------
// Google Gemini defaults
// Source: https://ai.google.dev/gemini-api/docs/rate-limits
//
// Free: 5-15 RPM, Paid (Tier 1): 150-300 RPM depending on model
// Pro models get lower RPM, Flash models get higher RPM
// ---------------------------------------------------------------------------

fn google_config(model_id: &str) -> RateLimitConfig {
    let id = model_id.to_lowercase();
    let rpm = if id.contains("flash") { 300 } else { 150 };
    config(30, rpm)
}

// ---------------------------------------------------------------------------
// xAI (Grok) defaults
// Source: https://docs.x.ai/docs/rate-limits
//
// Tiers 0-4. Grok 4.x models can go up to 1800 RPM / 10M TPM at enterprise.
// ---------------------------------------------------------------------------

fn xai_config(_model_id: &str, params: &HashMap<String, SecretString>) -> RateLimitConfig {
    let tier =
        parse_param_u32(params, "xai_usage_tier").or_else(|| parse_param_u32(params, "usage_tier"));

    match tier {
        Some(0) => config(2, 5),
        Some(2) => config(25, 200),
        Some(3) => config(50, 500),
        Some(4) => config(100, 1000),
        _ => config(10, 60), // Default: Tier 1
    }
}

// ---------------------------------------------------------------------------
// Azure OpenAI defaults
// Source: https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits
//
// Quotas are per-deployment. GlobalStandard Tier 1 defaults vary by model.
// Mini models get much higher RPM (5K-20K), full-size 300-1000 RPM.
// ---------------------------------------------------------------------------

fn azure_config(model_id: &str) -> RateLimitConfig {
    let id = model_id.to_lowercase();
    let rpm = if id.contains("mini") || id.contains("nano") {
        5000
    } else {
        1000
    };
    config(50, rpm)
}

fn config(max_concurrent: usize, rpm: u32) -> RateLimitConfig {
    RateLimitConfig {
        max_concurrency: Some(max_concurrent),
        requests_per_minute: Some(rpm),
    }
}

fn parse_param_u32(params: &HashMap<String, SecretString>, key: &str) -> Option<u32> {
    params
        .get(key)
        .map(secrecy::ExposeSecret::expose_secret)
        .and_then(|v| v.parse::<u32>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;

    fn make_params(pairs: &[(&str, &str)]) -> HashMap<String, SecretString> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), SecretString::from(v.to_string())))
            .collect()
    }

    fn resolve(from: &str, name: &str, params: &[(&str, &str)]) -> RateLimitConfig {
        let model = Model::new(from, name);
        resolve_rate_limit_config(&model, &make_params(params))
    }

    // -- User override tests --

    #[test]
    fn test_explicit_params_override_provider_defaults() {
        let cfg = resolve(
            "openai:gpt-4o",
            "gpt-4o",
            &[
                ("usage_tier", "free"),
                ("max_concurrency", "50"),
                ("requests_per_minute_limit", "2000"),
            ],
        );
        assert_eq!(cfg.max_concurrency, Some(50));
        assert_eq!(cfg.requests_per_minute, Some(2000));
    }

    #[test]
    fn test_explicit_concurrency_only() {
        let cfg = resolve("openai:gpt-4o", "gpt-4o", &[("max_concurrency", "10")]);
        assert_eq!(cfg.max_concurrency, Some(10));
        assert_eq!(cfg.requests_per_minute, Some(500)); // rpm set even when no 'usage_tier'.
    }

    #[test]
    fn test_explicit_rpm_only() {
        let cfg = resolve(
            "openai:gpt-4o",
            "gpt-4o",
            &[("requests_per_minute_limit", "500")],
        );
        assert_eq!(cfg.max_concurrency, Some(50)); // max_concurrency set even when no 'usage_tier'.
        assert_eq!(cfg.requests_per_minute, Some(500));
    }

    #[test]
    fn test_zero_concurrency_ignored() {
        // max_concurrency=0 is invalid (would deadlock); treated as None
        let cfg = resolve("openai:gpt-4o", "gpt-4o", &[("max_concurrency", "0")]);
        assert_eq!(cfg.max_concurrency, None);
    }

    #[test]
    fn test_invalid_param_falls_back_to_defaults() {
        let cfg = resolve("openai:gpt-4o", "gpt-4o", &[("max_concurrency", "abc")]);
        // Falls back to OpenAI Tier1 full-size defaults
        assert_eq!(cfg.max_concurrency, Some(50));
        assert_eq!(cfg.requests_per_minute, Some(500));
    }

    // -- OpenAI tier tests --

    #[test]
    fn test_openai_defaults_to_tier1_full_size() {
        let cfg = resolve("openai:gpt-4o", "gpt-4o", &[]);
        assert_eq!(cfg.max_concurrency, Some(50));
        assert_eq!(cfg.requests_per_minute, Some(500));
    }

    #[test]
    fn test_openai_free_tier() {
        let cfg = resolve(
            "openai:gpt-4o-mini",
            "gpt-4o-mini",
            &[("usage_tier", "free")],
        );
        assert_eq!(cfg.max_concurrency, Some(1));
        assert_eq!(cfg.requests_per_minute, Some(3));
    }

    #[test]
    fn test_openai_tier1_mini_higher_rpm_than_full() {
        let mini = resolve(
            "openai:gpt-4o-mini",
            "gpt-4o-mini",
            &[("usage_tier", "tier1")],
        );
        let full = resolve("openai:gpt-4o", "gpt-4o", &[("usage_tier", "tier1")]);
        assert_eq!(mini.requests_per_minute, Some(1000));
        assert_eq!(full.requests_per_minute, Some(500));
        assert_eq!(mini.max_concurrency, full.max_concurrency); // same concurrency
    }

    #[test]
    fn test_openai_tier5_nano() {
        let cfg = resolve(
            "openai:gpt-4.1-nano",
            "gpt-4.1-nano",
            &[("usage_tier", "tier5")],
        );
        assert_eq!(cfg.max_concurrency, Some(200));
        assert_eq!(cfg.requests_per_minute, Some(30_000));
    }

    #[test]
    fn test_openai_tier5_full_size() {
        let cfg = resolve("openai:gpt-4o", "gpt-4o", &[("usage_tier", "tier5")]);
        assert_eq!(cfg.max_concurrency, Some(200));
        assert_eq!(cfg.requests_per_minute, Some(10_000));
    }

    // -- Anthropic tier tests --

    #[test]
    fn test_anthropic_defaults_to_tier2() {
        let cfg = resolve("anthropic:claude-sonnet-4-6", "claude-sonnet", &[]);
        assert_eq!(cfg.max_concurrency, Some(50));
        assert_eq!(cfg.requests_per_minute, Some(1000));
    }

    #[test]
    fn test_anthropic_tier1() {
        let cfg = resolve(
            "anthropic:claude-haiku-4-5",
            "haiku",
            &[("anthropic_usage_tier", "1")],
        );
        assert_eq!(cfg.max_concurrency, Some(10));
        assert_eq!(cfg.requests_per_minute, Some(50));
    }

    #[test]
    fn test_anthropic_tier4() {
        let cfg = resolve(
            "anthropic:claude-opus-4-6",
            "opus",
            &[("anthropic_usage_tier", "4")],
        );
        assert_eq!(cfg.max_concurrency, Some(200));
        assert_eq!(cfg.requests_per_minute, Some(4000));
    }

    // -- Google tests --

    #[test]
    fn test_google_flash_vs_pro() {
        let flash = resolve("google:gemini-2.5-flash", "flash", &[]);
        let pro = resolve("google:gemini-2.5-pro", "pro", &[]);
        assert_eq!(flash.requests_per_minute, Some(300));
        assert_eq!(pro.requests_per_minute, Some(150));
        assert_eq!(flash.max_concurrency, pro.max_concurrency);
    }

    // -- xAI tests --

    #[test]
    fn test_xai_defaults_to_tier1() {
        let cfg = resolve("xai:grok-4.20", "grok", &[]);
        assert_eq!(cfg.max_concurrency, Some(10));
        assert_eq!(cfg.requests_per_minute, Some(60));
    }

    #[test]
    fn test_xai_tier4() {
        let cfg = resolve("xai:grok-4.20", "grok", &[("xai_usage_tier", "4")]);
        assert_eq!(cfg.max_concurrency, Some(100));
        assert_eq!(cfg.requests_per_minute, Some(1000));
    }

    // -- Azure tests --

    #[test]
    fn test_azure_mini_vs_full() {
        let mini = resolve("azure:gpt-4.1-mini", "gpt-4.1-mini", &[]);
        let full = resolve("azure:gpt-4.1", "gpt-4.1", &[]);
        assert_eq!(mini.requests_per_minute, Some(5000));
        assert_eq!(full.requests_per_minute, Some(1000));
    }

    // -- Local model tests --

    #[test]
    fn test_local_model_single_concurrent_no_rpm() {
        let cfg = resolve("huggingface:model", "model", &[]);
        assert_eq!(cfg.max_concurrency, Some(1));
        assert_eq!(cfg.requests_per_minute, None);
    }

    #[test]
    fn test_local_model_overridable() {
        let cfg = resolve("file:data/model", "local", &[("max_concurrency", "4")]);
        assert_eq!(cfg.max_concurrency, Some(4));
    }

    // -- Bedrock / Databricks --

    #[test]
    fn test_bedrock_defaults() {
        let cfg = resolve("bedrock:anthropic.claude-v2", "claude", &[]);
        assert_eq!(cfg.max_concurrency, Some(20));
        assert_eq!(cfg.requests_per_minute, Some(800));
    }

    #[test]
    fn test_databricks_defaults() {
        let cfg = resolve("databricks:model", "model", &[]);
        assert_eq!(cfg.max_concurrency, Some(10));
        assert_eq!(cfg.requests_per_minute, Some(500));
    }

    // -- Rate controller integration --

    #[tokio::test]
    async fn test_built_controller_respects_concurrency() {
        let rc = config(2, 10000).build();

        let _p1 = rc.acquire().await.expect("p1 should be acquired");
        let p2 = rc.acquire().await.expect("p2 should be acquired");

        tokio::select! {
            _ = rc.acquire() => panic!("Expected semaphore to block with concurrency=2"),
            () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }

        drop(p2);
        tokio::select! {
            result = rc.acquire() => { result.expect("permit should be acquired after drop"); },
            () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("Expected to acquire permit after drop");
            }
        }
    }
}
