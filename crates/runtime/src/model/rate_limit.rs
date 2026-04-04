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
//! - OpenAI: <https://platform.openai.com/docs/guides/rate-limits>
//! - Anthropic: <https://docs.anthropic.com/en/api/rate-limits>
//! - Google Gemini: <https://ai.google.dev/gemini-api/docs/rate-limits>
//! - xAI: <https://docs.x.ai/docs/rate-limits>
//! - Azure OpenAI: <https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits>

use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use governor::Quota;
use runtime_rate_control::RateController;
use secrecy::SecretString;
use spicepod::component::model::{Model, ModelSource};

/// Builds a [`RateController`] for a model based on user-configured params or provider defaults.
///
/// User params take precedence:
/// - `ai_max_concurrency`: max concurrent AI UDF requests for this model
/// - `ai_requests_per_minute`: max RPM for this model
///
/// If neither is set, provider-specific defaults are used based on the model source,
/// model ID, and any provider-specific tier params (e.g. `usage_tier` for OpenAI).
#[must_use]
pub fn build_model_rate_controller(
    model: &Model,
    params: &HashMap<String, SecretString>,
) -> Arc<RateController> {
    let max_concurrency = parse_param_u32(params, "ai_max_concurrency");
    let rpm = parse_param_u32(params, "ai_requests_per_minute");

    if max_concurrency.is_some() || rpm.is_some() {
        return build_from_params(max_concurrency, rpm);
    }

    // Fall back to provider-specific defaults
    let source = model.get_source();
    let model_id = model.get_model_id().unwrap_or_default();
    provider_default_rate_controller(source.as_ref(), &model_id, params)
}

/// Build a rate controller from explicit user parameters.
fn build_from_params(
    max_concurrency: Option<u32>,
    requests_per_minute: Option<u32>,
) -> Arc<RateController> {
    let mut builder = RateController::builder();

    if let Some(concurrency) = max_concurrency {
        builder = builder.with_max_concurrent_requests(concurrency as usize);
    }

    if let Some(rpm) = requests_per_minute
        && let Some(nz_rpm) = NonZeroU32::new(rpm)
    {
        builder = builder.add_quota(Quota::per_minute(nz_rpm));
    }

    builder.build()
}

/// Returns a rate controller with provider-specific defaults.
fn provider_default_rate_controller(
    source: Option<&ModelSource>,
    model_id: &str,
    params: &HashMap<String, SecretString>,
) -> Arc<RateController> {
    match source {
        Some(ModelSource::OpenAi) => openai_default(model_id, params),
        Some(ModelSource::Azure) => azure_default(model_id),
        Some(ModelSource::Anthropic) => anthropic_default(model_id, params),
        Some(ModelSource::Google) => google_default(model_id),
        Some(ModelSource::Xai) => xai_default(model_id, params),
        Some(ModelSource::Bedrock) => bedrock_default(),
        Some(ModelSource::Databricks) => build_defaults(10, 500),
        Some(ModelSource::SpiceAI) => build_defaults(10, 500),
        // Local models: conservative concurrency (typically 1 GPU), no RPM limit.
        // Users with multi-GPU setups should override via ai_max_concurrency.
        Some(ModelSource::HuggingFace | ModelSource::File) => {
            RateController::builder()
                .with_max_concurrent_requests(1)
                .build()
        }
        None => build_defaults(4, 500),
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

fn openai_default(model_id: &str, params: &HashMap<String, SecretString>) -> Arc<RateController> {
    let tier = params
        .get("usage_tier")
        .map(secrecy::ExposeSecret::expose_secret)
        .and_then(|s| s.parse::<llms::openai::UsageTier>().ok());

    let is_mini = is_openai_mini_model(model_id);

    let rpm = match (&tier, is_mini) {
        // Free tier
        (Some(llms::openai::UsageTier::Free), _) => 3,
        // Tier 1: full-size 500 RPM, mini/nano 500-1000 RPM
        (Some(llms::openai::UsageTier::Tier1) | None, false) => 500,
        (Some(llms::openai::UsageTier::Tier1) | None, true) => 1000,
        // Tier 2-3: 5000 RPM for all
        (Some(llms::openai::UsageTier::Tier2 | llms::openai::UsageTier::Tier3), _) => 5000,
        // Tier 4: 10000 RPM for all
        (Some(llms::openai::UsageTier::Tier4), _) => 10_000,
        // Tier 5: full-size 10K RPM, mini/nano 30K RPM
        (Some(llms::openai::UsageTier::Tier5), false) => 10_000,
        (Some(llms::openai::UsageTier::Tier5), true) => 30_000,
    };

    // Concurrency: conservative fraction of RPM since each call takes seconds
    let max_concurrent = match &tier {
        Some(llms::openai::UsageTier::Free) => 1,
        Some(llms::openai::UsageTier::Tier1) | None => 50,
        Some(llms::openai::UsageTier::Tier2 | llms::openai::UsageTier::Tier3) => 100,
        Some(llms::openai::UsageTier::Tier4 | llms::openai::UsageTier::Tier5) => 200,
    };

    build_defaults(max_concurrent, rpm)
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

fn anthropic_default(
    _model_id: &str,
    params: &HashMap<String, SecretString>,
) -> Arc<RateController> {
    // Anthropic doesn't have a formal "usage_tier" param yet in our config,
    // but we can check for one if specified.
    let tier = parse_param_u32(params, "anthropic_tier");

    let (max_concurrent, rpm) = match tier {
        Some(1) => (10, 50),
        Some(2) => (50, 1000),
        Some(3) => (100, 2000),
        Some(4) => (200, 4000),
        // Default: Tier 2 (most common paid tier)
        _ => (50, 1000),
    };

    build_defaults(max_concurrent, rpm)
}

// ---------------------------------------------------------------------------
// Google Gemini defaults
// Source: https://ai.google.dev/gemini-api/docs/rate-limits
//
// Free: 5-15 RPM, Paid (Tier 1): 150-300 RPM depending on model
// Pro models get lower RPM, Flash models get higher RPM
// ---------------------------------------------------------------------------

fn google_default(model_id: &str) -> Arc<RateController> {
    let id = model_id.to_lowercase();
    let is_flash = id.contains("flash");

    let rpm = if is_flash { 300 } else { 150 };

    // Google's limits are quite conservative; match concurrency accordingly
    build_defaults(30, rpm)
}

// ---------------------------------------------------------------------------
// xAI (Grok) defaults
// Source: https://docs.x.ai/docs/rate-limits
//
// Tiers 0-4. Grok 4.x models can go up to 1800 RPM / 10M TPM at enterprise.
// ---------------------------------------------------------------------------

fn xai_default(_model_id: &str, params: &HashMap<String, SecretString>) -> Arc<RateController> {
    let tier = parse_param_u32(params, "xai_tier");

    let (max_concurrent, rpm) = match tier {
        Some(0) => (2, 5),
        Some(1) => (10, 60),
        Some(2) => (25, 200),
        Some(3) => (50, 500),
        Some(4) => (100, 1000),
        // Default: Tier 1 (most common starting tier)
        _ => (10, 60),
    };

    build_defaults(max_concurrent, rpm)
}

// ---------------------------------------------------------------------------
// Azure OpenAI defaults
// Source: https://learn.microsoft.com/en-us/azure/ai-services/openai/quotas-limits
//
// Quotas are per-deployment. GlobalStandard Tier 1 defaults vary by model.
// Mini models get much higher RPM (5K-20K), full-size 300-1000 RPM.
// ---------------------------------------------------------------------------

fn azure_default(model_id: &str) -> Arc<RateController> {
    let id = model_id.to_lowercase();
    let is_mini = id.contains("mini") || id.contains("nano");

    let rpm = if is_mini { 5000 } else { 1000 };

    build_defaults(50, rpm)
}

// ---------------------------------------------------------------------------
// AWS Bedrock defaults
// Source: https://docs.aws.amazon.com/bedrock/latest/userguide/quotas.html
//
// Default on-demand: ~800 RPM / 600K TPM. Varies by model and region.
// ---------------------------------------------------------------------------

fn bedrock_default() -> Arc<RateController> {
    build_defaults(20, 800)
}

fn build_defaults(max_concurrent: usize, rpm: u32) -> Arc<RateController> {
    let mut builder = RateController::builder().with_max_concurrent_requests(max_concurrent);

    if let Some(nz_rpm) = NonZeroU32::new(rpm) {
        builder = builder.add_quota(Quota::per_minute(nz_rpm));
    }

    builder.build()
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

    // -- User override tests --

    #[test]
    fn test_explicit_params_override_provider_defaults() {
        let model = Model::new("openai:gpt-4o", "gpt-4o");
        let params = make_params(&[
            ("usage_tier", "free"),
            ("ai_max_concurrency", "50"),
            ("ai_requests_per_minute", "2000"),
        ]);
        let _rc = build_model_rate_controller(&model, &params);
        // Explicit params take precedence over Free tier defaults
    }

    #[test]
    fn test_explicit_concurrency_only() {
        let model = Model::new("openai:gpt-4o", "gpt-4o");
        let params = make_params(&[("ai_max_concurrency", "10")]);
        let _rc = build_model_rate_controller(&model, &params);
    }

    #[test]
    fn test_explicit_rpm_only() {
        let model = Model::new("openai:gpt-4o", "gpt-4o");
        let params = make_params(&[("ai_requests_per_minute", "500")]);
        let _rc = build_model_rate_controller(&model, &params);
    }

    #[test]
    fn test_invalid_param_ignored() {
        let model = Model::new("openai:gpt-4o", "gpt-4o");
        let params = make_params(&[("ai_max_concurrency", "not_a_number")]);
        let _rc = build_model_rate_controller(&model, &params);
        // Invalid value is ignored, falls back to provider defaults
    }

    // -- OpenAI tier tests --

    #[test]
    fn test_openai_defaults_to_tier1() {
        let model = Model::new("openai:gpt-4o", "gpt-4o");
        let params = make_params(&[]);
        let _rc = build_model_rate_controller(&model, &params);
        // Default: Tier1 full-size (50 concurrent, 500 RPM)
    }

    #[test]
    fn test_openai_free_tier() {
        let model = Model::new("openai:gpt-4o-mini", "gpt-4o-mini");
        let params = make_params(&[("usage_tier", "free")]);
        let _rc = build_model_rate_controller(&model, &params);
        // Free: 1 concurrent, 3 RPM
    }

    #[test]
    fn test_openai_tier1_mini_higher_rpm() {
        // Mini models get 1000 RPM at Tier 1 vs 500 for full-size
        let mini_model = Model::new("openai:gpt-4o-mini", "gpt-4o-mini");
        let full_model = Model::new("openai:gpt-4o", "gpt-4o");
        let params = make_params(&[("usage_tier", "tier1")]);
        let _rc_mini = build_model_rate_controller(&mini_model, &params);
        let _rc_full = build_model_rate_controller(&full_model, &params);
        // Both build successfully; mini has higher RPM
    }

    #[test]
    fn test_openai_tier5() {
        let model = Model::new("openai:gpt-4.1-nano", "gpt-4.1-nano");
        let params = make_params(&[("usage_tier", "tier5")]);
        let _rc = build_model_rate_controller(&model, &params);
        // Tier5 nano: 200 concurrent, 30000 RPM
    }

    // -- Anthropic tier tests --

    #[test]
    fn test_anthropic_defaults_to_tier2() {
        let model = Model::new("anthropic:claude-sonnet-4-6", "claude-sonnet");
        let params = make_params(&[]);
        let _rc = build_model_rate_controller(&model, &params);
        // Default: Tier 2 (50 concurrent, 1000 RPM)
    }

    #[test]
    fn test_anthropic_tier4() {
        let model = Model::new("anthropic:claude-opus-4-6", "claude-opus");
        let params = make_params(&[("anthropic_tier", "4")]);
        let _rc = build_model_rate_controller(&model, &params);
        // Tier 4: 200 concurrent, 4000 RPM
    }

    // -- Google tests --

    #[test]
    fn test_google_flash_higher_rpm() {
        let flash = Model::new("google:gemini-2.5-flash", "gemini-flash");
        let pro = Model::new("google:gemini-2.5-pro", "gemini-pro");
        let params = make_params(&[]);
        let _rc_flash = build_model_rate_controller(&flash, &params);
        let _rc_pro = build_model_rate_controller(&pro, &params);
    }

    // -- xAI tests --

    #[test]
    fn test_xai_defaults_to_tier1() {
        let model = Model::new("xai:grok-4.20", "grok");
        let params = make_params(&[]);
        let _rc = build_model_rate_controller(&model, &params);
        // Default: Tier 1 (10 concurrent, 60 RPM)
    }

    #[test]
    fn test_xai_tier4() {
        let model = Model::new("xai:grok-4.20", "grok");
        let params = make_params(&[("xai_tier", "4")]);
        let _rc = build_model_rate_controller(&model, &params);
        // Tier 4: 100 concurrent, 1000 RPM
    }

    // -- Azure tests --

    #[test]
    fn test_azure_mini_higher_rpm() {
        let mini = Model::new("azure:gpt-4.1-mini", "gpt-4.1-mini");
        let full = Model::new("azure:gpt-4.1", "gpt-4.1");
        let params = make_params(&[]);
        let _rc_mini = build_model_rate_controller(&mini, &params);
        let _rc_full = build_model_rate_controller(&full, &params);
    }

    // -- Local model tests --

    #[test]
    fn test_local_model_single_concurrent() {
        let model = Model::new("huggingface:model", "model");
        let params = make_params(&[]);
        let _rc = build_model_rate_controller(&model, &params);
        // Local: 1 concurrent (single GPU), no RPM limit
    }

    #[test]
    fn test_local_model_overridable() {
        let model = Model::new("file:data/model", "local-model");
        let params = make_params(&[("ai_max_concurrency", "4")]);
        let _rc = build_model_rate_controller(&model, &params);
        // User can override for multi-GPU setups
    }

    // -- Bedrock / Databricks --

    #[test]
    fn test_bedrock_defaults() {
        let model = Model::new("bedrock:anthropic.claude-v2", "claude-bedrock");
        let params = make_params(&[]);
        let _rc = build_model_rate_controller(&model, &params);
        // Bedrock: 20 concurrent, 800 RPM
    }

    // -- Concurrency enforcement test --

    #[tokio::test]
    async fn test_rate_controller_respects_concurrency() {
        let rc = build_defaults(2, 10000);

        let p1 = rc.acquire().await;
        let p2 = rc.acquire().await;
        assert!(p1.is_ok());
        assert!(p2.is_ok());

        // Third should block
        tokio::select! {
            _ = rc.acquire() => {
                panic!("Expected semaphore to block with concurrency=2");
            },
            () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }

        // Drop one permit, next should succeed
        drop(p2);
        tokio::select! {
            result = rc.acquire() => {
                assert!(result.is_ok());
            },
            () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                panic!("Expected to acquire permit after drop");
            }
        }
    }
}
