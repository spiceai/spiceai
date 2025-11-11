/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use cache::result::CacheStatus;
use tonic::{
    Response,
    metadata::{Ascii, MetadataValue},
};

use runtime_request_context::{AsyncMarker, Protocol, RequestContext};

use crate::datafusion::request_context_extension::DataFusionContextExtension;

pub fn attach_cache_metadata<T>(
    response: &mut Response<T>,
    results_cache_status: CacheStatus,
    context: &RequestContext,
) {
    if let Some(val) = status_to_x_cache_value(results_cache_status) {
        response.metadata_mut().insert("x-cache", val);
    }

    if let Some(val) = status_to_results_cache_value(results_cache_status) {
        response.metadata_mut().insert("results-cache-status", val);
    }

    // Add Cache-Control response metadata with stale-while-revalidate if configured
    // Access the DataFusion instance to get the pre-parsed cache configuration
    if let Some(df_ext) = context.extension::<DataFusionContextExtension>() {
        let df = df_ext.datafusion();
        if let Some(cache_provider) = df.results_cache_provider()
            && let Some(stale_duration) = cache_provider.stale_while_revalidate_ttl()
        {
            let max_age = cache_provider.ttl().as_secs();
            let cache_control_value = format!(
                "max-age={}, stale-while-revalidate={}",
                max_age,
                stale_duration.as_secs()
            );

            if let Ok(metadata_value) = cache_control_value.parse() {
                response
                    .metadata_mut()
                    .insert("cache-control", metadata_value);
            } else {
                tracing::warn!(
                    "Failed to parse cache-control metadata value: {}",
                    cache_control_value
                );
            }
        }
    }
}

/// This is the legacy cache header, preserved for backwards compatibility.
fn status_to_x_cache_value(results_cache_status: CacheStatus) -> Option<MetadataValue<Ascii>> {
    match results_cache_status {
        CacheStatus::CacheHit | CacheStatus::CacheStaleWhileRevalidate => {
            "Hit from spiceai".parse().ok()
        }
        CacheStatus::CacheMiss => "Miss from spiceai".parse().ok(),
        CacheStatus::CacheDisabled | CacheStatus::CacheBypass => None,
    }
}

fn status_to_results_cache_value(
    results_cache_status: CacheStatus,
) -> Option<MetadataValue<Ascii>> {
    match results_cache_status {
        CacheStatus::CacheHit => "HIT".parse().ok(),
        CacheStatus::CacheMiss => "MISS".parse().ok(),
        CacheStatus::CacheBypass => "BYPASS".parse().ok(),
        CacheStatus::CacheStaleWhileRevalidate => "STALE".parse().ok(),
        CacheStatus::CacheDisabled => None,
    }
}

pub(crate) async fn set_flightsql_protocol() {
    let request_context = RequestContext::current(AsyncMarker::new().await);
    request_context.update_protocol(Protocol::FlightSQL);
}
