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

use arrow_flight::FlightInfo;
use bytes::Bytes;
use cache::result::CacheStatus;
use tonic::{
    Response,
    metadata::{Ascii, MetadataValue},
};

use runtime_request_context::{AsyncMarker, Protocol, RequestContext, SPICE_TRACE_ID_HEADER};

use crate::datafusion::request_context_extension::DataFusionContextExtension;

/// Returns the trace id to the caller as gRPC response metadata, under the
/// same name the request header that pins one uses.
///
/// gRPC metadata *is* HTTP/2 headers, so this is the Flight counterpart of the
/// HTTP response header — and what a client with a Flight middleware or a
/// gRPC interceptor reads. A Flight SQL JDBC caller cannot see response
/// metadata and reads [`with_trace_id_app_metadata`] instead.
pub(crate) fn attach_trace_id_metadata<T>(response: &mut Response<T>, trace_id: &str) {
    match trace_id.parse::<MetadataValue<Ascii>>() {
        Ok(value) => {
            response.metadata_mut().insert(SPICE_TRACE_ID_HEADER, value);
        }
        // Unreachable for a normalized id — 32 hexadecimal characters are
        // always valid metadata — and losing the header costs correlation
        // rather than the response.
        Err(e) => tracing::warn!("Failed to return trace id '{trace_id}': {e}"),
    }
}

/// Returns the trace id in `FlightInfo.app_metadata`, as
/// `{"trace_id":"<32 hex characters>"}`.
///
/// This is the one place a Flight SQL JDBC caller can read it: the driver
/// surfaces it as `ArrowFlightJdbcFlightStreamResultSet.getAppMetadata()`, and
/// surfaces neither response metadata nor per-message `app_metadata`. JSON
/// rather than the bare id so a second field can be added without breaking a
/// client that already parses this one.
///
/// Interpolated rather than serialized because a trace id is normalized before
/// it reaches here — 32 lowercase hexadecimal characters — so there is nothing
/// a JSON encoder would escape.
#[must_use]
pub(crate) fn with_trace_id_app_metadata(info: FlightInfo, trace_id: &str) -> FlightInfo {
    debug_assert!(
        trace_id.bytes().all(|b| b.is_ascii_hexdigit()),
        "a trace id reaching the wire is normalized hexadecimal"
    );

    FlightInfo {
        app_metadata: Bytes::from(format!(r#"{{"trace_id":"{trace_id}"}}"#)),
        ..info
    }
}

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
            // When serving stale content, set max-age=0 to indicate the response is not fresh
            // The results-cache-status metadata will indicate STALE
            let max_age = if results_cache_status == CacheStatus::CacheStaleWhileRevalidate {
                0
            } else {
                cache_provider.ttl().as_secs()
            };

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
