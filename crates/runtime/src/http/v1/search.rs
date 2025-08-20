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
use crate::{
    request::{AsyncMarker, RequestContext},
    search::{
        Error as VectorSearchError,
        request::{SearchRequest, SearchRequestAIJson, SearchRequestHTTPJson},
        types::{Match, to_matches_sorted},
        vector_search::VectorSearch,
    },
};
use axum::{
    Extension, Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use http::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct SearchResponse {
    /// List of matches that were found in the datasets
    pub results: Vec<Match>,

    /// Total time taken to execute the search, in milliseconds
    pub duration_ms: u128,
}

/// Search
///
/// Perform a vector similarity search (VSS) operation on a dataset.
///
/// The search operation will return the most relevant matches based on cosine similarity with the input `text`.
/// The datasets queries should have an embedding column, and the appropriate embedding model loaded.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/search",
    operation_id = "post_search",
    tag = "SQL",
    request_body(
        description = "Search request parameters",
        content((
            SearchRequestHTTPJson = "application/json",
                example = json!({
                    "datasets": ["app_messages"],
                    "text": "Tokyo plane tickets",
                    "where": "user=1234321",
                    "additional_columns": ["timestamp"],
                    "limit": 3,
                    "keywords": ["plane", "tickets"]
                })
            )
        )
    ),
    responses(
        (status = 200, description = "Search completed successfully", content((
            SearchResponse = "application/json",
                example = json!({
                    "results": [
                        {
                            "matches": {
                                "message": "I booked use some tickets"
                            },
                            "dataset": "app_messages",
                            "primary_key": { "id": "6fd5a215-0881-421d-ace0-b293b83452b5" },
                            "data": { "timestamp": 1_724_716_542 },
                            "score": 0.914_321
                        },
                        {
                            "matches": {
                                "message": "direct to Narata"
                            },
                            "dataset": "app_messages",
                            "primary_key": { "id": "8a25595f-99fb-4404-8c82-e1046d8f4c4b" },
                            "data": { "timestamp": 1_724_715_881 },
                            "score": 0.83221
                        },
                        {
                            "matches": {
                                "message": "Yes, we're sitting together"
                            },
                            "dataset": "app_messages",
                            "primary_key": { "id": "8421ed84-b86d-4b10-b4da-7a432e8912c0" },
                            "data": { "timestamp": 1_724_716_123 },
                            "score": 0.787_654_321

                        }
                    ],
                    "duration_ms": 42
                })
            )
        )),
        (status = 400, description = "Invalid request parameters", content((
            serde_json::Value = "application/json", example = json!({
                    "error": "No data sources provided"
                })
            ))
        ),
        (status = 500, description = "Internal server error", content((
            serde_json::Value = "application/json", example = json!({
                    "error": "Unexpected internal server error occurred"
                })
            ))
        )
    )
))]
pub(crate) async fn post(
    Extension(vs): Extension<Arc<VectorSearch>>,
    Json(payload): Json<SearchRequestHTTPJson>,
) -> Response {
    let start_time = Instant::now();

    // For now, force the user to specify which data.
    if payload
        .base
        .datasets
        .as_ref()
        .is_some_and(std::vec::Vec::is_empty)
    {
        return (StatusCode::BAD_REQUEST, "No data sources provided").into_response();
    }

    if payload.base.limit.is_some_and(|limit| limit == 0) {
        return (StatusCode::BAD_REQUEST, "Limit must be greater than 0").into_response();
    }

    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "vector_search", input = %payload.base.text);

    let search_request = match SearchRequest::try_from(SearchRequestAIJson::from(payload)) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };

    let request_context = RequestContext::current(AsyncMarker::new().await);
    let cache_provider = vs.df.search_cache_provider();
    match vs
        .search_with_cache(
            &search_request,
            cache_provider,
            Arc::clone(&request_context),
        )
        .await
    {
        Ok((resp, cache_status)) => match to_matches_sorted(resp, search_request.limit).await {
            Ok(m) => {
                let mut headers = HeaderMap::new();

                if let Some(val) = cache_status.to_header_string().and_then(|v| v.parse().ok()) {
                    headers.insert("Search-Results-Cache-Status", val);
                }

                // Tell CDN entry is unique per user cache key
                if request_context.client_supplied_cache_key().is_some() {
                    headers.insert("Vary", HeaderValue::from_static("Spice-Cache-Key"));
                }

                (
                    StatusCode::OK,
                    headers,
                    Json(SearchResponse {
                        results: m,
                        duration_ms: start_time.elapsed().as_millis(),
                    }),
                )
                    .into_response()
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        Err(e) => {
            let error_type = match e {
                VectorSearchError::NoTablesWithSearchFound {}
                | VectorSearchError::CannotVectorSearchDataset { .. } => StatusCode::BAD_REQUEST,
                VectorSearchError::SearchPipelineError { ref source } if source.is_user_error() => {
                    StatusCode::BAD_REQUEST
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            tracing::error!(target: "task_history", parent: &span, "{e}");
            (error_type, e.to_string()).into_response()
        }
    }
}
