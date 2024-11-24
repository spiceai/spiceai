/*
Copyright 2024 The Spice.ai OSS Authors

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
use crate::embeddings::vector_search::{
    to_matches_sorted, Match, SearchRequest, SearchRequestJson, VectorSearch,
};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SearchResponse {
    matches: Vec<Match>,
    duration_ms: u128,
}

pub(crate) async fn post(
    Extension(vs): Extension<Arc<VectorSearch>>,
    Json(payload): Json<SearchRequestJson>,
) -> Response {
    let start_time = Instant::now();

    // For now, force the user to specify which data.
    if payload
        .datasets
        .as_ref()
        .is_some_and(std::vec::Vec::is_empty)
    {
        return (StatusCode::BAD_REQUEST, "No data sources provided").into_response();
    }

    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "vector_search", input = %payload.text);

    let search_request = match SearchRequest::try_from(payload) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return (StatusCode::BAD_REQUEST, e).into_response();
        }
    };

    match vs.search(&search_request).await {
        Ok(resp) => match to_matches_sorted(&resp) {
            Ok(m) => (
                StatusCode::OK,
                Json(SearchResponse {
                    matches: m,
                    duration_ms: start_time.elapsed().as_millis(),
                }),
            )
                .into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        Err(e) => {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
