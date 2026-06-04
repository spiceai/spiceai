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
#![allow(clippy::missing_errors_doc)]

#[cfg(feature = "text_search")]
use std::sync::Arc;

#[cfg(feature = "text_search")]
use datafusion::sql::TableReference;
#[cfg(feature = "text_search")]
use search::generation::CandidateGeneration;
#[cfg(feature = "text_search")]
use search::generation::text_search::index::FullTextDatabaseIndex;

#[cfg(feature = "text_search")]
use runtime_query_engine::query_engine::QueryEngine;

#[cfg(feature = "text_search")]
use crate::candidate::text::TextSearchCandidate;

/// Constructs a [`CandidateGeneration`] for full text search on the underlying [`tantivy::Index`] with full filter and column support via the underlying [`TableProvider`].
#[cfg(feature = "text_search")]
pub async fn as_candidate_generations(
    database_index: &FullTextDatabaseIndex,
    df: Arc<dyn QueryEngine>,
    tbl: TableReference,
) -> Result<Vec<Arc<dyn CandidateGeneration>>, search::generation::Error> {
    let mut generators = vec![];
    for search_field in database_index.search_fields.as_slice() {
        let base = database_index
            .full_text_search_field_index(search_field as &str)
            .map_err(|source| search::generation::Error::TextSearchError { source })?;

        let candidate: TextSearchCandidate =
            TextSearchCandidate::new(Arc::new(base), Arc::clone(&df), tbl.clone());

        generators.push(Arc::new(candidate) as Arc<dyn CandidateGeneration>);
    }

    Ok(generators)
}

/// Constructs [`CandidateGeneration`]s for Elasticsearch BM25 full-text search,
/// one per indexed search field.
#[cfg(feature = "elasticsearch")]
pub async fn as_es_text_candidate_generations(
    indexes: Vec<&search::index::elasticsearch::ElasticsearchTextIndex>,
    df: Arc<dyn QueryEngine>,
    tbl: TableReference,
) -> Result<Vec<Arc<dyn CandidateGeneration>>, search::generation::Error> {
    use crate::candidate::elasticsearch_text::ElasticsearchTextSearchCandidate;

    let mut generators = vec![];
    for idx in indexes {
        for field in &idx.search_fields {
            generators.push(Arc::new(ElasticsearchTextSearchCandidate::new(
                field.clone(),
                Arc::clone(&df),
                tbl.clone(),
            )) as Arc<dyn CandidateGeneration>);
        }
    }
    Ok(generators)
}
