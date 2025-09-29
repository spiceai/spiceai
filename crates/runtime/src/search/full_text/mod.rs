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
use std::sync::Arc;

use datafusion::sql::TableReference;
use search::generation::{CandidateGeneration, text_search::index::FullTextDatabaseIndex};

use crate::{datafusion::DataFusion, search::candidate::text::TextSearchCandidate};

pub mod connector;
pub mod udtf;

/// Constructs a [`CandidateGeneration`] for full text search on the underlying [`tantivy::Index`] with full filter and column support via the underlying [`TableProvider`].
pub async fn as_candidate_generations(
    database_index: &FullTextDatabaseIndex,
    df: Arc<DataFusion>,
    tbl: TableReference,
) -> Result<Vec<Arc<dyn CandidateGeneration>>, search::generation::Error> {
    let mut generators = vec![];
    for search_field in database_index.search_fields.as_slice() {
        let base = database_index
            .full_text_search_field_index(search_field.as_str())
            .map_err(|source| search::generation::Error::TextSearchError { source })?;

        let candidate: TextSearchCandidate =
            TextSearchCandidate::new(Arc::new(base), Arc::clone(&df), tbl.clone());

        generators.push(Arc::new(candidate) as Arc<dyn CandidateGeneration>);
    }

    Ok(generators)
}
