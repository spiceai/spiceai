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

//! Search a sidecar delegates to the upstream Spice runtime holding a table's
//! embeddings and search indexes.

use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use datafusion::{catalog::TableProvider, common::TableReference};
use prost::Message;
use runtime_proto::{TextSearchArgs, UdtfArgs, VectorSearchArgs, udtf_args::Args};

/// One `vector_search` or `text_search` call, without its table: the upstream's
/// name for the table is filled in when the call is delegated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DelegatedSearchCall {
    Vector {
        query: String,
        column: Option<String>,
        limit: Option<usize>,
        include_score: Option<bool>,
        distance_metric: Option<String>,
    },
    Text {
        query: String,
        column: Option<String>,
        limit: Option<usize>,
        include_score: Option<bool>,
    },
}

impl DelegatedSearchCall {
    /// Whether the result carries a score column. Defaults to `true`, as the
    /// table functions do.
    #[must_use]
    pub fn include_score(&self) -> bool {
        match self {
            Self::Vector { include_score, .. } | Self::Text { include_score, .. } => {
                include_score.unwrap_or(true)
            }
        }
    }

    /// The call as the upstream receives it, against its table `remote`.
    #[must_use]
    pub fn encode(&self, remote: &str) -> Vec<u8> {
        let limit = |limit: &Option<usize>| limit.map(|limit| limit as u64);
        let args = match self {
            Self::Vector {
                query,
                column,
                limit: max,
                include_score,
                distance_metric,
            } => Args::VectorSearch(VectorSearchArgs {
                table: remote.to_string(),
                query: query.clone(),
                column: column.clone(),
                limit: limit(max),
                include_score: *include_score,
                distance_metric: distance_metric.clone(),
            }),
            Self::Text {
                query,
                column,
                limit: max,
                include_score,
            } => Args::TextSearch(TextSearchArgs {
                table: remote.to_string(),
                query: query.clone(),
                column: column.clone(),
                limit: limit(max),
                include_score: *include_score,
            }),
        };
        UdtfArgs { args: Some(args) }.encode_to_vec()
    }
}

/// The search `call` over `dataset`, run by the upstream Spice runtime, when
/// `provider` is a sidecar's Spice table. `None` when `provider` is searched
/// locally or not at all.
#[must_use]
pub fn delegated_search(
    provider: &dyn TableProvider,
    dataset: &TableReference,
    call: &DelegatedSearchCall,
) -> Option<Arc<dyn TableProvider>> {
    data_components::flight::delegated_search(
        provider,
        &dataset.to_string(),
        call.include_score(),
        |remote| call.encode(remote),
    )
}

/// The columns the upstream embeds, for a table whose search is delegated:
/// each `<column>_embedding` list column beside a `<column>` in the upstream's
/// schema. Empty when search over `provider` is not delegated.
#[must_use]
pub fn delegated_embedding_columns(provider: &dyn TableProvider) -> Vec<String> {
    data_components::flight::delegated_search_schema(provider)
        .map(|schema| embedding_columns(&schema))
        .unwrap_or_default()
}

fn embedding_columns(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|field| {
            let column = field.name().strip_suffix("_embedding")?;
            let is_vector = matches!(
                field.data_type(),
                DataType::FixedSizeList(..) | DataType::List(_) | DataType::LargeList(_)
            );
            (is_vector && schema.field_with_name(column).is_ok()).then(|| column.to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An embedded column shows in the upstream's schema as a list column named
    /// after it; other columns, and a lone `_embedding` column, are not embedded.
    #[test]
    fn embedded_columns_are_read_off_the_upstream_schema() {
        use arrow_schema::Field;
        let vector = |name: &str| {
            Field::new(
                name,
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 64),
                true,
            )
        };
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            vector("label_embedding"),
            Field::new("body", DataType::Utf8, true),
            Field::new("body_embedding", DataType::Utf8, true),
            vector("orphan_embedding"),
        ]);
        assert_eq!(embedding_columns(&schema), vec!["label".to_string()]);
    }

    /// The encoded call names the upstream's table, keeps every argument, and
    /// decodes as the message the upstream reads.
    #[test]
    fn calls_encode_against_the_upstream_table() {
        let vector = DelegatedSearchCall::Vector {
            query: "snow".to_string(),
            column: Some("label".to_string()),
            limit: Some(5),
            include_score: None,
            distance_metric: Some("l2".to_string()),
        };
        assert!(vector.include_score());
        assert_eq!(
            UdtfArgs::decode(vector.encode("\"Remote\".docs").as_slice())
                .expect("a vector call decodes"),
            UdtfArgs {
                args: Some(Args::VectorSearch(VectorSearchArgs {
                    table: "\"Remote\".docs".to_string(),
                    query: "snow".to_string(),
                    column: Some("label".to_string()),
                    limit: Some(5),
                    include_score: None,
                    distance_metric: Some("l2".to_string()),
                })),
            }
        );
        let text = DelegatedSearchCall::Text {
            query: "snow".to_string(),
            column: None,
            limit: None,
            include_score: Some(false),
        };
        assert!(!text.include_score());
        assert_eq!(
            UdtfArgs::decode(text.encode("docs").as_slice()).expect("a text call decodes"),
            UdtfArgs {
                args: Some(Args::TextSearch(TextSearchArgs {
                    table: "docs".to_string(),
                    query: "snow".to_string(),
                    column: None,
                    limit: None,
                    include_score: Some(false),
                })),
            }
        );
    }
}
