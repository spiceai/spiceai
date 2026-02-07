/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{collections::HashMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::json;
use spicepod::{
    acceleration::Acceleration,
    component::{dataset::Dataset, view::View},
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig, FullTextSearchConfig},
};

use crate::search::SearchTestType;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ColumnConfigOptions {
    Basic,
    MultiColumn,
    HybridSingleColumn,
    HybridMultipleColumn,
    TextSearch,
    MultiTextColumn,
    TextSearchMetadata,
    VectorSearchMetadata,
    MultiEmbeddings,
}

impl fmt::Display for ColumnConfigOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ColumnConfigOptions::Basic => "basic",
            ColumnConfigOptions::MultiColumn => "multi_column",
            ColumnConfigOptions::HybridSingleColumn => "hybrid_single_column",
            ColumnConfigOptions::HybridMultipleColumn => "hybrid_multiple_column",
            ColumnConfigOptions::TextSearch => "text_search",
            ColumnConfigOptions::MultiTextColumn => "multi_text_column",
            ColumnConfigOptions::TextSearchMetadata => "text_search_metadata",
            ColumnConfigOptions::VectorSearchMetadata => "vector_search_metadata",
            ColumnConfigOptions::MultiEmbeddings => "multi_embeddings",
        };
        write!(f, "{s}")
    }
}

impl ColumnConfigOptions {
    pub(crate) fn is_fts(&self) -> bool {
        matches!(
            self,
            ColumnConfigOptions::HybridSingleColumn
                | ColumnConfigOptions::HybridMultipleColumn
                | ColumnConfigOptions::TextSearch
                | ColumnConfigOptions::MultiTextColumn
                | ColumnConfigOptions::TextSearchMetadata
        )
    }
    pub(crate) fn to_columns(&self) -> Vec<Column> {
        match self {
            ColumnConfigOptions::Basic => {
                vec![Column::new("answer").with_embedding(
                    ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                )]
            }
            ColumnConfigOptions::MultiColumn => vec![
                Column::new("question").with_embedding(
                    ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                ),
                Column::new("answer").with_embedding(
                    ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                ),
            ],
            ColumnConfigOptions::HybridSingleColumn => vec![
                Column::new("answer")
                    .with_embedding(
                        ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                    )
                    .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
            ],
            ColumnConfigOptions::HybridMultipleColumn => vec![
                Column::new("question").with_embedding(
                    ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                ),
                Column::new("answer")
                    .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
            ],
            ColumnConfigOptions::TextSearch => {
                vec![
                    Column::new("answer")
                        .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
                ]
            }
            ColumnConfigOptions::MultiTextColumn => vec![
                Column::new("question")
                    .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
                Column::new("answer")
                    .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
            ],
            ColumnConfigOptions::TextSearchMetadata => vec![
                Column::new("answer")
                    .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
                Column::new("subject").with_metadata(HashMap::from([(
                    "vectors".to_string(),
                    serde_json::Value::String("non-filterable".to_string()),
                )])),
            ],
            ColumnConfigOptions::VectorSearchMetadata => vec![
                Column::new("answer").with_embedding(
                    ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                ),
                Column::new("subject").with_metadata(HashMap::from([(
                    "vectors".to_string(),
                    serde_json::Value::String("filterable".to_string()),
                )])),
            ],
            ColumnConfigOptions::MultiEmbeddings => {
                vec![
                    Column::new("answer")
                        .with_embedding(
                            ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                        )
                        .with_embedding(
                            ColumnLevelEmbeddingConfig::model("openai_embeddings")
                                .with_row_id("id"),
                        ),
                ]
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TableOptions {
    ViewUnionAllJoin,
    Dataset,
}

impl fmt::Display for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TableOptions::ViewUnionAllJoin => "view_union_all_join",
            TableOptions::Dataset => "dataset",
        };
        write!(f, "{s}")
    }
}

impl TableOptions {
    pub(crate) fn table_to_search_on(&self) -> &str {
        match self {
            TableOptions::Dataset | TableOptions::ViewUnionAllJoin => "qs",
        }
    }

    pub(crate) fn to_tables(&self) -> (Vec<View>, Vec<Dataset>) {
        match self {
            TableOptions::ViewUnionAllJoin => (
                vec![
                    View::new("v1".to_string())
                        .with_sql("SELECT id, question, reference_answer FROM megascience where subject!='math'"),
                    View::new("v2".to_string())
                        .with_sql("SELECT id, answer, source, subject FROM megascience where subject!='math'")
                        .with_acceleration(Acceleration { enabled: true, ..Default::default() }),
                    View::new("v3".to_string())
                        .with_sql("SELECT * FROM megascience where subject='math'"),
                    View::new("qs".to_string()).with_sql(
                        "SELECT v1.*, v2.answer, v2.source, v2.subject \
                            FROM v1 \
                            INNER JOIN v2 ON v1.id = v2.id \
                            UNION ALL ( \
                                SELECT id, question, reference_answer, answer, source, subject FROM v3 \
                            )"
                    )
                ],
                vec![
                    Dataset::new(
                        "s3://spiceai-public-datasets/MegaScience/mega-science-small.jsonl",
                        "megascience",
                    )
                    .with_params(Params::from_string_map(HashMap::from([(
                        "client_timeout".to_string(),
                        "120s".to_string(),
                    )]))),
                ],
            ),
            TableOptions::Dataset => (
                vec![],
                vec![
                    Dataset::new(
                        "s3://spiceai-public-datasets/MegaScience/mega-science-small.jsonl",
                        "qs",
                    )
                    .with_params(Params::from_string_map(HashMap::from([(
                        "client_timeout".to_string(),
                        "120s".to_string(),
                    )]))),
                ],
            ),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TestCases {
    Basic,
    Keywords,
    AdditionalColumns,
    WithWhere,
    VectorSearchSqlBasic,
    VectorSearchSqlProjection,
    VectorSearchSqlFilters,
    VectorSearchSqlNoScore,
    VectorSearchSqlRandom,
    VectorSearchSqlVectors,
    VectorSearchSqlIndexOnly,
}

impl fmt::Display for TestCases {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TestCases::Basic => "basic",
            TestCases::Keywords => "keywords",
            TestCases::AdditionalColumns => "additional_columns",
            TestCases::WithWhere => "with_where",
            TestCases::VectorSearchSqlBasic => "vector_search_sql_basic",
            TestCases::VectorSearchSqlProjection => "vector_search_sql_projection",
            TestCases::VectorSearchSqlFilters => "vector_search_sql_filters",
            TestCases::VectorSearchSqlNoScore => "vector_search_sql_no_score",
            TestCases::VectorSearchSqlRandom => "vector_search_sql_random",
            TestCases::VectorSearchSqlVectors => "vector_search_sql_vectors",
            TestCases::VectorSearchSqlIndexOnly => "vector_search_sql_index_only",
        };
        write!(f, "{s}")
    }
}

impl TestCases {
    pub(crate) fn all() -> Vec<TestCases> {
        vec![
            TestCases::Basic,
            TestCases::Keywords,
            TestCases::AdditionalColumns,
            TestCases::WithWhere,
            TestCases::VectorSearchSqlBasic,
            TestCases::VectorSearchSqlProjection,
            TestCases::VectorSearchSqlFilters,
            TestCases::VectorSearchSqlNoScore,
            TestCases::VectorSearchSqlRandom,
            TestCases::VectorSearchSqlVectors,
            TestCases::VectorSearchSqlIndexOnly,
        ]
    }

    pub(crate) fn to_input(&self) -> SearchTestType {
        match self {
            Self::Basic => SearchTestType::Http(json!({
                "text": "second",
                "limit": 4,
            })),
            Self::Keywords => SearchTestType::Http(json!({
                "text": "second",
                "limit": 4,
                "keywords": ["number"]
            })),
            Self::AdditionalColumns => SearchTestType::Http(json!({
                "text": "second",
                "limit": 4,
                "additional_columns": ["question"]
            })),
            Self::WithWhere => SearchTestType::Http(json!({
                "text": "secondary",
                "where": "subject!='math'",
                "limit": 4
            })),
            Self::VectorSearchSqlBasic => SearchTestType::Sql(
                "SELECT id, answer, trunc(_score, 3) FROM vector_search(qs, 'second', answer) order by _score desc, id LIMIT 4".to_string()
            ),
            Self::VectorSearchSqlProjection => SearchTestType::Sql(
                "SELECT id, answer, question, subject, trunc(_score, 3) as _score FROM vector_search(qs, 'second', answer) order by _score desc, id LIMIT 4".to_string()
            ),
            Self::VectorSearchSqlFilters => SearchTestType::Sql(
                "SELECT id, answer, trunc(_score, 3) as _score FROM vector_search(qs, 'secondary', answer) where subject!='math' order by _score desc, id LIMIT 4".to_string()
            ),
            Self::VectorSearchSqlNoScore => SearchTestType::Sql(
                "SELECT id, answer FROM vector_search(qs, 'second', answer) order by _score desc, id LIMIT 4".to_string()
            ),
            Self::VectorSearchSqlRandom => SearchTestType::Sql(
                "SELECT subject FROM vector_search(qs, 'second', answer) order by _score desc LIMIT 4".to_string()
            ),
            Self::VectorSearchSqlVectors => SearchTestType::Sql(
                "SELECT id, answer, array_length(answer_embedding), trunc(_score, 3) as _score  FROM vector_search(qs, 'second', answer) order by _score desc, id desc LIMIT 4;".to_string()
            ),
            Self::VectorSearchSqlIndexOnly => SearchTestType::Sql(
                "SELECT id, trunc(_score, 3) as _score  FROM vector_search(qs, 'second', answer) order by _score desc, id desc LIMIT 4;".to_string()
            ),
       }
    }
}
