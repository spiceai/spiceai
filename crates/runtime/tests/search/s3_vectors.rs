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

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use s3_vectors::Client;
use serde_json::json;
use snafu::ResultExt;
use spicepod::{
    acceleration::Acceleration,
    component::dataset::Dataset,
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig},
};

use crate::{
    configure_test_datafusion,
    models::{
        get_mega_science_dataset, get_mega_science_view,
        hf::{get_huggingface_embeddings, get_model_to_vec_embeddings},
    },
    search::{
        SearchTestCase, SearchTestType, basic_vector_search_tests_on_table, run_search_w_explain,
        vectors_nonfilterable_col,
    },
    utils::verify_env_secret_exists,
};

use anyhow::anyhow;
use app::AppBuilder;
use datafusion::sql::TableReference;
use spicepod::{
    component::embeddings::EmbeddingChunkConfig, param::ParamValue, semantic::FullTextSearchConfig,
    vector::VectorStore,
};
use std::sync::Arc;

use app::App;
use futures::StreamExt;
use runtime::Runtime;

use crate::DEFAULT_TRACING_MODELS;
use crate::utils::runtime_ready_check;

async fn add_mega_science_view_from_ds(
    mut app: AppBuilder,
    ds: &Dataset,
) -> Result<AppBuilder, anyhow::Error> {
    let (view_ds, mut views) = get_mega_science_view(
        Some("qs_view"),
        ds.columns.iter().find(|c| c.name == "question").cloned(),
        ds.columns.iter().find(|c| c.name == "answer").cloned(),
    );
    app = app.with_dataset(view_ds);

    let bkt = ds
        .vectors
        .as_ref()
        .and_then(|v| v.params.as_ref())
        .and_then(|p| p.data.get("s3_vectors_bucket"))
        .map(ParamValue::as_string)
        .clone()
        .ok_or(anyhow!("Dataset has no 's3_vectors_bucket'"))?;

    let idx = ds
        .vectors
        .as_ref()
        .and_then(|v| v.params.as_ref())
        .and_then(|p| p.data.get("s3_vectors_index"))
        .map(ParamValue::as_string)
        .clone()
        .ok_or(anyhow!("Dataset has no 's3_vectors_index'"))?;

    // Last is `view` we want to test upon
    if let Some(mut v) = views.pop() {
        let store =
            init_vector_store_w_index_name(&bkt, format!("{idx}-view").as_str(), true).await?;
        v.vectors = Some(store);

        app = app.with_view(v);
    }

    // Add dependent views.
    for v in views {
        app = app.with_view(v);
    }

    Ok(app)
}

// mod old {
//     pub use super::*;
//     #[tokio::test]
//     async fn basic_functionality() -> Result<(), anyhow::Error> {
//         let mut app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
//             "minishlab/potion-base-2M",
//             "hf_minilm",
//         ));

//         let mut ds =
//             get_mega_science_dataset(
//                 Some("qs"),
//                 None,
//                 Some(Column::new("answer").with_embedding(
//                     ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//                 )),
//             );
//         ds.vectors = Some(init_vector_store("spice-ci-tests-s3-vectors-basic", true).await?);
//         app = add_mega_science_view_from_ds(app, &ds).await?;
//         app = app.with_dataset(ds);

//         run_search_w_explain(
//             app.build(),
//             [
//                 basic_vector_search_tests_on_table("s3vectors_basic", "qs"),
//                 basic_vector_search_tests_on_table("s3vectors_basic_view", "qs_view"),
//             ]
//             .concat(),
//             true,
//         )
//         .await
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn hybrid_w_vector_engine() -> Result<(), anyhow::Error> {
//         let mut app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
//             "minishlab/potion-base-2M",
//             "hf_minilm",
//         ));
//         let mut ds =
//             get_mega_science_dataset(
//                 Some("qs"),
//                 Some(
//                     Column::new("question")
//                         .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
//                 ),
//                 Some(Column::new("answer").with_embedding(
//                     ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//                 )),
//             );
//         ds.vectors = Some(init_vector_store("spice-ci-tests-s3-vectors-hybrid", true).await?);
//         app = add_mega_science_view_from_ds(app, &ds).await?;
//         app = app.with_dataset(ds);

//         let cases = vec![
//             SearchTestCase::new(
//                 "s3vectors_hybrid_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_additional_columns2",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["answer"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_vector_search_sql_basic",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_vector_search_sql_w_question",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_vector_search_text_search",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc, id LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_vector_search_text_search_w_embedding",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, array_length(answer_embedding), trunc(score, 3) FROM text_search(qs, 'second') order by score desc, id LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "s3vectors_hybrid_vector_search_text_search_w_answer",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc, id LIMIT 4",
//                 ),
//             ),
//         ];
//         run_search_w_explain(
//             app.build(),
//             [
//                 // Run all tests cases on dataset `qs`, and view `qs_view`.
//                 cases
//                     .iter()
//                     .map(|c| {
//                         let mut case = c.replace_table(
//                             &TableReference::parse_str("qs"),
//                             &TableReference::parse_str("qs_view"),
//                         );
//                         case.name = case
//                             .name
//                             .replace("s3vectors_hybrid_", "s3vectors_hybrid_view_");
//                         case
//                     })
//                     .collect(),
//                 cases,
//             ]
//             .concat(),
//             true,
//         )
//         .await
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn multiple_embeddings() -> Result<(), anyhow::Error> {
//         let mut app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
//             "minishlab/potion-base-2M",
//             "hf_minilm",
//         ));

//         let mut ds =
//             get_mega_science_dataset(
//                 Some("qs"),
//                 Some(Column::new("question").with_embedding(
//                     ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//                 )),
//                 Some(Column::new("answer").with_embedding(
//                     ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//                 )),
//             );
//         let vector_store = init_vector_store("spice-ci-tests-s3-vectors-hybrid", true).await?;
//         ds.vectors = Some(vector_store);

//         app = add_mega_science_view_from_ds(app, &ds).await?;
//         app = app.with_dataset(ds);

//         run_search_w_explain(
//             app.build(),
//             [
//                 basic_vector_search_tests_on_table("s3vectors_multiple_embeddings", "qs"),
//                 basic_vector_search_tests_on_table("s3vectors_multiple_embeddings_view", "qs_view"),
//                 vec![
//                 SearchTestCase::new(
//                     "s3vectors_multiple_embeddings_additional_columns2",
//                     SearchTestType::Http(json!({
//                         "text": "second",
//                         "limit": 4,
//                         "datasets": ["qs"],
//                         "additional_columns": ["answer"],
//                     })),
//                 ),
//                 SearchTestCase::new(
//                     "s3vectors_multiple_embeddings_view_additional_columns2",
//                     SearchTestType::Http(json!({
//                         "text": "second",
//                         "limit": 4,
//                         "datasets": ["qs", "qs_view"],
//                         "additional_columns": ["answer"],
//                     })),
//                 ),
//                 SearchTestCase::new(
//                     "s3vectors_multiple_embeddings_vector_search_questions",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer, trunc(score, 3) FROM vector_search(qs, 'second', question) order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vectors_multiple_embeddings_view_vector_search_questions",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer, trunc(score, 3) FROM vector_search(qs_view, 'second', question) order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vectors_multiple_embeddings_vector_search_w_embeddings",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer, array_length(question_embedding), array_length(answer_embedding), trunc(score, 3) FROM vector_search(qs, 'second', question) order by score desc, id LIMIT 4",
//                     ),
//                 ),

//                 SearchTestCase::new(
//                     "s3vectors_multiple_embeddings_view_vector_search_w_embeddings",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer, array_length(question_embedding), array_length(answer_embedding), trunc(score, 3) FROM vector_search(qs_view, 'second', question) order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 ]].concat(),
//             true
//         )
//         .await
//     }

//     #[tokio::test]
//     async fn multi_column_primary_key() -> Result<(), anyhow::Error> {
//         let mut app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
//             "minishlab/potion-base-2M",
//             "hf_minilm",
//         ));
//         let mut ds = get_mega_science_dataset(
//             Some("qs"),
//             None,
//             Some(
//                 Column::new("answer").with_embedding(
//                     ColumnLevelEmbeddingConfig::model("hf_minilm")
//                         .with_row_id("id")
//                         .with_row_id("question"),
//                 ),
//             ),
//         );
//         let vector_store = init_vector_store("spice-ci-tests-s3-vectors-compose-pk", true).await?;
//         ds.vectors = Some(vector_store);
//         app = add_mega_science_view_from_ds(app, &ds).await?;
//         app = app.with_dataset(ds);

//         run_search_w_explain(
//             app.build(),

//             [basic_vector_search_tests_on_table("s3vectors_composite", "qs"),
//                 basic_vector_search_tests_on_table("s3vectors_composite_view", "qs_view"),
//                 vec![
//                 SearchTestCase::new(
//                     "s3vector_composite_vector_search_sql_composite_key",
//                     SearchTestType::from_sql(
//                         "SELECT id, question, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_composite_view_vector_search_sql_composite_key",
//                     SearchTestType::from_sql(
//                         "SELECT id, question, answer, trunc(score, 3) FROM vector_search(qs_view, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_composite_vector_search_sql_filters",
//                     SearchTestType::from_sql(
//                         "SELECT question, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary') where id > 10 order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_composite_view_vector_search_sql_filters",
//                     SearchTestType::from_sql(
//                         "SELECT question, answer, trunc(score, 3) as score FROM vector_search(qs_view, 'secondary') where id > 10 order by score desc, id LIMIT 4",
//                     ),
//                 )]].concat(),
//             true
//         )
//         .await
//     }

//     #[tokio::test]
//     async fn with_chunking_metadata() -> Result<(), anyhow::Error> {
//         let mut app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
//             "minishlab/potion-base-2M",
//             "hf_minilm",
//         ));
//         let mut ds = get_mega_science_dataset(
//             Some("qs"),
//             None,
//             Some(vectors_nonfilterable_col(
//                 Column::new("answer").with_embedding(
//                     ColumnLevelEmbeddingConfig::model("hf_minilm")
//                         .with_row_id("id")
//                         .chunking(
//                             EmbeddingChunkConfig::enabled()
//                                 .target_chunk_size(64)
//                                 .trim_whitespace(true),
//                         ),
//                 ),
//             )),
//         );
//         let vector_store =
//             init_vector_store("spice-ci-tests-s3-vectors-chunking-metadata", true).await?;
//         ds.vectors = Some(vector_store);

//         app = add_mega_science_view_from_ds(app, &ds).await?;
//         app = app.with_dataset(ds);

//         run_search_w_explain(
//             app.build(),
//             [basic_vector_search_tests_on_table("s3vectors_chunking_metadata", "qs"),
//                 basic_vector_search_tests_on_table("s3vectors_chunking_metadata_view", "qs_view"),
//                 vec![
//                 SearchTestCase::new(
//                     "s3vector_chunking_metadata_vector_search_sql_match",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_metadata_view_vector_search_sql_match",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, trunc(score, 3) FROM vector_search(qs_view, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_metadata_vector_search_sql_offset",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer_offset, trunc(score, 3) FROM vector_search(qs, 'second') order by score DESC, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_metadata_view_vector_search_sql_offset",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer_offset, trunc(score, 3) FROM vector_search(qs_view, 'second') order by score DESC, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_metadata_vector_search_sql_match_and_underlying",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_metadata_view_vector_search_sql_match_and_underlying",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, answer, trunc(score, 3) FROM vector_search(qs_view, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 )]].concat(),
//             true
//         )
//         .await
//     }

//     #[tokio::test]
//     async fn with_chunking() -> Result<(), anyhow::Error> {
//         let mut app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
//             "minishlab/potion-base-2M",
//             "hf_minilm",
//         ));
//         let mut ds = get_mega_science_dataset(
//             Some("qs"),
//             None,
//             Some(
//                 Column::new("answer").with_embedding(
//                     ColumnLevelEmbeddingConfig::model("hf_minilm")
//                         .with_row_id("id")
//                         .chunking(
//                             EmbeddingChunkConfig::enabled()
//                                 .target_chunk_size(64)
//                                 .trim_whitespace(true),
//                         ),
//                 ),
//             ),
//         );
//         let vector_store = init_vector_store("spice-ci-tests-s3-vectors-chunking", true).await?;
//         ds.vectors = Some(vector_store);

//         app = add_mega_science_view_from_ds(app, &ds).await?;
//         app = app.with_dataset(ds);

//         run_search_w_explain(
//             app.build(),
//             [
//                 basic_vector_search_tests_on_table("s3vectors_chunking", "qs"),
//                 basic_vector_search_tests_on_table("s3vectors_chunking_view", "qs_view"),
//                 vec![
//                 SearchTestCase::new(
//                     "s3vector_chunking_vector_search_sql_match",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_view_vector_search_sql_match",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, trunc(score, 3) FROM vector_search(qs_view, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_vector_search_sql_offset",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer_offset, trunc(score, 3) FROM vector_search(qs, 'second') order by score DESC, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_view_vector_search_sql_offset",
//                     SearchTestType::from_sql(
//                         "SELECT id, answer_offset, trunc(score, 3) FROM vector_search(qs_view, 'second') order by score DESC, id LIMIT 4",
//                     ),
//                 ),
//                 // TODO: This is performing a needless join (since search_field is in vector index, `match` can be computed without base table).
//                 // Tracking: `<https://github.com/spiceai/spiceai/issues/7512>`
//                 SearchTestCase::new(
//                     "s3vector_chunking_vector_search_sql_match_and_underlying",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 ),
//                 SearchTestCase::new(
//                     "s3vector_chunking_view_vector_search_sql_match_and_underlying",
//                     SearchTestType::from_sql(
//                         "SELECT id, match, answer, trunc(score, 3) FROM vector_search(qs_view, 'second') order by score desc, id LIMIT 4",
//                     ),
//                 )]].concat(),
//             true
//         )
//         .await
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn metadata_columns() -> Result<(), anyhow::Error> {
//         let mut app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
//             "minishlab/potion-base-2M",
//             "hf_minilm",
//         ));
//         // Metadata columns: question, subject (filterable), answer
//         // Base columns:     reference_answer,source
//         let mut ds = get_mega_science_dataset(
//             Some("qs"),
//             None,
//             Some(
//                 Column::new("answer")
//                     .with_embedding(
//                         ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//                     )
//                     .with_metadata(
//                         [(
//                             "vectors".to_string(),
//                             serde_json::Value::String("non-filterable".to_string()),
//                         )]
//                         .into(),
//                     ),
//             ),
//         );
//         ds.columns.extend([
//             vectors_nonfilterable_col("question"),
//             vectors_filterable_col("subject"),
//         ]);

//         let vector_store =
//             init_vector_store("spice-ci-tests-s3-vectors-metadata-columns", true).await?;
//         ds.vectors = Some(vector_store);

//         app = add_mega_science_view_from_ds(app, &ds).await?;
//         app = app.with_dataset(ds);

//         let mut app = app.build();
//         if let Some(v) = app.views.iter_mut().find(|v| v.name == "qs_view") {
//             v.columns.extend([
//                 // `question` column already added in `add_mega_science_view_from_ds`.
//                 // vectors_nonfilterable_col("question"),
//                 vectors_filterable_col("subject"),
//             ]);
//         }

//         run_search_w_explain(
//             app,
//             [
//                 basic_vector_search_tests_on_table("s3vectors_metadata", "qs"),
//                 basic_vector_search_tests_on_table("s3vectors_metadata_view", "qs_view"),
//                 vec![
//                     SearchTestCase::new(
//                         "s3vector_metadata_additional_columns_metadata",
//                         SearchTestType::Http(json!({
//                             "text": "second",
//                             "limit": 4,
//                             "datasets": ["qs"],
//                             "additional_columns": ["reference_answer", "source"],
//                         })),
//                     ),
//                     SearchTestCase::new(
//                         "s3vector_metadata_view_additional_columns_metadata",
//                         SearchTestType::Http(json!({
//                             "text": "second",
//                             "limit": 4,
//                             "datasets": ["qs_view"],
//                             "additional_columns": ["reference_answer", "source"],
//                         })),
//                     ),
//                     SearchTestCase::new(
//                         "s3vector_metadata_with_where_metadata",
//                         SearchTestType::Http(json!({
//                             "text": "secondary",
//                             "datasets": ["qs"],
//                             "where": "subject!='math'",
//                             "limit": 4,
//                         })),
//                     ),
//                     SearchTestCase::new(
//                         "s3vector_metadata_view_with_where_metadata",
//                         SearchTestType::Http(json!({
//                             "text": "secondary",
//                             "datasets": ["qs_view"],
//                             "where": "subject!='math'",
//                             "limit": 4,
//                         })),
//                     ),
//                     SearchTestCase::new(
//                         "s3vector_metadata_vector_search_sql_projection_metadata",
//                         SearchTestType::from_sql(
//                             "SELECT id, answer, question, subject, trunc(score, 3) as score FROM vector_search(qs, 'second') order by score desc, id LIMIT 4",
//                         ),
//                     ),
//                     SearchTestCase::new(
//                         "s3vector_metadata_view_vector_search_sql_projection_metadata",
//                         SearchTestType::from_sql(
//                             "SELECT id, answer, question, subject, trunc(score, 3) as score FROM vector_search(qs_view, 'second') order by score desc, id LIMIT 4",
//                         ),
//                     ),
//                     SearchTestCase::new(
//                         "s3vector_metadata_vector_search_sql_filters_metadata",
//                         SearchTestType::from_sql(
//                             "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary') where subject!='math' order by score desc, id LIMIT 4",
//                         ),
//                     ),
//                     SearchTestCase::new(
//                         "s3vector_metadata_view_vector_search_sql_filters_metadata",
//                         SearchTestType::from_sql(
//                             "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs_view, 'secondary') where subject!='math' order by score desc, id LIMIT 4",
//                         ),
//                     ),
//                 ],
//             ]
//             .concat(),
//             true,
//         )
//         .await
//     }

//     #[tokio::test]
//     async fn s3_vectors_filters_pushdown() -> Result<(), anyhow::Error> {
//         let _tracing = crate::init_tracing(DEFAULT_TRACING_MODELS);

//         let bucket_name = "spice-ci-tests-s3-vectors-filters-pushdown";
//         let vector_store = init_vector_store(bucket_name, true).await?;

//         let mut test_dataset = get_package_delivery_dataset("data/", "delivery", None, "hf_minilm");
//         test_dataset.vectors = Some(vector_store);

//         let app = AppBuilder::new("search_app")
//             .with_dataset(test_dataset)
//             .with_embedding(get_huggingface_embeddings(
//                 "sentence-transformers/all-MiniLM-L6-v2",
//                 "hf_minilm",
//             ))
//             .build();

//         let rt = start_app(app).await?;

//         // Failed sms notifications on heavy deliveries sent to the wrong location"
//         run_and_snapshot_query(
//             &rt,
//             r#"
//             explain SELECT
//                 "message.body",
//                 attempt_count, "message.status",
//                 package_weight_kg,
//                 round(score, 1)
//             FROM vector_search(delivery, 'wrong location')
//             WHERE attempt_count > 1 AND package_weight_kg > 5.0 AND "message.status"='FAILED'
//             ORDER BY package_weight_kg desc, score DESC
//             LIMIT 10;
//             "#,
//             "filters_pushdown_explain",
//         )
//         .await?;

//         run_and_snapshot_query(
//             &rt,
//             r#"
//             SELECT
//               "message.body",
//               attempt_count, "message.status",
//               package_weight_kg,
//               round(score, 1)
//             FROM vector_search(delivery, 'wrong location')
//             WHERE attempt_count > 1 AND package_weight_kg > 5.0 AND "message.status"='FAILED'
//             ORDER BY package_weight_kg desc, score DESC
//             LIMIT 10;
//             "#,
//             "filters_pushdown",
//         )
//         .await?;

//         // WHERE clause on non-filterable column should not pushdown filter to S3vector.
//         run_and_snapshot_query(
//             &rt,
//             r#"
//             explain SELECT
//               "event.id",
//               round(score, 1)
//             FROM vector_search(delivery, 'wrong location')
//             WHERE "account.tier" = 'BUSINESS'
//             ORDER BY "event.id" desc, score DESC
//             LIMIT 10;
//             "#,
//             "non_filters_pushdown_explain",
//         )
//         .await?;

//         Ok(())
//     }
// }
async fn init_vector_store(
    bucket_name: &str,
    predelete_index: bool,
) -> Result<VectorStore, anyhow::Error> {
    init_vector_store_w_index_name(
        bucket_name,
        format!("test-index-{}", rand::random::<u8>() % 11).as_str(),
        predelete_index,
    )
    .await
}

async fn prepare_for_aws_tests(
    store: &VectorStore,
    predelete_index: bool,
) -> Result<(), anyhow::Error> {
    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let bucket_name = store
        .params
        .as_ref()
        .and_then(|p| p.as_string_map().get("s3_vectors_bucket").cloned())
        .unwrap_or_default();
    let index_name = store
        .params
        .as_ref()
        .and_then(|p| p.as_string_map().get("s3_vectors_index").cloned())
        .unwrap_or_default();

    if predelete_index {
        return delete_index(bucket_name.as_str(), index_name.as_str())
                .await
                .map_err(|e| {
                    tracing::warn!("failed to delete index {index_name} before test. This may just be because index does not exist. Error: {e}. ");
                    anyhow::anyhow!(e)
                });
    }
    Ok(())
}

pub(crate) async fn init_vector_store_w_index_name(
    bucket_name: &str,
    index_name: &str,
    predelete_index: bool,
) -> Result<VectorStore, anyhow::Error> {
    let params = spicepod::param::Params::from_string_map(
        vec![
            ("s3_vectors_aws_region".to_string(), "us-east-2".to_string()),
            ("s3_vectors_bucket".to_string(), bucket_name.to_string()),
            ("s3_vectors_index".to_string(), index_name.to_string()),
            (
                "s3_vectors_aws_access_key_id".to_string(),
                "${env:AWS_S3_VECTORS_KEY}".to_string(),
            ),
            (
                "s3_vectors_aws_secret_access_key".to_string(),
                "${env:AWS_S3_VECTORS_SECRET}".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
    );

    let store = VectorStore {
        enabled: true,
        engine: Some("s3_vectors".to_string()),
        params: Some(params),
        partition_by: vec![],
    };
    let () = prepare_for_aws_tests(&store, predelete_index).await?;
    Ok(store)
}

async fn start_app(app: App) -> Result<Arc<Runtime>, anyhow::Error> {
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(90)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;

    Ok(rt)
}

async fn run_and_snapshot_query(
    rt: &Runtime,
    query: &str,
    test_name: &str,
) -> Result<(), anyhow::Error> {
    let mut query_result = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let mut batches = vec![];
    while let Some(batch) = query_result.data.next().await {
        batches.push(batch?);
    }

    let formatted = arrow::util::pretty::pretty_format_batches(&batches)
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
    insta::assert_snapshot!(test_name, formatted);
    Ok(())
}

pub fn get_package_delivery_dataset(
    path: &str,
    ds_name: &str,
    refresh_sql: Option<&str>,
    embedding_model: &str,
) -> Dataset {
    let mut dataset = Dataset::new(
        format!("s3://spiceai-public-datasets/test_array_json/package-delivery/{path}"),
        ds_name.to_string(),
    );
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "json".to_string()),
            ("json_format".to_string(), "array".to_string()),
            ("flatten_json".to_string(), "true".to_string()),
            (
                "schema_source_path".to_string(),
                "s3://spiceai-public-datasets/test_array_json/package-delivery/data/01.json"
                    .to_string(),
            ),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_sql: Some(
            refresh_sql
                .unwrap_or(&format!("SELECT * FROM {ds_name}"))
                .to_string(),
        ),
        ..Default::default()
    });

    dataset.columns = vec![
        Column::new("message.body").with_embeddings(vec![ColumnLevelEmbeddingConfig {
            model: embedding_model.to_string(),
            chunking: None,
            row_ids: Some(vec!["event.id".to_string()]),
            vector_size: None,
        }]),
        vectors_filterable_col("message.status"),
        vectors_filterable_col("event.created"),
        vectors_nonfilterable_col("account.tier"),
        vectors_filterable_col("account.account_sid"),
        vectors_filterable_col("package_weight_kg"),
        vectors_filterable_col("attempt_count"),
    ];

    dataset
}

async fn delete_index(
    bucket_name: &str,
    index_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = aws_config::defaults(BehaviorVersion::v2025_08_07())
        .region(Region::from_static("us-east-2"))
        .credentials_provider(Credentials::new(
            std::env::var("AWS_S3_VECTORS_KEY").ok().unwrap_or_default(),
            std::env::var("AWS_S3_VECTORS_SECRET")
                .ok()
                .unwrap_or_default(),
            None,
            None,
            "S3Vectors",
        ))
        .load()
        .await;

    let s3_vector_client = Client::new(&config);
    s3_vector_client
        .delete_index()
        .set_index_name(Some(index_name.to_string()))
        .set_vector_bucket_name(Some(bucket_name.to_string()))
        .send()
        .await
        .boxed()?;

    Ok(())
}

fn vectors_filterable_col(col: impl Into<Column>) -> Column {
    col.into().with_metadata(
        [(
            "vectors".to_string(),
            serde_json::Value::String("filterable".to_string()),
        )]
        .into(),
    )
}
