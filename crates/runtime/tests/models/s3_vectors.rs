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
use snafu::ResultExt;
use spicepod::{
    acceleration::Acceleration,
    component::dataset::Dataset,
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig},
};

mod search {
    use crate::{
        configure_test_datafusion,
        models::{
            get_mega_science_dataset,
            hf::get_huggingface_embeddings,
            s3_vectors::{delete_index, vectors_filterable_col, vectors_nonfilterable_col},
            search::{SearchTestCase, SearchTestType, run_search_w_explain},
        },
        utils::verify_env_secret_exists,
    };

    use app::AppBuilder;
    use serde_json::json;
    use spicepod::{
        semantic::{Column, ColumnLevelEmbeddingConfig},
        vector::VectorStore,
    };
    use std::{collections::HashMap, sync::Arc};

    use app::App;
    use futures::StreamExt;
    use runtime::Runtime;

    use crate::DEFAULT_TRACING_MODELS;
    use crate::models::s3_vectors::get_package_delivery_dataset;
    use crate::utils::runtime_ready_check;

    #[tokio::test]
    async fn basic_functionality() -> Result<(), anyhow::Error> {
        let mut ds = get_mega_science_dataset(
            Some("qs"),
            None,
            Some(Column {
                name: "answer".to_string(),
                embeddings: vec![ColumnLevelEmbeddingConfig {
                    model: "hf_minilm".to_string(),
                    row_ids: Some(vec!["id".to_string()]),
                    chunking: None,
                    vector_size: None,
                }],
                description: None,
                full_text_search: None,
                metadata: HashMap::new(),
            }),
        );
        let bucket_name = "spice-ci-tests-s3-vectors-basic";
        let vector_store = init_vector_store(bucket_name, true).await?;
        ds.vectors = Some(vector_store);

        run_search_w_explain(
            AppBuilder::new("search_app")
                .with_embedding(get_huggingface_embeddings(
                    "sentence-transformers/all-MiniLM-L6-v2",
                    "hf_minilm",
                ))
                .with_dataset(ds)
                .build(),
            vec![
                SearchTestCase::new(
                    "s3vectors_basic_basic",
                    SearchTestType::Http(json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                    })),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_additional_columns",
                    SearchTestType::Http(json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                        "additional_columns": ["question"],
                    })),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_with_where",
                    SearchTestType::Http(json!({
                        "text": "secondary",
                        "datasets": ["qs"],
                        "where": "subject!='math'",
                        "limit": 4,
                    })),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_vector_search_sql_basic",
                    SearchTestType::Sql(
                        "SELECT id, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_vector_search_sql_projection",
                    SearchTestType::Sql(
                        "SELECT id, answer, question, subject, trunc(score, 3) as score FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_vector_search_sql_filters",
                    SearchTestType::Sql(
                        "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary') where subject!='math' order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_vector_search_sql_no_score",
                    SearchTestType::Sql(
                        "SELECT id, answer FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_vector_search_sql_random",
                    SearchTestType::Sql(
                        "SELECT subject FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vectors_basic_vector_search_sql_vectors",
                    SearchTestType::Sql(
                        "SELECT id, answer, array_length(answer_embedding), round(score, 1) FROM vector_search(qs, 'second') order by score desc LIMIT 4;",
                    ))
            ],
            true
        )
        .await
    }

    #[tokio::test]
    async fn multi_column_primary_key() -> Result<(), anyhow::Error> {
        let mut ds = get_mega_science_dataset(
            Some("qs"),
            None,
            Some(Column {
                name: "answer".to_string(),
                embeddings: vec![ColumnLevelEmbeddingConfig {
                    model: "hf_minilm".to_string(),
                    row_ids: Some(vec!["id".to_string(), "question".to_string()]),
                    chunking: None,
                    vector_size: None,
                }],
                description: None,
                full_text_search: None,
                metadata: HashMap::new(),
            }),
        );
        let bucket_name = "spice-ci-tests-s3-vectors-compose-pk";
        let vector_store = init_vector_store(bucket_name, true).await?;
        ds.vectors = Some(vector_store);

        run_search_w_explain(
            AppBuilder::new("search_app")
                .with_embedding(get_huggingface_embeddings(
                    "sentence-transformers/all-MiniLM-L6-v2",
                    "hf_minilm",
                ))
                .with_dataset(ds)
                .build(),
            vec![
                SearchTestCase::new(
                    "s3vector_composite_basic",
                    SearchTestType::Http(json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                    })),
                ),
                SearchTestCase::new(
                    "s3vector_composite_additional_columns",
                    SearchTestType::Http(json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                        "additional_columns": ["subject"],
                    })),
                ),
                SearchTestCase::new(
                    "s3vector_composite_with_where",
                    SearchTestType::Http(json!({
                        "text": "secondary",
                        "datasets": ["qs"],
                        "where": "subject!='math'",
                        "limit": 4,
                    })),
                ),
                SearchTestCase::new(
                    "s3vector_composite_vector_search_sql_single_column",
                    SearchTestType::Sql(
                        "SELECT id, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),

                SearchTestCase::new(
                    "s3vector_composite_vector_search_sql_composite_key",
                    SearchTestType::Sql(
                        "SELECT id, question, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vector_composite_vector_search_sql_filters",
                    SearchTestType::Sql(
                        "SELECT question, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary') where id> 10 order by score desc LIMIT 4",
                    ),
                ),
            ],
            true
        )
        .await
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn metadata_columns() -> Result<(), anyhow::Error> {
        // Metadata columns: question, subject (filterable), answer
        // Base columns:     reference_answer,source
        let mut ds = get_mega_science_dataset(
            Some("qs"),
            None,
            Some(Column {
                name: "answer".to_string(),
                embeddings: vec![ColumnLevelEmbeddingConfig {
                    model: "hf_minilm".to_string(),
                    row_ids: Some(vec!["id".to_string()]),
                    chunking: None,
                    vector_size: None,
                }],
                description: None,
                full_text_search: None,
                metadata: [(
                    "vectors".to_string(),
                    serde_json::Value::String("non-filterable".to_string()),
                )]
                .into(),
            }),
        );
        ds.columns.extend([
            vectors_nonfilterable_col("question"),
            vectors_filterable_col("subject"),
        ]);

        let bucket_name = "spice-ci-tests-s3-vectors-metadata-columns";
        let vector_store = init_vector_store(bucket_name, true).await?;
        ds.vectors = Some(vector_store);

        run_search_w_explain(
            AppBuilder::new("search_app")
                .with_embedding(get_huggingface_embeddings(
                    "sentence-transformers/all-MiniLM-L6-v2",
                    "hf_minilm",
                ))
                .with_dataset(ds)
                .build(),
            vec![
                SearchTestCase::new(
                    "s3vector_metadata_basic",
                    SearchTestType::Http(json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                    })),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_additional_columns_metadata",
                    SearchTestType::Http(json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                        "additional_columns": ["reference_answer", "source"],
                    })),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_with_where",
                    SearchTestType::Http(json!({
                        "text": "secondary",
                        "datasets": ["qs"],
                        "where": "source='textbook_reasoning'",
                        "limit": 4,
                    })),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_with_where_metadata",
                    SearchTestType::Http(json!({
                        "text": "secondary",
                        "datasets": ["qs"],
                        "where": "subject!='math'",
                        "limit": 4,
                    })),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_vector_search_sql_basic",
                    SearchTestType::Sql(
                        "SELECT id, answer, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_vector_search_sql_projection",
                    SearchTestType::Sql(
                        "SELECT id, answer, reference_answer, source, trunc(score, 3) as score FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_vector_search_sql_projection_metadata",
                    SearchTestType::Sql(
                        "SELECT id, answer, question, subject, trunc(score, 3) as score FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_vector_search_sql_filters_metadata",
                    SearchTestType::Sql(
                        "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary') where subject!='math' order by score desc LIMIT 4",
                    ),
                ),
                SearchTestCase::new(
                    "s3vector_metadata_vector_search_sql_filters",
                    SearchTestType::Sql(
                        "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary') where source='textbook_reasoning' order by score desc LIMIT 4",
                    ),
                ),
            ],
            true
        )
        .await
    }

    #[tokio::test]
    async fn s3_vectors_filters_pushdown() -> Result<(), anyhow::Error> {
        let _tracing = crate::init_tracing(DEFAULT_TRACING_MODELS);

        let bucket_name = "spice-ci-tests-s3-vectors-filters-pushdown";
        let vector_store = init_vector_store(bucket_name, true).await?;

        let mut test_dataset = get_package_delivery_dataset("data/", "delivery", None, "hf_minilm");
        test_dataset.vectors = Some(vector_store);

        let app = AppBuilder::new("search_app")
            .with_dataset(test_dataset)
            .with_embedding(get_huggingface_embeddings(
                "sentence-transformers/all-MiniLM-L6-v2",
                "hf_minilm",
            ))
            .build();

        let rt = start_app(app).await?;

        // Failed sms notifications on heavy deliveries sent to the wrong location"
        run_and_snapshot_query(
            &rt,
            r#"
            explain SELECT
                "message.body",
                attempt_count, "message.status",
                package_weight_kg,
                round(score, 1)
            FROM vector_search(delivery, 'wrong location')
            WHERE attempt_count > 1 AND package_weight_kg > 5.0 AND "message.status"='FAILED'
            ORDER BY package_weight_kg desc, score DESC
            LIMIT 10;
            "#,
            "filters_pushdown_explain",
        )
        .await?;

        run_and_snapshot_query(
            &rt,
            r#"
            SELECT
              "message.body",
              attempt_count, "message.status",
              package_weight_kg,
              round(score, 1)
            FROM vector_search(delivery, 'wrong location')
            WHERE attempt_count > 1 AND package_weight_kg > 5.0 AND "message.status"='FAILED'
            ORDER BY package_weight_kg desc, score DESC
            LIMIT 10;
            "#,
            "filters_pushdown",
        )
        .await?;

        // WHERE clause on non-filterable column should not pushdown filter to S3vector.
        run_and_snapshot_query(
            &rt,
            r#"
            explain SELECT
              "event.id",
              round(score, 1)
            FROM vector_search(delivery, 'wrong location')
            WHERE "account.tier" = 'BUSINESS'
            ORDER BY "event.id" desc, score DESC
            LIMIT 10;
            "#,
            "non_filters_pushdown_explain",
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn s3_vectors_data_update() -> Result<(), anyhow::Error> {
        let _tracing = crate::init_tracing(DEFAULT_TRACING_MODELS);

        // Generate a unique index name so the same test can be run in parallel
        let bucket_name = "spice-ci-tests-s3-vectors-overwrite";

        for (data_path, test_name) in [
            ("update/data_v1.json", "data_v1"),
            ("update/data_v2.json", "data_v2"),
        ] {
            let vector_store = init_vector_store(bucket_name, true).await?;

            let mut ds = get_package_delivery_dataset(data_path, "delivery", None, "hf_minilm");
            ds.vectors = Some(vector_store);

            let app = AppBuilder::new("search_app")
                .with_dataset(ds)
                .with_embedding(get_huggingface_embeddings(
                    "sentence-transformers/all-MiniLM-L6-v2",
                    "hf_minilm",
                ))
                .build();

            let rt = start_app(app).await?;

            run_and_snapshot_query(
                &rt,
                r#"SELECT "account.account_sid", "message.body", round(score, 1) as score, attempt_count, customer_note FROM vector_search(delivery, 'delivery issue') WHERE "event.id" = 'SM8856d9da23ab4a7c8b26'"#,
                test_name,
            )
            .await?;
        }

        Ok(())
    }

    async fn init_vector_store(
        bucket_name: &str,
        predelete_index: bool,
    ) -> Result<VectorStore, anyhow::Error> {
        for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
            verify_env_secret_exists(env_var)
                .await
                .map_err(anyhow::Error::msg)?;
        }

        let index_name = format!("test-index-{}", rand::random::<u8>() % 11);
        if predelete_index {
            let _ = delete_index(bucket_name, index_name.as_str())
                .await
                .inspect_err(|e| {
                    tracing::warn!("failed to delete index {index_name} before test. This may just be because index does not exist. Error: {e}. ");
                });
        }

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

        Ok(VectorStore {
            enabled: true,
            engine: Some("s3_vectors".to_string()),
            params: Some(params),
        })
    }

    async fn start_app(app: App) -> Result<Arc<Runtime>, anyhow::Error> {
        let rt = Arc::new(
            Runtime::builder()
                .with_app(app)
                .with_datafusion_configuration_fn(configure_test_datafusion)
                .build()
                .await,
        );

        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
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
    let config = aws_config::defaults(BehaviorVersion::v2025_01_17())
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

fn vectors_filterable_col(name: &str) -> Column {
    Column::new(name).with_metadata(
        [(
            "vectors".to_string(),
            serde_json::Value::String("filterable".to_string()),
        )]
        .into(),
    )
}

fn vectors_nonfilterable_col(name: &str) -> Column {
    Column::new(name).with_metadata(
        [(
            "vectors".to_string(),
            serde_json::Value::String("non-filterable".to_string()),
        )]
        .into(),
    )
}
