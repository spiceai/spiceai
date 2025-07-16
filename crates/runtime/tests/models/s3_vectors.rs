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

mod search {
    use crate::{
        models::{
            bedrock::embeddings::create_titan_v2_embedding, search::item_tpch_dataset_w_embeddings,
        },
        utils::verify_env_secret_exists,
    };
    use app::AppBuilder;
    use spicepod::vector::VectorStore;
    use std::sync::Arc;

    use app::App;
    use futures::StreamExt;
    use runtime::Runtime;

    use crate::utils::runtime_ready_check;

    // S3 Vectors search test is based on the Bedrock embeddings
    #[cfg(feature = "bedrock")]
    #[tokio::test]
    async fn s3_vectors_basic() -> Result<(), anyhow::Error> {
        use crate::DEFAULT_TRACING_MODELS;

        for env_var in [
            "AWS_BEDROCK_KEY",
            "AWS_BEDROCK_SECRET",
            "AWS_S3_VECTORS_KEY",
            "AWS_S3_VECTORS_SECRET",
        ] {
            verify_env_secret_exists(env_var)
                .await
                .map_err(anyhow::Error::msg)?;
        }

        let _tracing = crate::init_tracing(DEFAULT_TRACING_MODELS);

        // created model name is `titan-v2`
        let titan_embeddings = create_titan_v2_embedding();
        let mut test_dataset = item_tpch_dataset_w_embeddings(
            "item",
            "titan-v2",
            Some(vec!["i_item_sk".to_string()]),
            None,
        );

        // Generate a unique index name for each test run
        let index_name = format!("test-index-{}", rand::random::<u8>() % 11);

        test_dataset.vectors = Some(new_s3_vector_store(&index_name));

        let app = AppBuilder::new("search_app")
            .with_dataset(test_dataset)
            .with_embedding(titan_embeddings)
            .build();

        let rt = start_app(app).await?;

        run_and_snapshot_query(
            &rt,
            "SELECT i_item_sk, i_item_desc, round(score, 2) FROM vector_search(item, 'Patient') where i_item_sk > 5 order by score desc LIMIT 4;",
            "basic",
        )
        .await?;

        run_and_snapshot_query(
            &rt,
            "explain SELECT i_item_sk, i_item_desc, round(score, 2) FROM vector_search(item, 'Patient') where i_item_sk > 5 order by score desc LIMIT 4;",
            "basic_explain",
        )
        .await?;

        Ok(())
    }

    /// Creates a new S3 `VectorStore`.
    fn new_s3_vector_store(index_name: &str) -> VectorStore {
        let params = spicepod::param::Params::from_string_map(
            vec![
                ("s3_vectors_aws_region".to_string(), "us-east-2".to_string()),
                (
                    "s3_vectors_bucket".to_string(),
                    "spice-ci-s3-vectors".to_string(),
                ),
                ("s3_vectors_index".to_string(), index_name.to_string()),
                (
                    "s3_vectors_aws_access_key_id".to_string(),
                    "${env:AWS_S3_VECTORS_KEY}".to_string(),
                ),
                (
                    "s3_vectors_aws_secret_access_key".to_string(),
                    "${env:AWS_S3_VECTORS_SECRET}".to_string(),
                ),
                // Providing an endpoint for the S3 Vectors service is unnecessary for tests,
                // this is used to verify that the endpoint parameter is supported / can be defined
                (
                    "s3_vectors_endpoint".to_string(),
                    "s3vectors.us-east-2.api.aws".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        );

        VectorStore {
            enabled: true,
            engine: Some("s3_vectors".to_string()),
            params: Some(params),
        }
    }

    async fn start_app(app: App) -> Result<Arc<Runtime>, anyhow::Error> {
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

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
