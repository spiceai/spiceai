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
pub use runtime_search::embeddings::common;
pub mod connector;
pub use runtime_search::embeddings::execution_plan;

pub mod index;
pub mod metrics;
pub mod task;
pub mod udtf;

pub use runtime_search::embeddings::{EmbeddingModelStore, construct_chunker};

/// Integration tests for RRF + `vector_search` that require the full
/// `VectorSearchTableFunc` and a properly-wired `DataFusion` instance.
/// These tests were intentionally placed here (rather than in `runtime-search`)
/// because `VectorSearchTableFunc` lives in `crates/runtime` and
/// `runtime-search` must not depend on `runtime` to avoid a circular
/// dependency.
#[cfg(test)]
#[cfg(feature = "models")]
mod rrf_vector_search_tests {
    use std::collections::HashMap;
    use std::process::ExitCode;
    use std::sync::{Arc, LazyLock, Weak};

    use arrow::array::as_string_array;
    use datafusion::catalog::TableProvider;
    use datafusion::common::Result;
    use datafusion::common::cast::{as_float64_array, as_uint64_array};
    use datafusion::functions_window::expr_fn::row_number;
    use datafusion::logical_expr::col;
    use datafusion::prelude::{DataFrame, named_struct, now, to_unixtime};
    use datafusion_expr::ExprFunctionExt;
    use datafusion_expr::lit;
    use tokio::sync::RwLock;

    use llms::model2vec::Model2Vec;
    use runtime_datafusion_udfs::embed;
    use runtime_request_context::{Protocol, RequestContext};
    use runtime_search::embeddings::table::EmbeddingTable;
    use runtime_search::embeddings::EmbeddingModelStore;
    use runtime_search::rrf::{RRF_UDF_NAME, RRF_FUSED_SCORE_COLUMN_NAME, ReciprocalRankFusion};
    use runtime_search::udtf::{
        EmbeddingColumnConfig, EmbeddingInputMode, VECTOR_SEARCH_UDTF_NAME,
    };

    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use crate::datafusion::DataFusion;
    use crate::embeddings::udtf::VectorSearchTableFunc;
    use crate::{datafusion::udf::register_core_scalar_udfs, status};

    static TEST_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> =
        LazyLock::new(|| Arc::new(RequestContext::builder(Protocol::Internal).build()));

    /// Shared embedding model — a single `LazyLock` prevents concurrent
    /// `Model2Vec::from_params` calls from contending on the HuggingFace
    /// file lock when tests run in parallel.
    static TEST_EMBEDDING_MODEL: LazyLock<Arc<Model2Vec>> = LazyLock::new(|| {
        Arc::new(
            Model2Vec::from_params(
                "minishlab/potion-base-2M",
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("Must make embedding model"),
        )
    });

    macro_rules! extract_column {
        ($batches:expr, $column_name:expr, $array_cast_fn:ident, $nth:expr) => {
            $array_cast_fn(
                $batches[$nth]
                    .column_by_name($column_name)
                    .expect(format!("Must have {}", $column_name).as_str()),
            )
        };
        ($batches:expr, $column_name:expr, $array_cast_fn:ident) => {
            extract_column!($batches, $column_name, $array_cast_fn, 0)
        };
    }

    macro_rules! test_query {
        ($ctx:expr, $query:expr) => {{
            let df = $ctx.sql($query).await?;
            df.collect().await?
        }};
    }

    /// Build a minimal `DataFusion` instance with embed, vector_search, and
    /// RRF UDFs registered — enough to run end-to-end `vector_search` +
    /// `rrf(...)` SQL queries against in-memory `EmbeddingTable`s.
    async fn make_test_session() -> Result<(
        Arc<DataFusion>,
        Arc<RwLock<EmbeddingModelStore>>,
    )> {
        let df = Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                tokio::runtime::Handle::current(),
            )
            .build(),
        );

        let ctx = &df.ctx;
        ctx.state()
            .config_mut()
            .set_extension(Arc::clone(&TEST_REQUEST_CONTEXT));

        let embedding_models: Arc<RwLock<EmbeddingModelStore>> =
            Arc::new(RwLock::new(HashMap::new()));
        let embedding_model: Arc<dyn llms::embeddings::Embed> =
            Arc::clone(&TEST_EMBEDDING_MODEL) as _;
        embedding_models
            .write()
            .await
            .insert("test_model".to_string(), embedding_model);

        // Core scalar UDFs (cosine_distance, l2_distance, etc. — needed by
        // vector_search's fallback JIT path)
        register_core_scalar_udfs(ctx);

        // embed UDF
        ctx.register_udf(embed::Embed::new(Arc::clone(&embedding_models)).into());

        // vector_search UDF + UDTF — requires the valid `Weak<DataFusion>`
        let weak_df = Arc::downgrade(&df);
        ctx.register_udf(
            VectorSearchTableFunc::new(Weak::clone(&weak_df), HashMap::new()).into(),
        );
        ctx.register_udtf(
            VECTOR_SEARCH_UDTF_NAME,
            Arc::new(VectorSearchTableFunc::new(weak_df, HashMap::new())),
        );

        // RRF UDF + UDTF
        ctx.register_udf(ReciprocalRankFusion::from_ctx(ctx).into());
        ctx.register_udtf(
            RRF_UDF_NAME,
            Arc::new(ReciprocalRankFusion::from_ctx(ctx)),
        );

        Ok((df, embedding_models))
    }

    /// Wrap a `DataFrame` in an `EmbeddingTable` that treats `"content"` as
    /// the embedded column backed by `"test_model"`.
    fn df_as_embedding_table(
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
        df: DataFrame,
    ) -> Arc<dyn TableProvider> {
        let mut embedded_columns = HashMap::new();
        embedded_columns.insert(
            "content".to_string(),
            EmbeddingColumnConfig {
                model_name: "test_model".to_string(),
                vector_size: 64,
                in_base_table: true,
                chunker: None,
                input_mode: EmbeddingInputMode::Scalar,
            },
        );

        Arc::new(EmbeddingTable {
            base_table: df.into_view(),
            embedded_columns,
            embedding_models,
        }) as Arc<dyn TableProvider>
    }

    async fn make_fruit_dataframe(df: &Arc<DataFusion>) -> Result<DataFrame> {
        let ctx = &df.ctx;
        let frame = ctx
            .sql(
                "SELECT
              unnest([
                  'banana yellow curved fruit',
                  'orange citrus round juicy',
                  'apple fruit sweet red crispy'
              ]) as content",
            )
            .await?;

        let rowid_expr = row_number()
            .order_by(vec![col("content").sort(false, false)])
            .build()?
            .alias("id");

        let embed_expr = frame.parse_sql_expr("embed(content, 'test_model')")?;

        frame
            .window(vec![rowid_expr])?
            .with_column("content_embedding", embed_expr)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recency_scoring() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df)
            .await?
            .with_column("picked_at", now())?
            .with_column(
                "picked_at",
                to_unixtime(vec![col("picked_at")]) - (lit(43200) * col("id")),
            )?;

        let picked_at_expr = fruit_df.parse_sql_expr("to_timestamp(cast(picked_at as bigint))")?;

        let fruit_df = fruit_df
            .with_column("picked_at", picked_at_expr)?
            .sort(vec![col("picked_at").sort(false, false)])?;

        let fruit_embedding_table =
            df_as_embedding_table(Arc::clone(&embedding_models), fruit_df.clone());

        df.ctx.register_table("foo", fruit_embedding_table)?;

        // decay_constant is made more aggressive to further deprioritize old results.
        let results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(foo, 'red crispy'), vector_search(foo, 'fruit'), time_column => 'picked_at', decay_constant => 0.1)"
        );

        let content = extract_column!(results, "content", as_string_array);

        let fruit_df_batches = fruit_df.collect().await?;
        let fruit_df_recent = extract_column!(fruit_df_batches, "content", as_string_array);

        assert_eq!(content.value(0), fruit_df_recent.value(0));

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Temporarily disabled due DataFusion order-property planning instability; covered by integration regression test test_rrf_recency_unboosting_disjoint_regression"]
    async fn test_recency_unboosting_disjoint() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df)
            .await?
            .with_column("picked_at", now())?
            .with_column(
                "picked_at",
                to_unixtime(vec![col("picked_at")]) - (lit(86400) * col("id")),
            )?;

        let picked_at_expr = fruit_df.parse_sql_expr("to_timestamp(cast(picked_at as bigint))")?;

        // Rows ordered picked_at DESC
        let fruit_df = fruit_df
            .with_column("picked_at", picked_at_expr)?
            .sort(vec![col("picked_at").sort(false, false)])?;

        // left_fruit: id (2, 3) with (now() - 1 day, now() - 2 day) respectively
        let left_fruit = df_as_embedding_table(
            Arc::clone(&embedding_models),
            fruit_df.clone().limit(1, Some(2))?,
        );
        // right_fruit: id (1) with timestamp 1970-01-01
        let right_fruit = df_as_embedding_table(
            Arc::clone(&embedding_models),
            fruit_df.clone().limit(0, Some(1))?.with_column(
                "picked_at",
                fruit_df.parse_sql_expr("to_timestamp(cast(0 as timestamp))")?,
            )?,
        );

        df.ctx.register_table("left_fruit", left_fruit)?;
        df.ctx.register_table("right_fruit", right_fruit)?;

        let results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(left_fruit, 'red crispy'), vector_search(right_fruit, 'red crispy'), k => 0, time_column => 'picked_at', decay_constant => 0.25)"
        );

        assert_ne!(extract_column!(results, "id", as_uint64_array)?.value(0), 1);

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rank_weighting() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df).await?;
        let fruit_embedding_table = df_as_embedding_table(Arc::clone(&embedding_models), fruit_df);

        df.ctx.register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(foo, 'yellow', rank_weight => 100), vector_search(foo, 'red', rank_weight => 10))"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "banana yellow curved fruit"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_queries() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df).await?;
        let fruit_embedding_table = df_as_embedding_table(Arc::clone(&embedding_models), fruit_df);

        df.ctx.register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), join_key => 'id', k => 600.0)"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "apple fruit sweet red crispy"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_queries_auto_hash_and_special_idents() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df)
            .await?
            .with_column("meta_a", named_struct(vec![lit("k1"), lit("v1")]))?
            .with_column("meta_b.special", named_struct(vec![lit("k2"), lit(133.7)]))?;
        let fruit_embedding_table = df_as_embedding_table(Arc::clone(&embedding_models), fruit_df);

        df.ctx.register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), k => 600.0)"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "apple fruit sweet red crispy"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_with_case_sensitive_columns() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df).await?.select(vec![
            col("id").alias("Id"),
            col("content"),
            col("content_embedding"),
            now().alias("pIckEd_AT"),
        ])?;

        let fruit_embedding_table = df_as_embedding_table(Arc::clone(&embedding_models), fruit_df);

        df.ctx.register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), join_key => 'Id', k => 600.0, time_column => 'pIckEd_AT')"
        );

        assert_eq!(
            extract_column!(results, "content", as_string_array).value(0),
            "apple fruit sweet red crispy"
        );

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_with_dupes() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df).await?;
        let fruit_df = fruit_df.clone().union(fruit_df)?;
        let fruit_embedding_table = df_as_embedding_table(Arc::clone(&embedding_models), fruit_df);

        df.ctx.register_table("foo", fruit_embedding_table)?;

        let results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), join_key => 'id', k => 600.0)"
        );

        // There are only 3 unique rows for (id)
        assert_eq!(results[0].num_rows(), 3);

        Ok(ExitCode::SUCCESS)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_score_computation() -> Result<ExitCode> {
        let (df, embedding_models) = make_test_session().await?;

        let fruit_df = make_fruit_dataframe(&df)
            .await?
            .with_column("timestamp", now())?;
        let fruit_table =
            df_as_embedding_table(Arc::clone(&embedding_models), fruit_df.clone());

        let no_fruit_df = fruit_df.clone().limit(0, Some(0)).expect("Must have fruit DF");
        let no_fruit_table = df_as_embedding_table(Arc::clone(&embedding_models), no_fruit_df);

        df.ctx.register_table("foo", fruit_table)?;
        df.ctx.register_table("bar", no_fruit_table)?;

        let query_empty_red_results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(bar, 'empty'), vector_search(foo, 'red')) order by _fused_score desc"
        );
        let query_empty_red_score = extract_column!(
            query_empty_red_results,
            RRF_FUSED_SCORE_COLUMN_NAME,
            as_float64_array
        )?
        .value(0);

        let query_red_empty_results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(foo, 'red'), vector_search(bar, 'empty'))"
        );
        let query_red_empty_score = extract_column!(
            query_red_empty_results,
            RRF_FUSED_SCORE_COLUMN_NAME,
            as_float64_array
        )?
        .value(0);

        // Score must be consistent regardless of argument order
        let score_diff = (query_red_empty_score - query_empty_red_score).abs();
        assert!(score_diff < 0.0001f64);

        // If timestamp column is missing due to FULL OUTER JOIN, a score must still be output
        let query_empty_red_recency_results = test_query!(
            df.ctx,
            "select * from rrf(vector_search(bar, 'empty'), vector_search(foo, 'red'), time_column => 'timestamp')"
        );
        let query_empty_red_recency_scores = extract_column!(
            query_empty_red_recency_results,
            RRF_FUSED_SCORE_COLUMN_NAME,
            as_float64_array
        )?;

        assert!(
            query_empty_red_recency_scores
                .into_iter()
                .all(|f| f.is_some())
        );

        Ok(ExitCode::SUCCESS)
    }
}
