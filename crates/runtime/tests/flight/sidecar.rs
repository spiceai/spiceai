/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Real Flight round trips through the configured sidecar connector. The input
//! table and direct local evaluation are the round-trip oracle. Process outage
//! and concurrent-load coverage belongs to the external sidecar scenario.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    util::pretty::pretty_format_batches,
};
use data_components::flight::FlightFactory;
use datafusion::{
    common::TableReference, prelude::SessionContext, sql::unparser::dialect::DefaultDialect,
};
use flight_client::{Credentials, FlightClient};
use futures::TryStreamExt;
use runtime::{Runtime, datafusion::query::QueryBuilder};
use spicepod::{
    component::{
        dataset::Dataset,
        runtime::{Runtime as RuntimeConfig, RuntimeMode},
    },
    param::Params,
};

use super::start_spice_test_app_with_metrics_port;
use crate::utils::test_request_context;

async fn rows(runtime: &Arc<Runtime>, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    Ok(QueryBuilder::new(sql, runtime.datafusion())
        .build()
        .run()
        .await?
        .into_record_batch_stream()
        .try_collect()
        .await?)
}

#[tokio::test]
async fn sidecar_substrait_connector_roundtrip() -> anyhow::Result<()> {
    test_request_context().scope(async {
        let (_channel, upstream, metrics_port) =
            start_spice_test_app_with_metrics_port(None, None, None, None).await?;
        let endpoint = format!("http://127.0.0.1:{}", metrics_port - 1);
        let input = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as arrow::array::ArrayRef),
            ("value", Arc::new(Int64Array::from(vec![Some(11), None, Some(-7), Some(0)]))),
            ("label", Arc::new(StringArray::from(vec![Some("雪"), Some(""), None, Some("x")]))),
        ])?;
        upstream.ctx.register_batch("remote_orders", input.clone())?;
        let oracle = SessionContext::new();
        oracle.register_batch("orders", input.clone())?;
        oracle.register_batch("other_orders", input.clone())?;

        let mut app = AppBuilder::new("sidecar_roundtrip").with_runtime(RuntimeConfig {
            mode: RuntimeMode::Sidecar,
            ..Default::default()
        });
        for (prefix, name) in [("spice.ai", "orders"), ("spiceai", "other_orders")] {
            let mut dataset = Dataset::new(format!("{prefix}:remote_orders"), name.to_string());
            dataset.params = Some(Params::from_string_map(HashMap::from([
                ("spiceai_endpoint".to_string(), endpoint.clone()),
            ])));
            app = app.with_dataset(dataset);
        }
        let sidecar = Arc::new(Runtime::builder().with_app(app.build()).build().await);
        tokio::time::timeout(Duration::from_secs(30), Arc::clone(&sidecar).load_components()).await?;
        assert_eq!(sidecar.datafusion().ctx.state().config().target_partitions(), 1);

        let local_function = datafusion::logical_expr::create_udf(
            "sidecar_roundtrip_identity",
            vec![arrow::datatypes::DataType::Int64],
            arrow::datatypes::DataType::Int64,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(|arguments| Ok(arguments[0].clone())),
        );
        runtime_udfs_api::add_user_function("sidecar_roundtrip_identity");
        sidecar.datafusion().ctx.register_udf(local_function.clone());
        oracle.register_udf(local_function);

        let corpus = [
            "SELECT id, value, label FROM orders ORDER BY id",
            "SELECT id, label FROM other_orders WHERE value IS NULL ORDER BY id",
            "SELECT id FROM orders WHERE value < 0 OR value = 0 ORDER BY id",
            "SELECT COUNT(*) AS n, COUNT(value) AS nn, SUM(value) AS total FROM orders",
            "SELECT label, SUM(value) AS total FROM orders GROUP BY label ORDER BY label NULLS FIRST",
            "SELECT l.id, l.label, r.value FROM orders l JOIN other_orders r ON l.id = r.id WHERE l.id > 1 ORDER BY l.id",
            "SELECT id FROM orders ORDER BY id DESC LIMIT 2 OFFSET 1",
            "SELECT id FROM orders WHERE id = 999",
            "SELECT CASE WHEN value IS NULL THEN 99 ELSE value END AS v FROM orders ORDER BY id",
            "SELECT arrow_cast(label, 'Utf8View') AS label FROM orders ORDER BY id",
            "SELECT sidecar_roundtrip_identity(value) AS value FROM orders ORDER BY id",
        ];
        for sql in corpus {
            let expected_frame = oracle.sql(sql).await?;
            let result = QueryBuilder::new(sql, sidecar.datafusion()).build().run().await?;
            assert_eq!(result.data.schema().as_ref(), expected_frame.schema().as_arrow(), "{sql}");
            let expected = expected_frame.collect().await?;
            let actual: Vec<RecordBatch> = result.into_record_batch_stream().try_collect().await?;
            assert_eq!(pretty_format_batches(&actual)?.to_string(),
                pretty_format_batches(&expected)?.to_string(), "{sql}");
            if let (Some(actual), Some(expected)) = (actual.first(), expected.first()) {
                assert_eq!(actual.schema(), expected.schema(), "{sql}");
            }
        }
        let explained = rows(&sidecar,
            "EXPLAIN SELECT l.id FROM orders l JOIN other_orders r ON l.id = r.id").await?;
        let explain = pretty_format_batches(&explained)?.to_string();
        assert!(explain.contains("SubstraitExec"), "{explain}");
        assert!(!explain.contains("FlightExec sql="), "{explain}");

        // An ordinary DataFusion session has no federation analyzer. Its scan
        // must still send a Substrait plan rather than taking the SQL path.
        let client = FlightClient::try_new(endpoint.into(), Credentials::anonymous(), None, None).await?;
        #[expect(clippy::disallowed_methods, reason = "the test connects to a real Spice runtime")]
        let factory = FlightFactory::new("spice.ai", client, Arc::new(DefaultDialect {})).with_substrait();
        let provider = factory.table_provider(TableReference::bare("remote_orders")).await
            .map_err(anyhow::Error::from_boxed)?;
        let fallback = SessionContext::new();
        fallback.register_table("orders", provider)?;
        let actual = fallback.sql("SELECT id FROM orders WHERE value IS NULL ORDER BY id").await?.collect().await?;
        let expected = oracle.sql("SELECT id FROM orders WHERE value IS NULL ORDER BY id").await?.collect().await?;
        assert_eq!(pretty_format_batches(&actual)?.to_string(), pretty_format_batches(&expected)?.to_string());

        sidecar.shutdown().await;
        runtime_udfs_api::remove_user_function("sidecar_roundtrip_identity");
        Ok(())
    }).await
}
