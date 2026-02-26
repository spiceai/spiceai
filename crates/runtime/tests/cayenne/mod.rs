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
use std::time::Duration;

use crate::configure_test_datafusion;
use crate::utils::runtime_ready_check_with_timeout;
use crate::{
    RecordBatch, init_tracing,
    utils::{register_test_connectors, test_request_context},
};
use app::AppBuilder;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use spicepod::partitioning::PartitionedBy;
use test_framework::queries::QuerySet;

fn make_s3_tpch_dataset(name: &str, partition_by: Option<String>) -> Dataset {
    let mut dataset = Dataset::new(
        format!("s3://spiceai-demo-datasets/tpch/{name}/"),
        name.to_string(),
    );
    dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "parquet".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        refresh_sql: None,
        ..Acceleration::default()
    });

    if let Some(partition_by) = partition_by
        && let Some(accel) = dataset.acceleration.as_mut()
    {
        accel.partition_by = vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: partition_by,
        }];
    }

    dataset
}

#[tokio::test]
async fn test_cayenne_with_partitioned_tpch() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            // exclude lineitem, orders and customer to reduce egress
            let app = AppBuilder::new("test_cayenne_with_partitioned_tpch")
                .with_dataset(make_s3_tpch_dataset(
                    "nation",
                    Some("n_regionkey".to_string()),
                ))
                .with_dataset(make_s3_tpch_dataset("region", None))
                .with_dataset(make_s3_tpch_dataset(
                    "supplier",
                    Some("bucket(10, s_suppkey)".to_string()),
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "part",
                    Some("bucket(10, p_partkey)".to_string()),
                ))
                .with_dataset(make_s3_tpch_dataset(
                    "partsupp",
                    Some("bucket(10, ps_partkey)".to_string()),
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned_rt = Arc::new(rt.clone());

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err("Timed out waiting for datasets to load".to_string());
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check_with_timeout(&rt, Duration::from_secs(300)).await;

            let queries = QuerySet::Tpch
                .get_queries(None, None, None)
                .await
                .expect("to get queries");

            let queries = vec![
                queries.get(1).expect("TPCH q2 missing"),
                queries.get(10).expect("TPCH q11 missing"),
                queries.get(14).expect("TPCH q16 missing"),
            ];

            for query in queries {
                let query_result = rt
                    .datafusion()
                    .query_builder(&format!("EXPLAIN {}", query.sql))
                    .build()
                    .run()
                    .await
                    .expect("should run query");
                query_result
                    .data
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .expect("should collect batches");
            }

            Ok(())
        })
        .await
}
