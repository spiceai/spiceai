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

use std::time::Duration;

use app::AppBuilder;
use datafusion::assert_batches_eq;
use futures::StreamExt;
use futures::TryStreamExt;
use runtime::{datafusion::query, Runtime};
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params,
};

use crate::{
    init_tracing,
    utils::{test_request_context, wait_until_true},
};

#[cfg(feature = "postgres")]
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn acceleration_with_and_without_federation() -> Result<(), anyhow::Error> {
    use crate::get_test_datafusion;
    use crate::postgres::common;
    use arrow::array::RecordBatch;
    use datafusion::error::DataFusionError;
    use runtime::datafusion::error::SpiceExternalError;
    use runtime::status;
    use std::sync::Arc;

    let _guard = super::ACCELERATION_MUTEX.lock().await;
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let port: usize = 20962;
            let running_container = common::start_postgres_docker_container(port).await?;

            let pool = common::get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");
            db_conn
                .conn
                .execute(
                    "
        CREATE TABLE test (
            id UUID PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );",
                    &[],
                )
                .await
                .expect("table is created");
            db_conn
                .conn
                .execute(
                    "INSERT INTO test (id, created_at) VALUES ('5ea5a3ac-07a0-4d4d-b201-faff68d8356c', '2023-05-02 10:30:00-04:00');",
                    &[],
                )
                .await.expect("inserted data");

            let mut federated_acc = Dataset::new("postgres:test", "abc");
            let mut params = Params::from_string_map(
                vec![
                    ("pg_host".to_string(), "localhost".to_string()),
                    ("pg_port".to_string(), port.to_string()),
                    ("pg_user".to_string(), "postgres".to_string()),
                    ("pg_pass".to_string(), common::PG_PASSWORD.to_string()),
                    ("pg_sslmode".to_string(), "disable".to_string()),
                ]
                .into_iter()
                .collect(),
            );
            federated_acc.params = Some(params.clone());
            params.data.insert(
                "disable_query_push_down".to_string(),
                spicepod::component::params::ParamValue::Bool(false),
            );

            federated_acc.acceleration = Some(Acceleration {
                params: Some(params),
                enabled: true,
                engine: Some("postgres".to_string()),
                ..Acceleration::default()
            });

            let mut non_federated_acc = Dataset::new("postgres:test", "non_federated_abc");
            let mut non_federated_params = Params::from_string_map(
                vec![
                    ("pg_host".to_string(), "localhost".to_string()),
                    ("pg_port".to_string(), port.to_string()),
                    ("pg_user".to_string(), "postgres".to_string()),
                    ("pg_pass".to_string(), common::PG_PASSWORD.to_string()),
                    ("pg_sslmode".to_string(), "disable".to_string()),
                ]
                .into_iter()
                .collect(),
            );
            non_federated_acc.params = Some(non_federated_params.clone());
            non_federated_params.data.insert(
                "disable_query_push_down".to_string(),
                spicepod::component::params::ParamValue::Bool(true),
            );

            non_federated_acc.acceleration = Some(Acceleration {
                params: Some(non_federated_params),
                enabled: true,
                engine: Some("postgres".to_string()),
                ..Acceleration::default()
            });

            let status = status::RuntimeStatus::new();
            let df = get_test_datafusion(Arc::clone(&status));

            let app = AppBuilder::new("acceleration_federation")
                .with_dataset(federated_acc)
                .with_dataset(non_federated_acc)
                .build();

            let rt = Runtime::builder()
                .with_app(app)
                .with_datafusion(df)
                .with_runtime_status(status)
                .build()
                .await;

            // Set a timeout for the test
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = rt.load_components() => {}
            }

            assert!(
                wait_until_true(Duration::from_secs(30), || async {
                    let mut query_result = rt
                        .datafusion()
                        .query_builder("SELECT * FROM abc LIMIT 1")
                        .build()
                        .run()
                        .await
                        .expect("result returned");
                    let mut batches = vec![];
                    while let Some(batch) = query_result.data.next().await {
                        batches.push(batch.expect("batch"));
                    }
                    !batches.is_empty() && batches[0].num_rows() == 1
                })
                .await,
                "Expected 1 rows returned"
            );
            assert!(
                wait_until_true(Duration::from_secs(30), || async {
                    let mut query_result = match rt
                        .datafusion()
                        .query_builder("SELECT * FROM non_federated_abc LIMIT 1")
                        .build()
                        .run()
                        .await
                    {
                        Ok(result) => result,
                        Err(e) => match e {
                            query::Error::UnableToExecuteQuery { source } => match source {
                                DataFusionError::External(e) => {
                                    if let Some(e) = e.downcast_ref::<SpiceExternalError>() {
                                        match e {
                                            SpiceExternalError::AccelerationNotReady { .. } => {
                                                return false;
                                            }
                                        }
                                    }
                                    panic!("Expected SpiceExternalError");
                                }
                                _ => panic!("Expected SpiceExternalError"),
                            },
                            _ => panic!("Expected query::Error::UnableToExecuteQuery"),
                        },
                    };
                    let mut batches = vec![];
                    while let Some(batch) = query_result.data.next().await {
                        batches.push(batch.expect("batch"));
                    }
                    !batches.is_empty() && batches[0].num_rows() == 1
                })
                .await,
                "Expected 1 rows returned"
            );

            let plan_results: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT COUNT(1) FROM abc")
                .build()
                .run()
                .await
                .expect("sql working")
                .data
                .try_collect()
                .await
                .expect("collect working");

            let expected_plan = [
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                         |",
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | BytesProcessedNode                                                                                                                                                           |",
                "|               |   Federated                                                                                                                                                                  |",
                "|               |  Projection: count(Int64(1))                                                                                                                                                 |",
                "|               |   Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]                                                                                                                          |",
                "|               |     TableScan: abc projection=[]                                                                                                                                             |",
                "| physical_plan | BytesProcessedExec                                                                                                                                                           |",
                "|               |   SchemaCastScanExec                                                                                                                                                         |",
                "|               |     VirtualExecutionPlan name=postgres compute_context=host=Tcp(\"localhost\"),port=20962,user=postgres, sql=SELECT count(1) FROM abc rewritten_sql=SELECT count(1) FROM \"abc\" |",
                "|               |                                                                                                                                                                              |",
                "+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ];
            assert_batches_eq!(expected_plan, &plan_results);

            let _results: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("SELECT COUNT(1) FROM non_federated_abc")
                .build()
                .run()
                .await
                .expect("sql working")
                .data
                .try_collect()
                .await
                .expect("collect working");

            let plan_results: Vec<RecordBatch> = rt
                .datafusion()
                .query_builder("EXPLAIN SELECT COUNT(1) FROM non_federated_abc")
                .build()
                .run()
                .await
                .expect("sql working")
                .data
                .try_collect()
                .await
                .expect("collect working");

            let expected_plan = [
                "+---------------+--------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                           |",
                "+---------------+--------------------------------------------------------------------------------+",
                "| logical_plan  | Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]                              |",
                "|               |   BytesProcessedNode                                                           |",
                "|               |     TableScan: non_federated_abc projection=[]                                 |",
                "| physical_plan | AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]                      |",
                "|               |   CoalescePartitionsExec                                                       |",
                "|               |     AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]                |",
                "|               |       BytesProcessedExec                                                       |",
                "|               |         SchemaCastScanExec                                                     |",
                "|               |           RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1 |",
                "|               |             SqlExec sql=SELECT \"id\", \"created_at\" FROM non_federated_abc       |",
                "|               |                                                                                |",
                "+---------------+--------------------------------------------------------------------------------+",
            ];
            assert_batches_eq!(expected_plan, &plan_results);

            running_container.remove().await?;
            Ok(())
        }).await
}
