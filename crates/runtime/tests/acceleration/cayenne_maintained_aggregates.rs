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

//! Integration / e2e coverage for Cayenne maintained aggregates (`min`/`max`/
//! `sum`/`count`) through the real write path, including N>1 mem-tier CDC delete
//! retraction (the lag-lever + IVM co-existence path) and spicepod runtime wiring.

#![cfg(not(target_os = "windows"))]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::Int64Type;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateSpec,
};
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::optimizer_rules::CayenneMaintainedAggregateRewriter;
use cayenne::{
    CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    SlotAdvancer,
};
use datafusion::assert_batches_eq;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

use crate::utils::test_request_context;

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn min_max_sum_by_customer_spec() -> MaintainedAggregateSpec {
    MaintainedAggregateSpec {
        group_by: vec!["customer_id".to_string()],
        aggregates: vec![
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Min,
                column: Some("amount".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Max,
                column: Some("amount".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("amount".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            },
        ],
        filter: None,
    }
}

fn batch(ids: &[i64], customers: &[i64], amounts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(customers.to_vec())),
            Arc::new(Int64Array::from(amounts.to_vec())),
        ],
    )
    .expect("build orders batch")
}

/// Arms the in-memory CDC tier without advancing a real source slot.
struct NoopSlotAdvancer;

#[async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

/// SessionContext with the production maintained-aggregate physical rewrite.
fn cayenne_ctx() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(CayenneMaintainedAggregateRewriter::new()))
        .build();
    SessionContext::new_with_state(state)
}

async fn create_provider(
    table_name: &str,
    shards: usize,
) -> (Arc<CayenneTableProvider>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_string_lossy());
    let data_dir = format!("{}/data", temp_dir.path().to_string_lossy());
    tokio::fs::create_dir_all(&metadata_dir)
        .await
        .expect("metadata dir");

    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: orders_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: cayenne::metadata::CdcDurability::Memory,
            cdc_mem_tier_shards: shards,
            cdc_mem_tier_min_flush_bytes: 0,
            deletion_mode: DeletionMode::Key,
            inline_max_rows: 1024,
            ..VortexConfig::default()
        },
    };

    let ctx = SessionContext::new();
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .with_maintained_aggregates(vec![min_max_sum_by_customer_spec()])
        .create(options)
        .await
        .expect("create table");
    provider.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    assert!(
        provider.is_cdc_memory_mode(),
        "test requires memory-mode CDC tier"
    );
    assert!(
        provider.supports_in_memory_cdc_deletes(),
        "upsert + memory mode must absorb deletes"
    );
    (Arc::new(provider), temp_dir)
}

fn single_batch_stream(batch: RecordBatch) -> datafusion::physical_plan::SendableRecordBatchStream {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

async fn append_via_cdc(
    provider: &CayenneTableProvider,
    ctx: &SessionContext,
    ids: &[i64],
    customers: &[i64],
    amounts: &[i64],
) {
    let b = batch(ids, customers, amounts);
    let write = provider
        .write_cdc_append_stream(single_batch_stream(b), &ctx.task_ctx())
        .await
        .expect("cdc append");
    assert!(
        write.in_memory_epoch().is_some(),
        "write must land in the in-memory CDC tier"
    );
}

const AGG_SQL: &str = "SELECT customer_id, MIN(amount) AS min_a, MAX(amount) AS max_a, \
     SUM(amount) AS sum_a, COUNT(*) AS cnt \
     FROM orders GROUP BY customer_id ORDER BY customer_id";

/// Poll until the maintained rewrite serves a non-empty plan matching `predicate`.
async fn poll_maintained_query(
    ctx: &SessionContext,
    predicate: impl Fn(&[RecordBatch]) -> bool,
) -> Vec<RecordBatch> {
    for _ in 0..50 {
        let plan = ctx
            .sql(AGG_SQL)
            .await
            .expect("plan query")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let plan_str = displayable(plan.as_ref()).indent(false).to_string();
        if plan_str.contains("MaintainedAggregateExec") {
            let batches = ctx
                .sql(AGG_SQL)
                .await
                .expect("query")
                .collect()
                .await
                .expect("collect");
            if predicate(&batches) {
                return batches;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("MaintainedAggregateExec never served expected results within ~5s");
}

/// End-to-end: insert → query (min/max/sum/count via maintained plan) → upsert →
/// delete (N=1) → query matches ground truth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_cayenne_maintained_min_max_upsert_delete() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (provider, _tmp) = create_provider("orders", 1).await;
            let ctx = cayenne_ctx();
            ctx.register_table("orders", Arc::clone(&provider) as _)?;

            // customer 1: amounts 10, 30 → min 10 max 30 sum 40 cnt 2
            // customer 2: amount 20 → min 20 max 20 sum 20 cnt 1
            append_via_cdc(&provider, &ctx, &[1, 2, 3], &[1, 1, 2], &[10, 30, 20]).await;

            let batches =
                poll_maintained_query(&ctx, |batches| batches.iter().any(|b| b.num_rows() == 2))
                    .await;
            let expected = [
                "+-------------+-------+-------+-------+-----+",
                "| customer_id | min_a | max_a | sum_a | cnt |",
                "+-------------+-------+-------+-------+-----+",
                "| 1           | 10    | 30    | 40    | 2   |",
                "| 2           | 20    | 20    | 20    | 1   |",
                "+-------------+-------+-------+-------+-----+",
            ];
            assert_batches_eq!(expected, &batches);

            // Upsert id=1: amount 10 → 5 (new min for customer 1).
            append_via_cdc(&provider, &ctx, &[1], &[1], &[5]).await;
            let batches = poll_maintained_query(&ctx, |batches| {
                batches.first().is_some_and(|b| {
                    b.num_rows() == 2 && b.column(1).as_primitive::<Int64Type>().value(0) == 5
                })
            })
            .await;
            let expected = [
                "+-------------+-------+-------+-------+-----+",
                "| customer_id | min_a | max_a | sum_a | cnt |",
                "+-------------+-------+-------+-------+-----+",
                "| 1           | 5     | 30    | 35    | 2   |",
                "| 2           | 20    | 20    | 20    | 1   |",
                "+-------------+-------+-------+-------+-----+",
            ];
            assert_batches_eq!(expected, &batches);

            // Delete id=3 (customer 2 only row) → customer 2 group disappears.
            let pk_only = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![3_i64]))],
            )?;
            provider
                .write_cdc_delete_keys_in_memory(&pk_only)
                .await?
                .expect("delete absorbed");

            let batches = poll_maintained_query(&ctx, |batches| {
                batches.first().is_some_and(|b| b.num_rows() == 1)
            })
            .await;
            let expected = [
                "+-------------+-------+-------+-------+-----+",
                "| customer_id | min_a | max_a | sum_a | cnt |",
                "+-------------+-------+-------+-------+-----+",
                "| 1           | 5     | 30    | 35    | 2   |",
                "+-------------+-------+-------+-------+-----+",
            ];
            assert_batches_eq!(expected, &batches);

            Ok(())
        })
        .await
}

/// E2E: same min/max lifecycle under N>1 sharded mem-tier CDC (shards=4), the
/// path that combines the lag lever with IVM retract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_cayenne_maintained_min_max_n_gt_1_delete() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (provider, _tmp) = create_provider("orders", 4).await;
            let ctx = cayenne_ctx();
            ctx.register_table("orders", Arc::clone(&provider) as _)?;

            // 16 keys across customers 1..=4 so they fan out across shards.
            let ids: Vec<i64> = (1..=16).collect();
            let customers: Vec<i64> = ids.iter().map(|i| ((i - 1) % 4) + 1).collect();
            let amounts: Vec<i64> = ids.iter().map(|i| i * 10).collect();
            append_via_cdc(&provider, &ctx, &ids, &customers, &amounts).await;

            let _ = poll_maintained_query(&ctx, |batches| {
                batches.first().is_some_and(|b| b.num_rows() == 4)
            })
            .await;

            // Delete customer 1's current extrema (ids 1 and 13). Its surviving
            // values 50 and 90 must become the next MIN/MAX rather than dropping
            // the group or leaving either deleted extremum behind.
            let delete_ids = [1_i64, 13];
            let pk_only = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(delete_ids.to_vec()))],
            )?;
            provider
                .write_cdc_delete_keys_in_memory(&pk_only)
                .await?
                .expect("N>1 delete absorbed");

            let n_gt_1_sql = "SELECT customer_id, MIN(amount) AS min_a, MAX(amount) AS max_a, \
                 SUM(amount) AS sum_a, COUNT(*) AS cnt \
                 FROM orders GROUP BY customer_id ORDER BY customer_id";
            let mut after = None;
            for _ in 0..50 {
                let plan = ctx.sql(n_gt_1_sql).await?.create_physical_plan().await?;
                let plan_str = displayable(plan.as_ref()).indent(false).to_string();
                if plan_str.contains("MaintainedAggregateExec") {
                    let batches = ctx.sql(n_gt_1_sql).await?.collect().await?;
                    if let Some(b) = batches.first() {
                        let cids = b.column(0).as_primitive::<Int64Type>();
                        let mins = b.column(1).as_primitive::<Int64Type>();
                        let maxs = b.column(2).as_primitive::<Int64Type>();
                        if b.num_rows() == 4
                            && (0..b.num_rows()).any(|r| {
                                cids.value(r) == 1 && mins.value(r) == 50 && maxs.value(r) == 90
                            })
                        {
                            after = Some(batches);
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let batches = after
                .expect("N>1 delete must expose customer 1's next maintained extrema within ~5s");
            let expected = [
                "+-------------+-------+-------+-------+-----+",
                "| customer_id | min_a | max_a | sum_a | cnt |",
                "+-------------+-------+-------+-------+-----+",
                "| 1           | 50    | 90    | 140   | 2   |",
                "| 2           | 20    | 140   | 320   | 4   |",
                "| 3           | 30    | 150   | 360   | 4   |",
                "| 4           | 40    | 160   | 400   | 4   |",
                "+-------------+-------+-------+-------+-----+",
            ];
            assert_batches_eq!(expected, &batches);

            Ok(())
        })
        .await
}

/// Runtime wiring: spicepod `acceleration.maintained_aggregates` with min/max
/// reaches the Cayenne accelerator (list form enables maintenance).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_cayenne_spicepod_min_max_wiring() -> Result<(), anyhow::Error> {
    use app::AppBuilder;
    use runtime::Runtime;
    use spicepod::acceleration::{
        Acceleration, MaintainedAggregate, MaintainedAggregateExpr, MaintainedAggregateFunction,
        Mode, RefreshMode,
    };
    use spicepod::component::dataset::Dataset;

    use crate::utils::runtime_ready_check;

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let csv = temp_dir.path().join("orders.csv");
            tokio::fs::write(&csv, "id,customer_id,amount\n1,1,10\n2,1,30\n3,2,20\n").await?;

            crate::configure_test_datafusion();

            let mut dataset = Dataset::new(format!("file://{}", csv.display()), "orders_ivm");
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                primary_key: Some("id".into()),
                maintained_aggregates: vec![MaintainedAggregate {
                    group_by: vec!["customer_id".to_string()],
                    aggregates: vec![
                        MaintainedAggregateExpr {
                            function: MaintainedAggregateFunction::Min,
                            column: Some("amount".to_string()),
                        },
                        MaintainedAggregateExpr {
                            function: MaintainedAggregateFunction::Max,
                            column: Some("amount".to_string()),
                        },
                        MaintainedAggregateExpr {
                            function: MaintainedAggregateFunction::Sum,
                            column: Some("amount".to_string()),
                        },
                        MaintainedAggregateExpr {
                            function: MaintainedAggregateFunction::Count,
                            column: None,
                        },
                    ],
                    filter_sql: None,
                }]
                .into(),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_ivm_min_max")
                .with_dataset(dataset)
                .build();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(1)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            // After full refresh rebuild, the maintained view should answer.
            let mut ok = false;
            for _ in 0..50 {
                let result = rt
                    .datafusion()
                    .query_builder(
                        "SELECT customer_id, MIN(amount) AS min_a, MAX(amount) AS max_a, \
                         SUM(amount) AS sum_a, COUNT(*) AS cnt \
                         FROM orders_ivm GROUP BY customer_id ORDER BY customer_id",
                    )
                    .build()
                    .run()
                    .await;
                if let Ok(q) = result {
                    use futures::TryStreamExt;
                    if let Ok(batches) = q.data.try_collect::<Vec<_>>().await
                        && batches.first().is_some_and(|b| b.num_rows() == 2)
                    {
                        let expected = [
                            "+-------------+-------+-------+-------+-----+",
                            "| customer_id | min_a | max_a | sum_a | cnt |",
                            "+-------------+-------+-------+-------+-----+",
                            "| 1           | 10    | 30    | 40    | 2   |",
                            "| 2           | 20    | 20    | 20    | 1   |",
                            "+-------------+-------+-------+-------+-----+",
                        ];
                        assert_batches_eq!(expected, &batches);
                        ok = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                ok,
                "runtime-wired min/max maintained aggregates should serve after full refresh"
            );

            Ok(())
        })
        .await
}
