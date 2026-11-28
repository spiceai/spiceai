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

//! Two Cayenne catalogs in one process, same table name, different data
//! directories, must not serve each other's rows. Regression for in-process
//! cluster executors and a single process with more than one Cayenne instance.

use std::sync::Arc;

use crate::common::{self, TestFixture};
use arrow::array::{Array, Int64Array};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const TABLE: &str = "orders";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn vortex_config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        ..VortexConfig::default()
    }
}

fn create_options(base_path: String) -> CreateTableOptions {
    CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(),
        primary_key: vec![],
        on_conflict: None,
        base_path,
        partition_column: None,
        vortex_config: vortex_config(),
    }
}

fn ids_batch(ids: &[i64]) -> arrow::record_batch::RecordBatch {
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

async fn insert_ids(provider: &CayenneTableProvider, ids: &[i64]) -> TestResult {
    common::insert_batch(provider, ids_batch(ids)).await?;
    Ok(())
}

async fn scanned_ids(provider: Arc<CayenneTableProvider>) -> TestResult<Vec<i64>> {
    let ctx = SessionContext::new();
    ctx.register_table(TABLE, provider as Arc<dyn TableProvider>)?;
    let batches = ctx
        .sql(&format!("SELECT id FROM {TABLE} ORDER BY id"))
        .await?
        .collect()
        .await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64")
                .iter()
                .map(|v| v.expect("id is not null"))
        })
        .collect())
}

async fn table_on(
    fixture: &TestFixture,
    runtime_env: Arc<RuntimeEnv>,
) -> TestResult<Arc<CayenneTableProvider>> {
    Ok(Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&fixture.catalog), runtime_env)
            .create(create_options(
                fixture.data_path.to_string_lossy().into_owned(),
            ))
            .await?,
    ))
}

/// Two catalogs, two data directories, one shared `RuntimeEnv` (the shape a
/// process-global object-store / footer cache would see).
#[tokio::test]
async fn two_catalogs_same_table_name_keep_separate_rows() -> TestResult {
    let left = TestFixture::new(common::BackendType::Sqlite).await?;
    let right = TestFixture::new(common::BackendType::Sqlite).await?;
    let runtime_env = Arc::new(RuntimeEnv::default());

    let left_table = table_on(&left, Arc::clone(&runtime_env)).await?;
    let right_table = table_on(&right, runtime_env).await?;

    insert_ids(left_table.as_ref(), &[1, 2, 3]).await?;
    insert_ids(right_table.as_ref(), &[10, 20]).await?;

    assert_eq!(
        scanned_ids(Arc::clone(&left_table)).await?,
        vec![1, 2, 3],
        "left catalog must not serve the right catalog's rows"
    );
    assert_eq!(
        scanned_ids(Arc::clone(&right_table)).await?,
        vec![10, 20],
        "right catalog must not serve the left catalog's rows"
    );
    Ok(())
}

/// Same as [`two_catalogs_same_table_name_keep_separate_rows`], with a
/// `RuntimeEnv` of its own per table, so a pass on the shared-env case is not
/// hiding a per-env cache keyed only by table name.
#[tokio::test]
async fn two_catalogs_same_table_name_separate_runtime_envs_keep_separate_rows() -> TestResult {
    let left = TestFixture::new(common::BackendType::Sqlite).await?;
    let right = TestFixture::new(common::BackendType::Sqlite).await?;

    let left_table = table_on(&left, Arc::new(RuntimeEnv::default())).await?;
    let right_table = table_on(&right, Arc::new(RuntimeEnv::default())).await?;

    insert_ids(left_table.as_ref(), &[1, 2, 3]).await?;
    insert_ids(right_table.as_ref(), &[10, 20]).await?;

    assert_eq!(scanned_ids(Arc::clone(&left_table)).await?, vec![1, 2, 3]);
    assert_eq!(scanned_ids(Arc::clone(&right_table)).await?, vec![10, 20]);
    Ok(())
}
