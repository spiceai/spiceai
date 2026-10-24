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

//! Wiring tests: a full [`DataFusionBuilder`] session errors on `BIGINT` overflow.

use std::sync::Arc;

use crate::dataaccelerator::AcceleratorEngineRegistry;
use crate::datafusion::builder::DataFusionBuilder;
use crate::status;
use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

fn overflow_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("big", DataType::Int64, true),
            Field::new("i", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MAX),
                Some(i64::MAX - 1),
                Some(1),
                Some(2),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MAX),
                Some(i32::MAX - 1),
                Some(1),
                Some(2),
            ])),
        ],
    )
    .expect("overflow batch")
}

async fn query_error(ctx: &datafusion::prelude::SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(df) => match df.collect().await {
            Err(error) => error.to_string(),
            Ok(batches) => panic!("expected overflow for `{sql}`, got {batches:?}"),
        },
    }
}

#[tokio::test]
#[cfg(not(windows))]
async fn built_session_errors_on_bigint_sum_and_add() {
    let df = DataFusionBuilder::new(
        status::RuntimeStatus::new(),
        Arc::new(AcceleratorEngineRegistry::default()),
        tokio::runtime::Handle::current(),
    )
    .build();

    df.ctx
        .register_batch("t", overflow_batch())
        .expect("register overflow table");

    let sum = query_error(&df.ctx, "SELECT SUM(big) FROM t").await;
    assert!(
        sum.to_ascii_lowercase().contains("overflow"),
        "built session SUM(BIGINT) must overflow, got {sum}"
    );

    let distinct = query_error(&df.ctx, "SELECT SUM(DISTINCT big) FROM t").await;
    assert!(
        distinct.to_ascii_lowercase().contains("overflow"),
        "built session SUM(DISTINCT BIGINT) must overflow, got {distinct}"
    );

    let add = query_error(&df.ctx, "SELECT big + 1 FROM t WHERE id = 1").await;
    assert!(
        add.to_ascii_lowercase().contains("overflow"),
        "built session BIGINT + 1 must overflow, got {add}"
    );

    let safe = df
        .ctx
        .sql("SELECT SUM(big) FROM t WHERE id IN (3, 4)")
        .await
        .expect("plan safe SUM")
        .collect()
        .await
        .expect("run safe SUM");
    let value =
        arrow::util::display::array_value_to_string(safe[0].column(0), 0).expect("render safe SUM");
    assert_eq!(value, "3", "non-overflowing SUM(BIGINT) must stay 3");

    let safe_distinct = df
        .ctx
        .sql("SELECT SUM(DISTINCT big) FROM t WHERE id IN (3, 4)")
        .await
        .expect("plan safe SUM(DISTINCT)")
        .collect()
        .await
        .expect("run safe SUM(DISTINCT)");
    let distinct_value = arrow::util::display::array_value_to_string(safe_distinct[0].column(0), 0)
        .expect("render safe SUM(DISTINCT)");
    assert_eq!(
        distinct_value, "3",
        "non-overflowing SUM(DISTINCT BIGINT) must stay 3"
    );
}
