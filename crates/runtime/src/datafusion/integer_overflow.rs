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

    let cancel = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MAX),
                Some(i64::MAX),
                Some(-i64::MAX),
            ])),
        ],
    )
    .expect("cancel batch");
    df.ctx
        .register_batch("t_cancel", cancel)
        .expect("register cancel table");
    let mixed = df
        .ctx
        .sql("SELECT SUM(v), COUNT(DISTINCT k) FROM t_cancel")
        .await
        .expect("plan mixed SUM + COUNT(DISTINCT)")
        .collect()
        .await
        .expect("run mixed SUM + COUNT(DISTINCT)");
    let mixed_sum = arrow::util::display::array_value_to_string(mixed[0].column(0), 0)
        .expect("render mixed SUM");
    let mixed_count = arrow::util::display::array_value_to_string(mixed[0].column(1), 0)
        .expect("render mixed COUNT");
    assert_eq!(
        (mixed_sum.as_str(), mixed_count.as_str()),
        (i64::MAX.to_string().as_str(), "2"),
        "built session SUM + COUNT(DISTINCT) must keep the representable total"
    );
}

/// Codex round 4: `EagerAggregation` is on by default in `DataFusionBuilder`.
/// Join-key groups `[MAX, MAX]` and `[-MAX]` that land in one `GROUP BY`
/// must still return `MAX` with the rule on and off, and a true overflow
/// must still error. `Int32` `SUM` on the same stats still pushes (control).
#[tokio::test]
#[cfg(not(windows))]
async fn built_session_eager_aggregation_keeps_representable_integer_sum() {
    use arrow::array::StringArray;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::joins::HashJoinExec;

    fn aggregates_below_a_join(
        plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        below_a_join: bool,
    ) -> bool {
        use datafusion::physical_plan::aggregates::AggregateExec;
        if below_a_join && plan.is::<AggregateExec>() {
            return true;
        }
        let below_a_join = below_a_join || plan.is::<HashJoinExec>();
        plan.children()
            .into_iter()
            .any(|child| aggregates_below_a_join(child, below_a_join))
    }

    async fn plan_join_sum(
        eager: bool,
        amount: &[i64],
    ) -> (bool, String, std::result::Result<String, String>) {
        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            tokio::runtime::Handle::current(),
        )
        .eager_aggregation(Some(eager))
        .build();

        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("fk", DataType::Int32, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("n", DataType::Int32, false),
        ]));
        let fact = RecordBatch::try_new(
            Arc::clone(&fact_schema),
            vec![
                Arc::new(Int32Array::from_iter_values((1..=10_000).map(|i| i % 100))),
                Arc::new(Int64Array::from(amount.to_vec())),
                Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(1, 10_000))),
            ],
        )
        .expect("fact batch");
        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("dk", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let dim = RecordBatch::try_new(
            Arc::clone(&dim_schema),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(StringArray::from_iter_values((0..100).map(|k| {
                    if k == 1 || k == 2 {
                        "g".to_string()
                    } else {
                        format!("n{k}")
                    }
                }))),
            ],
        )
        .expect("dim batch");
        df.ctx.register_batch("fact", fact).expect("register fact");
        df.ctx.register_batch("dim", dim).expect("register dim");

        let sql = "SELECT d.name, SUM(f.amount) AS total FROM fact f JOIN dim d ON f.fk = d.dk \
                   WHERE f.fk IN (1, 2) GROUP BY d.name";
        let query = df.ctx.sql(sql).await.expect("plan");
        let plan = query
            .clone()
            .create_physical_plan()
            .await
            .expect("physical plan");
        let rendered = displayable(plan.as_ref()).indent(true).to_string();
        let pushed = aggregates_below_a_join(&plan, false);
        let rows = match query.collect().await {
            Ok(batches) => Ok(arrow::util::pretty::pretty_format_batches(&batches)
                .expect("format")
                .to_string()),
            Err(error) => Err(error.to_string()),
        };
        (pushed, rendered, rows)
    }

    let cancel_amounts: Vec<i64> = (1..=10_000)
        .map(|i| {
            let fk = i % 100;
            let nth = (i - 1) / 100;
            match (fk, nth) {
                (1, 0 | 1) => i64::MAX,
                (2, 0) => -i64::MAX,
                _ => 0,
            }
        })
        .collect();
    let overflow_amounts: Vec<i64> = (1..=10_000)
        .map(|i| {
            let fk = i % 100;
            let nth = (i - 1) / 100;
            match (fk, nth) {
                (1 | 2, 0 | 1) => i64::MAX,
                _ => 0,
            }
        })
        .collect();

    let (on_pushed, on_plan, on_rows) = plan_join_sum(true, &cancel_amounts).await;
    assert!(
        !on_pushed,
        "built session: checked integer SUM must not pre-aggregate below the join\n{on_plan}"
    );
    let on_rows = on_rows.unwrap_or_else(|error| {
        panic!("built session eager on must keep MAX, got {error}\n{on_plan}")
    });
    assert!(
        on_rows.contains(&i64::MAX.to_string()),
        "built session eager on must return MAX\n{on_rows}\n{on_plan}"
    );

    let (eager_off_pushed, eager_off_plan, eager_off_rows) =
        plan_join_sum(false, &cancel_amounts).await;
    assert!(
        !eager_off_pushed,
        "built session eager off must not pre-aggregate below the join\n{eager_off_plan}"
    );
    let eager_off_rows = eager_off_rows.unwrap_or_else(|error| {
        panic!("built session eager off must keep MAX, got {error}\n{eager_off_plan}")
    });
    assert_eq!(
        on_rows, eager_off_rows,
        "built session eager on and off must return the same rows"
    );

    let df = DataFusionBuilder::new(
        status::RuntimeStatus::new(),
        Arc::new(AcceleratorEngineRegistry::default()),
        tokio::runtime::Handle::current(),
    )
    .eager_aggregation(Some(true))
    .build();
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("fk", DataType::Int32, false),
        Field::new("n", DataType::Int32, false),
    ]));
    let fact = RecordBatch::try_new(
        Arc::clone(&fact_schema),
        vec![
            Arc::new(Int32Array::from_iter_values((1..=10_000).map(|i| i % 100))),
            Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(1, 10_000))),
        ],
    )
    .expect("int32 fact");
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("dk", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let dim = RecordBatch::try_new(
        Arc::clone(&dim_schema),
        vec![
            Arc::new(Int32Array::from_iter_values(0..100)),
            Arc::new(StringArray::from_iter_values((0..100).map(|k| {
                if k == 1 || k == 2 {
                    "g".to_string()
                } else {
                    format!("n{k}")
                }
            }))),
        ],
    )
    .expect("int32 dim");
    df.ctx
        .register_batch("fact", fact)
        .expect("register int32 fact");
    df.ctx
        .register_batch("dim", dim)
        .expect("register int32 dim");
    let query = df
        .ctx
        .sql(
            "SELECT d.name, SUM(f.n) AS total FROM fact f JOIN dim d ON f.fk = d.dk \
             WHERE f.fk IN (1, 2) GROUP BY d.name",
        )
        .await
        .expect("plan int32 SUM");
    let plan = query
        .clone()
        .create_physical_plan()
        .await
        .expect("int32 physical plan");
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        aggregates_below_a_join(&plan, false),
        "Int32 SUM must still push below the join so the cost gate is shown to fire:\n{rendered}"
    );
    let rows =
        arrow::util::pretty::pretty_format_batches(&query.collect().await.expect("run int32 SUM"))
            .expect("format int32")
            .to_string();
    assert!(
        rows.contains("200"),
        "Int32 SUM over 200 ones must be 200\n{rows}"
    );

    let (_overflow_pushed, overflow_plan, overflow_rows) =
        plan_join_sum(true, &overflow_amounts).await;
    let overflow_error = overflow_rows.expect_err("true overflow must error");
    assert!(
        overflow_error.to_ascii_lowercase().contains("overflow"),
        "built session true overflow must error, got {overflow_error}\n{overflow_plan}"
    );
}
