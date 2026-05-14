/*
Copyright 2026 The Spice.ai OSS Authors

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

use arrow::{
    array::{Array, ArrayRef, Decimal128Array, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};

#[tokio::test]
async fn chbench_q19_empty_join_does_not_error() -> datafusion::error::Result<()> {
    let order_line_schema = Arc::new(Schema::new(vec![
        Field::new("ol_i_id", DataType::Int32, false),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Decimal128(38, 10), false),
        Field::new("ol_w_id", DataType::Int32, false),
    ]));
    let order_line_batch = RecordBatch::try_new(
        Arc::clone(&order_line_schema),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(Decimal128Array::from(Vec::<i128>::new()).with_precision_and_scale(38, 10)?)
                as ArrayRef,
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
        ],
    )?;

    let item_schema = Arc::new(Schema::new(vec![
        Field::new("i_id", DataType::Int32, false),
        Field::new("i_data", DataType::Utf8, false),
        Field::new("i_price", DataType::Decimal128(38, 10), false),
    ]));
    let item_batch = RecordBatch::try_new(
        Arc::clone(&item_schema),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            Arc::new(Decimal128Array::from(Vec::<i128>::new()).with_precision_and_scale(38, 10)?)
                as ArrayRef,
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "order_line",
        Arc::new(MemTable::try_new(
            order_line_schema,
            vec![vec![order_line_batch]],
        )?),
    )?;
    ctx.register_table(
        "item",
        Arc::new(MemTable::try_new(item_schema, vec![vec![item_batch]])?),
    )?;

    let result = ctx
        .sql(
            r#"select
    sum(ol_amount) as revenue
from
    order_line, item
where
    (
        ol_i_id = i_id
        and i_data like '%a'
        and ol_quantity >= 1
        and ol_quantity <= 10
        and i_price between 1 and 400000
        and ol_w_id in (1,2,3)
    ) or (
        ol_i_id = i_id
        and i_data like '%b'
        and ol_quantity >= 1
        and ol_quantity <= 10
        and i_price between 1 and 400000
        and ol_w_id in (1,2,4)
    ) or (
        ol_i_id = i_id
        and i_data like '%c'
        and ol_quantity >= 1
        and ol_quantity <= 10
        and i_price between 1 and 400000
        and ol_w_id in (1,5,3)
    )"#,
        )
        .await?
        .collect()
        .await?;

    let total_rows: usize = result.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 1);
    assert_eq!(result[0].num_columns(), 1);
    assert!(result[0].column(0).is_null(0));
    Ok(())
}
