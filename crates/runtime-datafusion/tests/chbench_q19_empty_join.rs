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
    array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};

#[tokio::test]
async fn chbench_q19_empty_join_does_not_error() -> datafusion::error::Result<()> {
    let lineitem_schema = Arc::new(Schema::new(vec![
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Int64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
    ]));
    let lineitem_batch = RecordBatch::try_new(
        Arc::clone(&lineitem_schema),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
            Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        ],
    )?;

    let part_schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
    ]));
    let part_batch = RecordBatch::try_new(
        Arc::clone(&part_schema),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "lineitem",
        Arc::new(MemTable::try_new(
            lineitem_schema,
            vec![vec![lineitem_batch]],
        )?),
    )?;
    ctx.register_table(
        "part",
        Arc::new(MemTable::try_new(part_schema, vec![vec![part_batch]])?),
    )?;

    let result = ctx
        .sql(
            r#"select
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 10 and l_quantity <= 10 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 20 and l_quantity <= 20 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
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
