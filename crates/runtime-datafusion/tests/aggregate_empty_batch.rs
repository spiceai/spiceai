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
    array::{ArrayRef, Int64Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};

#[tokio::test]
async fn clickbench_q19_group_by_empty_batch() -> datafusion::error::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("UserID", DataType::UInt64, false),
        Field::new("EventTime", DataType::Int64, false),
        Field::new("SearchPhrase", DataType::Utf8, false),
    ]));
    let empty_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(Vec::<u64>::new())) as ArrayRef,
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        ],
    )?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "hits",
        Arc::new(MemTable::try_new(schema, vec![vec![empty_batch]])?),
    )?;

    let result = ctx
        .sql(
            r#"SELECT "UserID", extract(minute FROM to_timestamp("EventTime")::timestamp) AS m, "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10"#,
        )
        .await?
        .collect()
        .await?;

    assert!(result.iter().all(|batch| batch.num_rows() == 0));
    Ok(())
}
