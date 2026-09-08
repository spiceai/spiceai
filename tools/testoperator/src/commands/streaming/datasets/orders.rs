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

//! Orders dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Orders dataset (TPCH).
///
/// Generates orders data using `DuckDB`'s TPCH extension.
/// Contains 1,500,000 rows at SF=1.
pub struct OrdersDataset;

impl OrdersDataset {
    /// Get the Arrow schema for the orders table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Utf8, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int64, false),
            Field::new("o_comment", DataType::Utf8, false),
        ])
    }
}

impl StreamingDataset for OrdersDataset {
    fn table_name(&self) -> &'static str {
        "orders"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Orders
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Generating TPCH orders data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        conn.execute_batch("INSTALL tpch; LOAD tpch;")
            .context("Failed to load TPCH extension")?;

        conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
            .context("Failed to generate TPCH data")?;

        let mut stmt = conn
            .prepare(
                "SELECT o_orderkey, o_custkey, o_orderstatus,
                        CAST(o_totalprice AS DOUBLE) as o_totalprice,
                        o_orderdate::VARCHAR, o_orderpriority, o_clerk, o_shippriority, o_comment
                FROM orders
                ORDER BY o_orderkey",
            )
            .context("Failed to prepare orders query")?;

        let mut o_orderkey = Vec::new();
        let mut o_custkey = Vec::new();
        let mut o_orderstatus = Vec::new();
        let mut o_totalprice = Vec::new();
        let mut o_orderdate = Vec::new();
        let mut o_orderpriority = Vec::new();
        let mut o_clerk = Vec::new();
        let mut o_shippriority = Vec::new();
        let mut o_comment = Vec::new();

        let mut rows = stmt.query([]).context("Failed to query orders data")?;
        while let Some(row) = rows.next().context("Failed to read row")? {
            o_orderkey.push(row.get::<_, i64>(0)?);
            o_custkey.push(row.get::<_, i64>(1)?);
            o_orderstatus.push(row.get::<_, String>(2)?);
            o_totalprice.push(row.get::<_, f64>(3)?);
            o_orderdate.push(row.get::<_, String>(4)?);
            o_orderpriority.push(row.get::<_, String>(5)?);
            o_clerk.push(row.get::<_, String>(6)?);
            o_shippriority.push(row.get::<_, i64>(7)?);
            o_comment.push(row.get::<_, String>(8)?);
        }

        let record_count = o_orderkey.len();
        println!("Generated {record_count} orders records");

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(o_orderkey)),
                Arc::new(Int64Array::from(o_custkey)),
                Arc::new(StringArray::from(o_orderstatus)),
                Arc::new(Float64Array::from(o_totalprice)),
                Arc::new(StringArray::from(o_orderdate)),
                Arc::new(StringArray::from(o_orderpriority)),
                Arc::new(StringArray::from(o_clerk)),
                Arc::new(Int64Array::from(o_shippriority)),
                Arc::new(StringArray::from(o_comment)),
            ],
        )
        .context("Failed to create Arrow RecordBatch")?;

        Ok(vec![batch])
    }

    fn marker_record(&self) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(vec![-1i64])),
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(StringArray::from(vec!["X"])),
                Arc::new(Float64Array::from(vec![0.0])),
                Arc::new(StringArray::from(vec!["1970-01-01"])),
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(StringArray::from(vec!["BENCHMARK_MARKER"])),
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE o_orderkey = -1",
            self.table_name()
        )
    }

    fn schema(&self) -> arrow::datatypes::Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["o_orderkey"]
    }
}
