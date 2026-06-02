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

//! Customer dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Customer dataset (TPCH).
///
/// Generates customer data using `DuckDB`'s TPCH extension.
/// Contains 150,000 rows at SF=1.
pub struct CustomerDataset;

impl CustomerDataset {
    /// Get the Arrow schema for the customer table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ])
    }
}

impl StreamingDataset for CustomerDataset {
    fn table_name(&self) -> &'static str {
        "customer"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Customer
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Generating TPCH customer data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        conn.execute_batch("INSTALL tpch; LOAD tpch;")
            .context("Failed to load TPCH extension")?;

        conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
            .context("Failed to generate TPCH data")?;

        let mut stmt = conn
            .prepare(
                "SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, CAST(c_acctbal AS DOUBLE) as c_acctbal, c_mktsegment, c_comment
                FROM customer
                ORDER BY c_custkey",
            )
            .context("Failed to prepare customer query")?;

        let mut c_custkey = Vec::new();
        let mut c_name = Vec::new();
        let mut c_address = Vec::new();
        let mut c_nationkey = Vec::new();
        let mut c_phone = Vec::new();
        let mut c_acctbal = Vec::new();
        let mut c_mktsegment = Vec::new();
        let mut c_comment = Vec::new();

        let mut rows = stmt.query([]).context("Failed to query customer data")?;
        while let Some(row) = rows.next().context("Failed to read row")? {
            c_custkey.push(row.get::<_, i64>(0)?);
            c_name.push(row.get::<_, String>(1)?);
            c_address.push(row.get::<_, String>(2)?);
            c_nationkey.push(row.get::<_, i64>(3)?);
            c_phone.push(row.get::<_, String>(4)?);
            c_acctbal.push(row.get::<_, f64>(5)?);
            c_mktsegment.push(row.get::<_, String>(6)?);
            c_comment.push(row.get::<_, String>(7)?);
        }

        let record_count = c_custkey.len();
        println!("Generated {record_count} customer records");

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(c_custkey)),
                Arc::new(StringArray::from(c_name)),
                Arc::new(StringArray::from(c_address)),
                Arc::new(Int64Array::from(c_nationkey)),
                Arc::new(StringArray::from(c_phone)),
                Arc::new(Float64Array::from(c_acctbal)),
                Arc::new(StringArray::from(c_mktsegment)),
                Arc::new(StringArray::from(c_comment)),
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
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(Int64Array::from(vec![0i64])),
                Arc::new(StringArray::from(vec!["000-000-0000"])),
                Arc::new(Float64Array::from(vec![0.0])),
                Arc::new(StringArray::from(vec!["MARKER"])),
                Arc::new(StringArray::from(vec!["BENCHMARK_MARKER"])),
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE c_custkey = -1",
            self.table_name()
        )
    }

    fn schema(&self) -> arrow::datatypes::Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["c_custkey"]
    }
}
