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

//! Lineitem dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckdb::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Lineitem dataset (TPCH).
///
/// Generates lineitem data using `DuckDB`'s TPCH extension.
pub struct LineitemDataset;

impl LineitemDataset {
    /// Get the Arrow schema for the lineitem table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int64, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Utf8, false),
            Field::new("l_commitdate", DataType::Utf8, false),
            Field::new("l_receiptdate", DataType::Utf8, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ])
    }
}

impl StreamingDataset for LineitemDataset {
    fn table_name(&self) -> &'static str {
        "lineitem"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Lineitem
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Generating TPCH lineitem data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        // Load TPCH extension and generate data
        conn.execute_batch("INSTALL tpch; LOAD tpch;")
            .context("Failed to load TPCH extension")?;

        conn.execute_batch(&format!("CALL dbgen(sf={scale_factor});"))
            .context("Failed to generate TPCH data")?;

        // Query the lineitem table and collect into vectors
        let mut stmt = conn
            .prepare(
                "SELECT
                    l_orderkey, l_partkey, l_suppkey, l_linenumber,
                    CAST(l_quantity AS DOUBLE) as l_quantity,
                    CAST(l_extendedprice AS DOUBLE) as l_extendedprice,
                    CAST(l_discount AS DOUBLE) as l_discount,
                    CAST(l_tax AS DOUBLE) as l_tax,
                    l_returnflag, l_linestatus,
                    l_shipdate::VARCHAR, l_commitdate::VARCHAR, l_receiptdate::VARCHAR,
                    l_shipinstruct, l_shipmode, l_comment
                FROM lineitem
                ORDER BY l_orderkey, l_linenumber",
            )
            .context("Failed to prepare lineitem query")?;

        // Collect all rows into vectors
        let mut l_orderkey = Vec::new();
        let mut l_partkey = Vec::new();
        let mut l_suppkey = Vec::new();
        let mut l_linenumber = Vec::new();
        let mut l_quantity = Vec::new();
        let mut l_extendedprice = Vec::new();
        let mut l_discount = Vec::new();
        let mut l_tax = Vec::new();
        let mut l_returnflag = Vec::new();
        let mut l_linestatus = Vec::new();
        let mut l_shipdate = Vec::new();
        let mut l_commitdate = Vec::new();
        let mut l_receiptdate = Vec::new();
        let mut l_shipinstruct = Vec::new();
        let mut l_shipmode = Vec::new();
        let mut l_comment = Vec::new();

        let mut rows = stmt.query([]).context("Failed to query lineitem data")?;
        while let Some(row) = rows.next().context("Failed to read row")? {
            l_orderkey.push(row.get::<_, i64>(0)?);
            l_partkey.push(row.get::<_, i64>(1)?);
            l_suppkey.push(row.get::<_, i64>(2)?);
            l_linenumber.push(row.get::<_, i64>(3)?);
            l_quantity.push(row.get::<_, f64>(4)?);
            l_extendedprice.push(row.get::<_, f64>(5)?);
            l_discount.push(row.get::<_, f64>(6)?);
            l_tax.push(row.get::<_, f64>(7)?);
            l_returnflag.push(row.get::<_, String>(8)?);
            l_linestatus.push(row.get::<_, String>(9)?);
            l_shipdate.push(row.get::<_, String>(10)?);
            l_commitdate.push(row.get::<_, String>(11)?);
            l_receiptdate.push(row.get::<_, String>(12)?);
            l_shipinstruct.push(row.get::<_, String>(13)?);
            l_shipmode.push(row.get::<_, String>(14)?);
            l_comment.push(row.get::<_, String>(15)?);
        }

        let record_count = l_orderkey.len();
        println!("Generated {record_count} lineitem records");

        // Build Arrow arrays
        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(l_orderkey)),
                Arc::new(Int64Array::from(l_partkey)),
                Arc::new(Int64Array::from(l_suppkey)),
                Arc::new(Int64Array::from(l_linenumber)),
                Arc::new(Float64Array::from(l_quantity)),
                Arc::new(Float64Array::from(l_extendedprice)),
                Arc::new(Float64Array::from(l_discount)),
                Arc::new(Float64Array::from(l_tax)),
                Arc::new(StringArray::from(l_returnflag)),
                Arc::new(StringArray::from(l_linestatus)),
                Arc::new(StringArray::from(l_shipdate)),
                Arc::new(StringArray::from(l_commitdate)),
                Arc::new(StringArray::from(l_receiptdate)),
                Arc::new(StringArray::from(l_shipinstruct)),
                Arc::new(StringArray::from(l_shipmode)),
                Arc::new(StringArray::from(l_comment)),
            ],
        )
        .context("Failed to create Arrow RecordBatch")?;

        Ok(vec![batch])
    }

    fn marker_count(&self) -> usize {
        8
    }

    fn marker_record(&self) -> Result<RecordBatch> {
        // Create multiple marker records with different l_orderkey values to cover multiple shards.
        // DynamoDB partitions by l_orderkey (hash key), so using spread-out negative values
        // increases the chance of hitting different partitions/shards.
        // All markers use l_linenumber = -1 to avoid conflicts with real data.
        const MARKER_ORDER_KEYS: [i64; 8] = [
            -1,
            -1000,
            -10000,
            -100_000,
            -1_000_000,
            -2_000_000,
            -5_000_000,
            -10_000_000,
        ];
        let num_markers = MARKER_ORDER_KEYS.len();

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(MARKER_ORDER_KEYS.to_vec())), // l_orderkey
                Arc::new(Int64Array::from(vec![0i64; num_markers])),    // l_partkey
                Arc::new(Int64Array::from(vec![0i64; num_markers])),    // l_suppkey
                Arc::new(Int64Array::from(vec![-1i64; num_markers])),   // l_linenumber
                Arc::new(Float64Array::from(vec![0.0; num_markers])),   // l_quantity
                Arc::new(Float64Array::from(vec![0.0; num_markers])),   // l_extendedprice
                Arc::new(Float64Array::from(vec![0.0; num_markers])),   // l_discount
                Arc::new(Float64Array::from(vec![0.0; num_markers])),   // l_tax
                Arc::new(StringArray::from(vec!["X"; num_markers])),    // l_returnflag
                Arc::new(StringArray::from(vec!["X"; num_markers])),    // l_linestatus
                Arc::new(StringArray::from(vec!["1970-01-01"; num_markers])), // l_shipdate
                Arc::new(StringArray::from(vec!["1970-01-01"; num_markers])), // l_commitdate
                Arc::new(StringArray::from(vec!["1970-01-01"; num_markers])), // l_receiptdate
                Arc::new(StringArray::from(vec!["MARKER"; num_markers])), // l_shipinstruct
                Arc::new(StringArray::from(vec!["MARKER"; num_markers])), // l_shipmode
                Arc::new(StringArray::from(vec!["BENCHMARK_MARKER"; num_markers])), // l_comment
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        // Count all markers (any record with negative l_orderkey)
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE l_orderkey < 0",
            self.table_name()
        )
    }

    fn schema(&self) -> arrow::datatypes::Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["l_orderkey", "l_linenumber"]
    }
}
