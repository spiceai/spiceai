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

//! A single handle over the two ways the HTAP gates talk to a running Spice
//! instance: the high-level `spiceai` query client (Flight SQL `SELECT`s) and
//! the low-level [`FlightClient`] (Flight SQL metadata calls such as
//! `GetSchema`).
//!
//! Bundling them keeps the gates from threading two separate clients around and
//! gives each operation an intent-revealing method.

use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{Int64Type, SchemaRef};
use flight_client::FlightClient;
use futures::TryStreamExt;
use test_framework::anyhow;

/// The Spice clients the correctness/staleness gates query against.
pub struct SpiceClients {
    /// High-level query client for Flight SQL `SELECT`s.
    query: spiceai::Client,
    /// Low-level Flight client for Flight SQL metadata calls (`GetSchema`).
    flight: FlightClient,
}

impl SpiceClients {
    #[must_use]
    pub fn new(query: spiceai::Client, flight: FlightClient) -> Self {
        Self { query, flight }
    }

    /// Run a read-only query and collect the result as Arrow batches.
    pub async fn query_arrow(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let stream = self.query.sql(sql).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }

    /// `COUNT(*)` for `table`.
    pub async fn count(&self, table: &str) -> anyhow::Result<i64> {
        let mut stream = self
            .query
            .sql(&format!("SELECT COUNT(*) FROM {table}"))
            .await?;
        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            let col = batch
                .column(0)
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(|| anyhow::anyhow!("unexpected array type for COUNT(*) on {table}"))?;
            if !col.is_null(0) {
                return Ok(col.value(0));
            }
        }
        Ok(0)
    }

    /// `MAX(_bench_ts)` for `table`, in microseconds since epoch.
    pub async fn max_bench_ts(&self, table: &str) -> anyhow::Result<Option<i64>> {
        super::staleness::query_max_bench_ts_spice(&self.query, table).await
    }

    /// The dataset's Arrow schema, via the Flight SQL `GetSchema` RPC.
    ///
    /// Metadata-only — the Spice runtime returns the schema directly, with no
    /// query planning or row scan (unlike a `SELECT ... LIMIT n`).
    pub async fn table_schema(&self, table: &str) -> anyhow::Result<SchemaRef> {
        let schema = self
            .flight
            .get_schema(vec![table.to_string()])
            .await
            .map_err(|e| anyhow::anyhow!("get_schema for {table} failed: {e}"))?;
        Ok(Arc::new(schema))
    }
}
