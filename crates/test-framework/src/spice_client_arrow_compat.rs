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

//! The Spice Rust SDK (`spiceai`) is now built against the same Arrow version as the
//! workspace, so its `RecordBatch` is identical to ours. These helpers used to re-encode
//! batches over Arrow IPC to bridge an Arrow version gap between the SDK and the workspace;
//! that gap no longer exists, so the conversions are now thin pass-throughs kept only to
//! keep existing call sites stable.

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

pub async fn query_to_batches(
    spice_client: &spiceai::Client,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let mut stream = spice_client.sql(sql).await?;
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }

    Ok(batches)
}

pub async fn query_with_params_to_batches(
    spice_client: &spiceai::Client,
    sql: &str,
    params: Option<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let mut stream = spice_client.sql_with_params(sql, params).await?;
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }

    Ok(batches)
}
