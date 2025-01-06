/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use flight_client::FlightClient;
use futures::StreamExt;

/// Query a flight client and return the result as a vector of record batches
///
/// # Errors
///
/// - If the flight client fails to query
pub async fn query_to_batches(client: &FlightClient, sql: &str) -> Result<Vec<RecordBatch>> {
    let mut stream = client.query(sql).await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}
