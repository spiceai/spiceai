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

#![allow(clippy::missing_errors_doc)]

use arrow::array::RecordBatch;
use arrow_json::writer::JsonArray;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use schemars::{JsonSchema, schema_for};
use serde::Serialize;
use serde_json::Value;

pub fn parameters<T: JsonSchema + Serialize>() -> Option<Value> {
    match serde_json::to_value(schema_for!(T)) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::error!("Unexpectedly cannot serialize schema: {e}",);
            None
        }
    }
}

/// Serialize record batches to a JSON string.
pub fn write_to_json_string(
    data: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let buf = Vec::new();
    let mut writer = arrow_json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(buf);
    writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
    writer.finish()?;
    Ok(String::from_utf8(writer.into_inner())?)
}

/// Largest result, in bytes, a tool returns, built-in or MCP. A tool result is copied several
/// times on its way to the caller, so its size multiplies into peak memory.
pub const MAX_TOOL_RESULT_BYTES: usize = 16 * 1024 * 1024;

fn result_too_large(max_bytes: usize, fix: &str) -> Box<dyn std::error::Error + Send + Sync> {
    format!(
        "The result is larger than {} MiB, the limit for a tool result. {fix}",
        max_bytes / (1024 * 1024)
    )
    .into()
}

/// Writes the rows of `stream` as a JSON array, like [`write_to_json_string`]. Stops reading
/// and returns an error as soon as the JSON is longer than `max_bytes`.
pub async fn stream_to_json_string(
    mut stream: SendableRecordBatchStream,
    max_bytes: usize,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut writer = arrow_json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());
    let too_large = || {
        result_too_large(
            max_bytes,
            "Add a LIMIT clause, select fewer columns, or aggregate the rows.",
        )
    };
    while let Some(batch) = stream.next().await {
        writer.write(&batch?)?;
        if writer.get_ref().len() > max_bytes {
            return Err(too_large());
        }
    }
    writer.finish()?;
    let json = writer.into_inner();
    if json.len() > max_bytes {
        return Err(too_large());
    }
    Ok(String::from_utf8(json)?)
}

/// Collects `stream` into batches. Stops reading and returns an error as soon as the rows of the
/// batches take more than `max_bytes` of memory.
pub async fn collect_batches_bounded(
    mut stream: SendableRecordBatchStream,
    max_bytes: usize,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
    let mut batches = Vec::new();
    let mut size = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        size = size.saturating_add(rows_memory_size(&batch));
        if size > max_bytes {
            return Err(result_too_large(max_bytes, "Request fewer rows."));
        }
        batches.push(batch);
    }
    Ok(batches)
}

/// Memory that the rows of `batch` take. A batch sliced from a larger batch shares its buffers,
/// and `get_array_memory_size` counts those buffers in full.
fn rows_memory_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| {
            c.to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| c.get_array_memory_size())
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use super::*;

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..i64::try_from(rows).unwrap_or(i64::MAX)).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("row-{id}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("valid batch")
    }

    fn stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    #[tokio::test]
    async fn a_result_under_the_limit_is_the_same_json_as_without_a_limit() {
        let batches = vec![batch(100), batch(50)];
        let expected = write_to_json_string(&batches).expect("serialize");
        let json = stream_to_json_string(stream(batches.clone()), expected.len())
            .await
            .expect("a result of exactly the limit is returned");
        assert_eq!(json, expected);
        stream_to_json_string(stream(batches), expected.len() - 1)
            .await
            .expect_err("a result one byte over the limit fails");
    }

    #[tokio::test]
    async fn a_result_over_the_limit_is_an_error_before_the_stream_ends() {
        let first = batch(100);
        let limit = write_to_json_string(std::slice::from_ref(&first))
            .expect("serialize")
            .len()
            / 2;
        let endless = Box::pin(RecordBatchStreamAdapter::new(
            first.schema(),
            futures::stream::iter([Ok(first)]).chain(futures::stream::pending()),
        ));
        let e = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream_to_json_string(endless, limit),
        )
        .await
        .expect("the limit stops reading the stream")
        .expect_err("a result over the limit fails");
        assert!(e.to_string().contains("Add a LIMIT clause"), "{e}");
    }

    #[tokio::test]
    async fn collected_batches_are_bounded() {
        let batches = vec![batch(100), batch(50)];
        let size: usize = batches.iter().map(rows_memory_size).sum();
        let collected = collect_batches_bounded(stream(batches.clone()), size)
            .await
            .expect("batches of exactly the limit are returned");
        assert_eq!(collected, batches);
        let e = collect_batches_bounded(stream(batches), size - 1)
            .await
            .expect_err("batches over the limit fail");
        assert!(e.to_string().contains("Request fewer rows"), "{e}");
    }

    #[tokio::test]
    async fn a_few_rows_sliced_from_a_large_batch_are_not_charged_for_the_whole_batch() {
        let large = batch(100_000);
        let few = large.slice(0, 3);
        let collected = collect_batches_bounded(
            stream(vec![few.clone()]),
            large.get_array_memory_size() / 100,
        )
        .await
        .expect("three rows fit");
        assert_eq!(collected, vec![few]);
    }
}
