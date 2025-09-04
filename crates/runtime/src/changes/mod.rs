/*
Copyright 2025 The Spice.ai OSS Authors

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

use arrow::compute::concat_batches;
use data_components::cdc::{ChangeEnvelope, StreamError, replace_change_batch_data};
use futures::{StreamExt, stream::BoxStream};
use runtime_datafusion_index::IndexedTableProvider;

pub async fn index_change_envelope(
    maybe_envelopes: Vec<Result<ChangeEnvelope, StreamError>>,
    embedding_table: Arc<IndexedTableProvider>,
) -> Result<Vec<ChangeEnvelope>, StreamError> {
    let envelope = maybe_envelopes
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            tracing::debug!("Error in underlying base stream: {e:?}");
            e
        })?;

    let (change_committers, change_batches): (Vec<_>, Vec<_>) =
        envelope.into_iter().map(|e| e.into_parts()).unzip();
    let data_batches = change_batches
        .iter()
        .map(|cb| cb.data_batch())
        .collect::<Vec<_>>();

    // Store original row counts for splitting later
    let original_row_counts: Vec<usize> =
        data_batches.iter().map(|batch| batch.num_rows()).collect();

    // Concatenate all batches into a single batch
    let schema = data_batches[0].schema();
    let concat_batch =
        concat_batches(&schema, &data_batches).map_err(|e| StreamError::Arrow(e.to_string()))?;

    // Compute index on the concatenated batch
    let mut indexed_batches = vec![concat_batch];
    for index in &embedding_table.indexes {
        indexed_batches = index
            .compute_index(indexed_batches)
            .await
            .map_err(|e| StreamError::External(e.to_string()))?;
    }

    // Split the indexed batch back into original batch sizes
    let indexed_batch = &indexed_batches[0];
    let mut split_batches = Vec::new();
    let mut start_row = 0;

    for row_count in original_row_counts {
        let end_row = start_row + row_count;
        let split_batch = indexed_batch.slice(start_row, row_count);
        split_batches.push(split_batch);
        start_row = end_row;
    }

    // Recombine the split batches back with their original change envelope.
    let new_change_envelopes: Vec<ChangeEnvelope> = split_batches
        .into_iter()
        .zip(change_batches.into_iter())
        .zip(change_committers.into_iter())
        .map(|((batch, change), committer)| {
            Ok(ChangeEnvelope::new(
                committer,
                replace_change_batch_data(&batch, &change)
                    .map_err(|e| StreamError::Arrow(e.to_string()))?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(new_change_envelopes)
}

/// Flatten a `BoxStream` of `Result<Vec<ChangeEnvelope>, StreamError>` into a `BoxStream` of `Result<ChangeEnvelope, StreamError>`.
#[must_use]
pub fn flatten_change_envelope_stream(
    input: BoxStream<'static, Result<Vec<ChangeEnvelope>, StreamError>>,
) -> BoxStream<'static, Result<ChangeEnvelope, StreamError>> {
    input
        .flat_map(|result| {
            // Convert Result<Vec<ChangeEnvelope>, StreamError> into a stream
            match result {
                Ok(vec) => futures::stream::iter(vec.into_iter().map(Ok)).boxed(),
                Err(e) => futures::stream::once(async { Err(e) }).boxed(),
            }
        })
        .boxed()
}
