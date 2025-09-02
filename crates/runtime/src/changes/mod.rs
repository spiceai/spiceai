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

use data_components::cdc::{ChangeEnvelope, StreamError, replace_change_batch_data};
use datafusion::datasource::TableProvider;
use futures::{StreamExt, stream::BoxStream};
use runtime_datafusion_index::IndexedTableProvider;

use crate::embeddings::{
    execution_plan::{compute_additional_embedding_columns, construct_record_batch},
    table::EmbeddingTable,
};

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

    let (change_committers, change_batches): (Vec<_>, Vec<_>) = envelope
        .into_iter()
        .map(data_components::cdc::ChangeEnvelope::into_parts)
        .unzip();
    let mut data_batches = change_batches
        .iter()
        .map(data_components::cdc::ChangeBatch::data_batch)
        .collect::<Vec<_>>();

    for index in &embedding_table.indexes {
        data_batches = index
            .compute_index(data_batches)
            .await
            .map_err(|e| StreamError::External(e.to_string()))?;
    }

    let new_change_envelopes: Vec<ChangeEnvelope> = data_batches
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

pub async fn embed_change_envelope(
    maybe_envelope: Result<ChangeEnvelope, StreamError>,
    embedding_table: Arc<EmbeddingTable>,
) -> Result<ChangeEnvelope, StreamError> {
    let envelope = maybe_envelope.map_err(|e| {
        tracing::debug!("Error in underlying base stream: {e:?}");
        e
    })?;

    let (change_committer, batch) = envelope.into_parts();
    let data_batch = batch.data_batch();

    let embeddings = compute_additional_embedding_columns(
        &data_batch,
        &embedding_table.embedded_columns,
        Arc::clone(&embedding_table.embedding_models),
    )
    .await
    .map_err(|e| {
        tracing::debug!("Error when getting embedding columns: {e:?}");
        StreamError::Arrow(e.to_string())
    })?;

    for (column_name, embeddings) in &embeddings {
        tracing::trace!(
            "Embedding column computed: {column_name}, embeddings: {:?}",
            embeddings.len()
        );
    }

    let embedded_batch =
        construct_record_batch(&data_batch, &embedding_table.schema(), &embeddings)
            .map_err(|e| StreamError::Arrow(e.to_string()))?;

    let new_change_batch = replace_change_batch_data(&embedded_batch, &batch)
        .map_err(|e| StreamError::Arrow(e.to_string()))?;

    Ok(ChangeEnvelope::new(change_committer, new_change_batch))
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
