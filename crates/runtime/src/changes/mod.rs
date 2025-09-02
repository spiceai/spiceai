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
use runtime_datafusion_index::IndexedTableProvider;

pub async fn index_change_envelope(
    maybe_envelope: Result<ChangeEnvelope, StreamError>,
    embedding_table: Arc<IndexedTableProvider>,
) -> Result<ChangeEnvelope, StreamError> {
    let envelope = maybe_envelope.map_err(|e| {
        tracing::debug!("Error in underlying base stream: {e:?}");
        e
    })?;

    let (change_committer, batch) = envelope.into_parts();
    let mut batches = vec![batch.data_batch()];

    for index in &embedding_table.indexes {
        batches = index
            .compute_index(batches)
            .await
            .map_err(|e| StreamError::External(e.to_string()))?;
    }

    let new_change_batch = replace_change_batch_data(&batches[0], &batch)
        .map_err(|e| StreamError::Arrow(e.to_string()))?;

    Ok(ChangeEnvelope::new(change_committer, new_change_batch))
}
