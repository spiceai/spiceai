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

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    prelude::{Expr, SessionContext},
};
use futures::StreamExt;
use object_store::ObjectMeta;

/// Filters [`ObjectMeta`]s that satisfy all provided `filter`s.
pub async fn filter_object_meta(
    filter: &[Expr],
    metas: &[ObjectMeta],
) -> Result<Vec<ObjectMeta>, DataFusionError> {
    let Some(combined_filter) = filter.iter().cloned().reduce(Expr::and) else {
        return Ok(metas.to_vec());
    };

    let ctx = SessionContext::new();
    ctx.register_batch(
        "tmp",
        to_record_batch(metas).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some("Failed to convert 'ObjectMeta' to arrow".to_string()),
            )
        })?,
    )?;

    let mut stream = ctx
        .table("tmp")
        .await?
        .filter(combined_filter)?
        .execute_stream()
        .await?;

    let mut valid_locations = std::collections::HashSet::new();

    while let Some(batch_result) = stream.next().await {
        let rb = batch_result?;
        if let Some(location_array) = rb.column(0).as_any().downcast_ref::<StringArray>() {
            for loc_opt in location_array {
                if let Some(loc) = loc_opt {
                    valid_locations.insert(loc.to_string());
                }
            }
        };
    }

    Ok(metas
        .into_iter()
        .filter(|m| valid_locations.contains(&m.location.to_string()))
        .cloned()
        .collect())
}

fn to_record_batch(metas: &[ObjectMeta]) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("location", DataType::Utf8, false),
            Field::new(
                "last_modified",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("size", DataType::UInt64, false),
            Field::new("e_tag", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
        ])),
        vec![
            // location
            Arc::new(StringArray::from(
                metas
                    .iter()
                    .map(|meta| meta.location.as_ref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            // last_modified
            Arc::new(
                TimestampMillisecondArray::from(
                    metas
                        .iter()
                        .map(|meta| meta.last_modified.timestamp_millis())
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ) as ArrayRef,
            // size
            Arc::new(UInt64Array::from(
                metas.iter().map(|meta| meta.size).collect::<Vec<_>>(),
            )) as ArrayRef,
            // etag
            Arc::new(StringArray::from(
                metas
                    .iter()
                    .map(|meta| meta.e_tag.as_deref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            // version
            Arc::new(StringArray::from(
                metas
                    .iter()
                    .map(|meta| meta.version.as_deref())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
}
