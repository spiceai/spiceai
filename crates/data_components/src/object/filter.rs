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

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow_array::BooleanArray;
use datafusion::common::DFSchema;
use datafusion::logical_expr::ColumnarValue;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use datafusion::{
    error::DataFusionError,
    prelude::{Expr, SessionContext},
};
use object_store::ObjectMeta;

static OBJECT_META_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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
    ]))
});

/// Filters [`ObjectMeta`]s that satisfy all provided `filter`s.
///
/// If `filters` contains any [`Expr`] that is not parseable by [`SessionContext::default`], all [`ObjectMeta`] are returned.
pub async fn filter_object_meta(
    filters: &[Expr],
    metas: &[ObjectMeta],
) -> Result<Vec<ObjectMeta>, DataFusionError> {
    let Some(combined_filter) = filters.iter().cloned().reduce(Expr::and) else {
        return Ok(metas.to_vec());
    };

    let rb = to_record_batch(metas).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some("Failed to convert 'ObjectMeta' to arrow".to_string()),
        )
    })?;
    let ctx = SessionContext::default();

    let df_schema =
        DFSchema::from_unqualified_fields(OBJECT_META_SCHEMA.fields().clone(), HashMap::default())?;

    // First evaluate filters as physical expression.
    let ColumnarValue::Array(arr) = ctx
        .create_physical_expr(combined_filter, &df_schema)?
        .evaluate(&rb)?
    else {
        return Err(DataFusionError::Internal(
            "Unexpectedly recieved scalar value for 'location' column".to_string(),
        ));
    };

    let Some(bool_arr) = arr.as_any().downcast_ref::<BooleanArray>() else {
        return Err(DataFusionError::Internal(
            "Unexpectedly recieved scalar value for 'location' column".to_string(),
        ));
    };

    let filtered_rb = filter_record_batch(&rb, bool_arr)?;
    let valid_locations = filtered_rb
        .column_by_name("location")
        .ok_or_else(|| DataFusionError::Internal("location column not found".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|s| s.iter().filter_map(|s| s).collect::<HashSet<_>>())
        .unwrap_or_default();

    Ok(metas
        .into_iter()
        .filter(|m| valid_locations.contains(m.location.as_ref()))
        .cloned()
        .collect())
}

fn to_record_batch(metas: &[ObjectMeta]) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        Arc::clone(&OBJECT_META_SCHEMA),
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
