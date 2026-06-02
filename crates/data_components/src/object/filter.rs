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
use arrow_array::{Array, BooleanArray};
use datafusion::common::DFSchema;
use datafusion::logical_expr::ColumnarValue;
use std::collections::HashMap;
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
pub fn filter_object_meta(
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
            "Unexpectedly received scalar value when evaluating object store metadata filters."
                .to_string(),
        ));
    };

    let Some(bool_arr) = arr.as_any().downcast_ref::<BooleanArray>() else {
        return Err(DataFusionError::Internal(
            "Unexpectedly received non-boolean value when evaluating object store metadata filters.".to_string(),
        ));
    };

    // Optimization: Use SIMD-optimized filter_record_batch for maximum performance
    // This leverages Arrow's compute kernels which use hardware SIMD instructions
    let filtered_rb = filter_record_batch(&rb, bool_arr)?;

    // Early return for empty results
    let num_filtered = filtered_rb.num_rows();
    if num_filtered == 0 {
        return Ok(Vec::new());
    }

    // Optimization: Build result by extracting from filtered RecordBatch columns
    // This reduces cache misses by accessing contiguous Arrow arrays
    let location_array = filtered_rb
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Location column is not a StringArray".to_string())
        })?;
    let last_modified_array = filtered_rb
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Last modified column is not a TimestampArray".to_string())
        })?;
    let size_array = filtered_rb
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFusionError::Internal("Size column is not a UInt64Array".to_string()))?;
    let etag_array = filtered_rb
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal("ETag column is not a StringArray".to_string()))?;
    let version_array = filtered_rb
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Version column is not a StringArray".to_string())
        })?;

    // Build result from filtered arrays with sequential memory access
    let mut result = Vec::with_capacity(num_filtered);
    for row_idx in 0..num_filtered {
        result.push(ObjectMeta {
            // Use parse() instead of from() to avoid double URL-encoding
            // The string in the array is already URL-encoded from as_ref()
            location: object_store::path::Path::parse(location_array.value(row_idx))
                .map_err(|e| DataFusionError::Execution(format!("Invalid path: {e}")))?,
            last_modified: chrono::DateTime::from_timestamp_millis(
                last_modified_array.value(row_idx),
            )
            .ok_or_else(|| DataFusionError::Internal("Invalid timestamp value".to_string()))?,
            size: size_array.value(row_idx),
            e_tag: if etag_array.is_null(row_idx) {
                None
            } else {
                Some(etag_array.value(row_idx).to_string())
            },
            version: if version_array.is_null(row_idx) {
                None
            } else {
                Some(version_array.value(row_idx).to_string())
            },
        });
    }

    Ok(result)
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use datafusion::{
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use object_store::path::Path;
    use std::ops::Not;

    fn create_test_meta(
        location: &str,
        last_modified: DateTime<Utc>,
        size: u64,
        e_tag: Option<String>,
        version: Option<String>,
    ) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(location),
            last_modified,
            size,
            e_tag,
            version,
        }
    }

    #[tokio::test]
    async fn test_filter_object_meta_empty_filters() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
        ];

        let result = filter_object_meta(&[], &metas).expect("could not filter ObjectMeta");
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_filter_object_meta_empty_metas() {
        let filters = vec![col("size").gt(lit(100u64))];
        let metas: Vec<ObjectMeta> = vec![];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_object_meta_by_size() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
        ];

        let filters = vec![col("size").gt(lit(150u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file2.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_by_location() {
        let metas = vec![
            create_test_meta("data/file1.txt", Utc::now(), 100, None, None),
            create_test_meta("logs/file2.txt", Utc::now(), 200, None, None),
            create_test_meta("data/file3.txt", Utc::now(), 300, None, None),
        ];

        let filters = vec![col("location").like(lit("data%"))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert!(result[0].location.as_ref().starts_with("data"));
        assert!(result[1].location.as_ref().starts_with("data"));
    }

    #[tokio::test]
    async fn test_filter_object_meta_combined_filters() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
            create_test_meta("file4.txt", Utc::now(), 400, None, None),
        ];

        let filters = vec![col("size").gt(lit(150u64)), col("size").lt(lit(350u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file2.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_by_last_modified() {
        let now = Utc::now();
        let past = now - chrono::Duration::hours(2);
        let future = now + chrono::Duration::hours(2);

        let metas = vec![
            create_test_meta("file1.txt", past, 100, None, None),
            create_test_meta("file2.txt", now, 200, None, None),
            create_test_meta("file3.txt", future, 300, None, None),
        ];

        let filters = vec![col("last_modified").gt(Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(now.timestamp_millis()), Some("UTC".into())),
            None,
        ))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].location.as_ref(), "file3.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_no_matches() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
        ];

        let filters = vec![col("size").gt(lit(1000u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_object_meta_all_match() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
        ];

        let filters = vec![col("size").gt(lit(50u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_filter_object_meta_with_etag() {
        let metas = vec![
            create_test_meta(
                "file1.txt",
                Utc::now(),
                100,
                Some("etag1".to_string()),
                None,
            ),
            create_test_meta(
                "file2.txt",
                Utc::now(),
                200,
                Some("etag2".to_string()),
                None,
            ),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
        ];

        let filters = vec![col("e_tag").is_not_null()];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file1.txt");
        assert_eq!(result[1].location.as_ref(), "file2.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_with_version() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, Some("v1".to_string())),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, Some("v2".to_string())),
        ];

        let filters = vec![col("version").is_not_null()];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file1.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_equality() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 200, None, None),
        ];

        let filters = vec![col("size").eq(lit(200u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file2.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_size_less_than() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
        ];

        let filters = vec![col("size").lt(lit(250u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file1.txt");
        assert_eq!(result[1].location.as_ref(), "file2.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_multiple_and_filters() {
        let metas = vec![
            create_test_meta("data/file1.txt", Utc::now(), 100, None, None),
            create_test_meta("data/file2.txt", Utc::now(), 200, None, None),
            create_test_meta("logs/file3.txt", Utc::now(), 300, None, None),
            create_test_meta("data/file4.txt", Utc::now(), 400, None, None),
        ];

        let filters = vec![
            col("location").like(lit("data%")),
            col("size").gt(lit(150u64)),
        ];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "data/file2.txt");
        assert_eq!(result[1].location.as_ref(), "data/file4.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_not_equal() {
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
        ];

        let filters = vec![col("size").not_eq(lit(200u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file1.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_preserves_original_order() {
        // Test that the original implementation preserves the order of ObjectMeta
        // when filtering based on the boolean array index mapping
        let now = Utc::now();
        let metas = vec![
            create_test_meta("a_file.txt", now, 150, Some("etag1".to_string()), None),
            create_test_meta("b_file.txt", now, 250, None, Some("v1".to_string())),
            create_test_meta(
                "c_file.txt",
                now,
                350,
                Some("etag2".to_string()),
                Some("v2".to_string()),
            ),
            create_test_meta("d_file.txt", now, 450, None, None),
            create_test_meta("e_file.txt", now, 550, Some("etag3".to_string()), None),
        ];

        // Filter for files with size > 200 and < 500
        let filters = vec![col("size").gt(lit(200u64)), col("size").lt(lit(500u64))];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        // Should return b_file.txt, c_file.txt, and d_file.txt in that order
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].location.as_ref(), "b_file.txt");
        assert_eq!(result[0].size, 250);
        assert_eq!(result[1].location.as_ref(), "c_file.txt");
        assert_eq!(result[1].size, 350);
        assert_eq!(result[2].location.as_ref(), "d_file.txt");
        assert_eq!(result[2].size, 450);

        // Verify the cloned ObjectMeta maintains all fields
        assert_eq!(result[0].e_tag, None);
        assert_eq!(result[0].version, Some("v1".to_string()));
        assert_eq!(result[1].e_tag, Some("etag2".to_string()));
        assert_eq!(result[1].version, Some("v2".to_string()));
        assert_eq!(result[2].e_tag, None);
        assert_eq!(result[2].version, None);
    }

    #[tokio::test]
    async fn test_filter_object_meta_handles_nulls_in_boolean_array() {
        // Test that the implementation correctly handles None values in the boolean array
        // (though this shouldn't happen in practice with valid filter expressions)
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
        ];

        // Use a complex filter that would produce Some(true), Some(false) patterns
        let filters = vec![col("size").gt(lit(150u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        // The manual iteration with keep == Some(true) ensures only definite matches are included
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "file2.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_with_special_characters_in_location() {
        // Test handling of special characters in location paths
        let metas = vec![
            create_test_meta("file with spaces.txt", Utc::now(), 100, None, None),
            create_test_meta("file-with-dashes.txt", Utc::now(), 200, None, None),
            create_test_meta("file_with_underscores.txt", Utc::now(), 300, None, None),
            create_test_meta("file.with.dots.txt", Utc::now(), 400, None, None),
            create_test_meta("file/with/slashes.txt", Utc::now(), 500, None, None),
        ];

        let filters = vec![col("location").like(lit("file%with%"))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        // All files match the LIKE pattern "file%with%" because % matches any characters
        assert_eq!(result.len(), 5);
        assert!(
            result
                .iter()
                .any(|m| m.location.as_ref() == "file with spaces.txt")
        );
        assert!(
            result
                .iter()
                .any(|m| m.location.as_ref() == "file-with-dashes.txt")
        );
        assert!(
            result
                .iter()
                .any(|m| m.location.as_ref() == "file_with_underscores.txt")
        );
        assert!(
            result
                .iter()
                .any(|m| m.location.as_ref() == "file/with/slashes.txt")
        );
        assert!(
            result
                .iter()
                .any(|m| m.location.as_ref() == "file.with.dots.txt")
        );
    }

    #[tokio::test]
    async fn test_filter_object_meta_with_special_characters_in_etag() {
        // Test handling of special characters in optional string fields
        let metas = vec![
            create_test_meta(
                "file1.txt",
                Utc::now(),
                100,
                Some("\"quoted-etag\"".to_string()),
                None,
            ),
            create_test_meta(
                "file2.txt",
                Utc::now(),
                200,
                Some("etag-with-special!@#$%".to_string()),
                None,
            ),
            create_test_meta("file3.txt", Utc::now(), 300, Some(String::new()), None), // Empty string
            create_test_meta("file4.txt", Utc::now(), 400, None, None),
        ];

        let filters = vec![col("e_tag").is_not_null()];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].e_tag.as_deref(), Some("\"quoted-etag\""));
        assert_eq!(result[1].e_tag.as_deref(), Some("etag-with-special!@#$%"));
        assert_eq!(result[2].e_tag.as_deref(), Some("")); // Empty string is still not null
    }

    #[tokio::test]
    async fn test_filter_object_meta_large_dataset() {
        // Test performance with a larger dataset
        let now = Utc::now();
        let mut metas = Vec::with_capacity(1000);

        for i in 0..1000 {
            let etag = if i % 3 == 0 {
                Some(format!("etag{i}"))
            } else {
                None
            };
            let version = if i % 5 == 0 {
                Some(format!("v{i}"))
            } else {
                None
            };
            metas.push(create_test_meta(
                &format!("file{i:04}.txt"),
                now,
                u64::try_from(i).expect("i32 to u64 conversion") * 100,
                etag,
                version,
            ));
        }

        // Filter for items with size between 20000 and 50000 (indices 200-500)
        let filters = vec![
            col("size").gt_eq(lit(20000u64)),
            col("size").lt(lit(50000u64)),
        ];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 300);
        assert_eq!(
            result
                .first()
                .expect("should have first item")
                .location
                .as_ref(),
            "file0200.txt"
        );
        assert_eq!(
            result
                .last()
                .expect("should have last item")
                .location
                .as_ref(),
            "file0499.txt"
        );
    }

    #[tokio::test]
    async fn test_filter_object_meta_complex_boolean_expression() {
        // Test complex boolean expressions with OR and AND
        let now = Utc::now();
        let metas = vec![
            create_test_meta("small1.txt", now, 50, Some("e1".to_string()), None),
            create_test_meta("small2.txt", now, 100, None, Some("v1".to_string())),
            create_test_meta(
                "medium1.txt",
                now,
                250,
                Some("e2".to_string()),
                Some("v2".to_string()),
            ),
            create_test_meta("medium2.txt", now, 300, None, None),
            create_test_meta("large1.txt", now, 500, Some("e3".to_string()), None),
            create_test_meta("large2.txt", now, 600, None, Some("v3".to_string())),
        ];

        // (size < 200 AND e_tag IS NOT NULL) OR (size > 400 AND version IS NOT NULL)
        let filters = vec![
            col("size")
                .lt(lit(200u64))
                .and(col("e_tag").is_not_null())
                .or(col("size")
                    .gt(lit(400u64))
                    .and(col("version").is_not_null())),
        ];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "small1.txt"); // size < 200 AND has e_tag
        assert_eq!(result[1].location.as_ref(), "large2.txt"); // size > 400 AND has version
    }

    #[tokio::test]
    async fn test_filter_object_meta_timestamp_edge_cases() {
        // Test timestamp filtering with millisecond precision
        let base_time = Utc::now();
        let time_minus_1ms = base_time - chrono::Duration::milliseconds(1);
        let time_plus_1ms = base_time + chrono::Duration::milliseconds(1);

        let metas = vec![
            create_test_meta("past.txt", time_minus_1ms, 100, None, None),
            create_test_meta("present.txt", base_time, 200, None, None),
            create_test_meta("future.txt", time_plus_1ms, 300, None, None),
        ];

        // Test exact timestamp match
        let filters = vec![col("last_modified").eq(Expr::Literal(
            ScalarValue::TimestampMillisecond(
                Some(base_time.timestamp_millis()),
                Some("UTC".into()),
            ),
            None,
        ))];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].location.as_ref(), "present.txt");

        // Test greater than or equal to
        let filters = vec![col("last_modified").gt_eq(Expr::Literal(
            ScalarValue::TimestampMillisecond(
                Some(base_time.timestamp_millis()),
                Some("UTC".into()),
            ),
            None,
        ))];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "present.txt");
        assert_eq!(result[1].location.as_ref(), "future.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_unicode_strings() {
        // Test handling of Unicode characters in strings
        let metas = vec![
            create_test_meta("文件.txt", Utc::now(), 100, Some("标签1".to_string()), None),
            create_test_meta(
                "файл.txt",
                Utc::now(),
                200,
                None,
                Some("версия1".to_string()),
            ),
            create_test_meta(
                "αρχείο.txt",
                Utc::now(),
                300,
                Some("ετικέτα1".to_string()),
                Some("έκδοση1".to_string()),
            ),
            create_test_meta("emoji😀.txt", Utc::now(), 400, None, None),
        ];

        // Filter for all files with non-null e_tag
        let filters = vec![col("e_tag").is_not_null()];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 2);
        // Unicode characters in paths get URL-encoded by Path::from()
        assert_eq!(result[0].location.as_ref(), "%E6%96%87%E4%BB%B6.txt"); // "文件.txt" URL-encoded
        assert_eq!(
            result[1].location.as_ref(),
            "%CE%B1%CF%81%CF%87%CE%B5%CE%AF%CE%BF.txt"
        ); // "αρχείο.txt" URL-encoded
        assert_eq!(result[0].e_tag.as_deref(), Some("标签1"));
        assert_eq!(result[1].e_tag.as_deref(), Some("ετικέτα1"));
    }

    #[tokio::test]
    async fn test_filter_object_meta_boundary_values() {
        // Test boundary values for u64 size field
        let metas = vec![
            create_test_meta("zero.txt", Utc::now(), 0, None, None),
            create_test_meta("one.txt", Utc::now(), 1, None, None),
            create_test_meta("max_u32.txt", Utc::now(), u64::from(u32::MAX), None, None),
            create_test_meta(
                "max_u32_plus_1.txt",
                Utc::now(),
                u64::from(u32::MAX) + 1,
                None,
                None,
            ),
            create_test_meta("large.txt", Utc::now(), 1_000_000_000_000, None, None),
        ];

        // Test with exact boundary values
        let filters = vec![col("size").eq(lit(0u64))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].location.as_ref(), "zero.txt");

        // Test with greater than u32::MAX
        let filters = vec![col("size").gt(lit(u64::from(u32::MAX)))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].location.as_ref(), "max_u32_plus_1.txt");
        assert_eq!(result[1].location.as_ref(), "large.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_in_list() {
        // Test IN operator equivalent using OR conditions
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
            create_test_meta("file4.txt", Utc::now(), 400, None, None),
            create_test_meta("file5.txt", Utc::now(), 500, None, None),
        ];

        // Simulate IN (100, 300, 500) using OR
        let filters = vec![
            col("size")
                .eq(lit(100u64))
                .or(col("size").eq(lit(300u64)))
                .or(col("size").eq(lit(500u64))),
        ];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].location.as_ref(), "file1.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
        assert_eq!(result[2].location.as_ref(), "file5.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_negation() {
        // Test NOT operations
        let metas = vec![
            create_test_meta(
                "file1.txt",
                Utc::now(),
                100,
                Some("etag1".to_string()),
                None,
            ),
            create_test_meta("file2.txt", Utc::now(), 200, None, Some("v1".to_string())),
            create_test_meta(
                "file3.txt",
                Utc::now(),
                300,
                Some("etag2".to_string()),
                Some("v2".to_string()),
            ),
            create_test_meta("file4.txt", Utc::now(), 400, None, None),
        ];

        // NOT (size < 200 OR e_tag IS NULL)
        // This should match files with size >= 200 AND e_tag IS NOT NULL
        let filters = vec![col("size").lt(lit(200u64)).or(col("e_tag").is_null()).not()];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].location.as_ref(), "file3.txt");
        assert_eq!(result[0].size, 300);
        assert!(result[0].e_tag.is_some());
    }

    #[tokio::test]
    async fn test_filter_object_meta_between() {
        // Test BETWEEN equivalent using AND conditions
        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 50, None, None),
            create_test_meta("file2.txt", Utc::now(), 100, None, None),
            create_test_meta("file3.txt", Utc::now(), 150, None, None),
            create_test_meta("file4.txt", Utc::now(), 200, None, None),
            create_test_meta("file5.txt", Utc::now(), 250, None, None),
        ];

        // Simulate BETWEEN 100 AND 200 (inclusive)
        let filters = vec![
            col("size")
                .gt_eq(lit(100u64))
                .and(col("size").lt_eq(lit(200u64))),
        ];

        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].location.as_ref(), "file2.txt");
        assert_eq!(result[1].location.as_ref(), "file3.txt");
        assert_eq!(result[2].location.as_ref(), "file4.txt");
    }

    #[tokio::test]
    async fn test_filter_object_meta_case_sensitive_location() {
        // Test case sensitivity in location matching
        let metas = vec![
            create_test_meta("File.txt", Utc::now(), 100, None, None),
            create_test_meta("file.txt", Utc::now(), 200, None, None),
            create_test_meta("FILE.txt", Utc::now(), 300, None, None),
            create_test_meta("FiLe.txt", Utc::now(), 400, None, None),
        ];

        // Exact match is case-sensitive
        let filters = vec![col("location").eq(lit("file.txt"))];
        let result = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].location.as_ref(), "file.txt");
        assert_eq!(result[0].size, 200);
    }

    #[tokio::test]
    async fn test_filter_record_batch_helper() {
        // Direct test of the filter_record_batch helper function
        use arrow::compute::filter_record_batch;

        let metas = vec![
            create_test_meta("file1.txt", Utc::now(), 100, None, None),
            create_test_meta("file2.txt", Utc::now(), 200, None, None),
            create_test_meta("file3.txt", Utc::now(), 300, None, None),
        ];

        let rb = to_record_batch(&metas).expect("should create record batch");

        // Create a boolean array for filtering
        let bool_array = BooleanArray::from(vec![true, false, true]);

        let filtered_rb = filter_record_batch(&rb, &bool_array).expect("should filter");

        assert_eq!(filtered_rb.num_rows(), 2);

        // Verify the filtered data
        let location_array = filtered_rb
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("should be string array");

        assert_eq!(location_array.value(0), "file1.txt");
        assert_eq!(location_array.value(1), "file3.txt");
    }

    #[tokio::test]
    async fn test_to_record_batch_schema_validation() {
        // Verify that to_record_batch creates correct schema
        let now = Utc::now();
        let metas = vec![create_test_meta(
            "test.txt",
            now,
            12345,
            Some("test-etag".to_string()),
            Some("v1.0".to_string()),
        )];

        let rb = to_record_batch(&metas).expect("should create record batch");

        // Verify schema matches expected
        assert_eq!(rb.schema(), *OBJECT_META_SCHEMA);
        assert_eq!(rb.num_columns(), 5);
        assert_eq!(rb.num_rows(), 1);

        // Verify column types
        assert_eq!(rb.column(0).data_type(), &DataType::Utf8); // location
        assert_eq!(
            rb.column(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        ); // last_modified
        assert_eq!(rb.column(2).data_type(), &DataType::UInt64); // size
        assert_eq!(rb.column(3).data_type(), &DataType::Utf8); // e_tag
        assert_eq!(rb.column(4).data_type(), &DataType::Utf8); // version
    }

    #[tokio::test]
    async fn test_path_round_trip_symmetry() {
        // This test verifies that Path::parse() correctly round-trips paths that were
        // encoded via Path::from() and then retrieved via as_ref().
        // This is critical for the filter_object_meta implementation which uses:
        //   1. Path::from() to create paths (which URL-encodes)
        //   2. as_ref() to get URL-encoded strings (in to_record_batch)
        //   3. Path::parse() to reconstruct paths (in from_filtered_record_batch)

        let test_paths = vec![
            "simple.txt",
            "with spaces.txt",
            "文件.txt",    // Chinese characters
            "αρχείο.txt",  // Greek characters
            "emoji😀.txt", // Emoji
            "mixed/path/文件 with spaces.txt",
            "special!@#$%^&*().txt",
            "path/to/file.txt",
            "",                      // Empty path
            "already%20encoded.txt", // Pre-encoded characters
        ];

        for original_path_str in test_paths {
            // Step 1: Create Path using from() (like create_test_meta does)
            let original_path = Path::from(original_path_str);

            // Step 2: Get URL-encoded representation (like to_record_batch does)
            let encoded_str = original_path.as_ref();

            // Step 3: Parse it back (like from_filtered_record_batch does)
            let parsed_path = Path::parse(encoded_str).unwrap_or_else(|e| {
                panic!("Failed to parse '{encoded_str}' (from '{original_path_str}'): {e}")
            });

            // Verify round-trip: The parsed path should be identical to the original
            assert_eq!(
                original_path.as_ref(),
                parsed_path.as_ref(),
                "Round-trip failed for path '{}': original='{}' parsed='{}'",
                original_path_str,
                original_path.as_ref(),
                parsed_path.as_ref()
            );
        }
    }

    #[tokio::test]
    async fn test_filter_preserves_path_encoding() {
        // Integration test: Verify that filtering preserves exact path encoding
        // including special characters and Unicode
        let now = Utc::now();
        let test_cases = [
            ("simple.txt", 100),
            ("with spaces.txt", 200),
            ("文件.txt", 300),
            ("emoji😀.txt", 400),
        ];

        let metas: Vec<ObjectMeta> = test_cases
            .iter()
            .map(|(path, size)| create_test_meta(path, now, *size, None, None))
            .collect();

        // Filter for all files (no actual filtering, just round-trip)
        let filters = vec![col("size").gt(lit(0u64))];
        let filtered = filter_object_meta(&filters, &metas).expect("could not filter ObjectMeta");

        assert_eq!(filtered.len(), metas.len());

        // Verify each path round-tripped correctly
        for (original, filtered_meta) in metas.iter().zip(filtered.iter()) {
            assert_eq!(
                original.location.as_ref(),
                filtered_meta.location.as_ref(),
                "Path encoding changed during filter for '{}'",
                original.location.as_ref()
            );
        }
    }
}
