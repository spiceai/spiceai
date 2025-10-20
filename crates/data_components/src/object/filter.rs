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

    let filtered_rb = filter_record_batch(&rb, bool_arr)?;
    let valid_locations = filtered_rb
        .column_by_name("location")
        .ok_or_else(|| DataFusionError::Internal("location column not found".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|s| s.iter().flatten().collect::<HashSet<_>>())
        .unwrap_or_default();

    Ok(metas
        .iter()
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use datafusion::{
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use object_store::path::Path;

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
}
