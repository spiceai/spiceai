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

//! Tests relocated from `runtime-component` (`dataset::metadata` and
//! `dataset::declared_schema`) because they construct a full `Dataset` via
//! `DatasetBuilder` + `Runtime`, which only exist in the `runtime` crate. They
//! exercise the config methods (now on `DatasetSpec`) through the wrapper's `Deref`.

mod metadata_tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{Runtime, builder::RuntimeBuilder, component::dataset::builder::DatasetBuilder};
    use app::{App, AppBuilder};
    use arrow::datatypes::{DataType, Field};

    use arrow::datatypes::Schema;
    use data_components::object::metadata::MetadataColumn;

    #[must_use]
    fn test_app() -> Arc<App> {
        Arc::new(AppBuilder::new("test").build())
    }

    #[must_use]
    async fn test_runtime() -> Arc<Runtime> {
        Arc::new(RuntimeBuilder::new().build().await)
    }

    #[test]
    fn test_metadata_column_names() {
        assert_eq!(MetadataColumn::LastModified.name(), "_last_modified");
        assert_eq!(MetadataColumn::Location(None).name(), "_location");
        assert_eq!(MetadataColumn::Size.name(), "_size");
    }

    #[tokio::test]
    async fn test_needs_last_modified_no() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(!dataset.needs_last_modified(&schema));
    }

    #[tokio::test]
    async fn test_needs_last_modified_time_column() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_time_column("_last_modified".to_string())
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(dataset.needs_last_modified(&schema));
    }

    #[tokio::test]
    async fn test_needs_last_modified_time_column_unrelated() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_time_column("unrelated".to_string())
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(!dataset.needs_last_modified(&schema));
    }

    #[tokio::test]
    async fn test_needs_last_modified_time_partition_column() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_time_partition_column("_last_modified".to_string())
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(dataset.needs_last_modified(&schema));
    }

    #[tokio::test]
    async fn test_metadata_column_enabled_all() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(dataset.metadata_column_enabled(MetadataColumn::LastModified.name(), &schema));
        assert!(dataset.metadata_column_enabled(MetadataColumn::Location(None).name(), &schema));
        assert!(dataset.metadata_column_enabled(MetadataColumn::Size.name(), &schema));
    }

    #[tokio::test]
    async fn test_metadata_column_enabled_all_disabled() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "disabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "disabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "disabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(!dataset.metadata_column_enabled(MetadataColumn::LastModified.name(), &schema));
        assert!(!dataset.metadata_column_enabled(MetadataColumn::Location(None).name(), &schema));
        assert!(!dataset.metadata_column_enabled(MetadataColumn::Size.name(), &schema));
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_none() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(
            dataset
                .listing_table_metadata_columns("", &schema)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_needs_last_modified() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_time_column("_last_modified".to_string())
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        let columns = dataset
            .listing_table_metadata_columns("", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0], MetadataColumn::LastModified);
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_enabled() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        let columns = dataset
            .listing_table_metadata_columns("test", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 3);
        assert!(columns.contains(&MetadataColumn::LastModified));
        assert!(columns.contains(&MetadataColumn::Location(Some("test".into()))));
        assert!(columns.contains(&MetadataColumn::Size));
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_skip_existing() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![
            Field::new("test", DataType::Utf8, false),
            Field::new(MetadataColumn::Location(None).name(), DataType::Utf8, false),
        ]);
        let columns = dataset
            .listing_table_metadata_columns("", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0], MetadataColumn::LastModified);
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_schema_contains_last_modified() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![
            Field::new("test", DataType::Utf8, false),
            Field::new(MetadataColumn::LastModified.name(), DataType::Utf8, false),
        ]);
        let columns = dataset
            .listing_table_metadata_columns("test", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0], MetadataColumn::Location(Some("test".into())));
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_schema_contains_all() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![
            Field::new("test", DataType::Utf8, false),
            Field::new(MetadataColumn::LastModified.name(), DataType::Utf8, false),
            Field::new(MetadataColumn::Location(None).name(), DataType::Utf8, false),
            Field::new(MetadataColumn::Size.name(), DataType::Utf8, false),
        ]);
        assert!(
            dataset
                .listing_table_metadata_columns("", &schema)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_schema_contains_some() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![
            Field::new("test", DataType::Utf8, false),
            Field::new(MetadataColumn::LastModified.name(), DataType::Utf8, false),
            Field::new(MetadataColumn::Location(None).name(), DataType::Utf8, false),
        ]);
        let columns = dataset
            .listing_table_metadata_columns("", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0], MetadataColumn::Size);
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_combination() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_time_column("_last_modified".to_string())
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        let columns = dataset
            .listing_table_metadata_columns("test", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 3);
        assert!(columns.contains(&MetadataColumn::LastModified));
        assert!(columns.contains(&MetadataColumn::Location(Some("test".into()))));
        assert!(columns.contains(&MetadataColumn::Size));
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_disabled() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "disabled".to_string(),
                ),
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "disabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "disabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        assert!(
            dataset
                .listing_table_metadata_columns("", &schema)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_needs_last_modified_overrides_disabled() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_time_column("_last_modified".to_string())
            .with_metadata(HashMap::from([(
                MetadataColumn::LastModified.name().to_string(),
                "disabled".to_string(),
            )]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        let columns = dataset
            .listing_table_metadata_columns("", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0], MetadataColumn::LastModified);
    }

    #[tokio::test]
    async fn test_needs_last_modified_with_existing_schema_column() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_time_column("_last_modified".to_string())
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![
            Field::new("test", DataType::Utf8, false),
            Field::new(MetadataColumn::LastModified.name(), DataType::Utf8, false),
        ]);
        assert!(
            dataset
                .listing_table_metadata_columns("", &schema)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_skip_existing_size() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::LastModified.name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "enabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![
            Field::new("test", DataType::Utf8, false),
            Field::new(MetadataColumn::Size.name(), DataType::Utf8, false),
        ]);
        let columns = dataset
            .listing_table_metadata_columns("", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0], MetadataColumn::LastModified);
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_partial_enable_disable() {
        let dataset = DatasetBuilder::try_new("test".to_string(), "test")
            .expect("to get dataset builder")
            .with_app(test_app())
            .with_runtime(test_runtime().await)
            .with_metadata(HashMap::from([
                (
                    MetadataColumn::Location(None).name().to_string(),
                    "enabled".to_string(),
                ),
                (
                    MetadataColumn::Size.name().to_string(),
                    "disabled".to_string(),
                ),
            ]))
            .build()
            .expect("to build dataset");
        let schema = Schema::new(vec![Field::new("test", DataType::Utf8, false)]);
        let columns = dataset
            .listing_table_metadata_columns("test", &schema)
            .expect("to get columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0], MetadataColumn::Location(Some("test".into())));
    }
}

mod declared_schema_tests {
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::component::dataset::declared_schema::declared_schema_for;
    use app::AppBuilder;
    use spicepod::semantic::Column;

    async fn dataset_with_columns(cols: Vec<Column>) -> crate::component::dataset::Dataset {
        let app = std::sync::Arc::new(AppBuilder::new("test").build());
        let rt = std::sync::Arc::new(crate::Runtime::builder().build().await);
        let mut ds = DatasetBuilder::try_new("test:tbl".to_string(), "tbl")
            .expect("builder")
            .with_app(app)
            .with_runtime(rt)
            .build()
            .expect("dataset");
        ds.columns = cols;
        ds
    }

    #[tokio::test]
    async fn empty_columns_returns_none() {
        let ds = dataset_with_columns(vec![]).await;
        assert!(declared_schema_for(&ds).expect("no error").is_none());
    }

    #[tokio::test]
    async fn missing_type_returns_none() {
        let ds = dataset_with_columns(vec![
            Column::new("id").with_type("bigint"),
            Column::new("name"),
        ])
        .await;
        assert!(declared_schema_for(&ds).expect("no error").is_none());
    }

    #[tokio::test]
    async fn all_typed_returns_schema() {
        let ds = dataset_with_columns(vec![
            Column::new("id").with_type("bigint").with_nullable(false),
            Column::new("name").with_type("text"),
        ])
        .await;
        let schema = declared_schema_for(&ds).expect("no error").expect("some");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert!(schema.field(1).is_nullable());
    }

    #[tokio::test]
    async fn invalid_type_returns_error() {
        let ds = dataset_with_columns(vec![Column::new("bad").with_type("not_a_type")]).await;
        let result = declared_schema_for(&ds);
        assert!(result.is_err(), "expected error, got {result:?}");
    }
}
