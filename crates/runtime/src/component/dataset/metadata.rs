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

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_datasource::metadata::MetadataColumn;

use super::Dataset;

impl Dataset {
    /// Returns which `ListingTable` metadata columns are enabled for this dataset.
    #[must_use]
    pub fn listing_table_metadata_columns(
        &self,
        url_prefix: impl Into<Arc<str>>,
        schema: &Schema,
    ) -> Option<Vec<MetadataColumn>> {
        let needs_last_modified = self.needs_last_modified(schema);
        // Handle the common case where no metadata columns are enabled
        if !needs_last_modified && self.metadata.is_empty() {
            return None;
        }

        let mut columns = Vec::new();

        if self.metadata_column_enabled(MetadataColumn::LastModified.name(), schema)
            || needs_last_modified
        {
            columns.push(MetadataColumn::LastModified);
        }

        if self.metadata_column_enabled(MetadataColumn::Location(None).name(), schema) {
            columns.push(MetadataColumn::Location(Some(url_prefix.into())));
        }

        if self.metadata_column_enabled(MetadataColumn::Size.name(), schema) {
            columns.push(MetadataColumn::Size);
        }

        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    fn needs_last_modified(&self, schema: &Schema) -> bool {
        let needs_last_modified_time_col = self
            .time_column
            .as_ref()
            .is_some_and(|col| col == MetadataColumn::LastModified.name())
            || self
                .time_partition_column
                .as_ref()
                .is_some_and(|col| col == MetadataColumn::LastModified.name());

        needs_last_modified_time_col
            && schema
                .fields()
                .find(MetadataColumn::LastModified.name())
                .is_none()
    }

    // Checks if the metadata column is enabled for the dataset and if it is not already present in the schema
    fn metadata_column_enabled(&self, column: &str, schema: &Schema) -> bool {
        self.metadata
            .get(column)
            .is_some_and(|val| val == "enabled")
            && schema.fields().find(column).is_none()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{Runtime, builder::RuntimeBuilder, component::dataset::builder::DatasetBuilder};
    use app::{App, AppBuilder};
    use arrow::datatypes::{DataType, Field};

    use super::*;

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
        assert_eq!(MetadataColumn::LastModified.name(), "last_modified");
        assert_eq!(MetadataColumn::Location(None).name(), "location");
        assert_eq!(MetadataColumn::Size.name(), "size");
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
            .with_time_column("last_modified".to_string())
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
            .with_time_partition_column("last_modified".to_string())
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
            .with_time_column("last_modified".to_string())
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
            .with_time_column("last_modified".to_string())
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
            .with_time_column("last_modified".to_string())
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
            .with_time_column("last_modified".to_string())
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
