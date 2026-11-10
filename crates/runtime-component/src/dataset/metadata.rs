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

use std::sync::Arc;

use arrow::datatypes::Schema;
use data_components::object::metadata::MetadataColumn;
use datafusion::common::TableReference;

use super::{DatasetSpec, TimeFormat};

/// Pre-v2.0 name of the object-store listing metadata column `_last_modified`.
pub const LEGACY_LAST_MODIFIED_COLUMN: &str = "last_modified";

const LAST_MODIFIED_DOCS: &str =
    "https://spiceai.org/docs/features/data-acceleration/refresh-modes/append";

/// Time column settings after resolving the `_last_modified` metadata alias and
/// defaulting its `time_format` when unset.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTimeColumns {
    pub time_column: Option<String>,
    pub time_format: Option<TimeFormat>,
    pub time_partition_column: Option<String>,
    pub time_partition_format: Option<TimeFormat>,
}

fn requests_last_modified_metadata(column: &str, schema: &Schema) -> bool {
    column == MetadataColumn::LastModified.name()
        || (column == LEGACY_LAST_MODIFIED_COLUMN
            && schema.fields().find(LEGACY_LAST_MODIFIED_COLUMN).is_none())
}

fn resolve_time_column_alias(
    column: Option<&str>,
    time_format: Option<TimeFormat>,
    schema: &Schema,
    dataset_name: &TableReference,
    column_key: &str,
    format_key: &str,
) -> (Option<String>, Option<TimeFormat>) {
    let Some(column) = column else {
        return (None, time_format);
    };

    let metadata_name = MetadataColumn::LastModified.name();
    let mut resolved_column = column.to_string();
    let aliased = column == LEGACY_LAST_MODIFIED_COLUMN
        && schema.fields().find(LEGACY_LAST_MODIFIED_COLUMN).is_none()
        && schema.fields().find(metadata_name).is_some();
    if aliased {
        tracing::warn!(
            "Dataset '{dataset_name}' uses deprecated `{column_key}: {LEGACY_LAST_MODIFIED_COLUMN}`. The object-store metadata column was renamed to `{metadata_name}` in v2.0. Using `{metadata_name}`. Update the spicepod to `{column_key}: {metadata_name}` and `{format_key}: timestamptz`. See: {LAST_MODIFIED_DOCS}"
        );
        resolved_column = metadata_name.to_string();
    }

    let resolved_format = if resolved_column == metadata_name && time_format.is_none() {
        if !aliased {
            tracing::warn!(
                "Dataset '{dataset_name}' sets `{column_key}: {resolved_column}` without `{format_key}`. Defaulting `{format_key}` to `timestamptz` because the metadata column is Timestamp(µs, UTC). See: {LAST_MODIFIED_DOCS}"
            );
        }
        Some(TimeFormat::Timestamptz)
    } else {
        time_format
    };

    (Some(resolved_column), resolved_format)
}

impl DatasetSpec {
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

        let known_metadata_columns: &[&str] = &[
            MetadataColumn::LastModified.name(),
            MetadataColumn::Location(None).name(),
            MetadataColumn::Size.name(),
        ];
        for (key, value) in &self.metadata {
            // Only check "enabled" values — metadata can also contain arbitrary user-defined entries (e.g. instructions).
            if value == "enabled" && !known_metadata_columns.contains(&key.as_str()) {
                tracing::warn!(
                    "Dataset {}: '{key}: enabled' is not a recognized listing table metadata column and will be ignored. If this is a custom metadata entry, no action is needed. Otherwise, supported listing table metadata columns are: {known_metadata_columns:?}",
                    self.name
                );
            }
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

    #[must_use]
    pub fn needs_last_modified(&self, schema: &Schema) -> bool {
        let needs_last_modified_time_col = self
            .time_column
            .as_deref()
            .is_some_and(|col| requests_last_modified_metadata(col, schema))
            || self
                .time_partition_column
                .as_deref()
                .is_some_and(|col| requests_last_modified_metadata(col, schema));

        needs_last_modified_time_col
            && schema
                .fields()
                .find(MetadataColumn::LastModified.name())
                .is_none()
    }

    /// Resolves a legacy `last_modified` time column to `_last_modified` when the
    /// data column is absent and the listing metadata column is present, and
    /// defaults `time_format` to `timestamptz` for that metadata column when unset.
    #[must_use]
    pub fn resolve_time_columns(&self, schema: &Schema) -> ResolvedTimeColumns {
        let (time_column, time_format) = resolve_time_column_alias(
            self.time_column.as_deref(),
            self.time_format,
            schema,
            &self.name,
            "time_column",
            "time_format",
        );
        let (time_partition_column, time_partition_format) = resolve_time_column_alias(
            self.time_partition_column.as_deref(),
            self.time_partition_format,
            schema,
            &self.name,
            "time_partition_column",
            "time_partition_format",
        );
        ResolvedTimeColumns {
            time_column,
            time_format,
            time_partition_column,
            time_partition_format,
        }
    }

    // Checks if the metadata column is enabled for the dataset and if it is not already present in the schema
    #[must_use]
    pub fn metadata_column_enabled(&self, column: &str, schema: &Schema) -> bool {
        self.metadata
            .get(column)
            .is_some_and(|val| val == "enabled")
            && schema.fields().find(column).is_none()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion::common::TableReference;

    use super::*;
    use crate::dataset::DatasetSpec;

    fn spec_with_time_column(column: &str) -> DatasetSpec {
        let mut dataset = DatasetSpec::new("s3://bucket/data/", TableReference::bare("ev"));
        dataset.time_column = Some(column.to_string());
        dataset
    }

    fn schema(columns: &[(&str, DataType)]) -> Schema {
        Schema::new(
            columns
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    fn last_modified_type() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    #[test]
    fn needs_last_modified_legacy_name_when_data_column_absent() {
        let dataset = spec_with_time_column(LEGACY_LAST_MODIFIED_COLUMN);
        let schema = schema(&[("id", DataType::Int64)]);
        assert!(dataset.needs_last_modified(&schema));
    }

    #[test]
    fn needs_last_modified_false_when_legacy_name_is_a_data_column() {
        let dataset = spec_with_time_column(LEGACY_LAST_MODIFIED_COLUMN);
        let schema = schema(&[(LEGACY_LAST_MODIFIED_COLUMN, last_modified_type())]);
        assert!(!dataset.needs_last_modified(&schema));
    }

    #[test]
    fn resolve_aliases_legacy_last_modified_and_defaults_timestamptz() {
        let dataset = spec_with_time_column(LEGACY_LAST_MODIFIED_COLUMN);
        let schema = schema(&[
            ("id", DataType::Int64),
            (MetadataColumn::LastModified.name(), last_modified_type()),
        ]);
        let resolved = dataset.resolve_time_columns(&schema);
        assert_eq!(
            resolved.time_column.as_deref(),
            Some(MetadataColumn::LastModified.name())
        );
        assert_eq!(resolved.time_format, Some(TimeFormat::Timestamptz));
    }

    #[test]
    fn documented_last_modified_snippet_resolves_to_timestamptz() {
        // Documented pre-v2.0 snippet: time_column: last_modified, no time_format.
        let dataset = spec_with_time_column("last_modified");
        let schema = schema(&[
            ("id", DataType::Int64),
            ("_last_modified", last_modified_type()),
        ]);
        let resolved = dataset.resolve_time_columns(&schema);
        assert_eq!(resolved.time_column.as_deref(), Some("_last_modified"));
        assert_eq!(resolved.time_format, Some(TimeFormat::Timestamptz));
        assert!(resolved.time_partition_column.is_none());
        assert!(resolved.time_partition_format.is_none());
    }

    #[test]
    fn resolve_does_not_alias_when_legacy_name_exists_in_schema() {
        let dataset = spec_with_time_column(LEGACY_LAST_MODIFIED_COLUMN);
        let schema = schema(&[(LEGACY_LAST_MODIFIED_COLUMN, last_modified_type())]);
        let resolved = dataset.resolve_time_columns(&schema);
        assert_eq!(
            resolved.time_column.as_deref(),
            Some(LEGACY_LAST_MODIFIED_COLUMN)
        );
        assert_eq!(resolved.time_format, None);
    }

    #[test]
    fn resolve_defaults_timestamptz_for_metadata_column_without_format() {
        let dataset = spec_with_time_column(MetadataColumn::LastModified.name());
        let schema = schema(&[(MetadataColumn::LastModified.name(), last_modified_type())]);
        let resolved = dataset.resolve_time_columns(&schema);
        assert_eq!(
            resolved.time_column.as_deref(),
            Some(MetadataColumn::LastModified.name())
        );
        assert_eq!(resolved.time_format, Some(TimeFormat::Timestamptz));
    }

    #[test]
    fn resolve_does_not_override_explicit_time_format() {
        let mut dataset = spec_with_time_column(MetadataColumn::LastModified.name());
        dataset.time_format = Some(TimeFormat::Timestamp);
        let schema = schema(&[(MetadataColumn::LastModified.name(), last_modified_type())]);
        let resolved = dataset.resolve_time_columns(&schema);
        assert_eq!(resolved.time_format, Some(TimeFormat::Timestamp));
    }

    #[test]
    fn resolve_aliases_legacy_time_partition_column() {
        let mut dataset = DatasetSpec::new("s3://bucket/data/", TableReference::bare("ev"));
        dataset.time_column = Some("created_at".to_string());
        dataset.time_partition_column = Some(LEGACY_LAST_MODIFIED_COLUMN.to_string());
        let schema = schema(&[
            ("created_at", DataType::Utf8),
            (MetadataColumn::LastModified.name(), last_modified_type()),
        ]);
        let resolved = dataset.resolve_time_columns(&schema);
        assert_eq!(resolved.time_column.as_deref(), Some("created_at"));
        assert_eq!(resolved.time_format, None);
        assert_eq!(
            resolved.time_partition_column.as_deref(),
            Some(MetadataColumn::LastModified.name())
        );
        assert_eq!(
            resolved.time_partition_format,
            Some(TimeFormat::Timestamptz)
        );
    }
}
