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

use super::DatasetSpec;

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
    #[must_use]
    pub fn metadata_column_enabled(&self, column: &str, schema: &Schema) -> bool {
        self.metadata
            .get(column)
            .is_some_and(|val| val == "enabled")
            && schema.fields().find(column).is_none()
    }
}
