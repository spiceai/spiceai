/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Connector-agnostic parser that turns a dataset's `columns:` block into a
//! [`SchemaProjection`]. Replaces and de-duplicates the per-connector
//! `parse_json_nesting_static_fields` (`DynamoDB`) and `parse_http_json_nesting`
//! (HTTP) helpers.

use data_components::schema_projection::{
    ColumnSource, JSON_OBJECT_MARKER, JSON_OBJECT_WILDCARD, ProjectedColumn, SchemaProjection,
    SchemaProjectionError,
};
use serde_json::Value;

use crate::component::dataset::Dataset;
use crate::component::dataset::declared_type::parse_declared_type;
use crate::dataconnector::{ConnectorComponent, DataConnectorError, DataConnectorResult};

/// Per-connector knobs for parsing a projection.
pub struct ProjectionPolicy<'a> {
    /// Connector name, used only for error messages (e.g. `"dynamodb"`).
    connector: &'a str,
    /// Columns that must be declared explicitly (e.g. primary-key / CDC-key
    /// columns) and may not be folded into the `json_object` catch-all.
    required_columns: Vec<String>,
}

impl<'a> ProjectionPolicy<'a> {
    #[must_use]
    pub fn new(connector: &'a str) -> Self {
        Self {
            connector,
            required_columns: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_required_columns(mut self, columns: Vec<String>) -> Self {
        self.required_columns = columns;
        self
    }
}

/// Parse a dataset's `columns:` into a [`SchemaProjection`].
///
/// Returns `Ok(None)` when the dataset declares no columns at all (the
/// projection would be a no-op and callers can keep their existing inference
/// path). Otherwise every declared column becomes a [`ProjectedColumn`]:
///
/// - a column carrying `metadata.json_object: "*"` becomes the catch-all
///   ([`ColumnSource::JsonObject`]); only `"*"` is accepted;
/// - any other column becomes a kept [`ColumnSource::Field`] read by its name.
///
/// # Errors
/// Returns an invalid-configuration error for a non-`"*"` catch-all marker, an
/// unparseable declared `type`, or any structural problem surfaced by
/// [`SchemaProjection::new`] (multiple catch-alls, duplicate names, a required
/// column folded into the catch-all).
pub fn parse_schema_projection(
    dataset: &Dataset,
    policy: &ProjectionPolicy<'_>,
) -> DataConnectorResult<Option<SchemaProjection>> {
    if dataset.columns.is_empty() {
        return Ok(None);
    }

    let mut columns = Vec::with_capacity(dataset.columns.len());
    for column in &dataset.columns {
        let catch_all_marker = column.metadata.get(JSON_OBJECT_MARKER);

        let declared_type = match column.r#type.as_deref() {
            Some(type_str) => Some(parse_declared_type(type_str).map_err(|source| {
                invalid(
                    policy,
                    dataset,
                    format!("Column '{}' has an invalid 'type': {source}", column.name),
                )
            })?),
            None => None,
        };
        let nullable = column.nullable.unwrap_or(true);

        let source = if let Some(marker) = catch_all_marker {
            // Validate the marker value is exactly "*".
            let is_wildcard = matches!(marker, Value::String(s) if s == JSON_OBJECT_WILDCARD);
            if !is_wildcard {
                return Err(invalid(
                    policy,
                    dataset,
                    format!(
                        "Column '{}' has invalid '{JSON_OBJECT_MARKER}' value: {marker:?}. Only '{JSON_OBJECT_WILDCARD}' is supported.",
                        column.name
                    ),
                ));
            }
            ColumnSource::JsonObject
        } else {
            ColumnSource::Field
        };

        columns.push(ProjectedColumn {
            output_name: column.name.clone(),
            source,
            declared_type,
            nullable,
        });
    }

    let projection = SchemaProjection::new(columns, &policy.required_columns)
        .map_err(|e| projection_error(policy, dataset, &e))?;
    Ok(Some(projection))
}

fn invalid(
    policy: &ProjectionPolicy<'_>,
    dataset: &Dataset,
    message: String,
) -> DataConnectorError {
    DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: policy.connector.to_string(),
        connector_component: ConnectorComponent::from(dataset),
        message,
    }
}

fn projection_error(
    policy: &ProjectionPolicy<'_>,
    dataset: &Dataset,
    err: &SchemaProjectionError,
) -> DataConnectorError {
    invalid(policy, dataset, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use spicepod::semantic::Column;
    use std::collections::HashMap;

    async fn dataset_with_columns(cols: Vec<Column>) -> Dataset {
        let app = std::sync::Arc::new(AppBuilder::new("test").build());
        let rt = std::sync::Arc::new(crate::Runtime::builder().build().await);
        let mut ds = DatasetBuilder::try_new("dynamodb:tbl".to_string(), "tbl")
            .expect("builder")
            .with_app(app)
            .with_runtime(rt)
            .build()
            .expect("dataset");
        ds.columns = cols;
        ds
    }

    fn catch_all_column(name: &str) -> Column {
        let mut metadata = HashMap::new();
        metadata.insert("json_object".to_string(), Value::String("*".to_string()));
        Column::new(name).with_metadata(metadata)
    }

    #[tokio::test]
    async fn no_columns_returns_none() {
        let ds = dataset_with_columns(vec![]).await;
        let policy = ProjectionPolicy::new("dynamodb");
        assert!(parse_schema_projection(&ds, &policy).expect("ok").is_none());
    }

    #[tokio::test]
    async fn parses_nesting() {
        let ds = dataset_with_columns(vec![
            Column::new("id").with_type("bigint"),
            Column::new("title"),
            catch_all_column("data"),
        ])
        .await;
        let policy = ProjectionPolicy::new("dynamodb");
        let proj = parse_schema_projection(&ds, &policy)
            .expect("ok")
            .expect("some");
        assert!(proj.has_catch_all());
        assert!(!proj.is_identity());
        assert_eq!(proj.columns().len(), 3);
    }

    #[tokio::test]
    async fn rejects_non_wildcard_marker() {
        let mut metadata = HashMap::new();
        metadata.insert("json_object".to_string(), Value::String("nope".to_string()));
        let ds = dataset_with_columns(vec![Column::new("data").with_metadata(metadata)]).await;
        let policy = ProjectionPolicy::new("dynamodb");
        parse_schema_projection(&ds, &policy).expect_err("non-'*' marker should error");
    }

    #[tokio::test]
    async fn enforces_required_pk_declared() {
        let ds = dataset_with_columns(vec![catch_all_column("data")]).await;
        let policy =
            ProjectionPolicy::new("debezium").with_required_columns(vec!["id".to_string()]);
        // `id` is required but only the catch-all is declared.
        parse_schema_projection(&ds, &policy)
            .expect_err("required PK folded into catch-all should error");
    }

    #[tokio::test]
    async fn identity_projection_for_typed_columns() {
        let ds = dataset_with_columns(vec![
            Column::new("id").with_type("bigint"),
            Column::new("name").with_type("text"),
        ])
        .await;
        let policy = ProjectionPolicy::new("dynamodb");
        let proj = parse_schema_projection(&ds, &policy)
            .expect("ok")
            .expect("some");
        // pure type-pinning, no catch-all → identity (rows untouched)
        assert!(proj.is_identity());
        assert!(!proj.has_catch_all());
    }
}
