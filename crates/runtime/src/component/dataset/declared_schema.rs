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

//! Build an Arrow `Schema` from a dataset's `columns[].type` declarations.
//!
//! Returns `None` if any column lacks an explicit `type`. The runtime
//! treats this as "the user did not declare a full schema" and falls
//! back to source-derived schema inference (today's path).
//!
//! Used by `HttpsFactory::static_schema` to decide
//! whether a dataset can be registered with a placeholder
//! for a given dataset.

use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use snafu::{ResultExt, Snafu};

use crate::component::dataset::Dataset;
use crate::component::dataset::declared_type::{ParseTypeError, parse_declared_type};

#[derive(Debug, Snafu)]
pub enum DeclaredSchemaError {
    #[snafu(display(
        "Could not parse declared type for column `{column}` of dataset `{dataset}`: {source}"
    ))]
    InvalidColumnType {
        dataset: String,
        column: String,
        source: ParseTypeError,
    },
}

/// Build an Arrow schema from a dataset's `columns[]`, if every column
/// has an explicit `type`. Returns `None` otherwise.
///
/// `nullable` defaults to `true` when not specified — Arrow's standard
/// "unknown nullability is nullable" convention.
pub fn declared_schema_for(dataset: &Dataset) -> Result<Option<SchemaRef>, DeclaredSchemaError> {
    if dataset.columns.is_empty() {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(dataset.columns.len());
    for column in &dataset.columns {
        let Some(type_str) = column.r#type.as_deref() else {
            return Ok(None);
        };
        let dt = parse_declared_type(type_str).context(InvalidColumnTypeSnafu {
            dataset: dataset.name.to_string(),
            column: column.name.clone(),
        })?;
        let nullable = column.nullable.unwrap_or(true);
        fields.push(Field::new(&column.name, dt, nullable));
    }

    Ok(Some(Arc::new(Schema::new(fields))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use spicepod::semantic::Column;

    async fn dataset_with_columns(cols: Vec<Column>) -> Dataset {
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
