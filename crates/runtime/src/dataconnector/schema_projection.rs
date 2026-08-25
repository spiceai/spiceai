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

//! Re-export of the declared-columns projection parser, which lives in
//! `data-connector-api` so a connector can reach it without the runtime.
//!
//! [`parse_schema_projection`] takes a `&DatasetSpec`; a `Dataset` coerces to one
//! through its `Deref`, so call sites here and in the connectors are unchanged.
//! The tests stay on this side because building a `DatasetSpec` by hand is a
//! 20-plus-field literal, while `DatasetBuilder` is right here.

#[cfg(any(feature = "debezium", test))]
pub use data_connector_api::schema_projection::{ProjectionPolicy, parse_schema_projection};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::Dataset;
    use crate::component::dataset::builder::DatasetBuilder;
    use app::AppBuilder;
    use serde_json::Value;
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
