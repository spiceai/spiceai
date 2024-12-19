/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::{datafusion::is_spice_internal_schema, DataFusion};
use axum::{
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
    Extension, Json,
};
use error::IcebergResponseError;
use namespace::Namespace;
use serde::{self, Deserialize, Serialize};

mod error;
mod namespace;

pub(crate) async fn get_config() -> &'static str {
    r#"{
  "overrides": {},
  "defaults": {},
  "endpoints": [
    "GET /v1/iceberg/namespaces",
    "HEAD /v1/iceberg/namespaces/{namespace}",
    "GET /v1/iceberg/namespaces/{namespace}/tables",
    "HEAD /v1/iceberg/namespaces/{namespace}/tables/{table}",
    "GET /v1/iceberg/namespaces/{namespace}/tables/{table}"
  ]
}"#
}

#[derive(Debug, Deserialize)]
pub(crate) struct ParentNamespaceQueryParams {
    #[serde(default)]
    parent: Namespace,
}

#[derive(Debug, Serialize)]
struct NamespacesResponse {
    namespaces: Vec<Namespace>,
}

pub(crate) async fn get_namespaces(
    Extension(datafusion): Extension<Arc<DataFusion>>,
    Query(params): Query<ParentNamespaceQueryParams>,
) -> Response {
    let catalog_names = datafusion.ctx.catalog_names();

    match params.parent.parts.len() {
        0 => {
            let namespaces = catalog_names
                .into_iter()
                .map(Namespace::from_single_part)
                .collect();
            (
                status::StatusCode::OK,
                Json(NamespacesResponse { namespaces }),
            )
                .into_response()
        }
        1 => {
            // The user has specified a single namespace, so we want to return all of the schemas in that namespace
            let catalog_name = params.parent.parts[0].clone();
            let Some(catalog) = datafusion.ctx.catalog(catalog_name.as_str()) else {
                return IcebergResponseError::no_such_namespace(
                    "Namespace provided in the parent does not exist".to_string(),
                )
                .into_response();
            };

            let schema_names = catalog.schema_names().into_iter().filter(|schema_name| {
                !is_spice_internal_schema(catalog_name.as_str(), schema_name)
            });
            let namespaces = schema_names
                .map(|schema_name| Namespace::from_parts(vec![catalog_name.clone(), schema_name]))
                .collect();
            (
                status::StatusCode::OK,
                Json(NamespacesResponse { namespaces }),
            )
                .into_response()
        }
        2 => {
            let catalog_name = params.parent.parts[0].clone();
            let schema_name = params.parent.parts[1].clone();
            let Some(catalog) = datafusion.ctx.catalog(catalog_name.as_str()) else {
                return IcebergResponseError::no_such_namespace(
                    "Namespace provided in the parent does not exist".to_string(),
                )
                .into_response();
            };
            let mut schema_names = catalog.schema_names().into_iter().filter(|schema_name| {
                !is_spice_internal_schema(catalog_name.as_str(), schema_name)
            });
            if !schema_names.any(|s| s == schema_name) {
                return IcebergResponseError::no_such_namespace(
                    "Namespace provided in the parent does not exist".to_string(),
                )
                .into_response();
            }

            // There are no namespaces under this schema, so we return an empty list
            (
                status::StatusCode::OK,
                Json(NamespacesResponse { namespaces: vec![] }),
            )
                .into_response()
        }
        3.. => IcebergResponseError::no_such_namespace(
            "Namespace provided in the parent does not exist".to_string(),
        )
        .into_response(),
    }
}
