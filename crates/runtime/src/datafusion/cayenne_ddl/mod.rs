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

//! Cayenne DDL support: analyzer rule, logical nodes, extension planner,
//! and physical execution plans for `CREATE TABLE` / `DROP TABLE` / `CREATE SCHEMA` on
//! Cayenne-backed DDL-enabled catalogs.
//!
//! Reuses the shared DDL infrastructure from [`super::ddl`].

pub mod analyzer_rule;
pub mod logical_nodes;
pub mod physical_plans;
pub mod planner;

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::common::Constraint;
use datafusion::error::DataFusionError;
use datafusion::sql::{ResolvedTableReference, TableReference};

use super::composed_catalog::ComposedCatalogProvider;
use crate::catalogconnector::PartitionAwareCatalog;
use crate::catalogconnector::cayenne::provider::CayenneCatalogProvider;
use crate::datafusion::cayenne_ddl::physical_plans::arrow_datatype_to_sql;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

/// Check whether the given catalog provider is a Cayenne-backed catalog.
pub fn is_cayenne_catalog(provider: &dyn CatalogProvider) -> bool {
    get_cayenne_provider(provider).is_some()
}

/// Extract the [`CayenneCatalogProvider`] reference from a `CatalogProvider`.
///
/// Handles both direct `CayenneCatalogProvider` and `ComposedCatalogProvider`
/// wrapping a `CayenneCatalogProvider`.
pub fn get_cayenne_provider(provider: &dyn CatalogProvider) -> Option<&CayenneCatalogProvider> {
    if let Some(cayenne) = provider.as_any().downcast_ref::<CayenneCatalogProvider>() {
        return Some(cayenne);
    }
    if let Some(composed) = provider.as_any().downcast_ref::<ComposedCatalogProvider>() {
        return composed
            .external()
            .as_any()
            .downcast_ref::<CayenneCatalogProvider>();
    }

    None
}

/// If the catalog provider is Cayenne-backed and implements [`PartitionAwareCatalog`],
/// return a trait reference.
///
/// Handles both direct [`CayenneCatalogProvider`] providers and
/// [`ComposedCatalogProvider`] wrappers whose external provider is a
/// [`CayenneCatalogProvider`].
pub fn as_partition_aware(provider: &dyn CatalogProvider) -> Option<&dyn PartitionAwareCatalog> {
    let cayenne_catalog = get_cayenne_provider(provider)?;
    Some(cayenne_catalog as &dyn PartitionAwareCatalog)
}

/// Constructs a  `CREATE TABLE IF NOT EXISTS` DDL SQL query for the provided [`TableReference`].
pub async fn create_table_if_not_exists(
    tbl: &TableReference,
    provider: &Arc<dyn TableProvider>,
) -> Result<String, DataFusionError> {
    let ResolvedTableReference {
        catalog,
        schema,
        table,
    } = tbl
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
    let table_schema = provider.schema();

    let columns_sql: Vec<String> = table_schema
        .fields()
        .iter()
        .map(|f| {
            let null_str = if f.is_nullable() { "" } else { " NOT NULL" };
            let sql_type = arrow_datatype_to_sql(f.data_type())?;
            Ok::<String, DataFusionError>(format!("\"{}\" {sql_type}{null_str}", f.name()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut table_elements = columns_sql;

    let primary_key: Vec<String> = provider
        .constraints()
        .and_then(|c| {
            c.iter().find_map(|cc| {
                if let Constraint::PrimaryKey(v) = cc {
                    Some(
                        v.iter()
                            .map(|i| table_schema.field(*i).name().clone())
                            .collect(),
                    )
                } else {
                    None
                }
            })
        })
        .unwrap_or_default();

    if !primary_key.is_empty() {
        let pk_cols = primary_key
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        table_elements.push(format!("PRIMARY KEY ({pk_cols})"));
    }

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS \"{catalog}\".\"{schema}\".\"{table}\" ({})",
        table_elements.join(", ")
    ))
}
