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

//! Analyzer rule that intercepts DDL plans (`CREATE TABLE` / `DROP TABLE` / `CREATE SCHEMA`)
//! targeting Cayenne-backed DDL-enabled catalogs and rewrites them into
//! custom [`LogicalPlan::Extension`] nodes.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, RwLock, Weak};

use datafusion::catalog::CatalogProviderList;
use datafusion::common::Constraint;
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::DdlStatement;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;

use super::is_cayenne_catalog;
use super::logical_nodes::{CayenneCreateSchemaNode, CayenneCreateTableNode, CayenneDropTableNode};
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

/// Extract primary key column names from `DataFusion` [`Constraints`] using the
/// Arrow schema to resolve column indices to names.
fn extract_primary_key_columns(
    constraints: &datafusion::common::Constraints,
    arrow_schema: &arrow::datatypes::Schema,
) -> Vec<String> {
    constraints
        .iter()
        .find_map(|c| {
            if let Constraint::PrimaryKey(indices) = c {
                Some(indices)
            } else {
                None
            }
        })
        .map(|indices| {
            let fields = arrow_schema.fields();
            indices
                .iter()
                .filter_map(|&idx| fields.get(idx).map(|field| field.name().clone()))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default()
}

fn parse_qualified_schema_name(name: &str) -> (String, String) {
    match name.split_once('.') {
        Some((catalog_name, schema_name)) => (catalog_name.to_string(), schema_name.to_string()),
        None => (SPICE_DEFAULT_CATALOG.to_string(), name.to_string()),
    }
}

/// Analyzer rule that rewrites DDL targeting Cayenne catalogs into
/// custom extension nodes for Cayenne catalog operations.
///
/// Uses `Weak` references to avoid reference cycles.
pub struct CayenneDdlAnalyzerRule {
    /// Weak reference to the catalog list for catalog resolution.
    catalog_list: Weak<dyn CatalogProviderList>,
    /// Weak reference to the set of DDL-enabled catalog names.
    ddl_enabled_catalogs: Weak<RwLock<HashSet<String>>>,
}

impl fmt::Debug for CayenneDdlAnalyzerRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneDdlAnalyzerRule")
            .finish_non_exhaustive()
    }
}

impl CayenneDdlAnalyzerRule {
    #[must_use]
    pub fn new(
        catalog_list: &Arc<dyn CatalogProviderList>,
        ddl_enabled_catalogs: &Arc<RwLock<HashSet<String>>>,
    ) -> Self {
        Self {
            catalog_list: Arc::downgrade(catalog_list),
            ddl_enabled_catalogs: Arc::downgrade(ddl_enabled_catalogs),
        }
    }

    fn is_ddl_enabled(&self, catalog_name: &str) -> bool {
        self.ddl_enabled_catalogs
            .upgrade()
            .and_then(|catalogs| catalogs.read().ok().map(|set| set.contains(catalog_name)))
            .unwrap_or(false)
    }

    /// Check if the given catalog is backed by a Cayenne catalog provider.
    fn is_cayenne_backed(&self, catalog_name: &str) -> bool {
        let Some(catalog_list) = self.catalog_list.upgrade() else {
            return false;
        };
        let Some(df_catalog) = catalog_list.catalog(catalog_name) else {
            return false;
        };
        is_cayenne_catalog(df_catalog.as_ref())
    }
}

impl AnalyzerRule for CayenneDdlAnalyzerRule {
    fn name(&self) -> &'static str {
        "cayenne_ddl_rewrite"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        match &plan {
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(create)) => {
                let catalog_name = create
                    .name
                    .catalog()
                    .unwrap_or(SPICE_DEFAULT_CATALOG)
                    .to_string();

                if !self.is_ddl_enabled(&catalog_name) {
                    return Ok(plan);
                }

                if !self.is_cayenne_backed(&catalog_name) {
                    return Ok(plan);
                }

                let schema_name = create
                    .name
                    .schema()
                    .unwrap_or(SPICE_DEFAULT_SCHEMA)
                    .to_string();
                let table_name = create.name.table().to_string();

                // Extract the Arrow schema from the logical plan's input
                let arrow_schema = Arc::new(create.input.schema().inner().as_ref().clone());

                let primary_key = extract_primary_key_columns(&create.constraints, &arrow_schema);

                let node = CayenneCreateTableNode::new(
                    table_name,
                    arrow_schema,
                    create.if_not_exists,
                    create.or_replace,
                    catalog_name,
                    schema_name,
                    primary_key,
                );

                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }
            LogicalPlan::Ddl(DdlStatement::DropTable(drop)) => {
                let catalog_name = drop
                    .name
                    .catalog()
                    .unwrap_or(SPICE_DEFAULT_CATALOG)
                    .to_string();

                if !self.is_ddl_enabled(&catalog_name) {
                    return Ok(plan);
                }

                if !self.is_cayenne_backed(&catalog_name) {
                    return Ok(plan);
                }

                let schema_name = drop
                    .name
                    .schema()
                    .unwrap_or(SPICE_DEFAULT_SCHEMA)
                    .to_string();
                let table_name = drop.name.table().to_string();

                let node = CayenneDropTableNode::new(
                    table_name,
                    drop.if_exists,
                    catalog_name,
                    schema_name,
                );

                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(create)) => {
                let (catalog_name, schema_name) =
                    parse_qualified_schema_name(create.schema_name.as_str());

                if !self.is_ddl_enabled(&catalog_name) {
                    return Ok(plan);
                }

                if !self.is_cayenne_backed(&catalog_name) {
                    return Ok(plan);
                }

                let node =
                    CayenneCreateSchemaNode::new(schema_name, create.if_not_exists, catalog_name);

                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }))
            }
            _ => Ok(plan),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Constraint, Constraints};

    use super::{extract_primary_key_columns, parse_qualified_schema_name};
    use crate::datafusion::SPICE_DEFAULT_CATALOG;

    #[test]
    fn parse_qualified_schema_name_extracts_catalog_and_schema() {
        let (catalog, schema) = parse_qualified_schema_name("spicebench.bench");
        assert_eq!(catalog, "spicebench");
        assert_eq!(schema, "bench");
    }

    #[test]
    fn parse_qualified_schema_name_uses_default_catalog_when_unqualified() {
        let (catalog, schema) = parse_qualified_schema_name("bench");
        assert_eq!(catalog, SPICE_DEFAULT_CATALOG);
        assert_eq!(schema, "bench");
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ])
    }

    #[test]
    fn extract_primary_key_single_column() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]);
        let pk = extract_primary_key_columns(&constraints, &schema);
        assert_eq!(pk, vec!["id"]);
    }

    #[test]
    fn extract_primary_key_composite() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 2])]);
        let pk = extract_primary_key_columns(&constraints, &schema);
        assert_eq!(pk, vec!["id", "email"]);
    }

    #[test]
    fn extract_primary_key_none_when_no_constraints() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![]);
        let pk = extract_primary_key_columns(&constraints, &schema);
        assert!(pk.is_empty());
    }

    #[test]
    fn extract_primary_key_none_when_only_unique_constraint() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![Constraint::Unique(vec![1])]);
        let pk = extract_primary_key_columns(&constraints, &schema);
        assert!(pk.is_empty());
    }

    #[test]
    fn extract_primary_key_ignores_unique_uses_first_pk() {
        let schema = test_schema();
        let constraints = Constraints::new_unverified(vec![
            Constraint::Unique(vec![2]),
            Constraint::PrimaryKey(vec![0]),
        ]);
        let pk = extract_primary_key_columns(&constraints, &schema);
        assert_eq!(pk, vec!["id"]);
    }
}
