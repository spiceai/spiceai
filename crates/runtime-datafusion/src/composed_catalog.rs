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

//! A [`CatalogProvider`] that composes an external catalog (e.g., Iceberg)
//! with internal Spice schemas (`runtime`, `metadata`, `eval`, `scp`).
//!
//! When a user-defined catalog overrides the default `spice` catalog,
//! internal schemas must be preserved alongside the external catalog's schemas.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::Result as DFResult;

/// Internal schema names that are managed by Spice and must not be
/// overridden by external catalogs.
const INTERNAL_SCHEMA_NAMES: &[&str] = &["runtime", "metadata", "eval", "scp"];

/// A [`CatalogProvider`] that composes an external catalog with
/// Spice's internal schemas.
///
/// Internal schemas (e.g., `runtime`, `metadata`, `eval`, `scp`) take
/// priority over external schemas with the same name, preventing user
/// schemas from shadowing system schemas.
///
/// Schema registration for non-internal names is delegated to the
/// external catalog (enabling `CREATE SCHEMA` support).
#[derive(Debug)]
pub struct ComposedCatalogProvider {
    /// The external catalog (e.g., Iceberg) — provides user schemas.
    external: Arc<dyn CatalogProvider>,
    /// Internal schemas that must always be present.
    internal_schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl ComposedCatalogProvider {
    /// Create a new `ComposedCatalogProvider` that wraps `external` with
    /// the given `internal_schemas`.
    #[must_use]
    pub fn new(
        external: Arc<dyn CatalogProvider>,
        internal_schemas: HashMap<String, Arc<dyn SchemaProvider>>,
    ) -> Self {
        Self {
            external,
            internal_schemas,
        }
    }

    /// Returns a reference to the external catalog provider.
    #[must_use]
    pub fn external(&self) -> &Arc<dyn CatalogProvider> {
        &self.external
    }

    /// Returns `true` if `name` is an internal schema name.
    fn is_internal_schema(name: &str) -> bool {
        INTERNAL_SCHEMA_NAMES.contains(&name)
    }
}

impl CatalogProvider for ComposedCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.internal_schemas.keys().cloned().collect();
        for name in self.external.schema_names() {
            if !self.internal_schemas.contains_key(&name) {
                names.push(name);
            }
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Internal schemas take priority
        if let Some(schema) = self.internal_schemas.get(name) {
            return Some(Arc::clone(schema));
        }
        self.external.schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        if Self::is_internal_schema(name) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Cannot register schema '{name}': it is a reserved Spice internal schema"
            )));
        }
        self.external.register_schema(name, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::catalog::MemoryCatalogProvider;
    use datafusion::catalog::MemorySchemaProvider;

    fn make_test_provider() -> ComposedCatalogProvider {
        let external = Arc::new(MemoryCatalogProvider::new());
        external
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .expect("register public schema");
        external
            .register_schema("user_schema", Arc::new(MemorySchemaProvider::new()))
            .expect("register user_schema");

        let mut internal = HashMap::new();
        internal.insert(
            "runtime".to_string(),
            Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>,
        );
        internal.insert(
            "metadata".to_string(),
            Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>,
        );

        ComposedCatalogProvider::new(external, internal)
    }

    #[test]
    fn test_schema_names_union() {
        let provider = make_test_provider();
        let mut names = provider.schema_names();
        names.sort();
        assert_eq!(names, vec!["metadata", "public", "runtime", "user_schema"]);
    }

    #[test]
    fn test_internal_schema_priority() {
        let provider = make_test_provider();
        // Internal schemas should be retrievable
        assert!(provider.schema("runtime").is_some());
        assert!(provider.schema("metadata").is_some());
        // External schemas should also be retrievable
        assert!(provider.schema("public").is_some());
        assert!(provider.schema("user_schema").is_some());
        // Non-existent schema
        assert!(provider.schema("nonexistent").is_none());
    }

    #[test]
    fn test_register_internal_schema_rejected() {
        let provider = make_test_provider();
        let result = provider.register_schema("runtime", Arc::new(MemorySchemaProvider::new()));
        assert!(result.is_err());
        let err = result
            .expect_err("should reject internal schema")
            .to_string();
        assert!(err.contains("reserved Spice internal schema"));
    }

    #[test]
    fn test_register_user_schema() {
        let provider = make_test_provider();
        let result = provider.register_schema("new_schema", Arc::new(MemorySchemaProvider::new()));
        result.expect("register user schema should succeed");
        assert!(provider.schema("new_schema").is_some());
    }
}
