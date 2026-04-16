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

//! Iceberg DDL support: handler, physical execution plans for
//! `CREATE TABLE` / `DROP TABLE` / `CREATE SCHEMA` on Iceberg-backed catalogs.
//!
//! DDL interception is handled by `datafusion_ddl::DdlAnalyzerRule` paired with
//! [`IcebergDdlHandler`].  Physical plans live in [`physical_plans`].

pub mod handler;
pub mod physical_plans;

pub use handler::IcebergDdlHandler;

/// Re-exported DDL option types for use within `iceberg_ddl`.
pub mod acceleration_options {
    pub use datafusion_ddl::{
        CreateTableStatementExtension, DatasetOptions, DdlExtensionStore, SharedDdlExtensionStore,
        new_shared_store, parse_acceleration_options, parse_dataset_options,
        parse_ddl_table_options,
    };
}

// Re-exported for the physical plans.
pub use acceleration_options::DatasetOptions;

use std::sync::{Arc, OnceLock, Weak};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use data_components::iceberg::provider::IcebergCatalogProvider;
use datafusion::catalog::CatalogProvider;

use super::DataFusion;
use super::composed_catalog::ComposedCatalogProvider;

/// Coerce Arrow data types that are not natively supported by iceberg-rust's
/// `arrow_schema_to_schema` into their closest Iceberg-compatible equivalents.
///
/// The following coercions are applied (top-level fields only):
///
/// | Arrow type | Coerced to | Reason |
/// |---|---|---|
/// | `Timestamp(Second\|Millisecond\|Nanosecond, tz)` | `Timestamp(Microsecond, tz)` | Iceberg v2 does not support `timestamp_ns`.|
/// | `Date64` | `Date32` | iceberg-rust only maps `Date32` |
/// | `Time32(*)` | `Time64(Microsecond)` | iceberg-rust only maps `Time64(Microsecond)` |
/// | `Time64(Nanosecond)` | `Time64(Microsecond)` | Same |
pub(crate) fn coerce_arrow_schema_for_iceberg_v2(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let coerced = match f.data_type() {
                DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
                }
                DataType::Date64 => Some(DataType::Date32),
                DataType::Time32(_) | DataType::Time64(TimeUnit::Nanosecond) => {
                    Some(DataType::Time64(TimeUnit::Microsecond))
                }
                _ => None,
            };
            match coerced {
                Some(dt) => f.as_ref().clone().with_data_type(dt),
                None => f.as_ref().clone(),
            }
        })
        .collect();
    ArrowSchema::new_with_metadata(fields, schema.metadata().clone())
}

/// A shared, lazily-initialized weak reference to the [`DataFusion`] instance.
///
/// Created at build time and shared between the extension planner and the
/// `DataFusion` struct.  The `OnceLock` is populated once the `DataFusion` is
/// wrapped in an `Arc` (see [`DataFusion::set_self_ref`]).
pub type SharedDataFusionRef = Arc<OnceLock<Weak<DataFusion>>>;

/// Create a new, empty [`SharedDataFusionRef`].
#[must_use]
pub fn new_shared_datafusion_ref() -> SharedDataFusionRef {
    Arc::new(OnceLock::new())
}

/// Try to extract the Iceberg catalog from a `CatalogProvider`.
/// Handles both direct `IcebergCatalogProvider` and `ComposedCatalogProvider`
/// wrapping an `IcebergCatalogProvider`.
pub fn composed_catalog_to_iceberg(
    provider: &dyn CatalogProvider,
) -> Option<Arc<dyn iceberg::Catalog>> {
    // Try direct downcast
    if let Some(iceberg) = provider.as_any().downcast_ref::<IcebergCatalogProvider>() {
        return Some(Arc::clone(iceberg.catalog()));
    }
    // Try via ComposedCatalogProvider
    if let Some(composed) = provider.as_any().downcast_ref::<ComposedCatalogProvider>()
        && let Some(iceberg) = composed
            .external()
            .as_any()
            .downcast_ref::<IcebergCatalogProvider>()
    {
        return Some(Arc::clone(iceberg.catalog()));
    }
    None
}
