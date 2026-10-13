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
use crate::catalogconnector::RefreshingCatalogProvider;

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
        .map(
            |f| match coerce_temporal_type_for_iceberg_v2(f.data_type()) {
                Some(dt) => f.as_ref().clone().with_data_type(dt),
                None => f.as_ref().clone(),
            },
        )
        .collect();
    ArrowSchema::new_with_metadata(fields, schema.metadata().clone())
}

/// The temporal coercions of [`coerce_arrow_schema_for_iceberg_v2`] for a single
/// (non-nested) data type, or `None` when the type needs no coercion.
fn coerce_temporal_type_for_iceberg_v2(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
        }
        DataType::Date64 => Some(DataType::Date32),
        DataType::Time32(_) | DataType::Time64(TimeUnit::Nanosecond) => {
            Some(DataType::Time64(TimeUnit::Microsecond))
        }
        _ => None,
    }
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

/// Extract a concrete [`IcebergCatalogProvider`] reference, peeling the runtime's
/// transparent catalog wrappers ([`RefreshingCatalogProvider`] and
/// [`ComposedCatalogProvider`]) in any nesting order.
///
/// `DataFusion` 54 removed `CatalogProvider::as_any`, which these wrappers used to
/// delegate to their inner provider so that `downcast_ref::<IcebergCatalogProvider>()`
/// transparently saw through them. The wrappers must now be peeled explicitly.
pub fn iceberg_provider_ref(provider: &dyn CatalogProvider) -> Option<&IcebergCatalogProvider> {
    if let Some(iceberg) = provider.downcast_ref::<IcebergCatalogProvider>() {
        return Some(iceberg);
    }
    if let Some(refreshing) = provider.downcast_ref::<RefreshingCatalogProvider>() {
        return iceberg_provider_ref(refreshing.inner_catalog());
    }
    if let Some(composed) = provider.downcast_ref::<ComposedCatalogProvider>() {
        return iceberg_provider_ref(composed.external().as_ref());
    }
    None
}

/// Try to extract the Iceberg catalog from a `CatalogProvider`, peeling the
/// runtime's transparent catalog wrappers (see [`iceberg_provider_ref`]).
pub fn composed_catalog_to_iceberg(
    provider: &dyn CatalogProvider,
) -> Option<Arc<dyn iceberg::Catalog>> {
    iceberg_provider_ref(provider).map(|p| Arc::clone(p.catalog()))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    use super::coerce_arrow_schema_for_iceberg_v2;

    /// Coerces `schema` and converts it the way `CREATE TABLE` in an Iceberg
    /// catalog does.
    fn coerce_and_convert(schema: &ArrowSchema) -> iceberg::spec::Schema {
        let coerced = coerce_arrow_schema_for_iceberg_v2(schema);
        iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&coerced)
            .expect("a coerced schema converts to an Iceberg schema")
    }

    #[test]
    fn coerces_timestamps_of_every_unit_and_timezone() {
        let schema = ArrowSchema::new(vec![
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_s_utc",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ts_ms_utc",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ts_ns_utc",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ts_us_utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]);
        coerce_and_convert(&schema);
    }

    #[test]
    fn coerces_date_and_time_types() {
        let schema = ArrowSchema::new(vec![
            Field::new("d32", DataType::Date32, false),
            Field::new("d64", DataType::Date64, true),
            Field::new("t32_s", DataType::Time32(TimeUnit::Second), true),
            Field::new("t32_ms", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("t64_us", DataType::Time64(TimeUnit::Microsecond), false),
            Field::new("t64_ns", DataType::Time64(TimeUnit::Nanosecond), true),
        ]);
        coerce_and_convert(&schema);
    }

    #[test]
    fn coerces_a_mixed_schema() {
        // Types that need coercion next to types that do not.
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("event_date", DataType::Date64, true),
            Field::new("event_time", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("active", DataType::Boolean, false),
            Field::new("data", DataType::Binary, true),
        ]);
        coerce_and_convert(&schema);
    }
}
