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

//! Which schema a write to an accelerator should be cast to.
//!
//! For almost every accelerator this is just [`TableProvider::schema`] — what it
//! advertises is what it stores. Cayenne is the exception: with
//! `cayenne_force_view_types` it advertises a *read* schema whose string columns
//! are Arrow view types, so `DataFusion` plans joins and aggregates on view arrays,
//! while the bytes it stores keep the source's own string type.
//!
//! Casting a write to that advertised schema would be wrong in two ways, and both
//! are reachable rather than theoretical:
//!
//! * `LargeUtf8` -> `Utf8View` is not a widening cast (`arrow_tools::schema_evolution`),
//!   so every refresh of an ordinary `LargeUtf8` source would report a narrowing
//!   that silently loses data — a warning describing something that is not happening.
//! * Arrow's view builder is infallible on append and panics once a single value
//!   exceeds `u32::MAX` (`GenericByteViewBuilder::append_value` -> `try_append_value().unwrap()`).
//!   A `LONGTEXT`/JSON column that `LargeUtf8` holds comfortably would abort ingestion
//!   instead of returning a structured error.
//!
//! So writes target the stored schema and reads target the advertised one. The
//! accelerator still converts between them internally at its own boundary.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use cayenne::CayenneTableProvider;
use datafusion::catalog::TableProvider;

/// The schema a write to `provider` should be cast to.
///
/// Returns the accelerator's stored schema when it keeps one distinct from the
/// schema it advertises, and the advertised schema otherwise.
#[must_use]
pub fn write_target_schema(provider: &Arc<dyn TableProvider>) -> SchemaRef {
    provider
        .as_ref()
        .downcast_ref::<CayenneTableProvider>()
        .map_or_else(|| provider.schema(), CayenneTableProvider::write_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    /// A provider that stores what it advertises is unaffected: the write target
    /// is simply its schema.
    #[test]
    fn a_plain_provider_writes_to_its_advertised_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::LargeUtf8,
            true,
        )]));
        let provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("mem table"));
        assert_eq!(
            write_target_schema(&provider).as_ref(),
            schema.as_ref(),
            "a provider with no separate stored schema writes to what it advertises"
        );
    }
}
