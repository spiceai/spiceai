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

//! [`SwappableTableProvider`] is a [`TableProvider`] wrapper whose underlying
//! delegate can be replaced atomically at runtime.
//!
//! It is used by `refresh_mode: snapshot` to allow reloading the accelerator
//! table from a freshly downloaded snapshot file without tearing down the
//! enclosing [`AcceleratedTable`]. Schema, table type, and constraints are
//! captured at construction time and assumed to be invariant across reloads;
//! snapshots restore the same logical dataset so this invariant holds by
//! design (and is enforced by snapshot schema validation).

use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, Statistics};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use snafu::Snafu;

/// Errors returned by [`SwappableTableProvider::swap`].
#[derive(Debug, Snafu)]
pub enum SwapError {
    #[snafu(display(
        "swap rejected: candidate schema is incompatible with the cached schema (field count, names, data types, or nullability differ)"
    ))]
    SchemaMismatch,
}

/// Returns true when `candidate` is structurally compatible with `expected`
/// for the purposes of swapping a [`TableProvider`] under a
/// [`SwappableTableProvider`]: same number of fields in the same order with
/// matching names, data types, and nullability flags.
///
/// Per-field metadata and arrow `Schema`-level metadata are intentionally
/// ignored — different engines (e.g. `DuckDB`, `SQLite`, CSV) attach
/// engine-specific metadata for logically identical columns. Nullability is
/// included because a nullable↔non-nullable change is observable to
/// downstream planners (e.g. join key handling, predicate evaluation).
#[must_use]
pub fn schemas_compatible(candidate: &Schema, expected: &Schema) -> bool {
    if candidate.fields().len() != expected.fields().len() {
        return false;
    }
    candidate
        .fields()
        .iter()
        .zip(expected.fields().iter())
        .all(|(c, e)| {
            c.name() == e.name()
                && data_types_compatible(c.data_type(), e.data_type())
                && c.is_nullable() == e.is_nullable()
        })
}

/// A *view* string/binary type is interchangeable with the non-view members of
/// its family — `Utf8View` with `Utf8`/`LargeUtf8`, and `BinaryView` with
/// `Binary`/`LargeBinary` — because Cayenne's force-view read schema decouples the
/// query/scan types from the stored `Utf8`/`Binary` types, and the values are
/// losslessly castable (write paths normalize via `try_cast_to`).
///
/// Deliberately narrow: at least one side must be the view type. Non-view width
/// changes (e.g. `Utf8` vs `LargeUtf8`) alter the physical offset width, are never
/// introduced by the force-view path, and stay strict so genuine widenings are
/// still rejected by snapshot refresh and provider swap.
fn data_types_compatible(a: &DataType, b: &DataType) -> bool {
    fn is_string(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    }
    fn is_binary(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
    }
    if a == b {
        return true;
    }
    let string_view = is_string(a)
        && is_string(b)
        && (matches!(a, DataType::Utf8View) || matches!(b, DataType::Utf8View));
    let binary_view = is_binary(a)
        && is_binary(b)
        && (matches!(a, DataType::BinaryView) || matches!(b, DataType::BinaryView));
    string_view || binary_view
}

/// A [`TableProvider`] that delegates to an inner provider which may be
/// replaced at runtime via [`SwappableTableProvider::swap`].
///
/// All read/write methods (`scan`, `insert_into`, `supports_filters_pushdown`,
/// `statistics`) load the current inner provider on each call. Schema-shaped
/// metadata (`schema`, `table_type`, `constraints`) is captured at construction
/// from the initial inner provider and returned without re-locking; snapshot
/// reloads must preserve these.
pub struct SwappableTableProvider {
    inner: RwLock<Arc<dyn TableProvider>>,
    cached_schema: SchemaRef,
    cached_table_type: TableType,
    cached_constraints: Option<Constraints>,
}

impl std::fmt::Debug for SwappableTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwappableTableProvider")
            .field("schema", &self.cached_schema)
            .field("table_type", &self.cached_table_type)
            .finish_non_exhaustive()
    }
}

impl SwappableTableProvider {
    /// Wrap `inner`, caching its schema, table type, and constraints. Returns
    /// an `Arc` so it can be threaded directly to call sites expecting
    /// `Arc<dyn TableProvider>`.
    #[must_use]
    pub fn new(inner: Arc<dyn TableProvider>) -> Arc<Self> {
        let cached_schema = inner.schema();
        let cached_table_type = inner.table_type();
        let cached_constraints = inner.constraints().cloned();
        Arc::new(Self {
            inner: RwLock::new(inner),
            cached_schema,
            cached_table_type,
            cached_constraints,
        })
    }

    /// Returns the current inner provider.
    ///
    /// Lock poisoning is recovered transparently via
    /// [`std::sync::PoisonError::into_inner`]: a previous panic in another
    /// thread holding the lock does not propagate here.
    #[must_use]
    pub fn current(&self) -> Arc<dyn TableProvider> {
        Arc::clone(
            &self
                .inner
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Replace the inner provider. Validates that the new provider's schema
    /// is compatible with the cached schema (see [`schemas_compatible`]) and
    /// returns [`SwapError::SchemaMismatch`] otherwise without mutating
    /// state. Production callers should pre-validate too so they can surface
    /// dataset-aware error context, but this guard ensures incompatible
    /// providers cannot be installed even in release builds.
    ///
    /// Lock poisoning is recovered transparently via
    /// [`std::sync::PoisonError::into_inner`].
    pub fn swap(&self, new_inner: Arc<dyn TableProvider>) -> Result<(), SwapError> {
        if !schemas_compatible(new_inner.schema().as_ref(), self.cached_schema.as_ref()) {
            return Err(SwapError::SchemaMismatch);
        }
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = new_inner;
        Ok(())
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for SwappableTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.cached_schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.cached_constraints.as_ref()
    }

    fn table_type(&self) -> TableType {
        self.cached_table_type
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.current().scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.current().supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.current().statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.current().insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.current().delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.current().update(state, assignments, filters).await
    }

    fn get_table_definition(&self) -> Option<&str> {
        // Cannot delegate to current() as it returns a temporary Arc whose
        // lifetime does not extend to the returned &str borrow.
        None
    }

    fn get_logical_plan(
        &self,
    ) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        // Cannot delegate to current() as it returns a temporary Arc whose
        // lifetime does not extend to the returned borrow.
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        // Cannot delegate to current() as it returns a temporary Arc whose
        // lifetime does not extend to the returned borrow.
        None
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> DFResult<datafusion::catalog::ScanResult> {
        self.current().scan_with_args(state, args).await
    }

    async fn truncate(&self, state: &dyn Session) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.current().truncate(state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;

    fn mem_provider(value: i32) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![value]))],
        )
        .expect("build batch");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("build memtable");
        Arc::new(table)
    }

    #[test]
    fn current_and_swap_replace_inner_provider() {
        let initial = mem_provider(1);
        let initial_ptr = Arc::as_ptr(&initial).cast::<()>();
        let swappable = SwappableTableProvider::new(initial);

        let observed = swappable.current();
        assert_eq!(Arc::as_ptr(&observed).cast::<()>(), initial_ptr);

        let replacement = mem_provider(2);
        let replacement_ptr = Arc::as_ptr(&replacement).cast::<()>();
        swappable
            .swap(replacement)
            .expect("swap with compatible schema");

        let observed = swappable.current();
        assert_eq!(Arc::as_ptr(&observed).cast::<()>(), replacement_ptr);
    }

    #[test]
    fn schema_is_cached_at_construction() {
        let initial = mem_provider(1);
        let initial_schema = initial.schema();
        let swappable = SwappableTableProvider::new(initial);

        // Even after swapping in a (schema-equivalent) replacement, the wrapper
        // returns the same SchemaRef instance it cached at construction.
        let replacement = mem_provider(99);
        swappable
            .swap(replacement)
            .expect("swap with compatible schema");
        assert!(
            Arc::ptr_eq(&swappable.schema(), &initial_schema),
            "swappable.schema() should return the cached SchemaRef instance"
        );
    }

    #[test]
    fn swap_rejects_schema_mismatch() {
        let initial = mem_provider(1);
        let swappable = SwappableTableProvider::new(initial);

        let mismatched_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Utf8,
            false,
        )]));
        let mismatched_batch = RecordBatch::try_new(
            Arc::clone(&mismatched_schema),
            vec![Arc::new(datafusion::arrow::array::StringArray::from(vec![
                "x",
            ]))],
        )
        .expect("build batch");
        let mismatched = Arc::new(
            MemTable::try_new(mismatched_schema, vec![vec![mismatched_batch]])
                .expect("build memtable"),
        );
        let err = swappable
            .swap(mismatched)
            .expect_err("swap should reject mismatched schema");
        assert!(matches!(err, SwapError::SchemaMismatch));

        // The cached schema is unchanged and the inner provider is preserved.
        let observed = swappable.current();
        assert_eq!(observed.schema().fields().len(), 1);
        assert_eq!(observed.schema().field(0).name(), "v");
    }

    #[test]
    fn swap_rejects_nullability_change() {
        let initial = mem_provider(1);
        let swappable = SwappableTableProvider::new(initial);

        // Same name + data type, different nullability — must be rejected.
        let nullable_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&nullable_schema),
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )
        .expect("build batch");
        let nullable = Arc::new(
            MemTable::try_new(nullable_schema, vec![vec![batch]]).expect("build memtable"),
        );
        let err = swappable
            .swap(nullable)
            .expect_err("nullability change must be rejected");
        assert!(matches!(err, SwapError::SchemaMismatch));
    }

    #[test]
    fn schemas_compatible_treats_view_and_nonview_string_binary_as_equal() {
        // A view type is interchangeable with the non-view members of its family
        // (an accelerator advertising view types over a non-view source must not
        // be rejected) — but ONLY when one side is the view type.
        let utf8 = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let utf8_view = Schema::new(vec![Field::new("s", DataType::Utf8View, true)]);
        let large_utf8 = Schema::new(vec![Field::new("s", DataType::LargeUtf8, true)]);
        assert!(schemas_compatible(&utf8, &utf8_view));
        assert!(schemas_compatible(&utf8_view, &utf8)); // symmetric
        assert!(schemas_compatible(&large_utf8, &utf8_view));

        let binary = Schema::new(vec![Field::new("b", DataType::Binary, false)]);
        let binary_view = Schema::new(vec![Field::new("b", DataType::BinaryView, false)]);
        assert!(schemas_compatible(&binary, &binary_view));
        assert!(schemas_compatible(&binary_view, &binary));

        // Non-view width changes stay strict (no view type involved): a genuine
        // Utf8 -> LargeUtf8 widening must still be rejected.
        assert!(!schemas_compatible(&utf8, &large_utf8));

        // Cross-family and unrelated types stay incompatible.
        let int = Schema::new(vec![Field::new("s", DataType::Int32, true)]);
        assert!(!schemas_compatible(&utf8, &int));
        assert!(!schemas_compatible(&utf8, &binary));
        // Nullability mismatch is still rejected even within a family.
        let utf8_view_nonnull = Schema::new(vec![Field::new("s", DataType::Utf8View, false)]);
        assert!(!schemas_compatible(&utf8, &utf8_view_nonnull));
    }
}
