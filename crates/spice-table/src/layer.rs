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

//! A dataset's provider as a stack of layers over a base provider.
//!
//! A Spice dataset is a connector's own [`TableProvider`] with capabilities
//! stacked on top: indexes, embeddings, vector scans, spicepod metadata. Those
//! capabilities used to be individual `TableProvider` wrappers, which meant
//! every one of them owed all fourteen `TableProvider` methods (almost always
//! forwarding), and seeing through the stack meant downcasting to each wrapper
//! type in turn — so any crate that walked the stack had to depend on every
//! wrapper it might encounter.
//!
//! Here, [`SpiceTable`] is the only [`TableProvider`], and it implements those
//! fourteen methods once. A [`TableLayer`] declares just the behaviour it
//! changes; every method defaults to the layer beneath it, so a layer cannot
//! forget to forward. Navigation is a walk down [`SpiceTable`]s, needing no
//! downcast and naming no wrapper type.

use std::{any::Any, borrow::Cow, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use datafusion_federation::FederatedTableProviderAdaptor;

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{ScanArgs, ScanResult, Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::TableType,
    error::Result as DataFusionResult,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::Index;

/// The walk being performed over a stack of layers. Each layer declares, per
/// context, whether the walk may see past it — so the decision lives on the
/// layer instead of at every call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LayerWalk {
    /// Discovery on the read path: reads pass through this layer unchanged
    /// enough that looking *for* a provider may see past it.
    Read,
    /// Changes/append-stream detection. A layer that is itself what detection
    /// looks for — one carrying indexes, embeddings or a vector scan — is
    /// deliberately opaque here.
    CdcDetection,
    /// Peel to the raw source provider a connector stream should attach to.
    /// Sees past every layer whose schema additions must not leak into queries
    /// against the source.
    Source,
    /// Write pass-through: `insert_into` reaches the base unchanged. A layer
    /// that transforms writes is opaque here so its semantics are preserved.
    Write,
    /// Peel to the provider a retention delete should execute against.
    ///
    /// Narrower than [`LayerWalk::Source`]: retention must not see past a layer
    /// carrying delete semantics of its own.
    RetentionDelete,
    /// Find where a dataset's indexes live.
    Index,
}

/// One capability stacked onto a dataset's provider.
///
/// Every method defaults to the behaviour of the layer beneath (`below`), so an
/// implementation declares *only* what it changes and cannot silently fail to
/// forward the rest. A layer that exists purely to be found — to carry an index
/// or mark a provider — implements nothing at all.
///
/// `below` is the fully-composed table underneath this layer: calling
/// `below.scan(..)` runs every lower layer and then the base provider.
#[async_trait]
pub trait TableLayer: Any + Send + Sync + Debug + 'static {
    /// A short name for diagnostics. Defaults to the implementing type.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Where `walk` continues, or `None` when it stops at this layer.
    ///
    /// Defaults to the table beneath — transparent to everything. Two kinds of
    /// layer override it:
    ///
    /// * one that *is* what a walk looks for, or whose semantics a walk must
    ///   not route around, returns `None` for that walk;
    /// * a **router** owning more than one table returns whichever its own
    ///   semantics say that walk means. An accelerated table sends read and
    ///   source walks to the federated source but write and index walks to the
    ///   accelerator, and `None` when the side asked for is not yet resolved.
    ///
    /// Because the layer answers, no caller has to name a router's type or know
    /// it has two sides.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        let _ = walk;
        Some(below)
    }

    /// Indexes this layer binds to the table beneath it.
    ///
    /// Position matters: an index is bound to the table *below* the layer
    /// carrying it, which is what a search executes against.
    fn indexes(&self) -> &[Arc<dyn Index + Send + Sync>] {
        &[]
    }

    fn schema(&self, below: &Arc<dyn TableProvider>) -> SchemaRef {
        below.schema()
    }

    fn constraints<'a>(&'a self, below: &'a Arc<dyn TableProvider>) -> Option<&'a Constraints> {
        below.constraints()
    }

    fn table_type(&self, below: &Arc<dyn TableProvider>) -> TableType {
        below.table_type()
    }

    fn get_table_definition<'a>(&'a self, below: &'a Arc<dyn TableProvider>) -> Option<&'a str> {
        below.get_table_definition()
    }

    fn get_logical_plan<'a>(&'a self, below: &'a Arc<dyn TableProvider>) -> Option<Cow<'a, LogicalPlan>> {
        below.get_logical_plan()
    }

    fn get_column_default<'a>(
        &'a self,
        below: &'a Arc<dyn TableProvider>,
        column: &str,
    ) -> Option<&'a Expr> {
        below.get_column_default(column)
    }

    /// Builds the plan for a scan of this layer, or forwards to the table
    /// beneath.
    ///
    /// The *only* scan entry point a layer implements. `TableProvider` offers two
    /// — `scan` and `scan_with_args` — and a layer overriding one but not the
    /// other would have its work silently skipped for callers taking the other
    /// path. `SpiceTable` funnels both into this, so that cannot happen.
    ///
    /// Takes `ScanArgs`/`ScanResult` rather than loose parameters so that
    /// whatever DataFusion adds to them passes through untouched instead of
    /// being dropped in translation.
    async fn scan_with_args<'a>(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DataFusionResult<ScanResult> {
        below.scan_with_args(state, args).await
    }

    /// # Errors
    ///
    /// Propagates the error the layer beneath reports.
    fn supports_filters_pushdown(
        &self,
        below: &Arc<dyn TableProvider>,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        below.supports_filters_pushdown(filters)
    }

    fn statistics(&self, below: &Arc<dyn TableProvider>) -> Option<Statistics> {
        below.statistics()
    }

    async fn insert_into(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        below.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        below.delete_from(state, filters).await
    }

    async fn update(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        below.update(state, assignments, filters).await
    }

    async fn truncate(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        below.truncate(state).await
    }
}

/// A dataset's provider: a stack of [`TableLayer`]s over a base provider.
///
/// This is the only [`TableProvider`] Spice puts around a connector's own
/// provider. Each node holds one layer and the already-composed table beneath
/// it, so a layer never has to assemble what is under it and the composition
/// costs nothing per call.
#[derive(Debug, Clone)]
pub struct SpiceTable {
    layer: Arc<dyn TableLayer>,
    /// The fully-composed table beneath this layer, ready to hand to it.
    ///
    /// Layered or not: navigation recovers the layered case by downcasting to
    /// [`SpiceTable`]. That is the one downcast this design keeps, of the one
    /// type it owns, in the one crate that defines it — so a construction site
    /// can hand over any provider without knowing whether it is layered.
    below: Arc<dyn TableProvider>,
}

impl SpiceTable {
    /// Stacks `layer` onto `below`, which may be a connector's own provider or
    /// an already-layered table.
    #[must_use]
    pub fn over(layer: Arc<dyn TableLayer>, below: Arc<dyn TableProvider>) -> Arc<Self> {
        Arc::new(Self { layer, below })
    }

    /// This node's layer.
    #[must_use]
    pub fn layer(&self) -> &Arc<dyn TableLayer> {
        &self.layer
    }

    /// The composed table beneath this layer.
    #[must_use]
    pub fn below(&self) -> &Arc<dyn TableProvider> {
        &self.below
    }

    /// The layered table beneath this one, or `None` at the last layer.
    #[must_use]
    pub fn below_table(&self) -> Option<&SpiceTable> {
        self.below.downcast_ref::<SpiceTable>()
    }

    /// The base provider the stack terminates at.
    #[must_use]
    pub fn base_provider(&self) -> &Arc<dyn TableProvider> {
        let mut current = self;
        while let Some(table) = current.below_table() {
            current = table;
        }
        &current.below
    }

    /// Visits each layered node from this one down, stopping at the first layer
    /// opaque to `walk`.
    pub fn visit(&self, walk: LayerWalk, visit: &mut dyn FnMut(&SpiceTable)) {
        let mut current = self;
        loop {
            visit(current);
            let Some(next) = current.layer.route(walk, &current.below) else {
                return;
            };
            match next.downcast_ref::<SpiceTable>() {
                Some(table) => current = table,
                None => return,
            }
        }
    }

    /// The first layer carrying an index of type `T`, paired with the table
    /// that index is bound to.
    #[must_use]
    pub fn find_index<T: Index + 'static>(&self) -> Option<(Vec<&T>, Arc<dyn TableProvider>)> {
        let mut current = self;
        loop {
            let found: Vec<&T> = current
                .layer
                .indexes()
                .iter()
                .filter_map(|index| index.as_any().downcast_ref::<T>())
                .collect();
            if !found.is_empty() {
                return Some((found, Arc::clone(&current.below)));
            }
            let next = current.layer.route(LayerWalk::Index, &current.below)?;
            current = next.downcast_ref::<SpiceTable>()?;
        }
    }

    /// This node's layer as a concrete type, or `None` if it is another kind.
    ///
    /// Reaching a specific layer type is legitimate — a caller asking for one
    /// already depends on it. What it no longer has to name is the layers in
    /// between.
    #[must_use]
    pub fn layer_as<T: TableLayer>(&self) -> Option<&T> {
        (self.layer.as_ref() as &dyn Any).downcast_ref::<T>()
    }

    /// The first node reachable by `walk` whose layer is a `T`.
    ///
    /// Returns the node rather than the layer, so a caller can also inspect what
    /// the layer sits on — which is how "this layer is below that one" is
    /// expressed.
    #[must_use]
    pub fn find_node<T: TableLayer>(&self, walk: LayerWalk) -> Option<&SpiceTable> {
        let mut current = self;
        loop {
            if current.layer_as::<T>().is_some() {
                return Some(current);
            }
            let next = current.layer.route(walk, &current.below)?;
            current = next.downcast_ref::<SpiceTable>()?;
        }
    }

    /// The first layer reachable by `walk` that carries any index, together with
    /// the table those indexes are bound to.
    #[must_use]
    pub fn first_indexed(&self, walk: LayerWalk) -> Option<&SpiceTable> {
        let mut current = self;
        loop {
            if !current.layer.indexes().is_empty() {
                return Some(current);
            }
            let next = current.layer.route(walk, &current.below)?;
            current = next.downcast_ref::<SpiceTable>()?;
        }
    }

    /// Every index carried by any layer reachable by `walk`, outermost first.
    ///
    /// De-duplicated by identity: one index may be carried by more than one
    /// layer, and a caller driving write lifecycle hooks must not run them twice.
    #[must_use]
    pub fn all_indexes(&self, walk: LayerWalk) -> Vec<Arc<dyn Index + Send + Sync>> {
        let mut indexes: Vec<Arc<dyn Index + Send + Sync>> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut current = self;
        loop {
            for index in current.layer.indexes() {
                if seen.insert(Arc::as_ptr(index).cast::<()>()) {
                    indexes.push(Arc::clone(index));
                }
            }
            let Some(next) = current.layer.route(walk, &current.below) else {
                return indexes;
            };
            match next.downcast_ref::<SpiceTable>() {
                Some(table) => current = table,
                None => return indexes,
            }
        }
    }

    /// Rebuilds the stack with `transform` applied to the base provider,
    /// preserving every layer above it.
    #[must_use]
    pub fn rebuild_base(
        &self,
        transform: &dyn Fn(Arc<dyn TableProvider>) -> Arc<dyn TableProvider>,
    ) -> Arc<Self> {
        let rebuilt_below = match self.below_table() {
            Some(table) => table.rebuild_base(transform) as Arc<dyn TableProvider>,
            None => transform(Arc::clone(&self.below)),
        };
        Self::over(Arc::clone(&self.layer), rebuilt_below)
    }
}

/// Steps one level down from `current`, following `walk`.
///
/// Handles the two shapes a stack can present: a [`SpiceTable`] node, which asks
/// its layer where the walk goes, and a `FederatedTableProviderAdaptor`, which
/// datafusion-federation requires to stay *outermost* (its analyzer recovers the
/// federated source by downcasting the scan's provider to it), so it cannot be
/// wrapped in a layer and has to be stepped through here instead. That is the one
/// foreign type this module names, in the one place it names it.
#[must_use]
fn step<'a>(
    current: &'a dyn TableProvider,
    walk: LayerWalk,
) -> Option<&'a Arc<dyn TableProvider>> {
    if let Some(table) = current.downcast_ref::<SpiceTable>() {
        return table.layer.route(walk, &table.below);
    }
    if let Some(adaptor) = current.downcast_ref::<FederatedTableProviderAdaptor>() {
        // A write is not routed through federation, and an adaptor holding no
        // physical provider is where the walk legitimately ends.
        if walk == LayerWalk::Write {
            return None;
        }
        return adaptor.table_provider.as_ref();
    }
    None
}

/// Finds a specific concrete [`TableProvider`] in the stack, following `walk`.
///
/// Downcasting here does not reintroduce the dependency this module removes: a
/// caller asking for a *particular* type already depends on it. What it no
/// longer has to name is every wrapper in between.
#[must_use]
pub fn find_concrete<T: TableProvider + 'static>(
    top: &dyn TableProvider,
    walk: LayerWalk,
) -> Option<&T> {
    let mut current = top;
    loop {
        if let Some(found) = current.downcast_ref::<T>() {
            return Some(found);
        }
        current = step(current, walk)?.as_ref();
    }
}

/// Finds a specific [`TableLayer`] in the stack, following `walk`.
#[must_use]
pub fn find_layer<T: TableLayer>(top: &dyn TableProvider, walk: LayerWalk) -> Option<&T> {
    let mut current = top;
    loop {
        if let Some(found) = current
            .downcast_ref::<SpiceTable>()
            .and_then(SpiceTable::layer_as::<T>)
        {
            return Some(found);
        }
        current = step(current, walk)?.as_ref();
    }
}

/// The table reached by peeling every layer transparent to `walk`, stopping
/// *at* the first opaque layer — the result still includes that layer, whose
/// semantics the walk must not route around.
///
/// Returns `top` itself when it is not layered at all. Borrows into the stack:
/// every node is already held as an `Arc` by the node above it.
#[must_use]
pub fn peel_to(top: &Arc<dyn TableProvider>, walk: LayerWalk) -> &Arc<dyn TableProvider> {
    let mut current = top;
    while let Some(next) = step(current.as_ref(), walk) {
        current = next;
    }
    current
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for SpiceTable {
    fn schema(&self) -> SchemaRef {
        self.layer.schema(&self.below)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.layer.constraints(&self.below)
    }

    fn table_type(&self) -> TableType {
        self.layer.table_type(&self.below)
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.layer.get_table_definition(&self.below)
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.layer.get_logical_plan(&self.below)
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.layer.get_column_default(&self.below, column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let args = ScanArgs::default()
            .with_projection(projection.map(Vec::as_slice))
            .with_filters(Some(filters))
            .with_limit(limit);
        Ok(self
            .layer
            .scan_with_args(&self.below, state, args)
            .await?
            .into_inner())
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DataFusionResult<ScanResult> {
        self.layer
            .scan_with_args(&self.below, state, args)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.layer
            .supports_filters_pushdown(&self.below, filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.layer.statistics(&self.below)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.layer
            .insert_into(&self.below, state, input, insert_op)
            .await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.layer
            .delete_from(&self.below, state, filters)
            .await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.layer
            .update(&self.below, state, assignments, filters)
            .await
    }

    async fn truncate(&self, state: &dyn Session) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.layer.truncate(&self.below, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::any::Any;

    #[derive(Debug)]
    struct TestIndex(&'static str);

    impl Index for TestIndex {
        fn name(&self) -> &'static str {
            self.0
        }
        fn required_columns(&self) -> Vec<String> {
            vec!["id".to_string()]
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug, Default)]
    struct TestLayer {
        opaque_to: Vec<LayerWalk>,
        indexes: Vec<Arc<dyn Index + Send + Sync>>,
    }

    impl TestLayer {
        fn marker() -> Arc<dyn TableLayer> {
            Arc::new(Self::default())
        }

        fn opaque(walk: LayerWalk) -> Arc<dyn TableLayer> {
            Arc::new(Self {
                opaque_to: vec![walk],
                ..Self::default()
            })
        }

        fn indexed(name: &'static str) -> Arc<dyn TableLayer> {
            Arc::new(Self {
                indexes: vec![Arc::new(TestIndex(name))],
                ..Self::default()
            })
        }
    }

    impl TableLayer for TestLayer {
        fn route<'a>(
            &'a self,
            walk: LayerWalk,
            below: &'a Arc<dyn TableProvider>,
        ) -> Option<&'a Arc<dyn TableProvider>> {
            if self.opaque_to.contains(&walk) {
                return None;
            }
            Some(below)
        }

        fn indexes(&self) -> &[Arc<dyn Index + Send + Sync>] {
            &self.indexes
        }
    }

    /// Stands in for an accelerated table: two sides, and the walk decides which
    /// one it means. `below` is the accelerator; `source` is the federated side,
    /// which may not be resolved yet.
    #[derive(Debug)]
    pub(super) struct TestRouter {
        pub(super) source: Option<Arc<dyn TableProvider>>,
    }

    impl TableLayer for TestRouter {
        fn route<'a>(
            &'a self,
            walk: LayerWalk,
            below: &'a Arc<dyn TableProvider>,
        ) -> Option<&'a Arc<dyn TableProvider>> {
            match walk {
                LayerWalk::Read | LayerWalk::Source | LayerWalk::CdcDetection => {
                    self.source.as_ref()
                }
                LayerWalk::Write | LayerWalk::RetentionDelete | LayerWalk::Index => Some(below),
            }
        }
    }

    pub(super) fn layered_marker() -> Arc<dyn TableProvider> {
        SpiceTable::over(TestLayer::marker(), base())
    }

    /// Whether `reached` is `expected`, or the table `expected` wraps — peeling
    /// a transparent marker lands on its base.
    pub(super) fn reached_within(
        reached: &Arc<dyn TableProvider>,
        expected: &Arc<dyn TableProvider>,
    ) -> bool {
        if Arc::ptr_eq(reached, expected) {
            return true;
        }
        expected
            .downcast_ref::<SpiceTable>()
            .is_some_and(|table| Arc::ptr_eq(reached, table.below()))
    }

    fn base() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("mem table"))
    }

    /// Regression test: peeling must stop *at* an opaque layer, not below it.
    /// Returning the table underneath would route a walk past semantics the
    /// layer exists to enforce — a retention delete past a deletion provider,
    /// for instance, deletes from the wrong table.
    #[test]
    fn peel_stops_at_the_opaque_layer_not_below_it() {
        let bottom = SpiceTable::over(TestLayer::marker(), base());
        let opaque = SpiceTable::over(TestLayer::opaque(LayerWalk::RetentionDelete), bottom);
        let top: Arc<dyn TableProvider> = SpiceTable::over(TestLayer::marker(), opaque);

        let peeled = peel_to(&top, LayerWalk::RetentionDelete);
        let peeled_table = peeled
            .downcast_ref::<SpiceTable>()
            .expect("peel should stop at the opaque layer, which is layered");
        assert!(
            peeled_table
                .layer()
                .route(LayerWalk::RetentionDelete, peeled_table.below())
                .is_none(),
            "peel returned the table below the opaque layer instead of including it"
        );
    }

    #[test]
    fn peel_reaches_the_base_when_every_layer_is_transparent() {
        let inner = SpiceTable::over(TestLayer::marker(), base());
        let top: Arc<dyn TableProvider> = SpiceTable::over(TestLayer::marker(), inner);

        let peeled = peel_to(&top, LayerWalk::Read);
        assert!(
            peeled.downcast_ref::<MemTable>().is_some(),
            "a fully transparent stack should peel to the base provider"
        );
    }

    #[test]
    fn peel_returns_an_unlayered_provider_unchanged() {
        let plain = base();
        assert!(peel_to(&plain, LayerWalk::Read).downcast_ref::<MemTable>().is_some());
    }

    #[test]
    fn visit_walks_outermost_first_and_stops_after_the_opaque_layer() {
        let bottom = SpiceTable::over(TestLayer::indexed("bottom"), base());
        let middle = SpiceTable::over(TestLayer::opaque(LayerWalk::CdcDetection), bottom);
        let top = SpiceTable::over(TestLayer::indexed("top"), middle);

        let mut seen = Vec::new();
        top.visit(LayerWalk::CdcDetection, &mut |table| {
            seen.push(table.layer().indexes().first().map(|i| i.name()));
        });
        assert_eq!(
            seen,
            vec![Some("top"), None],
            "visit must include the opaque layer and stop there"
        );
    }

    /// An index is bound to the table *beneath* the layer carrying it — that is
    /// what a search executes against — so the first match must return its own
    /// `below`, not the base of the whole stack.
    #[test]
    fn find_index_returns_the_table_the_index_is_bound_to() {
        let bottom = SpiceTable::over(TestLayer::indexed("bottom"), base());
        let top = SpiceTable::over(TestLayer::indexed("top"), Arc::clone(&bottom) as Arc<dyn TableProvider>);

        let (found, bound) = top.find_index::<TestIndex>().expect("an index");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name(), "top", "outermost index layer wins");
        assert!(
            bound.downcast_ref::<SpiceTable>().is_some(),
            "the bound table is the stack below the matching layer, not the base"
        );
    }

    #[test]
    fn all_indexes_collects_every_layer_outermost_first() {
        let bottom = SpiceTable::over(TestLayer::indexed("bottom"), base());
        let middle = SpiceTable::over(TestLayer::marker(), bottom);
        let top = SpiceTable::over(TestLayer::indexed("top"), middle);

        let names: Vec<_> = top.all_indexes(LayerWalk::Index).iter().map(|i| i.name()).collect();
        assert_eq!(names, vec!["top", "bottom"]);
    }

    #[test]
    fn base_provider_reaches_through_every_layer() {
        let inner = SpiceTable::over(TestLayer::marker(), base());
        let top = SpiceTable::over(TestLayer::indexed("top"), inner);
        assert!(top.base_provider().downcast_ref::<MemTable>().is_some());
    }

    /// The defaulted trait methods must reach the base, or a layer that
    /// declares nothing would silently change the table it wraps.
    #[test]
    fn defaulted_methods_delegate_through_the_stack() {
        let base_table = base();
        let top = SpiceTable::over(TestLayer::marker(), SpiceTable::over(TestLayer::marker(), Arc::clone(&base_table)));
        assert_eq!(top.schema(), base_table.schema());
        assert_eq!(top.table_type(), base_table.table_type());
    }

    #[test]
    fn rebuild_base_replaces_the_base_and_keeps_every_layer() {
        let top = SpiceTable::over(TestLayer::indexed("top"), SpiceTable::over(TestLayer::marker(), base()));

        let replacement = base();
        let rebuilt = top.rebuild_base(&|_| Arc::clone(&replacement));

        assert_eq!(
            rebuilt.all_indexes(LayerWalk::Index).iter().map(|i| i.name()).collect::<Vec<_>>(),
            vec!["top"],
            "layers above the base must survive a rebuild"
        );
        assert!(Arc::ptr_eq(rebuilt.base_provider(), &replacement));
    }
}

#[cfg(test)]
mod router_tests {
    use super::tests::*;
    use super::*;

    /// A router owning two tables decides for itself which one a walk means, so
    /// no caller has to name its type or know it has two sides. Read walks reach
    /// the source; write and index walks reach the accelerator.
    #[test]
    fn a_router_sends_each_walk_to_its_own_side() {
        let accelerator = layered_marker();
        let source = layered_marker();
        let router: Arc<dyn TableProvider> = SpiceTable::over(
            Arc::new(TestRouter {
                source: Some(Arc::clone(&source)),
            }),
            Arc::clone(&accelerator),
        );

        for (walk, expected) in [
            (LayerWalk::Read, &source),
            (LayerWalk::Source, &source),
            (LayerWalk::Write, &accelerator),
            (LayerWalk::Index, &accelerator),
        ] {
            let reached = peel_to(&router, walk);
            assert!(
                reached_within(reached, expected),
                "{walk:?} reached the wrong side of the router"
            );
        }
    }

    /// The federated side of an accelerated table may not be resolved yet. A
    /// walk asking for it must stop at the router rather than silently fall
    /// through to the accelerator and report the wrong table.
    #[test]
    fn a_router_stops_a_walk_whose_side_is_unresolved() {
        let accelerator = layered_marker();
        let router: Arc<dyn TableProvider> =
            SpiceTable::over(Arc::new(TestRouter { source: None }), accelerator);

        let reached = peel_to(&router, LayerWalk::Read);
        assert!(
            Arc::ptr_eq(reached, &router),
            "an unresolved side must stop the walk at the router"
        );
    }
}
