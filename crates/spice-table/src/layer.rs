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
//! stacked on top: indexes, embeddings, vector scans, spicepod metadata.
//!
//! [`SpiceTable`] is the only [`TableProvider`] Spice puts around a connector's
//! provider, and it implements all fourteen `TableProvider` methods once. A
//! [`TableLayer`] declares just the behaviour it changes; every method defaults
//! to the layer beneath, so a layer cannot forget to forward. Navigation walks
//! down [`SpiceTable`]s, so a crate that walks a stack depends on none of the
//! capabilities in it.

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
    ///
    /// Implementations match on `walk` **exhaustively**, never with a wildcard
    /// arm. A wildcard hands every future walk kind an answer nobody considered,
    /// in whichever direction that layer happened to default — which is the
    /// silent-wrong-answer failure this trait exists to remove. An exhaustive
    /// match makes adding a walk a compile error at every layer that must decide.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        let _ = walk;
        Some(below)
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

    fn get_logical_plan<'a>(
        &'a self,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<Cow<'a, LogicalPlan>> {
        below.get_logical_plan()
    }

    fn get_column_default<'a>(
        &'a self,
        below: &'a Arc<dyn TableProvider>,
        column: &str,
    ) -> Option<&'a Expr> {
        below.get_column_default(column)
    }

    /// Whether a rebuild may push a transform *beneath* this layer.
    ///
    /// `false` stops the fold here and the transform is applied to the stack
    /// including this layer. A router answers `false`: it owns its children and
    /// routes writes to one of them, so a transform inserted underneath would sit
    /// where a write walk stops — the CDC write path would no longer find the
    /// accelerator it targets. It also means a router's `below` can never be
    /// replaced, so the child it holds and the table it is handed cannot diverge.
    fn rebuild_descends(&self) -> bool {
        true
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
    /// whatever `DataFusion` adds to them passes through untouched instead of
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
    /// The one capability this node adds.
    layer: Arc<dyn TableLayer>,
    /// The fully-composed table it sits over — another [`SpiceTable`] or a
    /// provider Spice does not manage. A node is therefore both "this layer" and
    /// "everything from here down", which is what lets a borrow of one stand for
    /// a sub-stack.
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

    /// The base provider the stack terminates at.
    #[must_use]
    pub fn base_provider(&self) -> &Arc<dyn TableProvider> {
        let mut current = self;
        while let Some(table) = current.below.downcast_ref::<SpiceTable>() {
            current = table;
        }
        &current.below
    }

    /// The indexes this node's layer carries, or empty when it carries none.
    ///
    /// Names [`IndexLayer`] rather than putting an accessor on [`TableLayer`]:
    /// a capability belongs to the layer that provides it, and `IndexLayer` lives
    /// here in the foundation crate, so naming it costs nothing.
    #[must_use]
    pub fn indexes(&self) -> &[Arc<dyn Index + Send + Sync>] {
        self.layer_as::<crate::IndexLayer>()
            .map_or(&[], crate::IndexLayer::indexes)
    }

    /// This node's layer as a concrete type, or `None` if it is another kind.
    ///
    /// Reaching a specific layer type is legitimate: a caller asking for one
    /// already depends on it. What it does not name is the layers in between.
    #[must_use]
    pub fn layer_as<T: TableLayer>(&self) -> Option<&T> {
        (self.layer.as_ref() as &dyn Any).downcast_ref::<T>()
    }
}

/// Steps one level down from `current`, following `walk`.
///
/// Handles the two shapes a stack can present: a [`SpiceTable`] node, which asks
/// its layer where the walk goes, and a `FederatedTableProviderAdaptor`, which
/// datafusion-federation requires to stay *outermost* (its analyzer recovers the
/// federated source by downcasting the scan's provider to it), so it cannot be
/// wrapped in a layer and has to be stepped through here instead.
///
/// A known temporary shape, not the settled design: it is the one foreign type
/// this module names, and the reason several call sites elsewhere go out of their
/// way to keep the adaptor outermost. Giving the fork a source resolver would let
/// the adaptor be an ordinary layer and remove both — see
/// <https://github.com/spiceai/spiceai/issues/12890>, and
/// <https://github.com/spiceai/spiceai/issues/12889> for the pushdown loss the
/// current shape allows.
fn step(current: &dyn TableProvider, walk: LayerWalk) -> Option<&Arc<dyn TableProvider>> {
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

/// The layered nodes reachable from `top` by `walk`, outermost first.
///
/// The single traversal primitive: every "find the layer that…" and "collect
/// the…" question over a stack is a fold across this, rather than its own
/// method. Non-layered links (a federation adaptor) are crossed rather than
/// yielded, so a caller never has to know one is there.
pub fn nodes(top: &dyn TableProvider, walk: LayerWalk) -> Nodes<'_> {
    Nodes {
        current: Some(top),
        walk,
    }
}

/// Iterator over the layered nodes of a stack. See [`nodes`].
pub struct Nodes<'a> {
    current: Option<&'a dyn TableProvider>,
    walk: LayerWalk,
}

impl<'a> Iterator for Nodes<'a> {
    type Item = &'a SpiceTable;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(current) = self.current {
            // A layered node answers for itself, so route straight off it rather
            // than downcasting the same pointer a second time inside `step`.
            if let Some(node) = current.downcast_ref::<SpiceTable>() {
                self.current = node.layer.route(self.walk, &node.below).map(Arc::as_ref);
                return Some(node);
            }
            self.current = step(current, self.walk).map(Arc::as_ref);
        }
        None
    }
}

/// The layer of type `T` reachable from `top` by `walk`.
///
/// A convenience over [`nodes`] for the common case. A caller that also needs the
/// table the layer sits over folds across [`nodes`] directly, so it holds the
/// node and cannot end up with a layer detached from its stack.
#[must_use]
pub fn find_layer<T: TableLayer>(top: &dyn TableProvider, walk: LayerWalk) -> Option<&T> {
    nodes(top, walk).find_map(SpiceTable::layer_as::<T>)
}

/// Rebuilds the stack with `transform` applied at its base, keeping every layer
/// above it.
///
/// Stops at a layer that does not let a rebuild descend (see
/// [`TableLayer::rebuild_descends`]) and applies `transform` to the stack
/// including that layer — which is what keeps a transform from landing beneath a
/// router, where a write walk would stop short of it.
#[must_use]
pub fn rebuild_base(
    top: &Arc<dyn TableProvider>,
    transform: &dyn Fn(Arc<dyn TableProvider>) -> Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    let Some(node) = top.downcast_ref::<SpiceTable>() else {
        return transform(Arc::clone(top));
    };
    if !node.layer.rebuild_descends() {
        return transform(Arc::clone(top));
    }

    let rebuilt_below = rebuild_base(&node.below, transform);
    SpiceTable::over(Arc::clone(&node.layer), rebuilt_below) as Arc<dyn TableProvider>
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
        self.layer.scan_with_args(&self.below, state, args).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.layer.supports_filters_pushdown(&self.below, filters)
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
        self.layer.delete_from(&self.below, state, filters).await
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
    }

    /// Stands in for an accelerated table: two sides, and the walk decides which
    /// one it means. `below` is the accelerator; `source` is the federated side,
    /// which may not be resolved yet.
    #[derive(Debug)]
    pub(super) struct TestRouter {
        pub(super) source: Option<Arc<dyn TableProvider>>,
    }

    impl TableLayer for TestRouter {
        fn rebuild_descends(&self) -> bool {
            false
        }

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
        assert!(
            peel_to(&plain, LayerWalk::Read)
                .downcast_ref::<MemTable>()
                .is_some()
        );
    }

    #[test]
    fn visit_walks_outermost_first_and_stops_after_the_opaque_layer() {
        let bottom = SpiceTable::over(TestLayer::indexed("bottom"), base());
        let middle = SpiceTable::over(TestLayer::opaque(LayerWalk::CdcDetection), bottom);
        let top = SpiceTable::over(TestLayer::indexed("top"), middle);

        let top: Arc<dyn TableProvider> = top;
        let seen: Vec<_> = nodes(top.as_ref(), LayerWalk::CdcDetection)
            .map(|table| {
                table
                    .layer_as::<TestLayer>()
                    .and_then(|l| l.indexes.first())
                    .map(|i| i.name())
            })
            .collect();
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
        let top = SpiceTable::over(
            TestLayer::indexed("top"),
            Arc::clone(&bottom) as Arc<dyn TableProvider>,
        );

        let top: Arc<dyn TableProvider> = top;
        let (found, bound) = nodes(top.as_ref(), LayerWalk::Index)
            .find_map(|node| {
                let layer = node.layer_as::<TestLayer>()?;
                layer.indexes.first().map(|index| (index, node.below()))
            })
            .expect("an index");
        assert_eq!(found.name(), "top", "outermost index layer wins");
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

        let top: Arc<dyn TableProvider> = top;
        let names: Vec<_> = nodes(top.as_ref(), LayerWalk::Index)
            .filter_map(|node| node.layer_as::<TestLayer>())
            .flat_map(|layer| layer.indexes.iter().map(|i| i.name()))
            .collect();
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
        let top = SpiceTable::over(
            TestLayer::marker(),
            SpiceTable::over(TestLayer::marker(), Arc::clone(&base_table)),
        );
        assert_eq!(top.schema(), base_table.schema());
        assert_eq!(top.table_type(), base_table.table_type());
    }

    /// A router owns its children and routes writes to one of them, so a
    /// transform pushed beneath it would sit where a write walk stops. The fold
    /// must stop above it and wrap the router instead.
    #[test]
    fn rebuild_stops_above_a_layer_that_does_not_let_it_descend() {
        let accelerator = base();
        let router: Arc<dyn TableProvider> = SpiceTable::over(
            Arc::new(TestRouter { source: None }),
            Arc::clone(&accelerator),
        );

        let marker = base();
        let rebuilt = rebuild_base(&router, &|inner| {
            // the transform receives the router itself, not its child
            assert!(
                Arc::ptr_eq(&inner, &router),
                "a rebuild must not descend past a router"
            );
            Arc::clone(&marker)
        });
        assert!(Arc::ptr_eq(&rebuilt, &marker));
    }

    #[test]
    fn rebuild_base_replaces_the_base_and_keeps_every_layer() {
        let top = SpiceTable::over(
            TestLayer::indexed("top"),
            SpiceTable::over(TestLayer::marker(), base()),
        );

        let replacement = base();
        let top: Arc<dyn TableProvider> = top;
        let rebuilt = rebuild_base(&top, &|_| Arc::clone(&replacement));

        assert_eq!(
            {
                nodes(rebuilt.as_ref(), LayerWalk::Index)
                    .filter_map(|node| node.layer_as::<TestLayer>())
                    .flat_map(|layer| layer.indexes.iter().map(|i| i.name()))
                    .collect::<Vec<_>>()
            },
            vec!["top"],
            "layers above the base must survive a rebuild"
        );
        assert!(Arc::ptr_eq(
            rebuilt
                .downcast_ref::<SpiceTable>()
                .expect("rebuilt stack is layered")
                .base_provider(),
            &replacement
        ));
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
