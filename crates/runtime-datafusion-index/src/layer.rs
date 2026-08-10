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

use std::{borrow::Cow, fmt::Debug, sync::Arc};

use async_trait::async_trait;
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
    /// Narrower than [`LayerWalk::Source`]: retention needs the accelerator's
    /// own provider, so it must not see past a layer that redirects to the
    /// source side or carries delete semantics of its own.
    RetentionDelete,
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
pub trait TableLayer: Send + Sync + Debug + 'static {
    /// A short name for diagnostics. Defaults to the implementing type.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Whether a walk may see past this layer.
    ///
    /// Defaults to transparent. Override for a layer that *is* what some walk
    /// looks for, or whose semantics a walk must not route around.
    fn transparent_to(&self, walk: LayerWalk) -> bool {
        let _ = walk;
        true
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

    async fn scan(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        below.scan(state, projection, filters, limit).await
    }

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
            if !current.layer.transparent_to(walk) {
                return;
            }
            match current.below_table() {
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
            match current.below_table() {
                Some(table) => current = table,
                None => return None,
            }
        }
    }

    /// Every index carried by any layer in the stack, outermost first.
    #[must_use]
    pub fn all_indexes(&self) -> Vec<Arc<dyn Index + Send + Sync>> {
        let mut indexes = Vec::new();
        let mut current = self;
        loop {
            indexes.extend(current.layer.indexes().iter().map(Arc::clone));
            match current.below_table() {
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

/// The table reached by peeling every layer transparent to `walk`, stopping
/// *at* the first opaque layer — the result still includes that layer, whose
/// semantics the walk must not route around.
///
/// Returns `top` itself when it is not layered at all. Borrows into the stack:
/// every node is already held as an `Arc` by the node above it.
#[must_use]
pub fn peel_to(top: &Arc<dyn TableProvider>, walk: LayerWalk) -> &Arc<dyn TableProvider> {
    let mut current = top;
    loop {
        let Some(table) = current.downcast_ref::<SpiceTable>() else {
            return current;
        };
        if !table.layer.transparent_to(walk) {
            return current;
        }
        current = &table.below;
    }
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
        self.layer
            .scan(&self.below, state, projection, filters, limit)
            .await
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
