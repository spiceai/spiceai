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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    error::Result as DataFusionResult,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown},
    prelude::Expr,
};
use spice_table::{LayerWalk, SpiceTable, TableLayer};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationAnalyzerForLogicalPlan,
    FederationProvider,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PolyTableProvider {
    write: Arc<dyn TableProvider>,
    fed: Arc<dyn TableProvider>,
    schema_metadata: HashMap<String, String>,
}

impl PolyTableProvider {
    /// Presents this read/write split as a layered table, with the writer side
    /// beneath so the layer and its `below` cannot disagree.
    #[must_use]
    pub fn into_table(self: Arc<Self>) -> Arc<SpiceTable> {
        let write = Arc::clone(&self.write);
        SpiceTable::over(self, write)
    }

    pub fn new(write: Arc<dyn TableProvider>, fed: Arc<dyn TableProvider>) -> Self {
        PolyTableProvider {
            write,
            fed,
            schema_metadata: HashMap::new(),
        }
    }

    pub fn new_with_schema_metadata(
        write: Arc<dyn TableProvider>,
        fed: Arc<dyn TableProvider>,
        schema_metadata: HashMap<String, String>,
    ) -> Self {
        PolyTableProvider {
            write,
            fed,
            schema_metadata,
        }
    }

    fn get_federation_provider(&self) -> Option<Arc<dyn FederationProvider>> {
        self.fed
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .map(|x| x.source.federation_provider())
    }

    #[must_use]
    pub fn get_table_source(&self) -> Option<Arc<dyn FederatedTableSource>> {
        let adaptor = self.fed.downcast_ref::<FederatedTableProviderAdaptor>();

        adaptor.map(|f| Arc::clone(&f.source))
    }

    #[must_use]
    pub fn get_federated_table_provider(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.fed)
    }

    #[must_use]
    pub fn writer(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.write)
    }

    /// Borrow the inner write provider without cloning the `Arc`, so callers can
    /// downcast through this wrapper to a concrete provider type with a borrow
    /// that lives as long as `&self` (e.g. the CDC apply path peeling to the
    /// inner `CayenneTableProvider`).
    #[must_use]
    pub fn writer_ref(&self) -> &Arc<dyn TableProvider> {
        &self.write
    }
}

impl FederationProvider for PolyTableProvider {
    fn name(&self) -> &'static str {
        "FederationProviderForPolyTableProvider"
    }

    fn compute_context(&self) -> Option<String> {
        self.get_federation_provider()
            .and_then(|f| f.compute_context())
    }

    fn analyzer(&self, plan: &LogicalPlan) -> Option<FederationAnalyzerForLogicalPlan> {
        self.get_federation_provider()
            .and_then(|f| f.analyzer(plan))
    }
}

#[async_trait]
impl TableLayer for PolyTableProvider {
    /// A rebuild must not push a transform beneath this layer: it owns its
    /// children and routes writes to one of them, so a transform landing
    /// underneath would sit where a write walk stops — the CDC write path would
    /// no longer find the accelerator it targets. Keeping the fold above it also
    /// means `below` is never replaced, so the child held here and the table
    /// handed to this layer cannot diverge.
    fn rebuild_descends(&self) -> bool {
        false
    }

    /// A read/write split around an accelerator: `below` is the writer side,
    /// which is what composition runs against. Only the write walk steps through
    /// — reads of an accelerated dataset reach the accelerator through the
    /// accelerated table, not by peeling this layer.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        match walk {
            LayerWalk::Write => Some(below),
            _ => None,
        }
    }

    fn schema(&self, _below: &Arc<dyn TableProvider>) -> SchemaRef {
        let schema = self.write.schema().as_ref().clone();
        let mut metadata = schema.metadata().clone();
        metadata.extend(self.schema_metadata.clone());
        Arc::new(schema.with_metadata(metadata))
    }

    fn supports_filters_pushdown(
        &self,
        _below: &Arc<dyn TableProvider>,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.fed.supports_filters_pushdown(filters)
    }





}
