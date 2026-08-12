/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{borrow::Cow, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    error::Result as DataFusionResult,
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::Index;
use crate::layer::{LayerWalk, TableLayer};

/// Carries the indexes attached to the table beneath it.
///
/// Indexes are bound to the table *below* this layer — that is what a search
/// executes against — so a stack may hold several index layers at different
/// depths, each with its own bound table.
#[derive(Debug, Clone, Default)]
pub struct IndexLayer {
    indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl IndexLayer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_indexes(indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Self {
        Self { indexes }
    }

    #[must_use]
    pub fn add_index(mut self, index: Arc<dyn Index + Send + Sync>) -> Self {
        self.indexes.push(index);
        self
    }

    #[must_use]
    pub fn get_index<T: Index + 'static>(&self) -> Option<&T> {
        self.indexes
            .iter()
            .find_map(|i| i.as_any().downcast_ref::<T>())
    }

    #[must_use]
    pub fn get_indexes<T: Index + 'static>(&self) -> Vec<&T> {
        self.indexes
            .iter()
            .filter_map(|i| i.as_any().downcast_ref::<T>())
            .collect()
    }

    /// The indexes this layer binds to the table beneath it.
    ///
    /// Position matters: an index is bound to what sits *below* its layer, which
    /// is what a search executes against.
    #[must_use]
    pub fn indexes(&self) -> &[Arc<dyn Index + Send + Sync>] {
        &self.indexes
    }

    #[must_use]
    pub fn get_all_indexes(&self) -> Vec<Arc<dyn Index + Send + Sync>> {
        self.indexes.clone()
    }
}

#[async_trait]
impl TableLayer for IndexLayer {
    /// An index layer is what CDC detection looks *for*, so detection must stop
    /// here rather than see past it. Every other walk passes through: an index
    /// adds no columns and rewrites no write.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        // Exhaustive on purpose: a wildcard would answer a future walk kind
        // for this layer without anyone deciding what it should say.
        match walk {
            // An index layer is what CDC detection looks *for*, so detection must
            // stop here rather than see past it.
            LayerWalk::CdcDetection => None,
            // An index adds no columns and rewrites no write, so everything else
            // reaches the table beneath.
            LayerWalk::Read
            | LayerWalk::Source
            | LayerWalk::Write
            | LayerWalk::RetentionDelete
            | LayerWalk::Index => Some(below),
        }
    }

    /// The layer is replaced by the indexed `LogicalPlan` during indexing, so
    /// the table beneath it must not supply one.
    fn get_logical_plan<'a>(
        &'a self,
        _below: &'a Arc<dyn TableProvider>,
    ) -> Option<Cow<'a, LogicalPlan>> {
        None
    }

    async fn delete_from(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Resolve each attached index's matching keys against `below`'s
        // *current* (pre-delete) rows first — there's nothing left to resolve
        // once they're gone. The row delete below remains authoritative and
        // runs first: only after it succeeds do we best-effort delete the
        // previously-resolved keys from each index. This way a failed/partial
        // row delete never leaves an index missing entries for rows that were
        // never actually removed. A resolve failure just skips that index's
        // cleanup this round (self-heals via full refresh); it never blocks the
        // row delete.
        let mut resolved_keys = Vec::with_capacity(self.indexes.len());
        for index in &self.indexes {
            match index
                .resolve_delete_keys(below, state, filters.clone())
                .await
            {
                Ok(Some(keys)) => resolved_keys.push((index, keys)),
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(
                        "Index '{}' failed to resolve entries for a table delete (skipping its cleanup this round): {e}",
                        index.name()
                    );
                }
            }
        }

        let result = below.delete_from(state, filters).await?;

        for (index, keys) in resolved_keys {
            if let Err(e) = index.delete_by_keys(keys).await {
                tracing::error!(
                    "Index '{}' failed to delete entries for a table delete (best-effort, continuing): {e}",
                    index.name()
                );
            }
        }

        Ok(result)
    }
}
