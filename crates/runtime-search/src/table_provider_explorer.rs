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

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use runtime_datafusion_index::Index;

/// Trait for unwrapping nested `TableProvider` layers to find concrete types.
///
/// Implementations peel through runtime-specific wrappers (e.g. `AcceleratedTable`,
/// `FederatedTableProviderAdaptor`, `EmbeddingTable`, etc.) to find a requested
/// concrete type via `as_any().downcast_ref::<T>()`.
///
/// `SearchEngine` is generic over this trait so it can find embedding tables
/// and search indexes without depending on the concrete wrapper types.
pub trait TableProviderExplorer: Send + Sync + std::fmt::Debug {
    /// Find a concrete `TableProvider` type inside a (possibly wrapped) provider.
    fn find_concrete<'a, T: TableProvider + 'static>(
        &self,
        tbl: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a T>;

    /// Find all indexes of a concrete `Index` type inside a (possibly wrapped) provider.
    fn find_index<'a, T: Index + 'static>(
        &self,
        tbl: &'a Arc<dyn TableProvider>,
    ) -> Option<(Vec<&'a T>, Arc<dyn TableProvider>)>;
}
