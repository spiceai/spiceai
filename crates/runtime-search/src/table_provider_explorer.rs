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

/// Handles finding specific [`TableProvider`] and [`Index`] for a given [`TableProvider`].
///
/// Various implementations of [`TableProvider`] are middleware atop other [`TableProvider`]s,
///  sometimes to support [`Index`], or for other reasons. This trait provide a mechanism for
/// finding or checking that a given specific [`TableProvider`] has/is a [`TableProvider`]
///  or [`Index`].
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
