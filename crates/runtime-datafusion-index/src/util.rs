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

use crate::IndexedTableProvider;

/// Returns a borrow of the inner provider of a single known wrapper layer of a
/// [`TableProvider`], or `None` if this accessor does not apply.
///
/// The borrow is tied to the input reference (`for<'a>`), so a chain of these
/// can be followed without cloning any `Arc`.
pub type InnerProviderFn = for<'a> fn(&'a dyn TableProvider) -> Option<&'a Arc<dyn TableProvider>>;

/// Inner-provider accessor for [`IndexedTableProvider`].
pub const INDEXED_INNER: InnerProviderFn = |tbl| {
    tbl.downcast_ref::<IndexedTableProvider>()
        .map(IndexedTableProvider::get_underlying_ref)
};

/// Attempt to return a concrete [`TableProvider`] type from a given
/// [`impl TableProvider`], peeling only the wrapper layers in `inner_fns`.
///
/// At each step the current provider is checked against `T`; if it does not
/// match, the first applicable accessor is followed to the inner provider.
/// When no accessor applies, the search ends. Callers can therefore restrict
/// the search to a specific set of layers by passing a narrower slice.
pub fn find_concrete_table_provider_with<'a, T: TableProvider + 'static>(
    tbl: &'a Arc<dyn TableProvider>,
    inner_fns: &[InnerProviderFn],
) -> Option<&'a T> {
    let mut current_tbl = tbl;

    loop {
        if let Some(found_table) = current_tbl.downcast_ref::<T>() {
            return Some(found_table);
        }

        current_tbl = inner_fns
            .iter()
            .find_map(|inner| inner(current_tbl.as_ref()))?;
    }
}
