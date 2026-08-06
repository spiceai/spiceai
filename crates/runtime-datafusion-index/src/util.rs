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

/// Rebuilds one wrapper layer around a transformed inner provider.
///
/// Receives the wrapper itself and a callback that recursively transforms an
/// inner provider (see [`rebuild_innermost_table_provider`]). Returns `None`
/// when the provider is not this layer's type, so the caller can try the next
/// layer. A layer that must stay outermost but cannot carry a transformed
/// inner (e.g. an adaptor whose inner provider is absent) may return the
/// provider unchanged.
pub type RebuildProviderFn = fn(
    &Arc<dyn TableProvider>,
    &dyn Fn(Arc<dyn TableProvider>) -> Arc<dyn TableProvider>,
) -> Option<Arc<dyn TableProvider>>;

/// The walk being performed over a stack of [`TableProvider`] wrappers. Each
/// [`TableProviderLayer`] declares, per context, whether the walk may step
/// through it — so the decision lives once, at the layer's descriptor, instead
/// of at every call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerWalk {
    /// Discovery on the read path: reads pass through this layer unchanged
    /// enough that looking *for* a provider type may step through it.
    Read,
    /// Changes/append-stream detection: the layer carries no CDC semantics of
    /// its own, so stream-capability detection must see through it. Layers
    /// that *are* what CDC detection looks for (index, embedding and
    /// vector-scan layers) are deliberately opaque here.
    CdcDetection,
    /// Peel to the raw source provider a connector stream should attach to.
    /// Steps through every layer whose schema additions (index, embedding or
    /// metadata columns) must not leak into queries against the source.
    Source,
    /// Write pass-through: `insert_into` delegates to the inner provider
    /// without rewriting the write. Layers that transform writes (e.g. upsert
    /// dedup) are opaque here so their semantics are preserved.
    Write,
}

/// A wrapper layer that can appear in a [`TableProvider`] stack, described
/// once: how each kind of walk steps through it, and how to rebuild it around
/// a transformed inner provider.
///
/// A field of `None` means the layer is opaque to that walk — walks stop at
/// it. Every wrapper type in the runtime must have exactly one descriptor in
/// the authoritative table (`runtime::table_layers`); a wrapper missing from
/// that table is invisible to every walk, which silently disables whatever
/// behavior the walk feeds (index writes, CDC streams, source peeling).
#[derive(Debug, Clone, Copy)]
pub struct TableProviderLayer {
    /// The wrapper's type name, for diagnostics.
    pub name: &'static str,
    /// See [`LayerWalk::Read`].
    pub read: Option<InnerProviderFn>,
    /// See [`LayerWalk::CdcDetection`].
    pub cdc_detection: Option<InnerProviderFn>,
    /// See [`LayerWalk::Source`].
    pub source: Option<InnerProviderFn>,
    /// See [`LayerWalk::Write`].
    pub write: Option<InnerProviderFn>,
    /// Rebuild this layer around a transformed inner provider, or `None` if
    /// transforms may not be pushed through it.
    pub rebuild: Option<RebuildProviderFn>,
}

impl TableProviderLayer {
    /// A descriptor with every walk opaque and no rebuild: the explicit way to
    /// record that a wrapper participates in no walk (rather than leaving it
    /// out of the table, which is indistinguishable from having forgotten it).
    #[must_use]
    pub const fn opaque(name: &'static str) -> Self {
        Self {
            name,
            read: None,
            cdc_detection: None,
            source: None,
            write: None,
            rebuild: None,
        }
    }

    /// The inner-provider accessor this layer exposes for `walk`, if any.
    #[must_use]
    pub fn inner_for(&self, walk: LayerWalk) -> Option<InnerProviderFn> {
        match walk {
            LayerWalk::Read => self.read,
            LayerWalk::CdcDetection => self.cdc_detection,
            LayerWalk::Source => self.source,
            LayerWalk::Write => self.write,
        }
    }
}

/// Steps from `tbl` to the inner provider of the first layer in `layers` that
/// matches it and is transparent to `walk`, or `None` when every layer is
/// opaque or non-matching (i.e. `tbl` is where this walk stops).
fn step_through_layers<'a>(
    tbl: &'a Arc<dyn TableProvider>,
    layers: &[TableProviderLayer],
    walk: LayerWalk,
) -> Option<&'a Arc<dyn TableProvider>> {
    layers
        .iter()
        .filter_map(|layer| layer.inner_for(walk))
        .find_map(|inner| inner(tbl.as_ref()))
}

/// Attempt to return a concrete [`TableProvider`] type from a given
/// [`impl TableProvider`], stepping only through layers transparent to `walk`.
#[must_use]
pub fn find_concrete_table_provider_in<'a, T: TableProvider + 'static>(
    tbl: &'a Arc<dyn TableProvider>,
    layers: &[TableProviderLayer],
    walk: LayerWalk,
) -> Option<&'a T> {
    let mut current_tbl = tbl;

    loop {
        if let Some(found_table) = current_tbl.downcast_ref::<T>() {
            return Some(found_table);
        }

        current_tbl = step_through_layers(current_tbl, layers, walk)?;
    }
}

/// Peels every layer transparent to `walk` and returns the innermost provider
/// reached — `tbl` itself when no layer applies.
#[must_use]
pub fn peel_to_innermost<'a>(
    tbl: &'a Arc<dyn TableProvider>,
    layers: &[TableProviderLayer],
    walk: LayerWalk,
) -> &'a Arc<dyn TableProvider> {
    let mut current = tbl;
    while let Some(inner) = step_through_layers(current, layers, walk) {
        current = inner;
    }
    current
}

/// Visits every provider along the chain of layers transparent to `walk`,
/// starting with `tbl` itself and ending at the innermost provider reached.
pub fn visit_provider_chain(
    tbl: &Arc<dyn TableProvider>,
    layers: &[TableProviderLayer],
    walk: LayerWalk,
    visit: &mut dyn FnMut(&Arc<dyn TableProvider>),
) {
    let mut current = tbl;
    loop {
        visit(current);
        match step_through_layers(current, layers, walk) {
            Some(inner) => current = inner,
            None => return,
        }
    }
}

/// Pushes `transform` to the innermost provider of the stack: layers with a
/// [`TableProviderLayer::rebuild`] are rebuilt around their transformed inner
/// provider; the first provider no layer can rebuild through receives
/// `transform` itself.
#[must_use]
pub fn rebuild_innermost_table_provider(
    provider: Arc<dyn TableProvider>,
    layers: &[TableProviderLayer],
    transform: &dyn Fn(Arc<dyn TableProvider>) -> Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    for rebuild in layers.iter().filter_map(|layer| layer.rebuild) {
        if let Some(rebuilt) = rebuild(&provider, &|inner| {
            rebuild_innermost_table_provider(inner, layers, transform)
        }) {
            return rebuilt;
        }
    }
    transform(provider)
}
