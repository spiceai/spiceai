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

//! Synchronous table resolution for cache-backed schema providers.
//!
//! `DataFusion`'s [`SchemaProvider::table`] is async, but some paths need a table
//! provider with no async context and without blocking on catalog I/O — most
//! importantly distributed-plan deserialization on a remote executor, which
//! rebuilds a registered Iceberg scan during (synchronous) decode.
//!
//! A schema provider may opt into synchronous resolution by implementing
//! [`SyncTableProvider`] — only sound when its tables are cached in memory.
//! [`resolve_table_sync`] is the single place that views a `dyn SchemaProvider`
//! as that capability, so callers never repeat the downcast chain.
//!
//! # Adding another catalog
//!
//! To make another catalog's tables resolvable synchronously, implement
//! [`SyncTableProvider`] for its schema provider and add one entry to
//! [`SYNC_CASTS`]. Note this is only possible for providers that cache their
//! tables; providers that fetch table metadata lazily (over the network) cannot
//! offer synchronous access without first caching.

use std::sync::Arc;

use data_components::iceberg::provider::IcebergSchemaProvider;
use datafusion::catalog::{SchemaProvider, TableProvider};
use runtime_datafusion::schema_provider::SpiceSchemaProvider;

/// A [`SchemaProvider`] whose tables are cached and can therefore be resolved
/// synchronously (no async, no catalog I/O).
pub(crate) trait SyncTableProvider {
    /// Look up a table by name from the provider's in-memory cache.
    fn sync_table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;
}

impl SyncTableProvider for SpiceSchemaProvider {
    fn sync_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.table_sync(name)
    }
}

impl SyncTableProvider for IcebergSchemaProvider {
    fn sync_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.table_sync(name)
    }
}

/// Views a `dyn SchemaProvider` as a [`SyncTableProvider`] when its concrete
/// type supports synchronous resolution. One entry per supporting type — this
/// list is the single extension point for adding a catalog's synchronous tables.
///
/// `downcast_ref` is called directly on the trait object: in `DataFusion` 54
/// `Any` is a supertrait of `SchemaProvider` (and `as_any` was removed).
type SyncCast = fn(&dyn SchemaProvider) -> Option<&dyn SyncTableProvider>;
const SYNC_CASTS: &[SyncCast] = &[
    |s| {
        s.downcast_ref::<SpiceSchemaProvider>()
            .map(|p| p as &dyn SyncTableProvider)
    },
    |s| {
        s.downcast_ref::<IcebergSchemaProvider>()
            .map(|p| p as &dyn SyncTableProvider)
    },
];

/// Resolve table `name` synchronously from `schema_provider`, or `None` if the
/// provider does not support synchronous access (see [`SyncTableProvider`]).
///
/// This is the centralized entry point: anything that needs a table without an
/// async context calls here rather than matching on schema-provider types.
pub fn resolve_table_sync(
    schema_provider: &dyn SchemaProvider,
    name: &str,
) -> Option<Arc<dyn TableProvider>> {
    SYNC_CASTS
        .iter()
        .find_map(|cast| cast(schema_provider))
        .and_then(|sync| sync.sync_table(name))
}
