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

//! Trait for table providers that need to perform maintenance after write operations.
//!
//! This is used by tables that maintain indexes (such as hash indexes) that need to be
//! rebuilt after data is inserted or updated.

use async_trait::async_trait;
use datafusion::{datasource::TableProvider, error::Result as DataFusionResult};

/// A trait for table providers that need to perform maintenance operations after writes.
///
/// This enables tables with indexes (e.g., `IndexedMemTable` with hash indexes) to rebuild
/// their indexes after data has been inserted or updated through the refresh mechanism.
#[async_trait]
pub trait IndexMaintenanceProvider: TableProvider {
    /// Performs post-write maintenance operations, such as rebuilding indexes.
    ///
    /// This method is called after data has been successfully written to the table.
    /// Implementations should perform any necessary index rebuilding or other
    /// maintenance tasks.
    ///
    /// # Returns
    /// * `Ok(())` - Maintenance completed successfully
    /// * `Err(_)` - An error occurred during maintenance
    async fn perform_maintenance(&self) -> DataFusionResult<()>;
}

/// Attempts to call [`IndexMaintenanceProvider::perform_maintenance`] on the provided table
/// provider if it implements [`IndexMaintenanceProvider`].
///
/// This function uses `Any::downcast_ref` to check if the concrete type implements
/// `IndexMaintenanceProvider` and can be used to call the maintenance logic without requiring
/// the caller to know the concrete type.
///
/// # Returns
/// * `Ok(true)` - Maintenance was performed successfully
/// * `Ok(false)` - The table provider does not support index maintenance
/// * `Err(_)` - An error occurred during maintenance
pub async fn perform_index_maintenance(
    table_provider: &(dyn TableProvider + Send + Sync),
) -> DataFusionResult<bool> {
    let any = table_provider.as_any();

    // Try each known concrete type that implements IndexMaintenanceProvider
    // This is a stopgap until Rust supports trait upcasting (RFC 3324)
    if let Some(provider) = any.downcast_ref::<crate::arrow::IndexedMemTable>() {
        provider.perform_maintenance().await?;
        return Ok(true);
    }

    // Handle DeletionTableProviderAdapter wrapping an IndexedMemTable
    if let Some(adapter) = any.downcast_ref::<crate::delete::DeletionTableProviderAdapter>() {
        // Try to get the inner provider and perform maintenance on it
        let inner_any = adapter.source().as_any();
        if let Some(provider) = inner_any.downcast_ref::<crate::arrow::IndexedMemTable>() {
            provider.perform_maintenance().await?;
            return Ok(true);
        }
    }

    // Add additional concrete types here as they implement IndexMaintenanceProvider
    // if let Some(provider) = any.downcast_ref::<SomeOtherType>() {
    //     provider.perform_maintenance().await?;
    //     return Ok(true);
    // }

    Ok(false)
}
