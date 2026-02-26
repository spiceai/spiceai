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

//! Trait for table providers that can skip refresh operations when data hasn't changed.

use async_trait::async_trait;
use datafusion::{datasource::TableProvider, error::Result as DataFusionResult};

/// A trait for table providers that can determine if a refresh operation should be skipped.
///
/// This is useful for optimization scenarios where the table provider can detect that
/// the underlying data hasn't changed (e.g., by checking `ETag`, version ID, or timestamps),
/// allowing the refresh to be skipped entirely to avoid unnecessary data fetching.
#[async_trait]
pub trait RefreshSkipTableProvider: TableProvider {
    /// Checks whether a refresh operation should be skipped.
    ///
    /// # Returns
    /// * `Ok(true)` - The refresh should be skipped (data hasn't changed)
    /// * `Ok(false)` - The refresh should proceed (data may have changed)
    /// * `Err(_)` - An error occurred checking if the refresh should be skipped
    ///
    /// Note: If this method returns an error, the caller should typically log the error
    /// and proceed with the refresh to ensure data consistency.
    async fn should_skip_refresh(&self) -> datafusion::error::Result<bool>;

    /// Returns a reference to self as a `RefreshSkipTableProvider` trait object.
    /// This enables dynamic dispatch without requiring knowledge of the concrete type.
    fn as_refresh_skip_provider(&self) -> &dyn RefreshSkipTableProvider
    where
        Self: Sized,
    {
        self
    }
}

/// Helper trait to enable checking if a `TableProvider` implements `RefreshSkipTableProvider`
/// without knowing the concrete type.
pub trait AsRefreshSkipProvider {
    /// Returns `Some` if this table provider supports refresh skipping, `None` otherwise.
    fn try_as_refresh_skip_provider(&self) -> Option<&dyn RefreshSkipTableProvider>;
}

// Blanket implementation for all types that implement RefreshSkipTableProvider
impl<T: RefreshSkipTableProvider> AsRefreshSkipProvider for T {
    fn try_as_refresh_skip_provider(&self) -> Option<&dyn RefreshSkipTableProvider> {
        Some(self)
    }
}

/// Attempts to call [`RefreshSkipTableProvider::should_skip_refresh`] on the provided table
/// provider if it implements [`RefreshSkipTableProvider`]. Returns `Ok(None)` when the table does
/// not support refresh skipping.
///
/// This function uses `Any::downcast_ref` to check if the concrete type implements both
/// `AsRefreshSkipProvider` and can be used to call the refresh skip logic without requiring
/// the caller to know the concrete type.
pub async fn should_skip_refresh_for_table_provider(
    table_provider: &(dyn TableProvider + Send + Sync),
) -> DataFusionResult<Option<bool>> {
    let any = table_provider.as_any();

    // Try each known concrete type that implements RefreshSkipTableProvider
    // This is a stopgap until Rust supports trait upcasting (RFC 3324)
    if let Some(provider) = any.downcast_ref::<crate::s3_single_file_cached::S3SingleFileCached>() {
        return provider.should_skip_refresh().await.map(Some);
    }

    // Add additional concrete types here as they implement RefreshSkipTableProvider
    // if let Some(provider) = any.downcast_ref::<SomeOtherType>() {
    //     return provider.should_skip_refresh().await.map(Some);
    // }

    Ok(None)
}
