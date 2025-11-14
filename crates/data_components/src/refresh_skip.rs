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
use datafusion::datasource::TableProvider;

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
}
