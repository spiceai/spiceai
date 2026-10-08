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

//! The table a Hugging Face dataset is queried through.
//!
//! A branch moves when the dataset is updated, so a scan that listed files at one commit and
//! read them at another could combine two versions of the dataset. Each scan therefore
//! resolves the revision to a commit first and reads a listing table whose location names that
//! commit: every file it lists and reads comes from one snapshot.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use data_connector_api::listing::ListingTableTemplate;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result as DFResult, Statistics};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parking_lot::RwLock;

use crate::location::DatasetLocation;
use crate::store::HuggingFaceStore;

/// The listing location of `location` at `commit`, under the URL of the store that reads it.
///
/// # Errors
///
/// Returns an error if the location's glob is not a valid pattern.
pub fn listing_url(
    store_url: &url::Url,
    location: &DatasetLocation,
    commit: &str,
) -> DFResult<ListingTableUrl> {
    let repo = location.repo();
    let mut url = store_url.clone();
    let (folder, glob) = match location.glob() {
        Some((folder, glob)) => (folder, Some(glob)),
        None => (location.path(), None),
    };
    {
        let mut segments = url
            .path_segments_mut()
            .unwrap_or_else(|()| unreachable!("an hf:// URL with a host has path segments"));
        segments
            .pop_if_empty()
            .push(repo.owner())
            .push(&format!("{}@{commit}", repo.name()));
        if !folder.is_empty() {
            segments.extend(folder.split('/'));
        }
        // A folder or a glob lists the objects under the location; a file is read directly.
        if location.is_folder() || glob.is_some() {
            segments.push("");
        }
    }
    let pattern = glob
        .map(|glob| {
            glob::Pattern::new(glob).map_err(|e| {
                DataFusionError::Configuration(format!(
                    "Invalid glob '{glob}' in the location of Hugging Face dataset '{repo}': {e}"
                ))
            })
        })
        .transpose()?;
    ListingTableUrl::try_new(url, pattern)
}

/// Checks, before the first scan of a commit, that the commit's files can be read with the
/// schema the dataset registered with.
pub type CommitCheck = Arc<dyn Fn(String) -> BoxFuture<'static, DFResult<()>> + Send + Sync>;

/// A table reading a Hugging Face dataset at the commit its revision names when each scan
/// starts.
pub struct HuggingFaceTable {
    location: DatasetLocation,
    store: Arc<HuggingFaceStore>,
    template: ListingTableTemplate,
    schema: SchemaRef,
    /// The table at the commit the latest scan read.
    pinned: RwLock<Pinned>,
    commit_check: Option<CommitCheck>,
}

struct Pinned {
    commit: String,
    table: Arc<dyn TableProvider>,
}

impl fmt::Debug for HuggingFaceTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HuggingFaceTable")
            .field("location", &self.location)
            .field("commit", &self.pinned.read().commit)
            .finish_non_exhaustive()
    }
}

impl HuggingFaceTable {
    /// Builds the table, starting at `commit`.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be built at `commit`.
    pub fn try_new(
        location: DatasetLocation,
        store: Arc<HuggingFaceStore>,
        template: ListingTableTemplate,
        commit: String,
        commit_check: Option<CommitCheck>,
    ) -> DFResult<Self> {
        let table = template.build(listing_url(store.url(), &location, &commit)?)?;
        Ok(Self {
            location,
            store,
            template,
            schema: table.schema(),
            pinned: RwLock::new(Pinned { commit, table }),
            commit_check,
        })
    }

    /// The table at the commit the dataset's revision names now.
    async fn current(&self, state: &dyn Session) -> DFResult<Arc<dyn TableProvider>> {
        // The scanning session may have its own object store registry (a refresh builds one
        // per run), so the store is registered with it before anything is listed or read.
        state.runtime_env().register_object_store(
            self.store.url(),
            Arc::clone(&self.store) as Arc<dyn ObjectStore>,
        );

        let commit = self
            .store
            .hub()
            .commit(self.location.repo(), self.location.revision())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        {
            let pinned = self.pinned.read();
            if pinned.commit == commit.sha {
                return Ok(Arc::clone(&pinned.table));
            }
        }

        if let Some(check) = &self.commit_check {
            check(commit.sha.clone()).await?;
        }
        let table =
            self.template
                .build(listing_url(self.store.url(), &self.location, &commit.sha)?)?;
        tracing::debug!(
            "Hugging Face dataset '{}' at revision '{}' moved to commit {}",
            self.location.repo(),
            self.location.revision(),
            commit.sha
        );
        *self.pinned.write() = Pinned {
            commit: commit.sha,
            table: Arc::clone(&table),
        };
        Ok(table)
    }
}

#[async_trait]
impl TableProvider for HuggingFaceTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    // Every commit's table is built from the same template, so whichever is pinned answers
    // for the one a scan will read.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let table = Arc::clone(&self.pinned.read().table);
        table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        let table = Arc::clone(&self.pinned.read().table);
        table.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.current(state).await?;
        table.scan(state, projection, filters, limit).await
    }
}
