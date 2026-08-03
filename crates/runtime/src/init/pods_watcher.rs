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

use std::sync::Arc;

use app::{App, AppBuilder};

use crate::Runtime;
use crate::component::dataset::Dataset;

impl Runtime {
    pub(crate) async fn start_pods_watcher(self: Arc<Self>) -> notify::Result<()> {
        let mut pods_watcher = self.pods_watcher.write().await;
        let Some(mut pods_watcher) = pods_watcher.take() else {
            return Ok(());
        };
        let mut rx = pods_watcher.watch().await?;

        while let Some(new_app_path) = rx.recv().await {
            let new_app = match AppBuilder::build_from_path(new_app_path).await {
                Ok(app) => app,
                Err(e) => {
                    tracing::warn!(
                        "Invalid app state detected, unable to load pods information: {e}"
                    );
                    continue;
                }
            };

            Arc::clone(&self).apply_app(Arc::new(new_app)).await;
        }

        Ok(())
    }

    /// Hot-apply a new [`App`] to the running runtime, reconciling catalogs,
    /// datasets, views, models, functions, and (without the `models` feature)
    /// workers against the currently-loaded app.
    ///
    /// This is the same diff-based reconcile the pods watcher performs when a
    /// spicepod file changes on disk, factored out so other drivers — e.g.
    /// Spice Cloud Connect's `apply_spicepod` — can hot-apply a
    /// control-plane-supplied configuration without restarting the process.
    ///
    /// Returns `true` if `new_app` differed from the current app and was
    /// applied, `false` if it was identical (a no-op). When there is no
    /// current app yet, `new_app` is installed and `true` is returned.
    ///
    /// Diffs are computed while holding only a read lock on the app; the write
    /// lock is taken only for the final swap. This whole method is serialized by
    /// [`Runtime::apply_app_lock`] because it now has two independent callers —
    /// the on-disk pods watcher loop and Spice Cloud Connect's `apply_spicepod`
    /// — which can invoke it concurrently. Without serialization two applies
    /// could diff against the same old app, interleave their catalog/dataset/
    /// view mutations, and overwrite `self.app` last-writer-wins. We hold the
    /// dedicated mutex (rather than the app write lock) for the duration so the
    /// diff phase can still read the app `RwLock` without deadlocking.
    pub async fn apply_app(self: Arc<Self>, new_app: Arc<App>) -> bool {
        // Serialize the entire diff-and-swap so concurrent callers (pods watcher
        // + Cloud Connect) apply one-at-a-time. Must be the first statement.
        let _serialize = self.apply_app_lock.lock().await;

        // It is safe to operate by read lock until we actually need to update
        // the app state: with applies serialized by `_serialize`, no other path
        // mutates the app during the diff phase, so a write lock is not needed
        // until the final swap.
        let current_app = self.read_app().await;
        Arc::clone(&self)
            .apply_app_diff(current_app.as_ref(), new_app)
            .await
    }

    /// Hot-apply `new_app` on top of a runtime whose initial component load was
    /// abandoned by [`Runtime::supersede_initial_load`].
    ///
    /// [`Runtime::apply_app`] diffs against the *declared* app, which after a
    /// cancelled load overstates what is registered: a dataset the load never
    /// reached is declared but absent, so the diff would find it in both apps,
    /// treat it as unchanged, and skip it — leaving the table missing for the
    /// life of the process. This diffs against the datasets that really are
    /// registered instead, so everything the load did not finish is applied as
    /// new. Datasets that did load are still diffed normally and are not
    /// re-registered.
    ///
    /// Scope note: only datasets are reconciled this way. They are the
    /// components the initial load registers through the unbounded-retry path,
    /// so they are the ones a cancelled load reliably leaves half-registered;
    /// catalogs, views, and models are diffed against the declared app exactly
    /// as [`Runtime::apply_app`] does.
    pub async fn apply_app_after_cancelled_load(self: Arc<Self>, new_app: Arc<App>) -> bool {
        let _serialize = self.apply_app_lock.lock().await;

        let baseline = self.read_app().await.map(|current| {
            let mut registered = (*current).clone();
            registered.datasets.retain(|ds| {
                Dataset::parse_table_reference(&ds.name)
                    .is_ok_and(|table| self.df.table_exists(&table))
            });
            Arc::new(registered)
        });

        Arc::clone(&self)
            .apply_app_diff(baseline.as_ref(), new_app)
            .await
    }

    /// Diff-and-apply shared by [`Runtime::apply_app`] and
    /// [`Runtime::apply_app_after_cancelled_load`].
    ///
    /// The caller holds `apply_app_lock`. `current_app` is what to reconcile
    /// *from*, which is not necessarily the installed app; `new_app` is
    /// installed either way.
    async fn apply_app_diff(
        self: Arc<Self>,
        current_app: Option<&Arc<App>>,
        new_app: Arc<App>,
    ) -> bool {
        if let Some(current_app) = current_app {
            if *current_app == new_app {
                return false;
            }

            tracing::debug!("Updated pods information: {new_app:?}");
            tracing::debug!("Previous pods information: {current_app:?}");

            Arc::clone(&self)
                .apply_catalog_diff(current_app, &new_app)
                .await;
            Arc::clone(&self)
                .apply_dataset_diff(current_app, &new_app)
                .await;
            Arc::clone(&self)
                .apply_view_diff(current_app, &new_app)
                .await;
            self.apply_model_diff(current_app, &new_app).await;
            crate::datafusion::udf::apply_function_diff(&self, current_app, &new_app).await;

            if !cfg!(feature = "models") {
                Arc::clone(&self)
                    .apply_worker_diff(current_app, &new_app)
                    .await;
            }
        }

        *self.app.write().await = Some(new_app);

        true
    }
}
