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
    /// This is the diff-based reconcile the pods watcher performs when a
    /// spicepod file changes on disk, and the one a Spice Cloud deployment takes
    /// when what it changes is confined to the sections reconciled here (see
    /// `spiced`'s `cloud_connect` module).
    ///
    /// Returns `true` if `new_app` differed from the current app and was
    /// applied, `false` if it was identical (a no-op). When there is no
    /// current app yet, `new_app` is installed and `true` is returned.
    ///
    /// Diffs are computed while holding only a read lock on the app; the write
    /// lock is taken only for the final swap. The whole method is serialized by
    /// [`Runtime::apply_app_lock`] so two applies cannot diff against the same
    /// old app, interleave their catalog/dataset/view mutations, and overwrite
    /// `self.app` last-writer-wins. We hold the dedicated mutex (rather than the
    /// app write lock) for the duration so the diff phase can still read the app
    /// `RwLock` without deadlocking.
    pub async fn apply_app(self: Arc<Self>, new_app: Arc<App>) -> bool {
        // Serialize the entire diff-and-swap so concurrent callers apply
        // one-at-a-time. Must be the first statement.
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

    /// Diff-and-apply behind [`Runtime::apply_app`].
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

            // `runtime.cpu` sizes thread pools that are already running, so it
            // is start-time only. Say so rather than silently ignoring the edit.
            if current_app.runtime.cpu != new_app.runtime.cpu {
                tracing::warn!(
                    "`runtime.cpu` changed, but the CPU budget sizes thread pools that are already running: the previous value stays in effect. Restart spiced to apply it."
                );
            }

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
