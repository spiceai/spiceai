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
    /// This is the same diff-based reconcile the pods watcher performs when a
    /// spicepod file changes on disk, factored out so other drivers — e.g.
    /// Spice Cloud Connect's `apply_spicepod` — can hot-apply a
    /// control-plane-supplied configuration without restarting the process.
    ///
    /// Returns `true` if `new_app` differed from the current app and was
    /// applied, `false` if it was identical (a no-op). When there is no
    /// current app yet, `new_app` is installed and `true` is returned.
    ///
    /// Diffs are applied while holding only a read lock on the app; the write
    /// lock is taken only for the final swap, matching the watcher's locking
    /// discipline (no other path mutates the app concurrently).
    pub async fn apply_app(self: Arc<Self>, new_app: Arc<App>) -> bool {
        // It is safe to operate by read lock until we actually need to update
        // the app state, as there is no other logic that can update the app,
        // so a write lock is not needed for the diff phase.
        if let Some(ref current_app) = self.read_app().await {
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

            let mut app_write_lock = self.app.write().await;
            let Some(current_app) = app_write_lock.as_mut() else {
                unreachable!("current app must exist");
            };
            *current_app = new_app;
        } else {
            let mut app_write_lock = self.app.write().await;
            *app_write_lock = Some(new_app);
        }

        true
    }
}
