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

use app::AppBuilder;

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

            // It is safe to operate by read lock until we actually need to update the app state
            // as there is no other logic that can update the app, so write lock is not needed
            let app_read_lock = self.app.read().await;
            if let Some(current_app) = app_read_lock.as_ref() {
                let new_app = Arc::new(new_app);
                if *current_app == new_app {
                    drop(app_read_lock);
                    continue;
                }

                tracing::debug!("Updated pods information: {:?}", new_app);
                tracing::debug!("Previous pods information: {:?}", current_app);

                Arc::clone(&self)
                    .apply_catalog_diff(current_app, &new_app)
                    .await;
                Arc::clone(&self)
                    .apply_dataset_diff(current_app, &new_app)
                    .await;
                Arc::clone(&self).apply_view_diff(current_app, &new_app);
                self.apply_model_diff(current_app, &new_app).await;

                if !cfg!(feature = "models") {
                    Arc::clone(&self)
                        .apply_worker_diff(current_app, &new_app)
                        .await;
                }

                drop(app_read_lock);

                let mut app_write_lock = self.app.write().await;
                let Some(current_app) = app_write_lock.as_mut() else {
                    unreachable!("current app must exist");
                };
                *current_app = new_app;
            } else {
                drop(app_read_lock);
                let mut app_write_lock = self.app.write().await;
                *app_write_lock = Some(Arc::new(new_app));
            }
        }

        Ok(())
    }
}
