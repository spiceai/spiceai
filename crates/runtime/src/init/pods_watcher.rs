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

use crate::Runtime;

impl Runtime {
    pub(crate) async fn start_pods_watcher(&self) -> notify::Result<()> {
        let mut pods_watcher = self.pods_watcher.write().await;
        let Some(mut pods_watcher) = pods_watcher.take() else {
            return Ok(());
        };
        let mut rx = pods_watcher.watch()?;

        while let Some(new_app) = rx.recv().await {
            let mut app_lock = self.app.write().await;
            if let Some(current_app) = app_lock.as_mut() {
                let new_app = Arc::new(new_app);
                if *current_app == new_app {
                    drop(app_lock);
                    continue;
                }

                tracing::debug!("Updated pods information: {:?}", new_app);
                tracing::debug!("Previous pods information: {:?}", current_app);

                self.apply_catalog_diff(current_app, &new_app).await;
                self.apply_dataset_diff(current_app, &new_app).await;
                self.apply_view_diff(current_app, &new_app);
                self.apply_model_diff(current_app, &new_app).await;

                *current_app = new_app;
            } else {
                *app_lock = Some(Arc::new(new_app));
            }
            drop(app_lock);
        }

        Ok(())
    }
}
