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
    /// spicepod file changes on disk. It is the *local* configuration path: a
    /// Spice Cloud deployment does not come through here, because it applies by
    /// persisting the spicepod and restarting onto it (see
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
        if current_app.is_some_and(|current_app| *current_app == new_app) {
            return false;
        }

        // Re-split the coordinated DuckDB accelerator memory budget for the
        // acceleration set `new_app` declares, before the diffs below initialize any
        // accelerator that reads it.
        self.duckdb_budget_context.publish_for(&new_app);

        if let Some(current_app) = current_app {
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

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use std::num::NonZeroU64;
    use std::path::Path;
    use std::sync::Arc;

    use app::AppBuilder;
    use spicepod::acceleration::{Acceleration, Mode};
    use spicepod::component::dataset::Dataset;
    use spicepod::param::Params;

    use crate::Runtime;
    use crate::accelerator_memory_budget::{
        DUCKDB_MIN_INSTANCE_CAP_BYTES, duckdb_auto_memory_limit_option,
        duckdb_total_reservation_bytes,
    };

    const MIB: u64 = 1024 * 1024;

    /// A dataset declaring one `DuckDB` instance of its own. No build of the runtime
    /// resolves its `from:`, so it fails its load permanently instead of retrying:
    /// the budget is planned from the accelerations the Spicepod declares, before
    /// any of them is initialized.
    fn duckdb_dataset(name: &str, duckdb_file: &Path) -> Dataset {
        let mut dataset = Dataset::new("not_a_real_connector:any", name);
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("duckdb".to_string()),
            mode: Mode::File,
            params: Some(Params::from_string_map(
                [(
                    "duckdb_file".to_string(),
                    duckdb_file.to_string_lossy().to_string(),
                )]
                .into_iter()
                .collect(),
            )),
            ..Acceleration::default()
        });
        dataset
    }

    /// The published per-instance cap, in whole MiB — the `memory_limit` the `DuckDB`
    /// accelerator gives an instance it creates for a dataset that set none itself.
    fn published_per_instance_mib() -> Option<u64> {
        duckdb_auto_memory_limit_option()?
            .strip_suffix("MiB")?
            .parse()
            .ok()
    }

    /// A reload changes which `DuckDB` instances exist, so it must republish the
    /// coordinated budget: a second instance splits what the first one held to
    /// itself, removing every `DuckDB` accelerator clears the budget rather than
    /// leaving the reservation the removed instances held, and a pod that gains its
    /// first accelerator on reload gets the per-instance floor — its query pool was
    /// sized without coordinating for `DuckDB` and only a restart re-sizes it.
    ///
    /// The budget is process-global and every `Runtime` built anywhere in this binary
    /// republishes it — an app with no `DuckDB` accelerator clears it — so a peer test
    /// building a runtime can land between an apply here and the read of what it
    /// published. That shows up as a cleared budget, which no step of this scenario
    /// produces, so [`observe`] retries the scenario instead of reporting it. A
    /// budget this reload leaves *unchanged* is the defect under test and is never
    /// retried.
    #[tokio::test]
    async fn apply_app_republishes_the_duckdb_memory_budget() {
        for attempt in 1..=OBSERVATION_ATTEMPTS {
            if republishes_the_duckdb_memory_budget(attempt == OBSERVATION_ATTEMPTS).await {
                return;
            }
        }
    }

    /// How many times [`apply_app_republishes_the_duckdb_memory_budget`] re-runs when
    /// a concurrently-built runtime clears the budget out from under it. Interference
    /// needs a peer test to publish inside a window of a few hundred microseconds, so
    /// a handful of attempts puts a spurious failure out of reach.
    const OBSERVATION_ATTEMPTS: u32 = 5;

    /// One run of the scenario. Returns whether it observed its own state throughout;
    /// `false` means a peer runtime cleared the budget mid-scenario and it proved
    /// nothing. When `final_attempt`, a cleared budget fails rather than returning
    /// `false`, so exhausting the retries can never pass silently.
    async fn republishes_the_duckdb_memory_budget(final_attempt: bool) -> bool {
        macro_rules! observe {
            ($value:expr, $what:expr) => {
                match $value {
                    Some(observed) => observed,
                    None if final_attempt => panic!("{}", $what),
                    None => return false,
                }
            };
        }

        let dir = tempfile::tempdir().expect("temp dir");
        let one = duckdb_dataset("one", &dir.path().join("one.db"));
        let two = duckdb_dataset("two", &dir.path().join("two.db"));

        let rt = Arc::new(
            Runtime::builder()
                .with_app(
                    AppBuilder::new("duckdb_budget_reload")
                        .with_dataset(one.clone())
                        .build(),
                )
                .build()
                .await,
        );
        let one_instance = observe!(
            published_per_instance_mib(),
            "building with a DuckDB accelerator publishes a per-instance cap"
        );

        let both = AppBuilder::new("duckdb_budget_reload")
            .with_dataset(one)
            .with_dataset(two)
            .build();
        assert!(
            Arc::clone(&rt).apply_app(Arc::new(both)).await,
            "the reload adds a dataset, so it must be applied"
        );

        let two_instances = observe!(
            published_per_instance_mib(),
            "a reload that keeps a DuckDB accelerator keeps a per-instance cap"
        );
        assert!(
            two_instances < one_instance,
            "the reload added a second DuckDB instance, so the published cap must shrink: {two_instances} MiB vs {one_instance} MiB"
        );
        assert!(
            two_instances.abs_diff(one_instance / 2) <= 1,
            "the two instances must split what the single instance held: {two_instances} MiB vs {one_instance} MiB"
        );
        // The instance that already exists keeps the memory_limit it was created
        // with, so the aggregate still has to cover it at the larger cap.
        let reservation_after_two = observe!(
            NonZeroU64::new(duckdb_total_reservation_bytes()),
            "a reload that keeps a DuckDB accelerator keeps a reservation"
        )
        .get();
        assert!(
            reservation_after_two >= 2 * one_instance * MIB,
            "the reservation must cover the first instance at the cap it was created with: {reservation_after_two} bytes"
        );

        let unaccelerated = AppBuilder::new("duckdb_budget_reload")
            .with_dataset(Dataset::new("not_a_real_connector:any", "plain"))
            .build();
        assert!(
            Arc::clone(&rt).apply_app(Arc::new(unaccelerated)).await,
            "the reload removes both datasets, so it must be applied"
        );
        assert_eq!(
            published_per_instance_mib(),
            None,
            "a reload that removes every DuckDB accelerator must retire the per-instance cap"
        );
        // Dropping the datasets does not evict the accelerator's cached pools, so the
        // instances can go on holding what they were created with.
        assert_eq!(
            duckdb_total_reservation_bytes(),
            reservation_after_two,
            "a reload that removes every DuckDB accelerator must keep reserving what its instances may still hold"
        );

        rt.shutdown().await;

        // A pod built without a DuckDB accelerator sized its query pool without
        // coordinating for one; the accelerator a reload adds is held to the
        // per-instance floor, because only a restart re-sizes that pool.
        let uncoordinated = Arc::new(
            Runtime::builder()
                .with_app(
                    AppBuilder::new("duckdb_budget_first")
                        .with_dataset(Dataset::new("not_a_real_connector:any", "plain"))
                        .build(),
                )
                .build()
                .await,
        );
        assert_eq!(
            published_per_instance_mib(),
            None,
            "a pod with no DuckDB accelerator publishes no cap"
        );

        let accelerated = AppBuilder::new("duckdb_budget_first")
            .with_dataset(duckdb_dataset("first", &dir.path().join("first.db")))
            .build();
        assert!(
            Arc::clone(&uncoordinated)
                .apply_app(Arc::new(accelerated))
                .await,
            "the reload adds a DuckDB-accelerated dataset, so it must be applied"
        );
        let first_instance = observe!(
            published_per_instance_mib(),
            "a reload that adds the first DuckDB accelerator publishes a per-instance cap"
        );
        assert_eq!(
            first_instance,
            DUCKDB_MIN_INSTANCE_CAP_BYTES / MIB,
            "the query pool already holds the splittable region, so the instance the reload adds gets the per-instance floor"
        );

        uncoordinated.shutdown().await;
        true
    }
}
