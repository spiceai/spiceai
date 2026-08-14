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

use crate::cluster::DistributedNode;
use crate::{
    Error, Result, Runtime, UnableToCreateBackendSnafu, datafusion::SPICE_RUNTIME_SCHEMA,
    task_history,
};
use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use std::fmt::Write;
use std::sync::Arc;

impl Runtime {
    /// Whether the configuration in effect asks for task history.
    ///
    /// The setting the first app decided, or — before any app has been read — the
    /// value this process started with. It answers whether an initialization is
    /// worth attempting; whether one has succeeded is `task_history_initialized`.
    pub(crate) fn task_history_is_wanted(&self) -> bool {
        self.task_history_setting
            .get()
            .copied()
            .unwrap_or_else(|| self.df.task_history_enabled_at_start())
    }

    pub async fn init_task_history(self: Arc<Self>) -> Result<()> {
        // Held across everything below, so that reading the app, deciding what
        // emission should be, registering the table and recording that this
        // runtime owns it are one step. Callers race each other here — a cluster
        // executor's component load runs alongside the bind that installs its app
        // — and any check made outside this lock can be acted on after another
        // caller has already changed the answer.
        let _initializing = self.task_history_init_lock.lock().await;

        // Skip task history initialization if there's no valid spicepod
        // Task history requires App infrastructure (datasets, table providers) to function
        let Some(app) = self.read_app().await else {
            // No configuration means no table, so emission has to be off as well:
            // the flag was built from a default this process never had an app to
            // confirm, and leaving it on makes every query report a table that
            // was deliberately not created.
            //
            // Unless this runtime has already registered the table, which a caller
            // that ran while the app was being installed may have done. Emission
            // describes the table, so the answer belongs to whoever brought it up,
            // not to a caller that found no configuration.
            if !self
                .task_history_initialized
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.df.set_task_history_enabled(false);
            }
            tracing::debug!(
                "Task history initialization skipped: no valid spicepod configuration."
            );
            return Ok(());
        };

        // The first app to be read decides the setting for this process, whatever
        // happens to the table below. A reload installs a new value in the app but
        // does not change what this process does with it.
        let _ = self
            .task_history_setting
            .set(app.runtime.task_history.enabled);

        if !app.runtime.task_history.enabled {
            self.df.set_task_history_enabled(false);
            tracing::debug!("Task history is disabled via configuration.");
            return Ok(());
        }

        // A runtime that came up with no spicepod has no task-history table:
        // there was no configuration to read it from. The app that arrives later
        // — deployed, or written into a watched directory — initializes it, so
        // this can be reached twice, and the second call has to be a no-op.
        //
        // What decides that is this runtime's own record of having registered the
        // table, never the table's name: a spicepod may declare a dataset called
        // `runtime.task_history`, and on the arriving-app path datasets are
        // registered before this runs. A name check would take that dataset for
        // the internal table, report success, and send every task-history write
        // to it. So a name that is taken while this runtime has registered
        // nothing is a conflict, and it is reported rather than written into.
        if self
            .task_history_initialized
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            // This runtime owns the table and this app enables it, so emission
            // belongs on — an earlier call that ran before the app arrived turned
            // it off, and that answer was only right while there was no app.
            self.df.set_task_history_enabled(true);
            tracing::debug!("Task history is already initialized.");
            return Ok(());
        }

        // Nothing is registered yet, so nothing may be emitted yet: a query built
        // during initialization would resolve a table that does not exist, and a
        // failure below — an unusable retention setting, a backend that cannot be
        // created — has to leave emission off rather than on, or every later query
        // repeats the same missing-table failure with nothing to fix it. It is
        // turned back on where the registration succeeds.
        self.df.set_task_history_enabled(false);

        let table = TableReference::partial(
            SPICE_RUNTIME_SCHEMA,
            task_history::DEFAULT_TASK_HISTORY_TABLE,
        );
        // Cheap answer first, so a spicepod that declares a dataset under this
        // name does not pay for a backend before being told. The claim below is
        // what decides it.
        if self.df.table_exists(&table) {
            return Err(Error::UnableToTrackTaskHistory {
                source: task_history::Error::TableNameTaken {
                    table: table.to_string(),
                },
            });
        }

        let retention_period_secs = app
            .runtime
            .task_history
            .retention_period_as_secs()
            .map_err(|e| Error::UnableToTrackTaskHistory {
                source: task_history::Error::InvalidConfiguration { source: e },
            })?;

        let retention_check_interval_secs = app
            .runtime
            .task_history
            .retention_check_interval_as_secs()
            .map_err(|e| Error::UnableToTrackTaskHistory {
                source: task_history::Error::InvalidConfiguration { source: e },
            })?;

        // Log task history configuration details
        let mut config_details = format!(
            "Task history enabled: retention_period={retention_period_secs}s, retention_check_interval={retention_check_interval_secs}s"
        );

        if app.runtime.task_history.captured_context.as_ref() != "truncated" {
            let captured_context = &app.runtime.task_history.captured_context;
            let _ = write!(config_details, ", captured_context={captured_context}");
        }

        // Add min_sql_duration if configured
        if let Some(min_sql_duration) = &app.runtime.task_history.min_sql_duration {
            let _ = write!(config_details, ", min_sql_duration={min_sql_duration}");
        }

        // Add captured_plan and min_plan_duration if configured
        if let Some(captured_plan) = &app.runtime.task_history.captured_plan
            && captured_plan.as_ref() != "none"
        {
            let _ = write!(config_details, ", captured_plan={captured_plan}");

            if let Some(min_plan_duration) = &app.runtime.task_history.min_plan_duration {
                let _ = write!(config_details, ", min_plan_duration={min_plan_duration}");
            }
        }

        tracing::info!("{}", config_details);

        // Determine if we're in cluster mode (node_id column needed)
        let effective_role = self.df.cluster_config.effective_role();
        let is_cluster_mode = effective_role.is_some();

        let local_table = task_history::TaskSpan::instantiate_table(
            self.status(),
            retention_period_secs,
            retention_check_interval_secs,
            Arc::clone(&self),
            is_cluster_mode,
        )
        .await
        .map_err(|source| Error::UnableToTrackTaskHistory { source })?;

        let local =
            TableReference::partial(SPICE_RUNTIME_SCHEMA, task_history::LOCAL_TASK_HISTORY_TABLE);
        // Two names are claimed in scheduler mode and the pair is not atomic, so a
        // failure after the first one is claimed hands it back rather than leaving
        // a reserved table nothing exposes.
        let mut reserved_local = false;

        // In cluster scheduler mode, wrap the local table with FederatedTaskHistoryTable
        // to enable cluster-wide task history queries, and also register the local table
        // separately for use by the GetTaskHistory RPC handler
        let table_to_register: Arc<dyn TableProvider> = match &self.distributed {
            Some(DistributedNode::Scheduler {
                peers,
                executor_registry,
                ..
            }) => {
                let schema = local_table.schema();

                // Compute node_id: {advertise_host}:{bind_port}
                let node_id =
                    if let Some(advertise_host) = self.df.cluster_config.node_advertise_address() {
                        let bind_port = self.df.cluster_config.node_bind_address().port();
                        format!("{advertise_host}:{bind_port}")
                    } else {
                        // Fallback: use bind address directly (shouldn't happen in valid scheduler config)
                        self.df.cluster_config.node_bind_address().to_string()
                    };

                tracing::debug!("Registering federated task_history table with node_id={node_id}");

                // Register the local table under a separate name for RPC handlers to use
                // This avoids infinite recursion when peers query each other
                let local_table_provider: Arc<dyn TableProvider> =
                    local_table.into_table() as Arc<dyn TableProvider>;
                // Reserved on the same terms as the federated name: peer queries
                // are rewritten to this one, so a dataset that took it would come
                // back to the scheduler as task history.
                if let Some(_taken) = self
                    .df
                    .reserve_internal_table(local.clone(), Arc::clone(&local_table_provider))
                    .context(UnableToCreateBackendSnafu)?
                {
                    return Err(Error::UnableToTrackTaskHistory {
                        source: task_history::Error::TableNameTaken {
                            table: local.to_string(),
                        },
                    });
                }
                reserved_local = true;

                let federated = task_history::federated::FederatedTaskHistoryTable::new(
                    schema,
                    local_table_provider,
                    Arc::clone(peers),
                    executor_registry.flight_sql_clients_handle(),
                    self.df.cluster_config.client_tls_config(),
                    node_id,
                );
                Arc::new(federated)
            }
            _ => local_table.into_table() as Arc<dyn TableProvider>,
        };

        // Claiming the name and finding it free are one step, and the name stays
        // this runtime's afterwards. Dataset loading runs concurrently with this
        // on both the normal bring-up and the arriving-app path, so a check made
        // earlier can be false by the time it is acted on — and acting on it
        // wrongly means either displacing a dataset or aiming internal task rows
        // at one. The exporter resolves this table by name at write time, so a
        // name this runtime does not hold leaves emission off (it already is).
        if let Some(_taken) = self
            .df
            .reserve_internal_table(table.clone(), table_to_register)
            .context(UnableToCreateBackendSnafu)?
        {
            if reserved_local {
                self.df.release_internal_table(&local);
            }
            return Err(Error::UnableToTrackTaskHistory {
                source: task_history::Error::TableNameTaken {
                    table: table.to_string(),
                },
            });
        }
        self.task_history_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // The table exists and is this runtime's, which is the whole condition
        // for recording into it.
        self.df.set_task_history_enabled(true);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::app::App;

    /// Every decision this makes is under the initialization lock, including the
    /// one a caller with no app makes.
    ///
    /// Otherwise that caller can read no app, find the table unregistered, and
    /// only then turn emission off — by which time another caller holding the lock
    /// may have registered the table and turned emission on, leaving it off for
    /// the life of the process. The window is a few instructions wide and cannot
    /// be hit on demand, so what is asserted instead is the property that closes
    /// it: while the lock is held, this makes no progress at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_decision_is_made_outside_the_initialization_lock() {
        let rt = Arc::new(Runtime::builder().with_app_opt(None).build().await);

        let initializing = rt.task_history_init_lock.lock().await;
        let (started, has_started) = tokio::sync::oneshot::channel();
        let racing = tokio::spawn({
            let rt = Arc::clone(&rt);
            async move {
                let _ = started.send(());
                rt.init_task_history().await
            }
        });

        // Waiting on the signal is what makes the negative below mean anything: a
        // task that has not been scheduled has also "not finished", and asserting
        // that would hold whether or not the lock covered anything. Past this
        // point the call is running, and the only thing left to stop it is the
        // lock — the no-app path this guards used to return in microseconds
        // without ever reaching it.
        has_started.await.expect("the racing call started");
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !racing.is_finished(),
            "initialization decided something while the lock was held"
        );

        drop(initializing);
        racing
            .await
            .expect("the racing call ran")
            .expect("a call that finds no app is not a failure");
        assert!(
            !rt.df.task_history_emission_enabled(),
            "and having found no app, it left emission off"
        );
    }

    /// Emission describes the table, so a caller that reads no app must not turn
    /// it off once the table exists.
    ///
    /// A cluster executor's component load reaches initialization concurrently
    /// with the bind that installs its app, so one caller can read no app while
    /// the other registers the table — and if the stale read is the last to write
    /// the flag, the executor comes up with a task-history table nothing ever
    /// writes to.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stale_no_app_read_does_not_stop_emission_from_a_table_that_exists() {
        let rt = Arc::new(
            Runtime::builder()
                .with_app_opt(Some(Arc::new(App::default())))
                .build()
                .await,
        );

        Arc::clone(&rt)
            .init_task_history()
            .await
            .expect("initialize task history");
        assert!(
            rt.df.task_history_emission_enabled(),
            "a registered table is emitted into"
        );

        // What the racing caller sees: the app read it made before the bind
        // installed one.
        *rt.app.write().await = None;

        Arc::clone(&rt)
            .init_task_history()
            .await
            .expect("a call that finds no app is not a failure");
        assert!(
            rt.df.task_history_emission_enabled(),
            "and it must not stop emission for a table this runtime registered"
        );
    }
}
