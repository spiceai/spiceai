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

//! Dataset initialization value type.
//!
//! `DatasetInitialization` is a pure-data planning value that captures
//! everything needed to materialize one dataset, with no I/O at
//! construction. Calling [`DatasetInitialization::initialize`] runs the
//! full bring-up.
//!
//! ## Connector / schema sources
//!
//! * [`ConnectorSource::Eager`] — connector already constructed at
//!   plan time. Used for the startup path, hot-reload, and any case
//!   where the trigger condition for deferral is not met.
//! * [`ConnectorSource::Lazy`] — connector built on first reference,
//!   inside `initialize`. Captures the factory + resolved params via a
//!   parked closure so plan construction stays I/O-free.
//! * [`SchemaSource::FromProvider`] — schema is inferred from the
//!   constructed connector via `read_provider`.
//! * [`SchemaSource::Known`] — schema is determined entirely from
//!   configuration (user-declared `columns[]` or
//!   `DataConnectorFactory::static_schema`). `initialize` skips the
//!   `read_provider` schema-inference step.
//!
//! `should_initialize_at_startup` returns `false` only when both
//! `ConnectorSource::Lazy` and `SchemaSource::Known` are picked, which
//! is the deferred path: a placeholder is registered up-front and the
//! real provider is materialized on first reference.

use crate::dataconnector::parameters::RuntimeConnectorContext;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use futures::future::BoxFuture;
use tokio::sync::Semaphore;

use crate::Error as RuntimeError;
use crate::component::dataset::Dataset;
use crate::dataconnector::{DataConnector, NewDataConnectorResult};
use crate::datafusion::AcceleratorBootstrap;
use crate::init::dataset::is_permanent_dataset_failure;
use crate::{Result, Runtime, accelerated::AcceleratedTable};

/// Where the `DataConnector` for this dataset comes from.
pub enum ConnectorSource {
    /// Connector is already constructed. Used for the eager startup
    /// path, the hot-reload path, and any case where the trigger
    /// condition for deferral is not met.
    Eager(Arc<dyn DataConnector>),

    /// Connector built lazily by `initialize`. The closure captures the
    /// factory plus resolved `ConnectorParams` so plan construction
    /// stays I/O-free; it must construct the connector and any
    /// post-construction wrappers (e.g. `EmbeddingConnector`,
    /// `FullTextConnector`).
    Lazy(LazyConnectorBuilder),
}

/// Builds the connector of a deferred dataset. `Fn`, not `FnOnce`: a first reference that
/// fails because the source is unavailable is retried on a later one, which builds again.
pub type LazyConnectorBuilder =
    Box<dyn Fn() -> BoxFuture<'static, NewDataConnectorResult> + Send + Sync>;

/// How the dataset's schema is resolved.
pub enum SchemaSource {
    /// Schema is unknown at planning time; `initialize` will call
    /// `DataConnector::read_provider` and use the provider's schema.
    FromProvider,

    /// Schema is known from configuration. `initialize` will not call
    /// `read_provider` for schema purposes; it will validate the real
    /// provider's schema matches on first scan.
    Known { schema: SchemaRef },
}

/// Outcome of a successful [`DatasetInitialization::initialize`] call.
///
/// `table_provider` is `Some` for the lazy-known path (which builds
/// the provider but does not register it; the caller — typically
/// `DatasetTableProvider::ensure_ready` — performs the catalog swap).
/// For the eager path, `initialize` still delegates to
/// `Runtime::register_loaded_dataset` which registers the provider as
/// a side effect, and `table_provider` is left `None`.
#[derive(Default)]
pub struct DatasetReady {
    pub table_provider: Option<Arc<dyn TableProvider>>,
    pub accelerated_table: Option<Arc<AcceleratedTable>>,
    pub data_connector: Option<Arc<dyn DataConnector>>,
}

impl std::fmt::Debug for DatasetReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetReady")
            .field("has_table_provider", &self.table_provider.is_some())
            .field("has_accelerated_table", &self.accelerated_table.is_some())
            .field("has_data_connector", &self.data_connector.is_some())
            .finish()
    }
}

/// Pure-data plan for materializing one dataset.
///
/// Construction is I/O-free. All side effects (connector construction,
/// `read_provider`, catalog registration, refresh-task spawning, health
/// monitor registration) happen inside [`DatasetInitialization::initialize`].
pub struct DatasetInitialization {
    dataset: Arc<Dataset>,
    runtime: Arc<Runtime>,
    connector_source: ConnectorSource,
    schema_source: SchemaSource,
    bootstrap_status: AcceleratorBootstrap,
    load_semaphore: Option<Arc<Semaphore>>,
    preloaded_accelerated_table: Option<crate::datafusion::PreparedAcceleratedTable>,
}

impl DatasetInitialization {
    /// Build an eager initialization plan for a dataset whose connector
    /// has already been constructed. Used by the hot-reload path and as
    /// a fallback when deferral preconditions are not met.
    pub fn plan_eager(
        dataset: Arc<Dataset>,
        runtime: Arc<Runtime>,
        connector: Arc<dyn DataConnector>,
        bootstrap_status: impl Into<AcceleratorBootstrap>,
        load_semaphore: Option<Arc<Semaphore>>,
        preloaded_accelerated_table: Option<crate::datafusion::PreparedAcceleratedTable>,
    ) -> Self {
        Self {
            dataset,
            runtime,
            connector_source: ConnectorSource::Eager(connector),
            schema_source: SchemaSource::FromProvider,
            bootstrap_status: bootstrap_status.into(),
            load_semaphore,
            preloaded_accelerated_table,
        }
    }

    /// Build a deferred initialization plan with a known schema and a
    /// lazily-constructed connector.
    ///
    /// I/O-free; the connector is built only inside `initialize`. The
    /// caller is responsible for ensuring `connector_builder` performs
    /// the same factory wrapping (`EmbeddingConnector`,
    /// `FullTextConnector`) that the eager path applies in
    /// `Runtime::load_dataset_connector`.
    pub fn plan_deferred(
        dataset: Arc<Dataset>,
        runtime: Arc<Runtime>,
        connector_builder: LazyConnectorBuilder,
        schema: SchemaRef,
        bootstrap_status: impl Into<AcceleratorBootstrap>,
        load_semaphore: Option<Arc<Semaphore>>,
    ) -> Self {
        Self {
            dataset,
            runtime,
            connector_source: ConnectorSource::Lazy(connector_builder),
            schema_source: SchemaSource::Known { schema },
            bootstrap_status: bootstrap_status.into(),
            load_semaphore,
            preloaded_accelerated_table: None,
        }
    }

    pub(crate) fn bootstrap(&self) -> &AcceleratorBootstrap {
        &self.bootstrap_status
    }

    /// Synchronous accessor: the dataset this plan will initialize.
    #[expect(dead_code)] // Reserved for upcoming deferred-initialization callers.
    pub(crate) fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// True iff this plan must run at startup. False iff it can be
    /// deferred to first reference.
    #[must_use]
    pub fn should_initialize_at_startup(&self) -> bool {
        !matches!(
            (&self.connector_source, &self.schema_source),
            (ConnectorSource::Lazy(_), SchemaSource::Known { .. })
        )
    }

    /// Consume the plan and run the dataset bring-up to completion.
    pub async fn initialize(self) -> Result<DatasetReady> {
        self.initialize_retriable().await.map_err(|f| f.error)
    }

    /// Like [`Self::initialize`], and when the bring-up fails for a reason a later attempt
    /// can clear (the source is unavailable), hands the plan back to try again.
    /// Configuration errors, which no retry clears, return no plan.
    pub async fn initialize_retriable(
        self,
    ) -> std::result::Result<DatasetReady, Box<InitializeFailure>> {
        let mut retry = None;
        self.run(&mut retry)
            .await
            .map_err(|error| Box::new(InitializeFailure { error, retry }))
    }

    async fn run(self, retry: &mut Option<Box<DatasetInitialization>>) -> Result<DatasetReady> {
        let Self {
            dataset,
            runtime,
            connector_source,
            schema_source,
            bootstrap_status,
            load_semaphore,
            preloaded_accelerated_table,
        } = self;

        match (connector_source, schema_source) {
            (ConnectorSource::Eager(connector), SchemaSource::FromProvider) => {
                Box::pin(runtime.register_loaded_dataset(
                    dataset,
                    connector,
                    preloaded_accelerated_table,
                    bootstrap_status,
                    load_semaphore,
                ))
                .await?;
                Ok(DatasetReady::default())
            }

            (ConnectorSource::Lazy(builder), SchemaSource::Known { schema }) => {
                // A deferred dataset whose existing acceleration can serve it answers
                // its first query from that acceleration rather than connecting to the
                // source first; the source is connected in the background.
                let connector = if Runtime::serves_existing_acceleration(&dataset).await {
                    Runtime::reconnecting_connector(&dataset)
                } else {
                    match builder().await {
                        Ok(connector) => connector,
                        Err(source) => {
                            // The production builder already returns a boxed `RuntimeError`
                            // (`UnableToInitializeDataConnector` for a factory failure); keep
                            // it as is, so the classifier sees the factory's own error and a
                            // configuration failure stays permanent.
                            let err = match source.downcast::<RuntimeError>() {
                                Ok(err) => *err,
                                Err(source) => {
                                    RuntimeError::UnableToInitializeDataConnector { source }
                                }
                            };
                            if !is_permanent_dataset_failure(&err) {
                                *retry = Some(Box::new(Self {
                                    dataset: Arc::clone(&dataset),
                                    runtime: Arc::clone(&runtime),
                                    connector_source: ConnectorSource::Lazy(builder),
                                    schema_source: SchemaSource::Known { schema },
                                    bootstrap_status,
                                    load_semaphore,
                                    preloaded_accelerated_table,
                                }));
                            }
                            return Err(report_deferred_failure(&runtime, &dataset, err));
                        }
                    }
                };

                // Accelerated deferred datasets: hand off to
                // the eager bring-up path; it constructs the
                // AcceleratedTable, registers metrics + health
                // monitor, and starts refresh. The placeholder must be
                // deregistered first so the eager path's
                // `ctx.register_table` slot is free.
                if dataset.acceleration.as_ref().is_some_and(|a| a.enabled) {
                    // Register the real (accelerated) provider first.
                    // `register_loaded_dataset` calls `ctx.register_table`
                    // which overwrites the placeholder in place. Only
                    // after the real provider is registered do we drop
                    // the pending bookkeeping, so the placeholder —
                    // which the resolver hook can find via the pending
                    // registry — stays valid right up until the swap
                    // succeeds. If `register_loaded_dataset` fails, the
                    // placeholder remains in place and a retry can run.
                    //
                    // An earlier attempt that got as far as building the
                    // acceleration consumed its storage generation, so this
                    // one starts from a fresh one, as `load_dataset`'s retry
                    // does.
                    let bootstrap_status = if bootstrap_status.needs_reinitialization() {
                        match runtime
                            .initialize_datasets_accelerators(std::slice::from_ref(&dataset))
                            .await
                            .remove(&dataset.name)
                        {
                            Some(Ok(bootstrap)) => bootstrap.restore_once().await,
                            Some(Err(err)) => {
                                if !is_permanent_load_failure(&err) {
                                    // `initialize_accelerator` revokes the previous owner
                                    // before `init` can fail. Keep an owned reinitialization
                                    // intent so the next retry does not skip to `borrow` on
                                    // that revoked bootstrap; if a newer configuration already
                                    // holds the slot, the dummy is revoked and `borrow` still
                                    // rejects the superseded plan.
                                    let bootstrap_status =
                                        if bootstrap_status.needs_reinitialization() {
                                            bootstrap_status
                                        } else {
                                            runtime.df.awaiting_accelerator_reinitialization(
                                                &dataset.name,
                                            )
                                        };
                                    *retry = Some(Box::new(Self {
                                        dataset: Arc::clone(&dataset),
                                        runtime: Arc::clone(&runtime),
                                        connector_source: ConnectorSource::Lazy(builder),
                                        schema_source: SchemaSource::Known { schema },
                                        bootstrap_status,
                                        load_semaphore,
                                        preloaded_accelerated_table,
                                    }));
                                }
                                return Err(err);
                            }
                            None => {
                                return Err(RuntimeError::UnableToInitializeDataConnector {
                                    source: "Missing accelerator bootstrap result".into(),
                                });
                            }
                        }
                    } else {
                        bootstrap_status
                    };

                    // `register_loaded_dataset` reports its own failure (the
                    // status, `LOAD_ERROR` and the log line), so it does not
                    // pass through `report_deferred_failure`.
                    if let Err(err) = Box::pin(Arc::clone(&runtime).register_loaded_dataset(
                        Arc::clone(&dataset),
                        Arc::clone(&connector),
                        None,
                        bootstrap_status.clone(),
                        load_semaphore.clone(),
                    ))
                    .await
                    {
                        if !is_permanent_load_failure(&err) {
                            *retry = Some(Box::new(Self {
                                dataset: Arc::clone(&dataset),
                                runtime: Arc::clone(&runtime),
                                connector_source: ConnectorSource::Lazy(builder),
                                schema_source: SchemaSource::Known { schema },
                                bootstrap_status,
                                load_semaphore,
                                preloaded_accelerated_table,
                            }));
                        }
                        return Err(err);
                    }

                    runtime
                        .df
                        .complete_pending_initialization(&dataset.name)
                        .await;

                    return Ok(DatasetReady {
                        table_provider: None, // registration happened as side-effect
                        accelerated_table: None,
                        data_connector: Some(connector),
                    });
                }

                let context = RuntimeConnectorContext::for_dataset(&dataset);
                let federated_provider = match connector.read_provider(&context, &dataset).await {
                    Ok(provider) => provider,
                    Err(e) => {
                        let err = RuntimeError::UnableToInitializeDataConnector {
                            source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                        };
                        if !is_permanent_dataset_failure(&err) {
                            *retry = Some(Box::new(Self {
                                dataset: Arc::clone(&dataset),
                                runtime: Arc::clone(&runtime),
                                connector_source: ConnectorSource::Lazy(builder),
                                schema_source: SchemaSource::Known { schema },
                                bootstrap_status,
                                load_semaphore,
                                preloaded_accelerated_table,
                            }));
                        }
                        return Err(report_deferred_failure(&runtime, &dataset, err));
                    }
                };

                // Validate that the resolved provider's schema matches
                // the declared/static schema. Mismatch is a config bug
                // and we fail fast rather than silently using the
                // provider's schema (which would diverge from what's
                // already in the catalog).
                if !arrow_schemas_equal(&schema, &federated_provider.schema()) {
                    return Err(report_deferred_failure(
                        &runtime,
                        &dataset,
                        RuntimeError::PermanentDatasetFailure {
                            dataset: dataset.name.clone(),
                            reason: format!(
                                "Declared schema does not match source schema for dataset `{}`. \
                                 Declared: {:?}; source: {:?}",
                                dataset.name,
                                schema.fields(),
                                federated_provider.schema().fields(),
                            ),
                        },
                    ));
                }

                // A failed earlier attempt left the dataset in `Error`
                // (`report_deferred_failure`); this one serves it, so it is `Ready`
                // again. The accelerated branch above reports through
                // `register_loaded_dataset` instead.
                runtime
                    .status
                    .update_dataset(&dataset.name, runtime_status::ComponentStatus::Ready);

                Ok(DatasetReady {
                    table_provider: Some(federated_provider),
                    accelerated_table: None,
                    data_connector: Some(connector),
                })
            }

            // Other combinations (Eager + Known, Lazy + FromProvider)
            // are not reachable because plan_eager and plan_deferred
            // are the only constructors and they fix the pairing.
            _ => Err(RuntimeError::PermanentDatasetFailure {
                dataset: dataset.name.clone(),
                reason: "DatasetInitialization built with an unsupported (ConnectorSource, \
                         SchemaSource) pairing"
                    .to_string(),
            }),
        }
    }
}

/// A failed bring-up, with the plan to try again when a later attempt can succeed.
pub struct InitializeFailure {
    pub error: RuntimeError,
    /// The plan, to run again: set when the failure is the source being unavailable;
    /// `None` for a configuration error no retry clears. Boxed: the plan is large, and
    /// it rides in the `Err` variant of every bring-up.
    pub retry: Option<Box<DatasetInitialization>>,
}

/// Report a deferred dataset's first-reference bring-up failure, and return the
/// error unchanged so the caller can propagate it.
///
/// The deferred path registers a placeholder and marks the dataset `Ready`
/// before any connector exists (`register_deferred_dataset`), so the bring-up
/// that runs on first reference is the only owner of that status. Nothing
/// downstream reports for it: the failure is memoized by
/// `DatasetTableProvider::ensure_ready` and converted into a query error, which
/// left a dataset that can serve no query at all reporting `Ready`, with no
/// `datasets::LOAD_ERROR` and no log line. See #12414.
///
/// The eager path reports the same three things from `load_dataset_connector`
/// and `register_loaded_dataset`. Failures raised *by* `register_loaded_dataset`
/// — the accelerated deferred branch — are therefore already reported by it and
/// must not pass through here, or they are counted twice (#12365).
///
/// `error!` rather than `warn!`, and no `spaced_tracer`: `ensure_ready` retries a
/// source-unavailable failure no sooner than its backoff allows, so this logs once
/// per attempt, and a configuration failure runs it once per registration.
fn report_deferred_failure(
    runtime: &Runtime,
    dataset: &Dataset,
    err: RuntimeError,
) -> RuntimeError {
    runtime.status.update_dataset(
        &dataset.name,
        runtime_status::ComponentStatus::error_with_message(err.to_string()),
    );
    runtime_metrics::datasets::LOAD_ERROR.add(1, &[]);
    tracing::error!(
        dataset = %dataset.name,
        "Error initializing deferred dataset on first reference. {err}"
    );
    err
}

/// Whether a failure of the accelerated hand-off is one no retry clears.
/// `register_loaded_dataset` raises such a failure as `PermanentDatasetFailure`,
/// which is also the one failure `load_dataset`'s retry loop stops on.
fn is_permanent_load_failure(err: &RuntimeError) -> bool {
    matches!(err, RuntimeError::PermanentDatasetFailure { .. }) || is_permanent_dataset_failure(err)
}

fn arrow_schemas_equal(a: &SchemaRef, b: &SchemaRef) -> bool {
    a.fields() == b.fields()
}

#[cfg(test)]
mod tests {
    use crate::component::dataset::Dataset;
    use crate::component::dataset::DatasetSpec;
    use crate::component::dataset::acceleration::{Acceleration, Engine};
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use data_components::arrow::write::MemTable;
    use datafusion::datasource::TableProvider;

    use super::{DatasetInitialization, LazyConnectorBuilder};
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataaccelerator::arrow::ArrowAccelerator;
    use crate::dataaccelerator::{AccelerationSource, BootstrapStatus, DataAccelerator};
    use crate::dataconnector::{
        ConnectorComponent, DataConnector, DataConnectorError, DataConnectorResult,
    };
    use runtime_status::ComponentStatus;

    /// Hang guard: accelerator init and table construction can take several seconds
    /// when many test processes start at once.
    const ACCELERATOR_RETRY_HANG_GUARD: Duration = Duration::from_secs(60);

    /// Where the deferred bring-up fails.
    #[derive(Clone, Copy)]
    enum FailAt {
        /// The connector is never constructed. This is the #12414 case: a
        /// deferred `FlightSQL` dataset with declared columns and no endpoint,
        /// whose factory raises a permanent `MissingParameter`.
        Build,
        /// The connector is built, and reading the source fails.
        ReadProvider,
        /// The source resolves, but its schema contradicts the declared one.
        SchemaMismatch,
    }

    /// The schema a deferred dataset in these tests declares.
    fn declared_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
    }

    /// A schema that deliberately does not match [`declared_schema`].
    fn other_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "a_different_column",
            DataType::Utf8,
            true,
        )]))
    }

    /// A connector that is constructed successfully, then either fails to serve
    /// the dataset or serves it with the wrong schema.
    #[derive(Debug)]
    struct StubConnector {
        fail_read: bool,
    }

    fn stub(fail_read: bool) -> Arc<dyn DataConnector> {
        Arc::new(StubConnector { fail_read })
    }

    #[async_trait]
    impl DataConnector for StubConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _context: &dyn crate::dataconnector::ConnectorContext,
            dataset: &DatasetSpec,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            if self.fail_read {
                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "test".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: "the source refused the connection".into(),
                });
            }

            let table = MemTable::try_new(other_schema(), vec![vec![]])
                .expect("MemTable from a single-field schema");
            Ok(Arc::new(table))
        }
    }

    /// Build a read-only, non-accelerated dataset and mark it `Ready` — the
    /// state `register_deferred_dataset` leaves a deferred dataset in before any
    /// connector exists.
    async fn ready_deferred_dataset() -> (Arc<crate::Runtime>, Arc<Dataset>) {
        let spicepod_dataset =
            spicepod::component::dataset::Dataset::new("test:source", "deferred_ds");
        let app = app::AppBuilder::new("deferred_failure_reporting")
            .with_dataset(spicepod_dataset.clone())
            .build();
        let runtime = Arc::new(crate::Runtime::builder().build().await);

        let dataset = DatasetBuilder::try_from(spicepod_dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::clone(&runtime))
            .build()
            .expect("valid dataset");

        runtime
            .status
            .update_dataset(&dataset.name, ComponentStatus::Ready);

        (runtime, Arc::new(dataset))
    }

    /// Run the deferred bring-up that `ensure_ready` runs on first reference,
    /// failing at `fail_at`, and return the status it leaves behind.
    async fn status_after_failed_first_reference(fail_at: FailAt) -> ComponentStatus {
        let (runtime, dataset) = ready_deferred_dataset().await;

        let builder: LazyConnectorBuilder = Box::new(move || {
            Box::pin(async move {
                match fail_at {
                    FailAt::Build => Err("endpoint is required".into()),
                    FailAt::ReadProvider => Ok(stub(true)),
                    FailAt::SchemaMismatch => Ok(stub(false)),
                }
            })
        });

        let init = DatasetInitialization::plan_deferred(
            Arc::clone(&dataset),
            Arc::clone(&runtime),
            builder,
            declared_schema(),
            BootstrapStatus::None,
            None,
        );

        init.initialize()
            .await
            .expect_err("the deferred bring-up must fail");

        runtime
            .status
            .get_dataset_status(&dataset.name)
            .expect("the dataset has a status")
    }

    /// Regression test for #12414: a deferred dataset whose connector can never
    /// be built stayed `Ready`, incremented no `datasets::LOAD_ERROR`, and
    /// logged nothing — only the individual queries failed.
    #[tokio::test]
    async fn a_deferred_connector_that_cannot_be_built_is_reported() {
        let status = status_after_failed_first_reference(FailAt::Build).await;

        assert!(
            status.is_error(),
            "a dataset that can serve no query must not report Ready, got: {status:?}"
        );
        let message = status.error_message().unwrap_or_default();
        assert!(
            message.contains("endpoint is required"),
            "the status must carry the connector failure, got: {message}"
        );
    }

    /// The same gap one step later in the same bring-up.
    #[tokio::test]
    async fn a_deferred_source_that_cannot_be_read_is_reported() {
        let status = status_after_failed_first_reference(FailAt::ReadProvider).await;

        assert!(
            status.is_error(),
            "a deferred source that cannot be read must be reported, got: {status:?}"
        );
        let message = status.error_message().unwrap_or_default();
        assert!(
            message.contains("the source refused the connection"),
            "the status must carry the read failure, got: {message}"
        );
    }

    /// And one step later again: a declared schema the source contradicts is a
    /// permanent configuration failure no retry can clear, so the dataset must
    /// not keep reporting `Ready`.
    #[tokio::test]
    async fn a_deferred_schema_mismatch_is_reported() {
        let status = status_after_failed_first_reference(FailAt::SchemaMismatch).await;

        assert!(
            status.is_error(),
            "a declared schema that does not match the source must be reported, got: {status:?}"
        );
        let message = status.error_message().unwrap_or_default();
        assert!(
            message.contains("Declared schema does not match source schema"),
            "the status must carry the schema mismatch, got: {message}"
        );
    }

    /// The reporting is failure-only: a deferred dataset that comes up
    /// successfully still reports `Ready`.
    #[tokio::test]
    async fn a_successful_deferred_bring_up_stays_ready() {
        let (runtime, dataset) = ready_deferred_dataset().await;

        let builder: LazyConnectorBuilder =
            Box::new(move || Box::pin(async move { Ok(stub(false)) }));

        // Declare the schema the stub actually serves, so the mismatch gate
        // passes and the bring-up succeeds.
        let init = DatasetInitialization::plan_deferred(
            Arc::clone(&dataset),
            Arc::clone(&runtime),
            builder,
            other_schema(),
            BootstrapStatus::None,
            None,
        );

        init.initialize()
            .await
            .expect("the deferred bring-up must succeed");

        assert_eq!(
            runtime.status.get_dataset_status(&dataset.name),
            Some(ComponentStatus::Ready),
            "a deferred dataset that came up must still report Ready"
        );
    }

    /// A builder that fails with `error()` for its first `failures` calls, then serves.
    fn failing_then_serving(
        failures: usize,
        error: impl Fn() -> Box<dyn std::error::Error + Send + Sync> + Send + Sync + 'static,
    ) -> (LazyConnectorBuilder, Arc<std::sync::atomic::AtomicUsize>) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let builder: LazyConnectorBuilder = Box::new(move || {
            let call = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let result = if call < failures {
                Err(error())
            } else {
                Ok(stub(false))
            };
            Box::pin(async move { result })
        });
        (builder, calls)
    }

    fn plan(
        runtime: &Arc<crate::Runtime>,
        dataset: &Arc<Dataset>,
        builder: LazyConnectorBuilder,
    ) -> DatasetInitialization {
        DatasetInitialization::plan_deferred(
            Arc::clone(dataset),
            Arc::clone(runtime),
            builder,
            other_schema(),
            BootstrapStatus::None,
            None,
        )
    }

    /// An unavailable source is not the end of the dataset: the failed bring-up hands its
    /// plan back, and running it again once the source is there brings the dataset up.
    #[tokio::test]
    async fn an_unavailable_source_hands_the_plan_back_to_retry() {
        let (runtime, dataset) = ready_deferred_dataset().await;
        let (builder, calls) = failing_then_serving(1, || "no data files yet".into());

        let Err(failure) = plan(&runtime, &dataset, builder)
            .initialize_retriable()
            .await
        else {
            panic!("the first attempt must fail");
        };
        let retry = failure.retry.expect("an unavailable source can be retried");
        assert!(
            runtime
                .status
                .get_dataset_status(&dataset.name)
                .is_some_and(|s| s.is_error())
        );

        retry
            .initialize()
            .await
            .expect("the retry brings the dataset up");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            runtime.status.get_dataset_status(&dataset.name),
            Some(ComponentStatus::Ready)
        );
    }

    /// A configuration error is a pure function of the spicepod: no plan to retry.
    #[tokio::test]
    async fn a_configuration_error_is_not_retried() {
        let (runtime, dataset) = ready_deferred_dataset().await;
        let component = ConnectorComponent::from(dataset.as_ref());
        let (builder, _) = failing_then_serving(usize::MAX, move || {
            Box::new(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "test".to_string(),
                connector_component: component.clone(),
                message: "endpoint is required".to_string(),
            })
        });

        let Err(failure) = plan(&runtime, &dataset, builder)
            .initialize_retriable()
            .await
        else {
            panic!("the attempt must fail");
        };
        assert!(
            failure.retry.is_none(),
            "a configuration error has no retry"
        );
    }

    /// The production builder boxes a `RuntimeError`; a configuration error inside it stays
    /// permanent rather than being wrapped and read as an unavailable source.
    #[tokio::test]
    async fn a_boxed_runtime_error_from_the_builder_keeps_its_classification() {
        let (runtime, dataset) = ready_deferred_dataset().await;
        let component = ConnectorComponent::from(dataset.as_ref());
        let (builder, _) = failing_then_serving(usize::MAX, move || {
            Box::new(crate::Error::UnableToInitializeDataConnector {
                source: Box::new(DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "test".to_string(),
                    connector_component: component.clone(),
                    message: "endpoint is required".to_string(),
                }),
            })
        });

        let Err(failure) = plan(&runtime, &dataset, builder)
            .initialize_retriable()
            .await
        else {
            panic!("the attempt must fail");
        };
        assert!(
            failure.retry.is_none(),
            "a configuration error has no retry"
        );
    }

    /// `ensure_ready` does not keep an unavailable-source failure: a call inside the backoff
    /// gets the last error without touching the source, and the first call after it builds
    /// the connector again and initializes the dataset.
    #[tokio::test]
    async fn ensure_ready_retries_an_unavailable_source_after_a_backoff() {
        use crate::datafusion::table::dataset_table_provider::DatasetTableProvider;

        let (runtime, dataset) = ready_deferred_dataset().await;
        let (builder, calls) = failing_then_serving(1, || "no data files yet".into());
        let provider = DatasetTableProvider::new(
            dataset.name.clone(),
            declared_schema(),
            plan(&runtime, &dataset, builder),
        );

        provider
            .ensure_ready()
            .await
            .expect_err("the source is unavailable");
        assert!(!provider.is_initialized());
        provider
            .ensure_ready()
            .await
            .expect_err("still inside the backoff");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        provider.ensure_ready().await.expect("the retry succeeds");
        assert!(provider.is_initialized());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        provider.ensure_ready().await.expect("the result is kept");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// A configuration error is kept: later calls get it without another attempt.
    #[tokio::test]
    async fn ensure_ready_keeps_a_configuration_error() {
        use crate::datafusion::table::dataset_table_provider::DatasetTableProvider;

        let (runtime, dataset) = ready_deferred_dataset().await;
        let component = ConnectorComponent::from(dataset.as_ref());
        let (builder, calls) = failing_then_serving(usize::MAX, move || {
            Box::new(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "test".to_string(),
                connector_component: component.clone(),
                message: "endpoint is required".to_string(),
            })
        });
        let provider = DatasetTableProvider::new(
            dataset.name.clone(),
            declared_schema(),
            plan(&runtime, &dataset, builder),
        );

        provider
            .ensure_ready()
            .await
            .expect_err("a configuration error");
        assert!(provider.is_initialized());
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        provider.ensure_ready().await.expect_err("kept");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Arrow accelerator whose `init` fails a configured number of times after
    /// `generation_lock` has already revoked the previous owner.
    struct FlakyInitAccelerator {
        inner: ArrowAccelerator,
        remaining_init_failures: AtomicUsize,
        init_calls: AtomicUsize,
    }

    impl FlakyInitAccelerator {
        fn new() -> Self {
            Self {
                inner: ArrowAccelerator::new(),
                remaining_init_failures: AtomicUsize::new(0),
                init_calls: AtomicUsize::new(0),
            }
        }

        fn fail_next_init(&self) {
            self.remaining_init_failures.store(1, Ordering::SeqCst);
        }

        fn init_calls(&self) -> usize {
            self.init_calls.load(Ordering::SeqCst)
        }
    }

    /// Decrement `counter` if it is still positive. Sequential tests use this instead of
    /// the deprecated `AtomicUsize::fetch_update`.
    fn consume_one(counter: &AtomicUsize) -> bool {
        loop {
            let current = counter.load(Ordering::SeqCst);
            if current == 0 {
                return false;
            }
            if counter
                .compare_exchange_weak(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    #[async_trait]
    impl DataAccelerator for FlakyInitAccelerator {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn prefix(&self) -> &'static str {
            self.inner.prefix()
        }

        fn parameters(&self) -> &'static [runtime_parameters::ParameterSpec] {
            self.inner.parameters()
        }

        async fn create_external_table(
            &self,
            cmd: datafusion::logical_expr::CreateExternalTable,
            source: Option<&dyn AccelerationSource>,
            partition_by: Vec<runtime_table_partition::expression::PartitionedBy>,
            runtime_env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
        ) -> std::result::Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>
        {
            self.inner
                .create_external_table(cmd, source, partition_by, runtime_env)
                .await
        }

        async fn sidecar(
            &self,
            source: &dyn AccelerationSource,
            open_option: runtime_acceleration::sidecar::OpenOption,
        ) -> std::result::Result<
            Arc<dyn runtime_acceleration::sidecar::AcceleratorSidecar>,
            runtime_checkpoint_api::CheckpointError,
        > {
            self.inner.sidecar(source, open_option).await
        }

        async fn validate_init(
            &self,
            source: &dyn AccelerationSource,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.inner.validate_init(source).await
        }

        async fn init(
            &self,
            source: &dyn AccelerationSource,
        ) -> std::result::Result<BootstrapStatus, Box<dyn std::error::Error + Send + Sync>>
        {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
            if consume_one(&self.remaining_init_failures) {
                return Err("accelerator unavailable".into());
            }
            self.inner.init(source).await
        }

        async fn writer_role(
            &self,
            source: &dyn AccelerationSource,
        ) -> Option<data_accelerator_api::WriterRole> {
            self.inner.writer_role(source).await
        }

        async fn shared_location_id(
            &self,
            source: &dyn AccelerationSource,
        ) -> std::result::Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
            self.inner.shared_location_id(source).await
        }
    }

    /// Serves `other_schema`, and fails accelerator setup until `fail_setup` is used up
    /// so a first accelerated bring-up consumes its bootstrap generation.
    #[derive(Debug)]
    struct ServingConnector {
        fail_setup: AtomicUsize,
    }

    #[async_trait]
    impl DataConnector for ServingConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _context: &dyn crate::dataconnector::ConnectorContext,
            _dataset: &DatasetSpec,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            let table = MemTable::try_new(other_schema(), vec![vec![]])
                .expect("MemTable from a single-field schema");
            Ok(Arc::new(table))
        }

        async fn on_accelerator_setup(
            &self,
            _dataset: &DatasetSpec,
            _accelerator: &mut dyn data_connector_api::accelerated::AcceleratorSetup,
        ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if consume_one(&self.fail_setup) {
                return Err("controlled transient setup failure".into());
            }
            Ok(())
        }
    }

    async fn accelerated_deferred_dataset(
        name: &str,
    ) -> (Arc<crate::Runtime>, Arc<Dataset>, Arc<FlakyInitAccelerator>) {
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let accelerator = Arc::new(FlakyInitAccelerator::new());
        runtime
            .accelerator_engine_registry()
            .register_accelerator_engine(
                Engine::Arrow,
                Arc::clone(&accelerator) as Arc<dyn DataAccelerator>,
            )
            .await;

        let spicepod_dataset = spicepod::component::dataset::Dataset::new("test:source", name);
        let app = app::AppBuilder::new("deferred_accelerator_retry")
            .with_dataset(spicepod_dataset.clone())
            .build();
        let mut builder = DatasetBuilder::try_from(spicepod_dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::clone(&runtime));
        builder.acceleration = Some(Acceleration {
            engine: Engine::Arrow,
            ..Acceleration::default()
        });
        let dataset = Arc::new(builder.build().expect("valid dataset"));
        runtime
            .status
            .update_dataset(&dataset.name, ComponentStatus::Ready);
        (runtime, dataset, accelerator)
    }

    fn serving_builder(fail_setup: usize) -> LazyConnectorBuilder {
        let connector = Arc::new(ServingConnector {
            fail_setup: AtomicUsize::new(fail_setup),
        });
        Box::new(move || {
            let connector = Arc::clone(&connector);
            Box::pin(async move { Ok(connector as Arc<dyn DataConnector>) })
        })
    }

    async fn owned_bootstrap(
        runtime: &Arc<crate::Runtime>,
        dataset: &Arc<Dataset>,
    ) -> crate::datafusion::AcceleratorBootstrap {
        let accelerator = runtime
            .accelerator_engine_registry()
            .get_accelerator_engine(Engine::Arrow)
            .await
            .expect("the flaky Arrow engine is registered");
        runtime
            .df
            .initialize_accelerator(Arc::clone(dataset), accelerator)
            .await
            .expect("the first accelerator init succeeds")
    }

    async fn plan_after_consumed_generation(
        runtime: &Arc<crate::Runtime>,
        dataset: &Arc<Dataset>,
        bootstrap: crate::datafusion::AcceleratorBootstrap,
    ) -> DatasetInitialization {
        let plan = DatasetInitialization::plan_deferred(
            Arc::clone(dataset),
            Arc::clone(runtime),
            serving_builder(1),
            other_schema(),
            bootstrap,
            None,
        );
        let Err(failure) = plan.initialize_retriable().await else {
            panic!("the first accelerated bring-up must fail in accelerator setup");
        };
        let retry = failure
            .retry
            .expect("a transient setup failure hands the plan back");
        assert!(
            retry.bootstrap().needs_reinitialization(),
            "a failed accelerated build consumes the generation so the retry reinitializes"
        );
        *retry
    }

    /// Replacement accelerator `init` fails after `generation_lock` revokes the old
    /// owner. The next retry must still reinitialize once the accelerator recovers.
    #[tokio::test]
    async fn replacement_accelerator_init_failure_retries_after_the_accelerator_recovers() {
        tokio::time::timeout(ACCELERATOR_RETRY_HANG_GUARD, async {
            let (runtime, dataset, accelerator) =
                accelerated_deferred_dataset("deferred_reinit_retry").await;
            let bootstrap = owned_bootstrap(&runtime, &dataset).await;
            assert_eq!(accelerator.init_calls(), 1);
            let retry = plan_after_consumed_generation(&runtime, &dataset, bootstrap).await;
            assert_eq!(accelerator.init_calls(), 1);

            accelerator.fail_next_init();
            let Err(failure) = retry.initialize_retriable().await else {
                panic!("replacement accelerator init must fail");
            };
            let retry = failure.retry.expect("a failed accelerator init is retriable");
            assert!(
                retry.bootstrap().needs_reinitialization(),
                "after replacement init fails, the retry must still reinitialize, not keep the revoked bootstrap"
            );
            assert_eq!(accelerator.init_calls(), 2);

            if let Err(failure) = retry.initialize_retriable().await {
                panic!(
                    "the accelerator recovered, so reinitialization must succeed: {}",
                    failure.error
                );
            }
            assert_eq!(
                accelerator.init_calls(),
                3,
                "the recovering retry must call accelerator init again"
            );
        })
        .await
        .expect("accelerator reinit retry must finish");
    }

    /// A newer dataset configuration that takes the generation must still be
    /// rejected: the superseded plan must not reinitialize over it.
    #[tokio::test]
    async fn a_bootstrap_superseded_by_a_newer_dataset_configuration_is_rejected() {
        tokio::time::timeout(ACCELERATOR_RETRY_HANG_GUARD, async {
            let (runtime, dataset, accelerator) =
                accelerated_deferred_dataset("deferred_superseded").await;
            let bootstrap = owned_bootstrap(&runtime, &dataset).await;
            let retry = plan_after_consumed_generation(&runtime, &dataset, bootstrap).await;
            assert_eq!(accelerator.init_calls(), 1);

            let newer = owned_bootstrap(&runtime, &dataset).await;
            assert_eq!(accelerator.init_calls(), 2);
            assert!(
                !retry.bootstrap().needs_reinitialization(),
                "generation_lock for the newer configuration revokes the old owner"
            );

            let Err(failure) = retry.initialize_retriable().await else {
                panic!("a superseded bootstrap must not initialize over the newer configuration");
            };
            let message = failure.error.to_string();
            assert!(
                message.contains("consumed or superseded"),
                "the superseded plan must fail in borrow, got: {message}"
            );
            assert_eq!(
                accelerator.init_calls(),
                2,
                "a superseded plan must not call accelerator init again"
            );

            if let Err(failure) = DatasetInitialization::plan_deferred(
                Arc::clone(&dataset),
                Arc::clone(&runtime),
                serving_builder(0),
                other_schema(),
                newer,
                None,
            )
            .initialize()
            .await
            {
                panic!("the newer configuration's bootstrap still initializes: {failure}");
            }
            assert_eq!(accelerator.init_calls(), 2);
        })
        .await
        .expect("superseded bootstrap rejection must finish");
    }

    /// After replacement `init` fails, a newer configuration that takes the
    /// generation must still be rejected: the failed retry must not steal it.
    #[tokio::test]
    async fn a_failed_replacement_init_does_not_reinitialize_over_a_newer_configuration() {
        tokio::time::timeout(ACCELERATOR_RETRY_HANG_GUARD, async {
            let (runtime, dataset, accelerator) =
                accelerated_deferred_dataset("deferred_failed_then_superseded").await;
            let bootstrap = owned_bootstrap(&runtime, &dataset).await;
            let retry = plan_after_consumed_generation(&runtime, &dataset, bootstrap).await;

            accelerator.fail_next_init();
            let Err(failure) = retry.initialize_retriable().await else {
                panic!("replacement accelerator init must fail");
            };
            let retry = failure
                .retry
                .expect("a failed accelerator init is retriable");
            assert_eq!(accelerator.init_calls(), 2);

            let newer = owned_bootstrap(&runtime, &dataset).await;
            assert_eq!(accelerator.init_calls(), 3);
            assert!(
                !retry.bootstrap().needs_reinitialization(),
                "the newer configuration must revoke the failed retry's reinitialization intent"
            );

            let Err(failure) = retry.initialize_retriable().await else {
                panic!("a superseded bootstrap must not initialize over the newer configuration");
            };
            let message = failure.error.to_string();
            assert!(
                message.contains("consumed or superseded"),
                "the superseded plan must fail in borrow, got: {message}"
            );
            assert_eq!(
                accelerator.init_calls(),
                3,
                "a superseded plan must not call accelerator init again"
            );

            if let Err(failure) = DatasetInitialization::plan_deferred(
                Arc::clone(&dataset),
                Arc::clone(&runtime),
                serving_builder(0),
                other_schema(),
                newer,
                None,
            )
            .initialize()
            .await
            {
                panic!("the newer configuration's bootstrap still initializes: {failure}");
            }
            assert_eq!(accelerator.init_calls(), 3);
        })
        .await
        .expect("failed-then-superseded rejection must finish");
    }
}
