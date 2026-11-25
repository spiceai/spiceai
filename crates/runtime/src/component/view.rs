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

use app::App;
use datafusion::common::TableReference;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::{Runtime, dataaccelerator::AccelerationSource};

use super::dataset::acceleration::{self, Acceleration};

// Config-only spec and its builder live in `runtime-component`; re-export for
// path compatibility (`crate::component::view::{ViewBuilder, ViewSpec}`).
pub use runtime_component::view::{Error, ViewBuilder, ViewSpec};

impl From<Error> for crate::Error {
    /// Maps each view parse error to the runtime error that reported it before the
    /// builder moved, so the user-facing messages do not change.
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidViewName { source } => crate::Error::ComponentError { source },
            Error::ViewNameIncludesCatalog { catalog, name } => {
                crate::Error::DatasetNameIncludesCatalog { catalog, name }
            }
            Error::UnableToLoadSqlFile { file, source } => {
                crate::Error::UnableToLoadSqlFile { file, source }
            }
            Error::NeedToSpecifySQLView { name } => crate::Error::NeedToSpecifySQLView { name },
            Error::InvalidAccelerationConfiguration { source } => source.into(),
            Error::VectorEngineRemoved { view_name } => {
                crate::Error::ViewVectorEngineRemoved { view_name }
            }
            Error::AccelerationEngineRemoved { view_name, engine } => {
                crate::Error::AcceleratedViewEngineRemoved { view_name, engine }
            }
            Error::AcceleratedViewInvalidConfiguration { view_name, reason } => {
                crate::Error::AcceleratedViewInvalidConfiguration { view_name, reason }
            }
        }
    }
}

/// `Arc<Runtime>`-bound wrapper over a [`ViewSpec`]. Derefs to the spec so
/// `view.acceleration`, `view.columns`, `view.is_accelerated()`, etc. keep
/// working unchanged.
#[derive(Clone)]
pub struct View {
    pub spec: ViewSpec,
    pub runtime: Arc<Runtime>,
    pub app: Arc<App>,
}

impl Deref for View {
    type Target = ViewSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

impl DerefMut for View {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.spec
    }
}

impl PartialEq for View {
    fn eq(&self, other: &Self) -> bool {
        self.spec == other.spec
    }
}

impl std::fmt::Debug for View {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("View")
            .field("name", &self.name)
            .field("sql", &self.sql)
            .field("metadata", &self.metadata)
            .field("columns", &self.columns)
            .field("acceleration", &self.acceleration)
            .field("ready_state", &self.ready_state)
            .field("vectors", &self.vectors)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl View {
    /// Attaches the runtime handles to a parsed [`ViewSpec`].
    #[must_use]
    pub fn new(spec: ViewSpec, runtime: Arc<Runtime>, app: Arc<App>) -> Self {
        Self { spec, runtime, app }
    }

    #[must_use]
    pub async fn is_accelerator_initialized(&self) -> bool {
        if let Some(acceleration_settings) = &self.acceleration {
            let Some(accelerator) = self
                .runtime
                .accelerator_engine_registry()
                .get_accelerator_engine(acceleration_settings.engine)
                .await
            else {
                return false; // if the accelerator engine is not found, it's impossible for it to be initialized
            };

            return accelerator.is_initialized(self);
        }

        false
    }

    /// The schema this view's acceleration was checkpointed with, when the view can
    /// be served from that acceleration before the tables it reads have loaded, as an
    /// accelerated dataset is served before its source answers.
    ///
    /// Only `mode: file` qualifies (`file_create` starts empty by design), and not a
    /// view with embedding or full-text search columns, whose indexes wrap the view's
    /// own plan. A checkpoint that does not record the acceleration's primary key
    /// (written by an earlier version) does not count either.
    pub(crate) async fn existing_acceleration_schema(&self) -> Option<arrow_schema::SchemaRef> {
        let acceleration = self.acceleration.as_ref().filter(|a| a.enabled)?;
        if !matches!(acceleration.mode, acceleration::Mode::File)
            || self.has_embeddings()
            || self.has_full_text_column()
        {
            return None;
        }
        let schema = crate::dataconnector::sink::recorded_acceleration_checkpoint_schema(
            self,
            self.runtime.accelerator_engine_registry(),
        )
        .await?;
        runtime_table::accelerated::checkpoint_primary_key::records_acceleration_primary_key(
            &schema,
        )
        .then_some(schema)
    }
}

/// How deep a view's definition follows views it reads. Views cannot read each
/// other in a cycle, so this only bounds a misconfiguration.
const MAX_VIEW_DEPTH: usize = 16;

/// `view`'s definition: its SQL, and the definition of each dataset and view
/// that SQL reads, so two views with one SQL over different sources differ.
fn view_definition(
    view: &View,
    depth: usize,
) -> futures::future::BoxFuture<
    '_,
    Result<runtime_acceleration::SourceDefinition, Box<dyn std::error::Error + Send + Sync>>,
> {
    Box::pin(async move {
        let mut reads = Vec::new();
        let names = read_table_names(&view.sql);
        let datasets = if names.is_empty() {
            Vec::new()
        } else {
            Arc::clone(&view.runtime).get_valid_datasets(&view.app, crate::LogErrors(false))
        };
        for name in names {
            let read = if let Some(dataset) = datasets.iter().find(|ds| ds.name == name) {
                dataset.definition().await?
            } else if let Some(spec) = view
                .app
                .views
                .iter()
                .find(|spec| TableReference::from(spec.name.as_str()) == name)
                && depth < MAX_VIEW_DEPTH
            {
                let builder = super::view::ViewBuilder::try_from(spec.clone())
                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
                let read =
                    View::new(builder.build(), Arc::clone(&view.runtime), Arc::clone(&view.app));
                Some(view_definition(&read, depth + 1).await?)
            } else {
                // A table the Spicepod does not define (a catalog's): its name.
                None
            };
            reads.push((
                name.to_string(),
                read.unwrap_or_else(|| runtime_acceleration::SourceDefinition {
                    from: name.to_string(),
                    ..Default::default()
                }),
            ));
        }
        reads.sort_by(|a, b| a.0.cmp(&b.0));
        reads.dedup_by(|a, b| a.0 == b.0);
        Ok(runtime_acceleration::SourceDefinition {
            from: view.sql.to_string(),
            connection: Vec::new(),
            refresh_sql: view
                .acceleration
                .as_ref()
                .and_then(|acceleration| acceleration.refresh_sql.clone()),
            columns: runtime_acceleration::SourceDefinition::declared_columns(&view.columns),
            row_settings: Vec::new(),
            reads,
        })
    })
}

/// The tables `sql` reads, or none when it does not parse (the view then fails
/// to register on its own).
fn read_table_names(sql: &str) -> Vec<TableReference> {
    use datafusion::sql::{parser::DFParser, sqlparser::dialect::PostgreSqlDialect};
    match DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}) {
        Ok(statements) if statements.len() == 1 => {
            statements.front().map_or_else(Vec::new, |statement| {
                runtime_datafusion::dependent_tables::get_dependent_table_names(statement)
            })
        }
        _ => Vec::new(),
    }
}

impl AccelerationSource for View {
    fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
        Arc::new(self.clone()) as Arc<dyn AccelerationSource>
    }

    fn is_file_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.enabled
                && matches!(
                    acceleration.mode,
                    acceleration::Mode::File | acceleration::Mode::FileCreate
                );
        }
        false
    }

    fn app(&self) -> Arc<app::App> {
        Arc::clone(&self.app)
    }

    fn secrets(&self) -> Arc<tokio::sync::RwLock<crate::secrets::Secrets>> {
        self.runtime.secrets()
    }

    fn snapshot_notifications(
        &self,
    ) -> Option<Arc<runtime_acceleration::snapshot::notifications::SnapshotNotifications>> {
        self.runtime.datafusion().snapshot_notifications()
    }

    fn acceleration(&self) -> Option<&Acceleration> {
        self.acceleration.as_ref()
    }

    fn name(&self) -> &TableReference {
        &self.name
    }

    fn connector_name(&self) -> Option<&str> {
        // A view has no `from:` — its rows come from its SQL, not a connector — so
        // there is no connector default to apply. `ViewBuilder::try_from` also
        // rejects every refresh mode except `full`, which is the fallback a `None`
        // resolves to.
        None
    }

    fn definition(&self) -> runtime_acceleration::DefinitionFuture<'_> {
        Box::pin(async move { view_definition(self, 0).await.map(Some) })
    }

    fn on_schema_change(&self) -> Option<runtime_acceleration::OnSchemaChange> {
        // A view declares no `on_schema_change`: its columns follow its SQL, so there is
        // no source schema for an accelerator to reconcile against.
        None
    }

    fn allows_write(&self) -> bool {
        // A view is not writable, and `ViewBuilder::try_from` rejects every refresh mode
        // except `full`, so a view is never the read-only CDC replica the scan-freshness
        // decision is about.
        false
    }

    fn time_column(&self) -> Option<&str> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialized_sources<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Vec<Arc<dyn runtime_acceleration::AccelerationSource>>>
                + Send
                + 'a,
        >,
    > {
        let app = self.app();
        let runtime = Arc::clone(&self.runtime);
        Box::pin(async move {
            let datasets: Vec<Arc<dyn runtime_acceleration::AccelerationSource>> =
                Arc::clone(&runtime)
                    .get_initialized_datasets(&app, crate::LogErrors(false))
                    .await
                    .into_iter()
                    .map(|ds| ds as Arc<dyn runtime_acceleration::AccelerationSource>)
                    .collect();
            datasets
        })
    }

    fn checkpointer_factory(
        &self,
        snapshot_behavior: runtime_acceleration::snapshot::SnapshotBehavior,
    ) -> runtime_acceleration::dataset_checkpoint::DatasetCheckpointerFactory {
        crate::dataaccelerator::spice_sys::checkpointer_factory(
            self,
            self.runtime.accelerator_engine_registry(),
            snapshot_behavior,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::ViewBuilder;
    use crate::component::{AcceleratedComponent, deprecated_ready_state_warning};
    use spicepod::component::view as spicepod_view;

    /// A view that still selects the removed `duckdb` vector engine fails to load with the
    /// line the runtime logs for it, naming the view and the Cayenne fix.
    #[test]
    fn a_view_selecting_the_duckdb_vector_engine_fails_to_load() {
        let view: spicepod_view::View = yaml::from_str(
            "name: daily_docs\nsql: SELECT 1\nvectors:\n  enabled: true\n  engine: duckdb\n",
        )
        .expect("yaml parses");

        let Err(err) = ViewBuilder::try_from(view) else {
            panic!("a view selecting the duckdb vector engine must fail to load");
        };
        let err = crate::Error::from(err);
        assert!(
            matches!(err, crate::Error::ViewVectorEngineRemoved { .. }),
            "unexpected error: {err}"
        );
        insta::assert_snapshot!("view_duckdb_vector_engine_removed", err.to_string());
    }

    /// The wording itself is asserted beside the shared builder in `component::tests`. What is
    /// specific to the view — and what makes that escaping load-bearing rather than decorative — is
    /// that a name carrying a newline gets through `ViewBuilder::try_from` at all: a *quoted*
    /// identifier may legally contain a newline, and `validate_identifier` accepts one, so a name
    /// that passes validation could otherwise break the line in two and forge a second record.
    /// `disabled_acceleration_warning` escapes for exactly this reason.
    #[test]
    fn a_view_name_carrying_a_newline_cannot_forge_a_second_log_line() {
        let hostile = "\"api\nWARN forged\"";

        // The escaping only matters if such a name reaches the warning at all, so assert that
        // the builder accepts it rather than assuming it does.
        let view: spicepod_view::View =
            yaml::from_str(&format!("name: {hostile:?}\nsql: SELECT 1\n")).expect("yaml parses");
        assert!(
            ViewBuilder::try_from(view).is_ok(),
            "a quoted identifier containing a newline is accepted by the builder, which is what \
             makes escaping load-bearing rather than decorative"
        );

        let message = deprecated_ready_state_warning(AcceleratedComponent::View, hostile);
        assert!(
            !message.contains('\n'),
            "an embedded newline must not survive into the log line: {message:?}"
        );
        assert!(
            message.contains("WARN forged"),
            "the name is still reported in full, only escaped: {message:?}"
        );
    }
}
