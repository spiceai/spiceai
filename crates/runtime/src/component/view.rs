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
}

impl AccelerationSource for View {
    fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
        Arc::new(self.clone()) as Arc<dyn AccelerationSource>
    }

    fn is_file_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            if acceleration.engine == acceleration::Engine::PostgreSQL {
                return false;
            }
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
            #[cfg(feature = "duckdb")]
            {
                let views: Vec<Arc<dyn runtime_acceleration::AccelerationSource>> =
                    Arc::clone(&runtime)
                        .get_initialized_views(&app, crate::LogErrors(false))
                        .await
                        .into_iter()
                        .map(|v| v as Arc<dyn runtime_acceleration::AccelerationSource>)
                        .collect();
                datasets.into_iter().chain(views).collect()
            }
            #[cfg(not(feature = "duckdb"))]
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
