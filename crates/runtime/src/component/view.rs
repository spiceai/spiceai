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
use datafusion::sql::TableReference;
use snafu::prelude::*;
use spicepod::{component::view as spicepod_view, vector::VectorStore};
use std::ops::{Deref, DerefMut};
use std::{collections::HashMap, fs, sync::Arc};

use crate::{Runtime, dataaccelerator::AccelerationSource};

use super::{
    AcceleratedComponent,
    dataset::{
        Dataset, ReadyState,
        acceleration::{self, Acceleration},
    },
    deprecated_ready_state_warning, validate_identifier,
};
use spicepod::semantic::Column;

// Config-only spec lives in `runtime-component`; re-export for path
// compatibility (`crate::component::view::ViewSpec`).
pub use runtime_component::view::ViewSpec;

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
    #[expect(clippy::result_large_err)]
    fn load_sql_ref(sql_ref: &str) -> crate::Result<String> {
        let sql = fs::read_to_string(sql_ref)
            .context(crate::UnableToLoadSqlFileSnafu { file: sql_ref })?;
        Ok(sql)
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

pub struct ViewBuilder {
    pub name: TableReference,
    pub sql: String,
    pub metadata: HashMap<String, String>,
    pub columns: Vec<Column>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub ready_state: ReadyState,
    pub vectors: Option<VectorStore>,
    pub params: HashMap<String, String>,
}

impl TryFrom<spicepod_view::View> for ViewBuilder {
    type Error = crate::Error;

    fn try_from(view: spicepod_view::View) -> Result<Self, Self::Error> {
        validate_identifier(&view.name).context(crate::ComponentSnafu)?;

        let table_reference = Dataset::parse_table_reference(&view.name)?;

        let sql = if let Some(view_sql) = &view.sql {
            view_sql.clone()
        } else if let Some(sql_ref) = &view.sql_ref {
            View::load_sql_ref(sql_ref)?
        } else {
            return Err(crate::Error::NeedToSpecifySQLView {
                name: table_reference.to_string(),
            });
        };

        let metadata = view.metadata();

        // `acceleration.ready_state` is a legitimate member of the acceleration block, so it
        // parses cleanly on a view as well as on a dataset. A dataset reads it out of the block
        // and applies it; resolve it the same way here so the key means one thing wherever it is
        // written, rather than being accepted and dropped on one of the two components. See
        // `DatasetBuilder::try_from` for the dataset side.
        #[expect(deprecated)]
        let ready_state = match view.acceleration.as_ref().map(|a| a.ready_state) {
            Some(Some(ready_state)) => {
                tracing::warn!(
                    "{}",
                    deprecated_ready_state_warning(AcceleratedComponent::View, &view.name)
                );
                ReadyState::from(ready_state)
            }
            _ => ReadyState::from(view.ready_state),
        };

        let acceleration = view
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()?;

        // verify that the acceleration configuration is fully supported
        if let Some(acc) = &acceleration {
            if acc.refresh_mode.is_some()
                && acc.refresh_mode != Some(acceleration::RefreshMode::Full)
            {
                return Err(crate::Error::AcceleratedViewInvalidConfiguration {
                    view_name: view.name,
                    reason: "Only 'refresh_mode: full' is supported".to_string(),
                });
            }

            if acc.refresh_sql.is_some() {
                return Err(crate::Error::AcceleratedViewInvalidConfiguration {
                    view_name: view.name,
                    reason: "'refresh_sql' is not supported".to_string(),
                });
            }
        }

        Ok(ViewBuilder {
            name: table_reference,
            sql,
            metadata,
            columns: view.columns,
            acceleration,
            ready_state,
            vectors: view.vectors,
            params: view
                .params
                .as_ref()
                .map(spicepod::param::Params::as_string_map)
                .unwrap_or_default(),
        })
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

impl ViewBuilder {
    #[must_use]
    pub fn new(name: TableReference, sql: String) -> Self {
        Self {
            name,
            sql,
            metadata: HashMap::default(),
            columns: vec![],
            acceleration: None,
            ready_state: ReadyState::default(),
            vectors: None,
            params: HashMap::default(),
        }
    }

    #[must_use]
    pub fn build_with(self, runtime: Arc<Runtime>, app: Arc<App>) -> View {
        View {
            spec: ViewSpec {
                name: self.name,
                sql: Arc::from(self.sql),
                metadata: self.metadata,
                columns: self.columns,
                acceleration: self.acceleration,
                ready_state: self.ready_state,
                vectors: self.vectors,
                params: self.params,
            },
            runtime,
            app,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AcceleratedComponent, ReadyState, ViewBuilder, deprecated_ready_state_warning};
    use spicepod::component::view as spicepod_view;

    /// Resolves a view from its Spicepod YAML, so the test covers the same parse that a
    /// `spicepod.yaml` goes through rather than a hand-built struct that could disagree with it.
    fn ready_state_of(view_yaml: &str) -> ReadyState {
        let view: spicepod_view::View = yaml::from_str(view_yaml).expect("view yaml parses");
        ViewBuilder::try_from(view)
            .expect("view builds")
            .ready_state
    }

    /// Regression test for #13615. The key parses on a view whether or not anything reads it, so
    /// assert the value the built view carries rather than that the Spicepod was accepted.
    #[test]
    fn acceleration_ready_state_is_applied_to_a_view() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
acceleration:
  enabled: true
  ready_state: on_registration
",
        );

        assert_eq!(
            ready_state,
            ReadyState::OnRegistration,
            "a view's `acceleration.ready_state` must reach the built view"
        );
    }

    /// The block being switched off does not discard the setting, matching the dataset. That is
    /// what `spicepod`'s `CONSUMED_WHEN_DISABLED` relies on when it leaves `ready_state` out of
    /// the "discarded because `enabled: false`" warning.
    #[test]
    fn acceleration_ready_state_is_applied_even_when_acceleration_is_disabled() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
acceleration:
  enabled: false
  ready_state: on_schema_resolved
",
        );

        assert_eq!(ready_state, ReadyState::OnSchemaResolved);
    }

    /// The deprecated key wins over the view's own field, the same precedence `DatasetBuilder`
    /// applies, so the two components cannot resolve the same pair of settings differently.
    ///
    /// Both values are non-default and differ from each other. `on_load` would be useless on
    /// either side: it is the `#[default]`, so a written-out `ready_state: on_load` is
    /// indistinguishable from an omitted one, and the assertion would hold for an implementation
    /// that ignored one of the two fields entirely.
    #[test]
    fn acceleration_ready_state_takes_precedence_over_the_views_own_field() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
ready_state: on_schema_resolved
acceleration:
  enabled: true
  ready_state: on_registration
",
        );

        assert_eq!(
            ready_state,
            ReadyState::OnRegistration,
            "the acceleration block's value must win over the view's own"
        );
    }

    #[test]
    fn the_views_own_ready_state_is_used_when_the_acceleration_block_omits_it() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
ready_state: on_registration
acceleration:
  enabled: true
",
        );

        assert_eq!(ready_state, ReadyState::OnRegistration);
    }

    #[test]
    fn a_view_with_no_acceleration_block_uses_its_own_ready_state() {
        assert_eq!(
            ready_state_of(
                r"
name: daily_totals
sql: SELECT 1
ready_state: on_schema_resolved
"
            ),
            ReadyState::OnSchemaResolved
        );
        assert_eq!(
            ready_state_of(
                r"
name: daily_totals
sql: SELECT 1
"
            ),
            ReadyState::OnLoad,
            "an unset `ready_state` keeps the default"
        );
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
