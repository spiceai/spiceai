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
    dataset::{
        Dataset, ReadyState, TimeFormat,
        acceleration::{self, Acceleration},
    },
    validate_identifier,
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
            .field("time_column", &self.time_column)
            .field("time_format", &self.time_format)
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
    pub time_column: Option<String>,
    pub time_format: Option<TimeFormat>,
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

        let acceleration = view
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()?;

        // verify that the acceleration configuration is fully supported
        if let Some(acc) = &acceleration {
            match acc.refresh_mode {
                None | Some(acceleration::RefreshMode::Full) => {}
                Some(acceleration::RefreshMode::Append) => {
                    if view.time_column.is_none() {
                        return Err(crate::Error::AcceleratedViewInvalidConfiguration {
                            view_name: view.name,
                            reason: "'refresh_mode: append' requires 'time_column' to be set"
                                .to_string(),
                        });
                    }
                }
                Some(_) => {
                    return Err(crate::Error::AcceleratedViewInvalidConfiguration {
                        view_name: view.name,
                        reason: "Only 'refresh_mode: full' or 'refresh_mode: append' is supported"
                            .to_string(),
                    });
                }
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
            time_column: view.time_column,
            time_format: view.time_format.map(TimeFormat::from),
            acceleration,
            ready_state: ReadyState::from(view.ready_state),
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

    fn time_column(&self) -> Option<&str> {
        self.time_column.as_deref()
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
}

impl ViewBuilder {
    #[must_use]
    pub fn new(name: TableReference, sql: String) -> Self {
        Self {
            name,
            sql,
            metadata: HashMap::default(),
            columns: vec![],
            time_column: None,
            time_format: None,
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
                time_column: self.time_column,
                time_format: self.time_format,
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
    use super::*;
    use spicepod::acceleration::{
        Acceleration as SpicepodAcceleration, RefreshMode as SpicepodRefreshMode,
    };

    fn view_with_acceleration(acceleration: SpicepodAcceleration) -> spicepod_view::View {
        let mut view = spicepod_view::View::new("my_view".to_string());
        view.sql = Some("SELECT id, created_at FROM my_dataset".to_string());
        view.acceleration = Some(acceleration);
        view
    }

    #[test]
    fn append_refresh_mode_requires_time_column() {
        let mut view = view_with_acceleration(SpicepodAcceleration {
            enabled: true,
            refresh_mode: Some(SpicepodRefreshMode::Append),
            ..SpicepodAcceleration::default()
        });
        view.time_column = None;

        let err = ViewBuilder::try_from(view)
            .err()
            .expect("append without time_column should fail");
        assert!(
            err.to_string().contains("time_column"),
            "error should mention time_column, got: {err}"
        );
    }

    #[test]
    fn append_refresh_mode_with_time_column_succeeds() {
        let mut view = view_with_acceleration(SpicepodAcceleration {
            enabled: true,
            refresh_mode: Some(SpicepodRefreshMode::Append),
            ..SpicepodAcceleration::default()
        });
        view.time_column = Some("created_at".to_string());

        ViewBuilder::try_from(view).expect("append with time_column should succeed");
    }

    #[test]
    fn changes_refresh_mode_is_rejected() {
        let view = view_with_acceleration(SpicepodAcceleration {
            enabled: true,
            refresh_mode: Some(SpicepodRefreshMode::Changes),
            ..SpicepodAcceleration::default()
        });

        let err = ViewBuilder::try_from(view)
            .err()
            .expect("refresh_mode: changes should be rejected");
        assert!(
            err.to_string().contains("full") && err.to_string().contains("append"),
            "error should name the supported modes, got: {err}"
        );
    }

    #[test]
    fn full_refresh_mode_succeeds_without_time_column() {
        let view = view_with_acceleration(SpicepodAcceleration {
            enabled: true,
            refresh_mode: Some(SpicepodRefreshMode::Full),
            ..SpicepodAcceleration::default()
        });

        ViewBuilder::try_from(view).expect("refresh_mode: full should succeed");
    }
}
