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
use serde_json::Value;
use snafu::prelude::*;
use spicepod::component::view as spicepod_view;
use std::{collections::HashMap, fs, sync::Arc, time::Duration};

use crate::{Runtime, dataaccelerator::AccelerationSource};

use super::{
    dataset::{
        Dataset, ReadyState,
        acceleration::{self, Acceleration},
    },
    validate_identifier,
};
use spicepod::semantic::Column;

/// [`View`] is the internal representation of the [`spicepod_view::View`] spicepod component.
#[derive(Clone)]
pub struct View {
    pub name: TableReference,
    pub sql: Arc<str>,
    pub metadata: HashMap<String, Value>,
    pub columns: Vec<Column>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub ready_state: ReadyState,
    pub runtime: Arc<Runtime>,
    pub app: Arc<App>,
}

impl PartialEq for View {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.sql == other.sql
            && self.metadata == other.metadata
            && self.columns == other.columns
            && self.acceleration == other.acceleration
            && self.ready_state == other.ready_state
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
            .finish_non_exhaustive()
    }
}

impl View {
    #[allow(clippy::result_large_err)]
    fn load_sql_ref(sql_ref: &str) -> crate::Result<String> {
        let sql = fs::read_to_string(sql_ref)
            .context(crate::UnableToLoadSqlFileSnafu { file: sql_ref })?;
        Ok(sql)
    }

    #[must_use]
    pub fn is_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.enabled;
        }

        false
    }

    #[must_use]
    pub fn refresh_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_check_interval;
        }
        None
    }

    #[must_use]
    pub fn refresh_max_jitter(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration
            && acceleration.refresh_jitter_enabled
        {
            // If `refresh_jitter_max` is not set, use 10% of `refresh_check_interval`.
            return match acceleration.refresh_jitter_max {
                Some(jitter) => Some(jitter),
                None => self.refresh_check_interval().map(|i| i.mul_f64(0.1)),
            };
        }
        None
    }

    #[must_use]
    pub fn refresh_retry_enabled(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_enabled;
        }
        false
    }

    #[must_use]
    pub fn refresh_retry_max_attempts(&self) -> Option<usize> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_max_attempts;
        }
        None
    }

    #[must_use]
    pub async fn is_accelerator_initialized(&self) -> bool {
        if let Some(acceleration_settings) = &self.acceleration {
            let Some(accelerator) = self
                .runtime()
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

    #[must_use]
    pub fn has_full_text_column(&self) -> bool {
        self.columns
            .iter()
            .any(|c| c.full_text_search.as_ref().is_some_and(|cfg| cfg.enabled))
    }
}

pub struct ViewBuilder {
    pub name: TableReference,
    pub sql: String,
    pub metadata: HashMap<String, Value>,
    pub columns: Vec<Column>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub ready_state: ReadyState,
}

impl TryFrom<spicepod_view::View> for ViewBuilder {
    type Error = crate::Error;

    fn try_from(view: spicepod_view::View) -> Result<Self, Self::Error> {
        validate_identifier(&view.name).context(crate::ComponentSnafu)?;

        let table_reference = Dataset::parse_table_reference(&view.name)?;

        let sql = if let Some(view_sql) = &view.sql {
            view_sql.to_string()
        } else if let Some(sql_ref) = &view.sql_ref {
            View::load_sql_ref(sql_ref)?
        } else {
            return Err(crate::Error::NeedToSpecifySQLView {
                name: table_reference.to_string(),
            });
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
                    view_name: view.name.to_string(),
                    reason: "Only 'refresh_mode: full' is supported".to_string(),
                });
            }

            if acc.refresh_sql.is_some() {
                return Err(crate::Error::AcceleratedViewInvalidConfiguration {
                    view_name: view.name.to_string(),
                    reason: "'refresh_sql' is not supported".to_string(),
                });
            }

            if acc.on_zero_results == acceleration::ZeroResultsAction::UseSource {
                return Err(crate::Error::AcceleratedViewInvalidConfiguration {
                    view_name: view.name.to_string(),
                    reason: "Only 'on_zero_results: return_empty' is supported".to_string(),
                });
            }
        }

        Ok(ViewBuilder {
            name: table_reference,
            sql,
            metadata: view.metadata,
            columns: view.columns,
            acceleration,
            ready_state: ReadyState::from(view.ready_state),
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
            return acceleration.enabled && acceleration.mode == acceleration::Mode::File;
        }
        false
    }

    fn app(&self) -> Arc<app::App> {
        Arc::clone(&self.app)
    }

    fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    fn acceleration(&self) -> Option<&Acceleration> {
        self.acceleration.as_ref()
    }

    fn name(&self) -> &TableReference {
        &self.name
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
        }
    }

    #[must_use]
    pub fn build_with(self, runtime: Arc<Runtime>, app: Arc<App>) -> View {
        View {
            name: self.name,
            sql: Arc::from(self.sql),
            metadata: self.metadata,
            columns: self.columns,
            acceleration: self.acceleration,
            ready_state: self.ready_state,
            runtime,
            app,
        }
    }
}
