/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Constraints, TableReference, ToDFSchema};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::CreateExternalTable;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use runtime_table_partition::expression::PartitionedBy;

use crate::acceleration::AccelerationSource;
use crate::{ParameterSpec, Parameters};

#[derive(Clone, Copy)]
pub struct AcceleratorRegistration {
    pub engine: Engine,
    pub constructor: fn() -> Arc<dyn DataAccelerator>,
}

impl AcceleratorRegistration {
    pub const fn new(engine: Engine, constructor: fn() -> Arc<dyn DataAccelerator>) -> Self {
        Self {
            engine,
            constructor,
        }
    }
}

/// Distributed slice that automatically collects all data accelerator registrations at link time
/// via the `linkme` crate. Entries are added using the [`register_data_accelerator!`] macro.
#[linkme::distributed_slice]
pub static DATA_ACCELERATOR_REGISTRATIONS: [AcceleratorRegistration] = [..];

/// Registers a data accelerator for a given engine.
///
/// This macro creates a constructor function for the specified accelerator type and
/// registers it in the global distributed slice of data accelerators. This allows
/// the runtime to discover and instantiate accelerators for supported engines.
#[macro_export]
macro_rules! register_data_accelerator {
    ($fn_name:ident, $static_name:ident, $engine:expr, $accelerator:path) => {
        fn $fn_name() -> ::std::sync::Arc<dyn $crate::dataaccelerator::DataAccelerator> {
            ::std::sync::Arc::new(<$accelerator>::new())
        }

        #[linkme::distributed_slice($crate::dataaccelerator::DATA_ACCELERATOR_REGISTRATIONS)]
        pub static $static_name: $crate::dataaccelerator::AcceleratorRegistration =
            $crate::dataaccelerator::AcceleratorRegistration::new($engine, $fn_name);
    };

    ($engine:expr, $accelerator:ident) => {
        ::paste::paste! {
            $crate::register_data_accelerator!(
                [<__register_data_accelerator_fn_ $accelerator:snake>],
                [<__REGISTER_DATA_ACCELERATOR_ $accelerator:upper>],
                $engine,
                $accelerator
            );
        }
    };
}

#[async_trait]
pub trait DataAccelerator: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;

    fn name(&self) -> &'static str;
    fn prefix(&self) -> &'static str;
    fn parameters(&self) -> &'static [ParameterSpec];

    async fn init(
        &self,
        _source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn is_initialized(&self, _source: &dyn AccelerationSource) -> bool {
        true
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec![]
    }

    fn file_path(&self, _source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        Err(FilePathError::FileModeUnsupported {})
    }

    fn is_valid_file(&self, source: &dyn AccelerationSource) -> bool {
        if let Ok(path) = self.file_path(source) {
            let path = std::path::Path::new(&path);

            !path.is_dir()
                && path
                    .extension()
                    .is_some_and(|ext| self.valid_file_extensions().iter().any(|&e| e == ext))
        } else {
            false
        }
    }

    fn has_existing_file(&self, source: &dyn AccelerationSource) -> bool {
        if let Ok(path) = self.file_path(source) {
            let path = std::path::Path::new(&path);
            path.is_file()
        } else {
            false
        }
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Engine {
    Arrow,
    Cayenne,
    DuckDB,
    Postgres,
    SQLite,
    Turso,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Memory,
    File,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Memory => "memory",
                Self::File => "file",
            }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    None,
    PrimaryKey,
    Unique,
}

#[derive(Debug)]
pub enum Error {
    InvalidConfiguration {
        msg: String,
    },
    UnknownEngine {
        engine: Arc<str> },
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug)]
pub enum FilePathError {
    AccelerationNotEnabled,
    AcceleratorEngineUnavailable {
        engine: Engine,
    },
    FileModeUnsupported {},
    External {
        engine: Engine,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct AcceleratorExternalTableBuilder {
    table_name: TableReference,
    schema: crate::datasets::SchemaRef,
    engine: Engine,
    mode: Mode,
    options: Option<Parameters>,
    indexes: HashMap<ColumnReference, IndexType>,
    constraints: Option<Constraints>,
    on_conflict: Option<OnConflict>,
}

impl AcceleratorExternalTableBuilder {
    #[must_use]
    pub fn new(
        table_name: TableReference,
        schema: crate::datasets::SchemaRef,
        engine: Engine,
    ) -> Self {
        Self {
            table_name,
            schema,
            engine,
            mode: Mode::Memory,
            options: None,
            indexes: HashMap::new(),
            constraints: None,
            on_conflict: None,
        }
    }

    #[must_use]
    pub fn indexes(mut self, indexes: HashMap<ColumnReference, IndexType>) -> Self {
        self.indexes = indexes;
        self
    }

    #[must_use]
    pub fn on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.on_conflict = Some(on_conflict);
        self
    }

    #[must_use]
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn options(mut self, options: Parameters) -> Self {
        self.options = Some(options);
        self
    }

    #[must_use]
    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    pub fn build(self) -> Result<CreateExternalTable> {
        if Mode::File == self.mode && matches!(self.engine, Engine::Arrow) {
            return Err(Error::InvalidConfiguration {
                msg: "File mode not supported for Arrow engine".to_string(),
            });
        }

        let mut options: HashMap<String, String> = self
            .options
            .map(|x| x.to_secret_map())
            .map(|x| {
                x.into_iter()
                    .map(|(k, v)| (k, v.expose_secret().to_string()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        options.insert("mode".to_string(), self.mode.to_string());

        let constraints = self
            .constraints
            .unwrap_or_else(|| Constraints::new_unverified(vec![]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&self.schema)).map_err(|e| {
            Error::InvalidConfiguration {
                msg: format!("Failed to convert schema: {e}"),
            }
        })?;

        Ok(CreateExternalTable {
            schema: df_schema,
            name: self.table_name.clone(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints,
            column_defaults: HashMap::default(),
            temporary: false,
        })
    }
}
