/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! An [`AccelerationSource`] for tests that run below `runtime`.
//!
//! An accelerator engine takes a `&dyn AccelerationSource`, and the runtime's own
//! implementation of it — `Dataset` — carries an `Arc<Runtime>`, so a test in an engine
//! crate cannot build one. What the engines ask their source for is mostly
//! configuration — `name`, `acceleration`, `is_file_accelerated`, `app` — which is what
//! this provides.
//!
//! It does **not** provide the runtime-backed answers. `initialized_sources` keeps the
//! trait's empty default, so a test that needs peer datasets — DuckDB attaching other
//! file-mode instances is the case in the tree — sees none, and belongs in the runtime's
//! own tests rather than here. `checkpointer_factory` reports that there is no
//! accelerator to checkpoint rather than handing back a no-op, which a snapshot
//! bootstrap would read as "checkpoint present and empty".
//!
//! This lives here, below every engine crate, deliberately. A test-support crate that
//! depended on the engines and was dev-depended on by them would make cargo build two
//! copies of the accelerator registration slice, and every test would then fail at run
//! time on a duplicate registration while compiling perfectly.

use std::sync::Arc;

use datafusion::common::TableReference;
use runtime_secrets::Secrets;
use tokio::sync::RwLock;

use crate::Engine;
use crate::acceleration::{Acceleration, Mode};
use crate::acceleration_source::AccelerationSource;
use crate::schema_change::OnSchemaChange;

/// A configuration-only [`AccelerationSource`].
#[derive(Clone)]
pub struct TestAccelerationSource {
    name: TableReference,
    acceleration: Option<Acceleration>,
    app: Arc<app::App>,
    secrets: Arc<RwLock<Secrets>>,
    connector_name: Option<String>,
    time_column: Option<String>,
    on_schema_change: Option<OnSchemaChange>,
    allows_write: bool,
}

impl TestAccelerationSource {
    /// A source named `name` with no acceleration configured.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: TableReference::bare(name.to_string()),
            acceleration: None,
            app: Arc::new(app::AppBuilder::new("test").build()),
            secrets: Arc::new(RwLock::new(Secrets::new())),
            connector_name: None,
            time_column: None,
            on_schema_change: None,
            allows_write: true,
        }
    }

    #[must_use]
    pub fn with_acceleration(mut self, acceleration: Acceleration) -> Self {
        self.acceleration = Some(acceleration);
        self
    }

    /// The `from:` connector name, which decides the mode an unset `refresh_mode`
    /// resolves to (see [`crate::acceleration_source::resolved_refresh_mode`]).
    #[must_use]
    pub fn with_connector_name(mut self, connector_name: &str) -> Self {
        self.connector_name = Some(connector_name.to_string());
        self
    }

    #[must_use]
    pub fn with_time_column(mut self, time_column: &str) -> Self {
        self.time_column = Some(time_column.to_string());
        self
    }

    #[must_use]
    pub fn with_on_schema_change(mut self, on_schema_change: OnSchemaChange) -> Self {
        self.on_schema_change = Some(on_schema_change);
        self
    }

    /// Whether the source takes writes of its own; `true` unless set otherwise, matching
    /// the conservative answer [`AccelerationSource::allows_write`] documents.
    #[must_use]
    pub fn with_allows_write(mut self, allows_write: bool) -> Self {
        self.allows_write = allows_write;
        self
    }
}

impl AccelerationSource for TestAccelerationSource {
    fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
        Arc::new(self.clone())
    }

    /// Mirrors `DatasetSpec::is_file_accelerated`, including its two quirks: a
    /// PostgreSQL acceleration counts as file-backed whatever its mode, and a disabled
    /// acceleration counts as nothing at all. A double that answered otherwise would
    /// send a test down an initialization path the runtime skips. (`View`'s
    /// implementation deliberately differs on PostgreSQL; this stands in for a dataset.)
    fn is_file_accelerated(&self) -> bool {
        let Some(acceleration) = self.acceleration.as_ref() else {
            return false;
        };
        if acceleration.engine == Engine::PostgreSQL {
            return true;
        }
        acceleration.enabled
            && matches!(
                acceleration.mode,
                Mode::File | Mode::FileCreate | Mode::FileUpdate
            )
    }

    fn app(&self) -> Arc<app::App> {
        Arc::clone(&self.app)
    }

    fn secrets(&self) -> Arc<RwLock<Secrets>> {
        Arc::clone(&self.secrets)
    }

    fn acceleration(&self) -> Option<&Acceleration> {
        self.acceleration.as_ref()
    }

    fn name(&self) -> &TableReference {
        &self.name
    }

    fn connector_name(&self) -> Option<&str> {
        self.connector_name.as_deref()
    }

    fn time_column(&self) -> Option<&str> {
        self.time_column.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn on_schema_change(&self) -> Option<OnSchemaChange> {
        self.on_schema_change
    }

    fn allows_write(&self) -> bool {
        self.allows_write
    }

    /// A test source has no accelerator behind it, so it reports that rather than
    /// handing back a no-op checkpointer, which a snapshot bootstrap would read as
    /// "checkpoint present and empty".
    fn checkpointer_factory(
        &self,
        _snapshot_behavior: crate::snapshot::SnapshotBehavior,
    ) -> crate::dataset_checkpoint::DatasetCheckpointerFactory {
        crate::dataset_checkpoint::make_checkpointer_factory(|| async {
            Err("a test acceleration source has no accelerator to checkpoint".into())
        })
    }
}
