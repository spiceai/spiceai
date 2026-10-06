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

//! What an accelerator does when the source schema changes.

use spicepod::component::dataset as spicepod_dataset;
use std::fmt::Display;

/// The `on_schema_change` policy a source declares.
///
/// Lives beside the acceleration contract rather than with the dataset config because
/// the accelerator is what acts on it: an engine asks
/// [`crate::AccelerationSource::on_schema_change`] for the policy and decides whether
/// it may widen its stored schema in place.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OnSchemaChange {
    #[default]
    Block,
    Fail,
    AppendNewColumns,
    SyncAllColumns,
    DropAndRecreate,
}

impl From<spicepod_dataset::OnSchemaChange> for OnSchemaChange {
    fn from(on_schema_change: spicepod_dataset::OnSchemaChange) -> Self {
        match on_schema_change {
            spicepod_dataset::OnSchemaChange::Block => OnSchemaChange::Block,
            spicepod_dataset::OnSchemaChange::Fail => OnSchemaChange::Fail,
            spicepod_dataset::OnSchemaChange::AppendNewColumns => OnSchemaChange::AppendNewColumns,
            spicepod_dataset::OnSchemaChange::SyncAllColumns => OnSchemaChange::SyncAllColumns,
            spicepod_dataset::OnSchemaChange::DropAndRecreate => OnSchemaChange::DropAndRecreate,
        }
    }
}

impl Display for OnSchemaChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnSchemaChange::Block => write!(f, "block"),
            OnSchemaChange::Fail => write!(f, "fail"),
            OnSchemaChange::AppendNewColumns => write!(f, "append_new_columns"),
            OnSchemaChange::SyncAllColumns => write!(f, "sync_all_columns"),
            OnSchemaChange::DropAndRecreate => write!(f, "drop_and_recreate"),
        }
    }
}
