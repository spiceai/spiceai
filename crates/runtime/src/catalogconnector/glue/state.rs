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

use super::DatabaseName;
use crate::{Runtime, dataconnector::parameters::ConnectorParams};
use aws_sdk_glue::types::Table;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub struct GlueCatalogState {
    pub(super) databases: HashMap<DatabaseName, Vec<Table>>,
    pub(super) parameters: ConnectorParams,
    pub(super) runtime: Arc<Runtime>,
}

impl GlueCatalogState {
    pub(super) fn new(
        databases: HashMap<DatabaseName, Vec<Table>>,
        parameters: ConnectorParams,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            databases,
            parameters,
            runtime,
        }
    }
}

impl fmt::Debug for GlueCatalogState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlueCatalogState")
            .field("databases", &self.databases)
            .finish_non_exhaustive()
    }
}
