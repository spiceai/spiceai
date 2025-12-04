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

pub use arrow_schema::SchemaRef;
pub use datafusion::sql::TableReference;

/// Minimal dataset view for connector/accelerator interfaces.
pub trait DatasetInfo: Send + Sync {
    fn name(&self) -> &TableReference;
    fn schema(&self) -> SchemaRef;
    fn as_any(&self) -> &dyn Any;
}

/// Minimal federated table view for connectors needing metadata hooks.
pub trait FederatedTableInfo: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &TableReference;
    fn schema(&self) -> SchemaRef;
}

/// Minimal accelerated table view for connectors needing registration hooks.
pub trait AcceleratedTableInfo: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn name(&self) -> &TableReference;
    fn schema(&self) -> SchemaRef;
}
