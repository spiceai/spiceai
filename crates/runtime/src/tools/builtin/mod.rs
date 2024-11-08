/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use document_similarity::DocumentSimilarityTool;
use get_readiness::GetReadinessTool;
use list_datasets::ListDatasetsTool;
use sample::{tool::SampleDataTool, SampleTableMethod};
use sql::SqlTool;
use table_schema::TableSchemaTool;

use super::SpiceModelTool;

pub mod catalog;
pub mod document_similarity;
pub mod get_readiness;
pub mod list_datasets;
pub mod sample;
pub mod sql;
pub mod table_schema;

// Builtin tools must also be added to [`catalog::BuiltinToolCatalog::construct_builtin`]
pub(crate) fn get_builtin_tools() -> Vec<Arc<dyn SpiceModelTool>> {
    vec![
        Arc::new(DocumentSimilarityTool::default()),
        Arc::new(TableSchemaTool::default()),
        Arc::new(SqlTool::default()),
        Arc::new(ListDatasetsTool::default()),
        Arc::new(GetReadinessTool::default()),
        Arc::new(SampleDataTool::new(SampleTableMethod::RandomSample)),
        Arc::new(SampleDataTool::new(SampleTableMethod::DistinctColumns)),
        Arc::new(SampleDataTool::new(SampleTableMethod::TopNSample)),
    ]
}
