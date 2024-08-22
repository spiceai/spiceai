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

use document_similarity::DocumentSimilarityTool;
use list_datasets::ListDatasetsTool;
use sql::SqlTool;
use table_schema::TableSchemaTool;

use super::SpiceModelTool;

pub mod document_similarity;
pub mod list_datasets;
pub mod sql;
pub mod table_schema;

#[must_use]
pub fn get_builtin_tools() -> Vec<Box<dyn SpiceModelTool>> {
    vec![
        Box::new(DocumentSimilarityTool {}),
        Box::new(TableSchemaTool {}),
        Box::new(SqlTool {}),
        Box::new(ListDatasetsTool {}),
    ]
}
