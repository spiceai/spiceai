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

use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateType {
    Append,
    Overwrite,
}

#[derive(Debug, Clone)]
pub struct DataUpdate {
    pub data: Vec<RecordBatch>,
    /// The type of update to perform.
    /// If UpdateType::Append, the runtime will append the data to the existing dataset.
    /// If UpdateType::Overwrite, the runtime will overwrite the existing data with the new data.
    pub update_type: UpdateType,
}
