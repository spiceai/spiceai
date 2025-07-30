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

use super::CommonArgs;
use clap::Parser;

#[derive(Parser)]
pub struct VectorSearchTestArgs {
    #[clap(flatten)]
    pub(crate) common: CommonArgs,

    /// Target test dataset to run the search test against.
    #[arg(long)]
    pub(crate) benchmark_dataset: Option<String>,
}
