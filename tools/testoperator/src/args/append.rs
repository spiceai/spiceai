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

use clap::Parser;

use super::DatasetTestArgs;

#[derive(Parser, Debug)]
pub struct AppendTestArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    /// Interval in seconds between append operations
    #[arg(long, default_value_t = 240)]
    pub(crate) load_interval: u64,

    /// Number of append steps/loads
    #[arg(long, default_value_t = 10)]
    pub(crate) load_steps: u16,

    /// Include additional conflict data to test ON CONFLICT upsert behavior during append operations
    #[arg(long)]
    pub(crate) with_conflict_data: bool,
}
