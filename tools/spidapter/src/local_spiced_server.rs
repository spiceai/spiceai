// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::args::{LocalSpicedArgs, StdioArgs};
use crate::stdio_server;

/// Construct a [`StdioArgs`] pinned to the local backend from the minimal
/// [`LocalSpicedArgs`], then delegate to the existing stdio JSON-RPC server.
pub async fn run_local_spiced_server(args: &LocalSpicedArgs) -> anyhow::Result<()> {
    let stdio_args = to_stdio_args(args);
    stdio_server::run_stdio_server(&stdio_args).await
}

fn to_stdio_args(args: &LocalSpicedArgs) -> StdioArgs {
    StdioArgs {
        verbose: args.verbose,
        scenario: args.scenario.clone(),
        scenario_base_path: None,
        ready_wait: args.ready_wait,
        spice_cloud_api_url: String::new(),
        api_key: None,
        spiced_binary: args.spiced_binary.clone(),
    }
}
