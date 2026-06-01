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

use crate::args::{AccelerationEngine, FederatedStorage, LocalSpicedArgs, SpiceCompute, StdioArgs};
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
        compute: SpiceCompute::Local,
        storage: FederatedStorage::Cayenne,
        acceleration: AccelerationEngine::Cayenne,
        ready_wait: args.ready_wait,
        aws_region: args.aws_region.clone(),
        cayenne_data_dir: args.cayenne_data_dir.clone(),
        cayenne_metadata_dir: args.cayenne_metadata_dir.clone(),
        scheduler_state_location: args.scheduler_state_location.clone(),
        query_memory_limit: args.query_memory_limit.clone(),
        // SCP-only fields — unused in local mode
        spice_cloud_api_url: String::new(),
        channel: None,
        image_tag: None,
        api_key: None,
        flight_url: None,
        app_memory_limit: None,
        app_cpu_limit: None,
        app_cpu_request: None,
        app_memory_request: None,
        app_replicas: None,
        executor_replicas: 1,
        executor_memory_limit: None,
        executor_cpu_limit: None,
        executor_cpu_request: None,
        executor_memory_request: None,
        app_storage_size_gb: None,
        executor_storage_size_gb: None,
        ephemeral_storage_limit_gb: None,
        organization_tag: None,
        pg_host: None,
        pg_port: 5432,
        pg_user: None,
        pg_password: String::new(),
        pg_database: None,
        // EC2 provisioning is SCP-only
        ec2_subnet_id: None,
        ec2_security_group_id: None,
        ec2_ami_id: None,
        ec2_instance_type: "m5.large".to_string(),
        ec2_associate_public_ip: false,
        ec2_iam_instance_profile: None,
        spiced_binary: "spiced".to_string(),
        auto_load_complete: false,
        mongodb_uri: None,
    }
}
