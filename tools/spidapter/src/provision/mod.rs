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

pub(super) mod ec2_debezium;
pub(super) mod ec2_mongodb;
pub(super) mod ec2_postgres;
pub(super) mod spice_cloud;
pub(super) mod spice_local;

pub(super) use ec2_debezium::launch_ec2_debezium;
pub(super) use ec2_mongodb::launch_mongodb_ec2;
pub(super) use ec2_postgres::{Ec2PostgresInstance, is_ec2_mode, launch_postgres_ec2, terminate_ec2_instance};
pub(super) use spice_cloud::provision_scp_app;
pub(super) use spice_local::{
    provision_local_single_node, provision_local_spiced_cluster, teardown_local_run,
};
