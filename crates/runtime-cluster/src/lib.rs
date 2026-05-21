/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Cluster partition management for the Spice runtime.
//!
//! This crate owns partition metadata, assignment, executor wiring, and
//! write-through routing. It's decoupled from the concrete `runtime::DataFusion`
//! struct via the traits in [`context`]; runtime-side code implements those
//! traits and passes trait objects into this crate.

pub mod cluster_state;
pub mod context;
pub mod correlated;
pub mod executor_registry;
pub mod executor_selection;
pub mod flight_config;
pub mod metadata;
pub mod scheduler_task_config;
pub mod service;
pub mod store;
pub mod write_through;

pub use cluster_state::{
    ClusterState, ClusterStateStore, MutateError, MutateOk, MutationOutcome, PartitionScope,
    SchedulerEntry, SchedulerId,
};
pub use executor_registry::{ExecutorRegistry, FederatedPartitionProvider, TablePartitions};
pub use metadata::{
    PartitionMetadata, PartitionValue, TablePartitionMetadata, normalized_table_name,
    partition_value_to_bytes,
};
pub use service::{AssignmentConfig, PartitionService};
pub use store::{AllocationResult, AssignmentRequest, CopyAssignmentsResult, PartitionStore};
