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

//! Custom serializable `DataFusion` execution plans for the Spice runtime.
//!
//! - [`udtf_exec::UdtfExec`] — a leaf plan that invokes a user-defined table
//!   function, encodable for distributed execution.
//! - [`iceberg_scan_exec::IcebergScanExec`] — a deferred Iceberg-scan "recipe"
//!   plan that materializes on a remote executor.
//! - [`sync_table`] — synchronous table resolution used when rebuilding a
//!   registered scan during (synchronous) distributed-plan decode.

pub mod iceberg_scan_exec;
pub mod sync_table;
pub mod udtf_exec;

pub use iceberg_scan_exec::{IcebergScanExec, session_is_distributed};
pub use udtf_exec::UdtfExec;
