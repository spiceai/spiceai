/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Temporary re-exports of the shared runtime interfaces to ease migration.
//! Call sites can begin switching to these modules without depending on
//! the concrete implementations in the runtime crate.

pub use runtime_interfaces::acceleration;
pub use runtime_interfaces::dataaccelerator;
pub use runtime_interfaces::dataconnector;
pub use runtime_interfaces::datasets;
pub use runtime_interfaces::metrics;
pub use runtime_interfaces::{register_data_accelerator, register_data_connector};

pub use runtime_interfaces::{ParameterSpec, Parameters};
