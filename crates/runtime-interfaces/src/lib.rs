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

//! Interface crate for data connector and accelerator abstractions.
//!
//! This crate will host traits, macros, and types that allow the runtime to
//! depend only on shared interfaces rather than concrete implementations.

pub mod metrics;

pub use runtime_parameters::{ParameterSpec, Parameters};
