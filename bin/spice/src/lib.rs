/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Spice.ai CLI - Command line interface for interacting with the Spice.ai runtime.

// Pedantic lints that are acceptable for CLI code
#![allow(clippy::missing_errors_doc)] // CLI functions have obvious error conditions
#![allow(clippy::missing_panics_doc)] // CLI panics are obvious from context
#![allow(clippy::must_use_candidate)] // CLI functions often have side effects
#![allow(clippy::cast_precision_loss)] // Acceptable for display formatting (bytes, progress)
#![allow(clippy::cast_sign_loss)] // Acceptable when values are known positive
#![allow(clippy::cast_possible_wrap)] // Acceptable for timestamp conversions

pub mod commands;
pub mod context;
pub mod error;
pub mod github;
pub mod output;
pub mod registry;

pub use context::RuntimeContext;
pub use error::{Error, Result};
pub use output::TableRow;
