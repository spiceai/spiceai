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

//! Runtime components (`dataset`/`catalog`/`view`).
//!
//! The pure-configuration cores of these components — and the component-level
//! helpers below — live in the [`runtime_component`] crate, which sits *below*
//! `runtime` so connectors can name a component's configuration without pulling
//! in the orchestrator. This module keeps the `Arc<Runtime>`-bound wrappers
//! (`dataset::Dataset`, `catalog::Catalog`, `view::View`) and re-exports the
//! moved items so existing `crate::component::…` paths keep resolving during the
//! migration.

// Component-level config helpers + config-only submodules moved down to
// `runtime-component`. Re-exported here for path compatibility.
pub use runtime_component::{
    ComponentInitialization, DatasetHealthMonitor, Error, StartupOptions, access, column,
    find_first_delimiter, validate_identifier,
};

// The `Arc<Runtime>`-bound wrappers stay in `runtime`.
pub mod catalog;
pub mod dataset;
pub mod view;
