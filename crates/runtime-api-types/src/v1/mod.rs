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

//! V1 API types for `/v1/*` endpoints.
//!
//! These types define the API contract for the v1 endpoints.
//! Breaking changes should not be made within the v1 version.

pub mod catalogs;
pub mod datasets;
pub mod models;
pub mod status;
pub mod workers;

pub use catalogs::CatalogInfo;
pub use datasets::DatasetInfo;
pub use models::{ModelInfo, ModelListResponse, ModelMetadata};
pub use status::ComponentStatus;
pub use workers::{WorkerInfo, WorkerListResponse};
