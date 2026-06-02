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

//! Catalog API types for `/v1/catalogs` endpoint.

use serde::{Deserialize, Serialize};

/// Catalog information returned by the `/v1/catalogs` endpoint.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct CatalogInfo {
    /// The source/provider of the catalog (e.g., `spiceai`, `unity`)
    pub from: String,

    /// The name of the catalog
    pub name: String,
}
