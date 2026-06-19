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

#[cfg(feature = "models")]
pub mod ai;
pub mod alias;
pub mod bucket;
pub mod cosine_distance;
pub mod digest_many;
#[cfg(feature = "models")]
pub mod embed;
pub mod flatten_json;
pub mod inner_product;
pub mod json_properties;
pub mod json_tree;
pub mod l2_distance;
pub mod l2_norm;
pub mod primitive_json_codec;
pub mod truncate;
pub mod user_functions;
pub(crate) mod vector_simd;
mod vendored_hash;
