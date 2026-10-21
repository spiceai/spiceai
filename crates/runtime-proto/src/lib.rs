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

// Allow clippy lints for tonic-generated code that we cannot control.
#![allow(
    clippy::pedantic,
    clippy::clone_on_ref_ptr,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::default_trait_access,
    clippy::allow_attributes,
    clippy::mixed_attributes_style
)]

include!(concat!(env!("OUT_DIR"), "/spice.protobuf.rs"));
