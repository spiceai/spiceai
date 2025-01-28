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
pub mod common;
pub mod connector;
pub mod cosine_distance;
pub mod execution_plan;
pub mod metrics;
pub mod table;
pub mod task;
pub mod vector_search;

/// Converts string-like Arrow types into an iterator [`Option<Box<dyn Iterator<Item = Option<&str>>>>`]. If the downcast conversion
/// fails, returns `None`.
#[macro_export]
macro_rules! convert_string_arrow_to_iterator {
    ($data:expr) => {{
        None.or($data
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| Box::new(arr.iter()) as Box<dyn Iterator<Item = Option<&str>> + Send>))
            .or($data
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(|arr| Box::new(arr.iter()) as Box<dyn Iterator<Item = Option<&str>> + Send>))
            .or($data
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|arr| Box::new(arr.iter()) as Box<dyn Iterator<Item = Option<&str>> + Send>))
    }};
}
