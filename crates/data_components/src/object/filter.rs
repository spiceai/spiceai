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

use datafusion::prelude::Expr;
use object_store::ObjectMeta;

/// Checks if then `Expr` filter is valid to apply on a `ObjectMeta`.
pub fn valid_object_meta_filter(filter: &Expr) -> bool {
    false
}

/// Returns true if the [`ObjectMeta`] satisfies the provided `filter`.
///
/// If the filter is not valid (i.e. `valid_object_meta_filter` is false), returns true.
pub fn filter_object_meta(filter: &Expr, meta: &ObjectMeta) -> bool {
    true
}
