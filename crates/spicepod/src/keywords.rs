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

/// These keywords, selected from <https://github.com/apache/datafusion-sqlparser-rs/blob/main/src/keywords.rs>, were found to be problematic when used as dataset names in a Spicepod.
/// They are the ONLY keywords from the list above that were found to cause issues; all other keywords in the list were not problematic when testing.
/// Note that some connectors may have additional reserved keywords that are not included here. A list of connector-specific reserved keywords can be found here: <https://spiceai.org/docs/reference>.
static RESERVED_KEYWORDS: &[&str] = &[
    "COUNT", "FALSE", "NULL", "TRUE", "END-EXEC", "LATERAL", "TABLE", "UNNEST",
];

pub(crate) fn is_reserved_keyword(keyword: &str) -> bool {
    RESERVED_KEYWORDS.contains(&keyword.to_ascii_uppercase().as_str())
}
