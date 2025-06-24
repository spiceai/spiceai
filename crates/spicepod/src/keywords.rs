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

// https://github.com/apache/datafusion-sqlparser-rs/blob/87d19073/src/keywords.rs
static PROTECTED_KEYWORDS: &[&str] = &[
    "WITH",
    "EXPLAIN",
    "ANALYZE",
    "SELECT",
    "WHERE",
    "GROUP",
    "SORT",
    "PIVOT",
    "UNPIVOT",
    "TOP",
    "LATERAL",
    "VIEW",
    "LIMIT",
    "OFFSET",
    "FETCH",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "MINUS",
    "ON",
    "JOIN",
    "INNER",
    "CROSS",
    "FULL",
    "LEFT",
    "RIGHT",
    "NATURAL",
    "USING",
    "CLUSTER",
    "DISTRIBUTE",
    "GLOBAL",
    "ANTI",
    "SEMI",
    "RETURNING",
    "ASOF",
    "MATCH_CONDITION",
    "TABLE",
    "FROM",
    "INTO",
    "END",
];

pub(crate) fn is_protected_keyword(keyword: &str) -> bool {
    PROTECTED_KEYWORDS.contains(&keyword.to_ascii_uppercase().as_str())
}
