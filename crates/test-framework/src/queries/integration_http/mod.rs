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

use crate::queries::Query;

/// HTTP connector integration test queries
pub fn get_queries() -> Vec<Query> {
    vec![
        Query::new(
            "http_single_object".into(),
            include_str!("./q1_single_object.sql").into(),
            false,
        ),
        Query::new(
            "http_multi_object".into(),
            include_str!("./q2_multi_object.sql").into(),
            false,
        ),
        Query::new(
            "http_combined_endpoints".into(),
            include_str!("./q3_combined_endpoints.sql").into(),
            false,
        ),
        Query::new(
            "http_multiple_ids".into(),
            include_str!("./q4_multiple_ids.sql").into(),
            false,
        ),
        Query::new(
            "http_verify_structure".into(),
            include_str!("./q5_verify_structure.sql").into(),
            false,
        ),
        Query::new(
            "http_count_all".into(),
            include_str!("./q6_count_all.sql").into(),
            false,
        ),
    ]
}
