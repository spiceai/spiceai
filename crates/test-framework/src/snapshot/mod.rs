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

use std::panic;

use flight_client::FlightClient;

use crate::flight::query_to_batches;

fn make_tmpdir_regex_pattern(tempdir: &str) -> String {
    format!(r"(?:{tempdir}|private/{tempdir})/[^/]*/(\.spice/)?data")
}

pub async fn record_explain_plan(
    client: &FlightClient,
    name: &str,
    query_name: &str,
    query: &str,
) -> Result<(), String> {
    // Check the plan
    let plan_results = query_to_batches(client, &format!("EXPLAIN {query}"))
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?;

    let Ok(explain_plan) = arrow::util::pretty::pretty_format_batches(&plan_results) else {
        return Err("Failed to format plan".to_string());
    };

    let mut assertion_err: Option<String> = None;

    let temp_dir = std::env::temp_dir();
    let temp_dir_str = temp_dir.to_str().unwrap_or_default();
    let temp_dir_clean = temp_dir_str.trim_end_matches('/').trim_start_matches('/');
    let temp_dir_pattern = regex::escape(temp_dir_clean);

    // Create two patterns:
    // 1. Exact match starting with the temp_dir: {temp_dir}/some_dir/data
    // 2. Match with "private" prefix: private{temp_dir}/some_dir/data
    let path_filter_pattern = make_tmpdir_regex_pattern(temp_dir_pattern.as_str());

    insta::with_settings!({
        description => format!("Query: {query_name}"),
        omit_expression => true,
        snapshot_path => "snapshots/explain",
        filters => vec![
            (path_filter_pattern.as_str(), "/data"),
            (r"required_guarantees=\[[^\]]*\]", "required_guarantees=[N]"),
            (r"Execution error: Placeholder '\$[0-9]+' was not provided a value for execution\.", "Execution error: Placeholder 'X' was not provided a value for execution."),
        ],
    }, {
        let result = panic::catch_unwind(|| {
            insta::assert_snapshot!(format!("{name}_{query_name}_explain"), explain_plan);
        });
        if result.is_err() {
            assertion_err = Some(format!("Snapshot assertion failed for {name}, {query_name}"));
        }
    });

    if let Some(assertion_err) = assertion_err {
        return Err(assertion_err);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_temp_dir_regex_pattern() -> Result<(), String> {
        let test_cases = [
            // Test case 1: Mac temp dir path without leading path
            (
                "/var/folders/hs/xq6mn_y9293d05rw5bvhfm_c0000gn/T/",
                "var/folders/hs/xq6mn_y9293d05rw5bvhfm_c0000gn/T/.tmpGbYR27/data/partsupp.parquet:3474778..5212167",
                "/data/partsupp.parquet:3474778..5212167",
            ),
            // Test case 2: Mac temp dir path with leading path
            (
                "/var/folders/hs/xq6mn_y9293d05rw5bvhfm_c0000gn/T/",
                "private/var/folders/hs/xq6mn_y9293d05rw5bvhfm_c0000gn/T/.tmpGbYR27/data/partsupp.parquet:3474778..5212167",
                "/data/partsupp.parquet:3474778..5212167",
            ),
            // Test case 3: Linux temp dir path
            (
                "/tmp",
                "tmp/.tmpJ1DebA/data/orders.parquet:0..2311466",
                "/data/orders.parquet:0..2311466",
            ),
            (
                "/tmp",
                "tmp/.tmpJ1DebA/.spice/data/accelerated_duckdb.db",
                "/data/accelerated_duckdb.db",
            ),
        ];

        for (tmp_dir, input, expected) in test_cases {
            let temp_dir_clean = tmp_dir.trim_end_matches('/').trim_start_matches('/');
            let temp_dir_pattern = regex::escape(temp_dir_clean);
            let path_filter_pattern = super::make_tmpdir_regex_pattern(temp_dir_pattern.as_str());

            let regex = regex::Regex::new(&path_filter_pattern).map_err(|e| format!("{e}"))?;
            let result = regex.replace(input, "/data");
            assert_eq!(result, expected, "Failed for input: {input}");
        }

        Ok(())
    }
}
