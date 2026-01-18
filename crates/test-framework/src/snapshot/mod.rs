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

use std::{panic, sync::Arc};

use crate::{flight::query_to_batches, queries::Query};
use spiceai::Client as SpiceClient;

pub const CAYENNE_PATH_FILTER_PATTERN: &str =
    r"(/data/[A-Za-z0-9_\-\[\]=]+)(?:/[A-Za-z0-9_\-\.\[\]=]+)+\.vortex";
pub const CAYENNE_PATH_FILTER_REPLACEMENT: &str = "$1/<CAYENNE_PATH>.vortex";
const VORTEX_RANGE_FILTER_PATTERN: &str = r"(\.vortex):\d+\.\.\d+";
const VORTEX_RANGE_FILTER_REPLACEMENT: &str = "$1:<RANGE>";

fn make_tmpdir_regex_pattern(tempdir: &str) -> String {
    format!(r"(?:{tempdir}|private/{tempdir})/[^/]*/(\.spice/)?data")
}

pub async fn record_explain_plan(
    spice_client: Arc<SpiceClient>,
    name: &str,
    query: &Query,
    scale_factor: f64,
) -> anyhow::Result<()> {
    // Check the plan
    let sql = Arc::clone(&query.sql);
    let query_name = Arc::clone(&query.name);
    let parameters = query.get_parameters_batch().transpose()?;
    let plan_results = query_to_batches(spice_client, &format!("EXPLAIN {sql}"), parameters)
        .await
        .map_err(|e| anyhow::anyhow!("query `{query_name}` to plan: {e}"))?;

    let explain_plan_raw = arrow::util::pretty::pretty_format_batches(&plan_results)?;

    // Sort PartitionedUnionExec children for deterministic snapshot comparison
    let explain_plan = sort_partitioned_union_children(&explain_plan_raw.to_string());

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
            (CAYENNE_PATH_FILTER_PATTERN, CAYENNE_PATH_FILTER_REPLACEMENT),
            (VORTEX_RANGE_FILTER_PATTERN, VORTEX_RANGE_FILTER_REPLACEMENT),
            (r"required_guarantees=\[[^\]]*\]", "required_guarantees=[N]"),
            (r#"grouping\((?:item|"item")\.(?:i_category|i_class|"i_category"|"i_class")\),\s*grouping\((?:item|"item")\.(?:i_category|i_class|"i_category"|"i_class")\)"#, "<GROUPING_PAIR>"),
            (r#"grouping\((?:store|"store")\.(?:s_state|s_county|"s_state"|"s_county")\),\s*grouping\((?:store|"store")\.(?:s_state|s_county|"s_state"|"s_county")\)"#, "<GROUPING_PAIR>")
        ],
    }, {
        let snapshot_name = if (scale_factor - 1.0).abs() < f64::EPSILON {
            format!("{name}_{query_name}_explain")
        } else {
            format!("{name}_{query_name}_explain_sf{scale_factor}")
        };

        let result = panic::catch_unwind(|| {
            insta::assert_snapshot!(snapshot_name, explain_plan);
        });
        if result.is_err() {
            assertion_err = Some(format!("Snapshot assertion failed for {name}, {query_name}"));
        }
    });

    if let Some(assertion_err) = assertion_err {
        return Err(anyhow::anyhow!(assertion_err));
    }

    Ok(())
}

/// Sorts children of `PartitionedUnionExec` nodes in the explain plan output
/// to ensure deterministic snapshot comparison.
///
/// The approach: when we find `PartitionedUnionExec`, we identify child subtrees
/// by their indentation level. Lines at the first child's indent level start new
/// subtrees. We sort all subtrees alphabetically.
fn sort_partitioned_union_children(explain_plan: &str) -> String {
    // if no PartitionedUnionExec, return unchanged
    if !explain_plan.contains("PartitionedUnionExec") {
        return explain_plan.to_string();
    }

    let lines: Vec<&str> = explain_plan.lines().collect();
    let mut result: Vec<String> = Vec::with_capacity(lines.len());

    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        result.push(line.to_string());

        // Check if this line contains PartitionedUnionExec
        if line.contains("PartitionedUnionExec") && i + 1 < lines.len() {
            let parent_indent = get_indent_level(line);
            let first_child_indent = get_indent_level(lines[i + 1]);

            // The first child should have greater indentation
            if first_child_indent <= parent_indent {
                i += 1;
                continue;
            }

            // Collect all lines that belong to PartitionedUnionExec children
            // Stop at empty-content lines (table separators) or lower indent
            let children_start = i + 1;
            let mut children_end = children_start;
            while children_end < lines.len() {
                let child_line = lines[children_end];
                // Stop at empty-content lines (table row separators)
                if is_empty_content_line(child_line) {
                    break;
                }
                let child_indent = get_indent_level(child_line);
                if child_indent <= parent_indent {
                    break;
                }
                children_end += 1;
            }

            // Split children into subtrees based on indent level
            let mut subtrees: Vec<Vec<&str>> = Vec::new();
            let mut current_subtree: Vec<&str> = Vec::new();

            for current_line in lines.iter().take(children_end).skip(children_start) {
                // A line at the first child's indent level starts a new subtree
                if get_indent_level(current_line) == first_child_indent
                    && !current_subtree.is_empty()
                {
                    subtrees.push(current_subtree);
                    current_subtree = Vec::new();
                }
                current_subtree.push(current_line);
            }
            if !current_subtree.is_empty() {
                subtrees.push(current_subtree);
            }

            // Sort all subtrees by their string representation
            subtrees.sort_by(|a, b| {
                let a_str = a.join("\n");
                let b_str = b.join("\n");
                a_str.cmp(&b_str)
            });

            // Add sorted subtrees to result
            for subtree in &subtrees {
                for subtree_line in subtree {
                    result.push((*subtree_line).to_string());
                }
            }

            i = children_end;
            continue;
        }
        i += 1;
    }

    result.join("\n")
}

/// Checks if a line contains only whitespace and `|` characters (empty table cell).
fn is_empty_content_line(line: &str) -> bool {
    line.chars().all(|c| c.is_whitespace() || c == '|')
}

/// Gets the indentation level of a line in the explain plan.
/// Counts leading whitespace and `|` characters before the first content.
fn get_indent_level(line: &str) -> usize {
    line.chars()
        .take_while(|c| c.is_whitespace() || *c == '|')
        .count()
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

    #[test]
    fn test_cayenne_file_filters() -> Result<(), String> {
        let test_cases = [
            (
                "/data/customer/5/019a22d7-f162-7be0-975f-417b334a95c6/tD0GMdUfbVhRvA6E_0.vortex:0..368070",
                "/data/customer/<CAYENNE_PATH>.vortex:<RANGE>",
            ),
            (
                "/data/customer/expression=22/5/019a4a83-a9a5-76b2-8cb4-3efdd70ce29b/7h45OnUbTA5PyuSE_0.vortex:",
                "/data/customer/<CAYENNE_PATH>.vortex:",
            ),
        ];

        let path_regex =
            regex::Regex::new(super::CAYENNE_PATH_FILTER_PATTERN).map_err(|e| format!("{e}"))?;
        let range_regex =
            regex::Regex::new(super::VORTEX_RANGE_FILTER_PATTERN).map_err(|e| format!("{e}"))?;

        for (input, expected) in test_cases {
            let path_redacted =
                path_regex.replace_all(input, super::CAYENNE_PATH_FILTER_REPLACEMENT);
            let fully_redacted = range_regex
                .replace_all(
                    path_redacted.as_ref(),
                    super::VORTEX_RANGE_FILTER_REPLACEMENT,
                )
                .into_owned();

            assert_eq!(fully_redacted, expected, "Failed for input: {input}");
        }

        Ok(())
    }

    #[test]
    fn test_sort_partitioned_union_children() {
        // Simplified explain plan with out-of-order PartitionedUnionExec children
        let input = r#"|               |                                       PartitionedUnionExec                                   |
|               |                                         CooperativeExec                                           |
|               |                                           BytesProcessedExec                                       |
|               |                                             DuckSqlExec sql= SELECT FROM "expression=3/orders"     |
|               |                                         CooperativeExec                                           |
|               |                                           BytesProcessedExec                                       |
|               |                                             DuckSqlExec sql= SELECT FROM "expression=1/orders"     |
|               |                                         CooperativeExec                                           |
|               |                                           BytesProcessedExec                                       |
|               |                                             DuckSqlExec sql= SELECT FROM "expression=2/orders"     |
|               |                         AggregateExec: mode=Final                                                  |"#;

        // All children sorted alphabetically (1, 2, 3)
        let expected = r#"|               |                                       PartitionedUnionExec                                   |
|               |                                         CooperativeExec                                           |
|               |                                           BytesProcessedExec                                       |
|               |                                             DuckSqlExec sql= SELECT FROM "expression=1/orders"     |
|               |                                         CooperativeExec                                           |
|               |                                           BytesProcessedExec                                       |
|               |                                             DuckSqlExec sql= SELECT FROM "expression=2/orders"     |
|               |                                         CooperativeExec                                           |
|               |                                           BytesProcessedExec                                       |
|               |                                             DuckSqlExec sql= SELECT FROM "expression=3/orders"     |
|               |                         AggregateExec: mode=Final                                                  |"#;

        let result = super::sort_partitioned_union_children(input);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sort_partitioned_union_children_plain_format() {
        // Plain format (non-table) with out-of-order children
        let input = r"SchemaCastScanExec
  PartitionedUnionExec
    CayenneAccelerationExec partition=3
      BytesProcessedExec
        DataSourceExec
    CayenneAccelerationExec partition=1
      BytesProcessedExec
        DataSourceExec
    CayenneAccelerationExec partition=2
      BytesProcessedExec
        DataSourceExec
  SomeOtherExec";

        // All children sorted alphabetically (1, 2, 3)
        let expected = r"SchemaCastScanExec
  PartitionedUnionExec
    CayenneAccelerationExec partition=1
      BytesProcessedExec
        DataSourceExec
    CayenneAccelerationExec partition=2
      BytesProcessedExec
        DataSourceExec
    CayenneAccelerationExec partition=3
      BytesProcessedExec
        DataSourceExec
  SomeOtherExec";

        let result = super::sort_partitioned_union_children(input);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sort_partitioned_union_children_no_union() {
        // Plan without PartitionedUnionExec should be unchanged
        let input = r"|               |   ProjectionExec                    |
|               |     SortExec                        |
|               |       AggregateExec                 |";

        let result = super::sort_partitioned_union_children(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_get_indent_level() {
        // Table format: counts whitespace and | before first content
        assert_eq!(
            super::get_indent_level("|               |   PartitionedUnionExec   |"),
            20 // |, 15 spaces, |, 3 spaces
        );
        assert_eq!(
            super::get_indent_level("|               |     CooperativeExec      |"),
            22 // |, 15 spaces, |, 5 spaces
        );
        assert_eq!(
            super::get_indent_level("|               | PartitionedUnionExec     |"),
            18 // |, 15 spaces, |, 1 space
        );
        // Plain format: counts leading spaces
        assert_eq!(super::get_indent_level("  PartitionedUnionExec"), 2);
        assert_eq!(super::get_indent_level("    CayenneAccelerationExec"), 4);
        assert_eq!(super::get_indent_level("SchemaCastScanExec"), 0);
    }

    #[test]
    fn test_sort_partitioned_union_children_empty() {
        // PartitionedUnionExec with no children (sibling follows at same indent)
        let input = r"|               |                                       PartitionedUnionExec                                   |
|               |                         AggregateExec: mode=Final                                                  |
|               |                           ProjectionExec                                                           |";

        // Should remain unchanged - no children to sort
        let result = super::sort_partitioned_union_children(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_sort_partitioned_union_children_trailing_empty_line() {
        // Table format with trailing empty line in last child - should be preserved at end
        let input = r"|               |                                       PartitionedUnionExec                                   |
|               |                                         CooperativeExec partition=2                                |
|               |                                         CooperativeExec partition=1                                |
|               |                                                                                                    |";

        // Children sorted (1, 2), trailing empty line stays at end
        let expected = r"|               |                                       PartitionedUnionExec                                   |
|               |                                         CooperativeExec partition=1                                |
|               |                                         CooperativeExec partition=2                                |
|               |                                                                                                    |";

        let result = super::sort_partitioned_union_children(input);
        assert_eq!(result, expected);
    }
}
