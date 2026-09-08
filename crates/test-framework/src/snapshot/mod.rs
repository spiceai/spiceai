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

use crate::{flight::query_to_batches, queries::Query, utils::sanitize_record_batches};
use arrow::{
    array::{ArrayRef, AsArray, Float32Array, Float64Array, RecordBatch},
    datatypes::{DataType, Float32Type, Float64Type},
};
use spiceai::Client as SpiceClient;

pub const CAYENNE_PATH_FILTER_PATTERN: &str =
    r"(/data/[A-Za-z0-9_\-\[\]=]+)(?:/[A-Za-z0-9_\-\.\[\]=]+)+\.vortex";
pub const CAYENNE_PATH_FILTER_REPLACEMENT: &str = "$1/<CAYENNE_PATH>.vortex";
const VORTEX_RANGE_FILTER_PATTERN: &str = r"(\.vortex):\d+\.\.\d+";
const VORTEX_RANGE_FILTER_REPLACEMENT: &str = "$1:<RANGE>";

/// Redact the per-environment connection context that federated connectors embed
/// in their physical plan (`VirtualExecutionPlan … compute_context=host=…`). The
/// host/port/db/user string is connection metadata, not plan structure, so leaving
/// it in the snapshot makes the explain check pass only against the exact endpoint
/// the snapshot was captured on. Normalizing it to a constant token lets identical
/// plans compare equal regardless of which host/db they ran against.
///
/// `,port=\d+` anchors the match, and each field forbids spaces (`[^, ]+`) so the
/// match cannot run past the connection context into the pushed-down SQL — critical
/// because a trailing field such as `user=root` is followed only by a space before
/// `base_sql=`, and that SQL may contain no comma to otherwise stop a greedy match.
/// `db`/`user` and the trailing comma are optional to cover connectors that omit
/// fields (e.g. `MySQL` has no trailing comma, `PostgreSQL` renders `host=Tcp("…")`
/// and a trailing comma).
const CONNECTION_CONTEXT_FILTER_PATTERN: &str =
    r"compute_context=host=[^, ]+,port=\d+(?:,db=[^, ]+)?(?:,user=[^, ]+)?,?";
const CONNECTION_CONTEXT_FILTER_REPLACEMENT: &str = "compute_context=<CONNECTION>";

/// The same normalization for connectors that render their compute context as an
/// endpoint URL rather than `host=…,port=…`, which the pattern above cannot match:
///
/// - Dremio — `url=grpc://<host>:<port>,username=<user>`
/// - Spark Connect — `sc://<host>:<port>/;user_id=<user>;x-databricks-cluster-id=…`
/// - Spice Cloud — `url=https://<host>,username=…,org=…,app=…`
///
/// Each of these embeds a host, a port, and often a username or workspace/cluster
/// identifier, so an otherwise-identical plan compares unequal across environments —
/// a renamed service, a different workspace, or a dev versus prod endpoint. None of
/// them contains whitespace, and every one is followed by a space before the next
/// plan field (`base_sql=`), so matching a non-whitespace run is bounded by the field
/// itself and cannot reach into the pushed-down SQL.
const ENDPOINT_CONTEXT_FILTER_PATTERN: &str = r"compute_context=(?:url=|sc://)\S+";
const ENDPOINT_CONTEXT_FILTER_REPLACEMENT: &str = CONNECTION_CONTEXT_FILTER_REPLACEMENT;

/// Redact the scan counters `CayenneAccelerationExec` surfaces in its plan display.
/// `snapshots_scanned`/`files_scanned` report read amplification, which depends on
/// ingestion batching and how far compaction has progressed when the query runs —
/// state that varies run to run, not plan structure. Left in the snapshot, the
/// explain check fails whenever the accelerator happens to hold a different file
/// count (e.g. `files_scanned=30` vs `=35`) even though the plan is identical.
const CAYENNE_SCAN_COUNTERS_FILTER_PATTERN: &str =
    r"CayenneAccelerationExec: snapshots_scanned=\d+, files_scanned=\d+";
const CAYENNE_SCAN_COUNTERS_FILTER_REPLACEMENT: &str =
    "CayenneAccelerationExec: snapshots_scanned=<N>, files_scanned=<N>";

/// Queries temporarily excluded from explain-plan snapshot validation because their
/// plans are not yet stable enough to snapshot deterministically.
const EXPLAIN_SNAPSHOT_SKIP_LIST: &[&str] = &["chbench_q5"];

fn make_tmpdir_regex_pattern(tempdir: &str) -> String {
    format!(r"(?:{tempdir}|private/{tempdir})/[^/]*/(\.spice/)?data")
}

/// Build the list of regex filters for normalizing explain plan output.
fn build_explain_filters(temp_dir: &std::path::Path) -> Vec<(String, &'static str)> {
    let temp_dir_str = temp_dir.to_str().unwrap_or_default();
    let temp_dir_clean = temp_dir_str.trim_end_matches('/').trim_start_matches('/');
    let temp_dir_pattern = regex::escape(temp_dir_clean);
    let path_filter_pattern = make_tmpdir_regex_pattern(temp_dir_pattern.as_str());

    vec![
        (path_filter_pattern, "/data"),
        (CAYENNE_PATH_FILTER_PATTERN.to_string(), CAYENNE_PATH_FILTER_REPLACEMENT),
        (VORTEX_RANGE_FILTER_PATTERN.to_string(), VORTEX_RANGE_FILTER_REPLACEMENT),
        (
            CONNECTION_CONTEXT_FILTER_PATTERN.to_string(),
            CONNECTION_CONTEXT_FILTER_REPLACEMENT,
        ),
        (
            ENDPOINT_CONTEXT_FILTER_PATTERN.to_string(),
            ENDPOINT_CONTEXT_FILTER_REPLACEMENT,
        ),
        (
            CAYENNE_SCAN_COUNTERS_FILTER_PATTERN.to_string(),
            CAYENNE_SCAN_COUNTERS_FILTER_REPLACEMENT,
        ),
        (r"required_guarantees=\[[^\]]*\]".to_string(), "required_guarantees=[N]"),
        (r"partition_sizes=\[[^\]]*\]".to_string(), "partition_sizes=[<redacted>]"),
        (r"file_groups=\{(\d+ groups?): [^}]+\}".to_string(), "file_groups={$1: [<redacted>]}"),
        (
            r#"grouping\((?:item|"item")\.(?:i_category|i_class|"i_category"|"i_class")\),\s*grouping\((?:item|"item")\.(?:i_category|i_class|"i_category"|"i_class")\)"#.to_string(),
            "<GROUPING_PAIR>",
        ),
        (
            r#"grouping\((?:store|"store")\.(?:s_state|s_county|"s_state"|"s_county")\),\s*grouping\((?:store|"store")\.(?:s_state|s_county|"s_state"|"s_county")\)"#.to_string(),
            "<GROUPING_PAIR>",
        ),
    ]
}

/// How a query's results are recorded into an insta snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Assert the exact rendered results.
    Exact,
    /// Round float columns to [`SNAPSHOT_FLOAT_SIGNIFICANT_DIGITS`] significant
    /// digits before asserting. For scenarios whose data sources surface numeric
    /// columns as floats: their aggregates sum floats across partitions, float
    /// addition is not associative, and the combine order follows the partition
    /// count and completion order — so the exact low bits differ per machine.
    RoundedFloats,
    /// Do not snapshot this query's results.
    Skip,
}

/// Decides per (scenario name, query name) how results are snapshotted.
pub type ResultsSnapshotPredicate = fn(&str, &str) -> SnapshotMode;

/// Number of significant digits a float column keeps in a
/// [`SnapshotMode::RoundedFloats`] results snapshot.
///
/// Observed cross-machine drift on float-source TPC-H aggregates is ~1e-15
/// relative, so the 1e-9..1e-10 relative quantum of ten significant digits sits
/// ~1e5 above the noise — equal results always render equal — while keeping
/// sensitivity to real drift: a single dropped row in a scale-factor-1 aggregate
/// shifts the result by ~1e-7 relative, well above the quantum.
const SNAPSHOT_FLOAT_SIGNIFICANT_DIGITS: i32 = 10;

/// Round a finite value to [`SNAPSHOT_FLOAT_SIGNIFICANT_DIGITS`] significant
/// digits. Zero (either sign), NaN, and infinities pass through (negative zero
/// normalizes to positive so a `-0.0`/`0.0` split between runs renders equal);
/// values whose scaling would overflow (subnormals) are returned unrounded.
fn round_to_significant_digits(value: f64) -> f64 {
    if value == 0.0 {
        return 0.0;
    }
    if !value.is_finite() {
        return value;
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "log10 of a finite non-zero f64 is within [-324, 309]"
    )]
    let magnitude = value.abs().log10().floor() as i32;
    // Scale with a positive power of ten in both directions: 10^k is exactly
    // representable for k <= 22, so multiplying back (or dividing back) by it
    // reproduces clean values like 56586600000.0 instead of ...99999.999996,
    // which dividing by an inexact negative power (1e-5) would yield.
    let exponent = SNAPSHOT_FLOAT_SIGNIFICANT_DIGITS - 1 - magnitude;
    if exponent >= 0 {
        let factor = 10f64.powi(exponent);
        let scaled = value * factor;
        if scaled.is_finite() {
            scaled.round() / factor
        } else {
            value
        }
    } else {
        let factor = 10f64.powi(-exponent);
        (value / factor).round() * factor
    }
}

fn round_f32_to_significant_digits(value: f32) -> f32 {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "narrowing back to the column's own f32 precision is the point"
    )]
    let rounded = round_to_significant_digits(f64::from(value)) as f32;
    rounded
}

/// Round every top-level `Float64`/`Float32` column to
/// [`SNAPSHOT_FLOAT_SIGNIFICANT_DIGITS`] significant digits so results snapshots
/// don't record the machine-dependent low bits of float aggregates. All other
/// column types — in particular `Decimal128`, which is exact — pass through
/// untouched, as do nulls.
pub fn round_float_columns(
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    batches
        .iter()
        .map(|batch| {
            let columns = batch
                .columns()
                .iter()
                .map(|column| match column.data_type() {
                    DataType::Float64 => {
                        let rounded: Float64Array = column
                            .as_primitive::<Float64Type>()
                            .iter()
                            .map(|value| value.map(round_to_significant_digits))
                            .collect();
                        Arc::new(rounded) as ArrayRef
                    }
                    DataType::Float32 => {
                        let rounded: Float32Array = column
                            .as_primitive::<Float32Type>()
                            .iter()
                            .map(|value| value.map(round_f32_to_significant_digits))
                            .collect();
                        Arc::new(rounded) as ArrayRef
                    }
                    _ => Arc::clone(column),
                })
                .collect();
            RecordBatch::try_new(batch.schema(), columns)
        })
        .collect()
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

    // Skip queries whose explain plans are not yet stable enough to snapshot.
    let qname: &str = &query_name;
    if EXPLAIN_SNAPSHOT_SKIP_LIST.contains(&qname) {
        println!(
            "Skipping explain-plan snapshot for '{query_name}' (temporarily excluded — see EXPLAIN_SNAPSHOT_SKIP_LIST)"
        );
        return Ok(());
    }

    let parameters = query.get_parameters_batch().transpose()?;
    let plan_results = query_to_batches(spice_client, &format!("EXPLAIN {sql}"), parameters)
        .await
        .map_err(|e| anyhow::anyhow!("query `{query_name}` to plan: {e}"))?;

    // Apply filters to raw RecordBatch values before formatting so that
    // pretty_format_batches computes column widths from normalized values,
    // eliminating non-deterministic padding diffs.
    let filters = build_explain_filters(&std::env::temp_dir());
    let sanitized = sanitize_record_batches(&plan_results, &filters)?;

    let explain_plan_raw = arrow::util::pretty::pretty_format_batches(&sanitized)?;

    // Sort PartitionedUnionExec children for deterministic snapshot comparison
    let explain_plan = sort_partitioned_union_children(&explain_plan_raw.to_string());

    let mut assertion_err: Option<String> = None;

    insta::with_settings!({
        description => format!("Query: {query_name}"),
        omit_expression => true,
        snapshot_path => "snapshots/explain",
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
#[must_use]
pub fn sort_partitioned_union_children(explain_plan: &str) -> String {
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
    use std::sync::Arc;

    use arrow::{
        array::{Array, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::{round_float_columns, round_to_significant_digits};

    /// The property the rounding exists for: the same aggregate computed with a
    /// different partial-sum combine order must render identically. Inputs are
    /// real value pairs from flapping `dynamodb`/`glue`/`iceberg` TPC-H snapshots.
    #[test]
    fn float_noise_collapses_to_equal_values() {
        let noisy_pairs = [
            (56_586_554_400.730_125, 56_586_554_400.729_97), // tpch_q1 sum_base_price
            (38_273.129_734_621_754, 38_273.129_734_621_65), // tpch_q1 avg_price
            (0.049_996_586_053_729_28, 0.049_996_586_053_729_36), // tpch_q1 avg_disc
            (53_741_292_684.603_99, 53_741_292_684.603_935), // tpch_q1 sum_disc_price
        ];
        for (a, b) in noisy_pairs {
            assert_eq!(
                round_to_significant_digits(a).to_bits(),
                round_to_significant_digits(b).to_bits(),
                "noise pair ({a}, {b}) must round to the same value"
            );
        }
    }

    /// Asserts the rounded value is bit-identical to the expected double —
    /// snapshot text is the shortest-roundtrip rendering of the bits, so
    /// bit equality is exactly "renders as the same string".
    fn assert_rounds_to(input: f64, expected: f64) {
        let rounded = round_to_significant_digits(input);
        assert_eq!(
            rounded.to_bits(),
            expected.to_bits(),
            "{input} rounded to {rounded}, expected {expected}"
        );
    }

    #[test]
    fn rounds_to_ten_significant_digits_across_magnitudes() {
        assert_rounds_to(38_273.129_734_621_754, 38_273.129_73);
        assert_rounds_to(0.049_985_295_838_382_654, 0.049_985_295_84);
        assert_rounds_to(-38_273.129_734_621_754, -38_273.129_73);
        assert_rounds_to(100.0, 100.0);
        assert_rounds_to(99.999_999_999_99, 100.0);
        assert_rounds_to(1_478_493_123_456.0, 1_478_493_123_000.0);
        // Values with <= 10 significant digits render exactly as before.
        assert_rounds_to(1_478_493.0, 1_478_493.0);
        assert_rounds_to(3.21, 3.21);
    }

    #[test]
    fn preserves_non_roundable_values() {
        assert_rounds_to(0.0, 0.0);
        // Negative zero normalizes so a -0.0/0.0 split between runs renders equal.
        assert_rounds_to(-0.0, 0.0);
        assert!(round_to_significant_digits(f64::NAN).is_nan());
        assert_rounds_to(f64::INFINITY, f64::INFINITY);
        assert_rounds_to(f64::NEG_INFINITY, f64::NEG_INFINITY);
        // Subnormal: scaling overflows, the raw value passes through.
        let subnormal = f64::MIN_POSITIVE / 2.0;
        assert_rounds_to(subnormal, subnormal);
    }

    #[test]
    fn rounds_only_float_columns_and_keeps_nulls() -> Result<(), arrow::error::ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f64", DataType::Float64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("count", DataType::Int64, false),
            Field::new("flag", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![Some(56_586_554_400.730_125), None])),
                Arc::new(Float32Array::from(vec![Some(38_273.13_f32), None])),
                Arc::new(Int64Array::from(vec![1_478_493_i64, 38_854])),
                Arc::new(StringArray::from(vec!["A", "N"])),
            ],
        )?;

        let rounded = round_float_columns(std::slice::from_ref(&batch))?;
        assert_eq!(rounded.len(), 1);

        let f64_col = rounded[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| arrow::error::ArrowError::CastError("f64 column".into()))?;
        assert_eq!(f64_col.value(0).to_bits(), 56_586_554_400.0f64.to_bits());
        assert!(f64_col.is_null(1), "null must survive rounding");

        // Non-float columns must be the same arrays, not copies.
        assert!(Arc::ptr_eq(batch.column(2), rounded[0].column(2)));
        assert!(Arc::ptr_eq(batch.column(3), rounded[0].column(3)));
        Ok(())
    }

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

    #[test]
    fn test_connection_context_filter() -> Result<(), String> {
        let regex = regex::Regex::new(super::CONNECTION_CONTEXT_FILTER_PATTERN)
            .map_err(|e| format!("{e}"))?;
        let replacement = super::CONNECTION_CONTEXT_FILTER_REPLACEMENT;

        // PostgreSQL: host wrapped in Tcp("…") with a trailing comma before base_sql.
        let input = r#"VirtualExecutionPlan name=postgres compute_context=host=Tcp("benchmarking-postgres-rw.dataplatform.svc.cluster.local"),port=5432,db=tpch_accelerated,user=postgres, base_sql=SELECT "l_orderkey" FROM "lineitem""#;
        let expected = r#"VirtualExecutionPlan name=postgres compute_context=<CONNECTION> base_sql=SELECT "l_orderkey" FROM "lineitem""#;
        assert_eq!(regex.replace_all(input, replacement), expected);

        // A different PostgreSQL host/db must redact to the identical token.
        let input = r#"VirtualExecutionPlan name=postgres compute_context=host=Tcp("localhost"),port=5433,db=tpch,user=alice, base_sql=SELECT "l_orderkey" FROM "lineitem""#;
        assert_eq!(regex.replace_all(input, replacement), expected);

        // MySQL: bare host, no trailing comma before base_sql.
        let input = "VirtualExecutionPlan name=mysql compute_context=host=benchmark-mysql.dataplatform.svc.cluster.local,port=3306,db=tpch_sf1,user=root base_sql=SELECT `l_orderkey` FROM `lineitem`";
        let expected = "VirtualExecutionPlan name=mysql compute_context=<CONNECTION> base_sql=SELECT `l_orderkey` FROM `lineitem`";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Idempotent: an already-redacted snapshot is left unchanged.
        let input =
            "VirtualExecutionPlan name=postgres compute_context=<CONNECTION> base_sql=SELECT 1";
        assert_eq!(regex.replace_all(input, replacement), input);

        // The anchor `,port=\d+` prevents the greedy host match from consuming the
        // pushed-down SQL when there is no connection context to redact.
        let input = "DuckSqlExec compute_context=./data/tpch.db sql=SELECT a, b FROM t";
        assert_eq!(regex.replace_all(input, replacement), input);

        Ok(())
    }

    #[test]
    fn test_endpoint_context_filter() -> Result<(), String> {
        let regex = regex::Regex::new(super::ENDPOINT_CONTEXT_FILTER_PATTERN)
            .map_err(|e| format!("{e}"))?;
        let replacement = super::ENDPOINT_CONTEXT_FILTER_REPLACEMENT;

        // Dremio: grpc URL plus a username, comma-separated.
        let input = "VirtualExecutionPlan name=dremio compute_context=url=grpc://dremio-client.example.internal:32010,username=bench base_sql=SELECT \"l_orderkey\" FROM \"lineitem\"";
        let expected = "VirtualExecutionPlan name=dremio compute_context=<CONNECTION> base_sql=SELECT \"l_orderkey\" FROM \"lineitem\"";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // A different Dremio endpoint and user must redact to the identical token.
        let input = "VirtualExecutionPlan name=dremio compute_context=url=grpc://localhost:32010,username=dev base_sql=SELECT \"l_orderkey\" FROM \"lineitem\"";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Spark Connect: `sc://` with a user_id and cluster id, semicolon-separated.
        let input = "VirtualExecutionPlan name=spark compute_context=sc://dbc-workspace.example.invalid:443/;user_id=svc-account;x-databricks-cluster-id=0000-000000-abcdefgh;use_ssl=true base_sql=SELECT `l_orderkey` FROM `lineitem`";
        let expected = "VirtualExecutionPlan name=spark compute_context=<CONNECTION> base_sql=SELECT `l_orderkey` FROM `lineitem`";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Spice Cloud: https URL with org and app.
        let input = "VirtualExecutionPlan name=spiceai compute_context=url=https://flight.spiceai.io,username=,org=spiceai,app=benchmark base_sql=SELECT 1";
        let expected =
            "VirtualExecutionPlan name=spiceai compute_context=<CONNECTION> base_sql=SELECT 1";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Idempotent: an already-redacted snapshot is left unchanged.
        let input =
            "VirtualExecutionPlan name=dremio compute_context=<CONNECTION> base_sql=SELECT 1";
        assert_eq!(regex.replace_all(input, replacement), input);

        // Local compute contexts carry no endpoint and must not be redacted.
        let input = "DuckSqlExec compute_context=./data/tpch.db sql=SELECT a, b FROM t";
        assert_eq!(regex.replace_all(input, replacement), input);
        let input = "SqliteExec compute_context=:memory: sql=SELECT a FROM t";
        assert_eq!(regex.replace_all(input, replacement), input);

        // The `host=` form stays the other pattern's job; this one must not half-match
        // it and leave a partially-redacted context behind.
        let input = "VirtualExecutionPlan name=mysql compute_context=host=db,port=3306,db=tpch,user=root base_sql=SELECT 1";
        assert_eq!(regex.replace_all(input, replacement), input);

        Ok(())
    }

    #[test]
    fn test_cayenne_scan_counters_filter() -> Result<(), String> {
        let regex = regex::Regex::new(super::CAYENNE_SCAN_COUNTERS_FILTER_PATTERN)
            .map_err(|e| format!("{e}"))?;
        let replacement = super::CAYENNE_SCAN_COUNTERS_FILTER_REPLACEMENT;

        // Different file counts must redact to the identical token.
        let input = "|               |   CayenneAccelerationExec: snapshots_scanned=1, files_scanned=30   |";
        let expected = "|               |   CayenneAccelerationExec: snapshots_scanned=<N>, files_scanned=<N>   |";
        assert_eq!(regex.replace_all(input, replacement), expected);

        let input = "|               |   CayenneAccelerationExec: snapshots_scanned=1, files_scanned=35   |";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // A not-yet-refreshed accelerator reports zero for both counters.
        let input = "CayenneAccelerationExec: snapshots_scanned=0, files_scanned=0";
        let expected = "CayenneAccelerationExec: snapshots_scanned=<N>, files_scanned=<N>";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Idempotent: an already-redacted snapshot is left unchanged.
        let input = "CayenneAccelerationExec: snapshots_scanned=<N>, files_scanned=<N>";
        assert_eq!(regex.replace_all(input, replacement), input);

        // Other operators' counters are not this filter's job.
        let input =
            "DataSourceExec: file_groups={16 groups: [<redacted>]}, projection=[o_orderkey]";
        assert_eq!(regex.replace_all(input, replacement), input);

        Ok(())
    }

    #[test]
    fn test_file_groups_filter() -> Result<(), String> {
        let regex = regex::Regex::new(r"file_groups=\{(\d+ groups?): [^}]+\}")
            .map_err(|e| format!("{e}"))?;
        let replacement = "file_groups={$1: [<redacted>]}";

        // Multiple groups with vortex ranges and trailing `...`
        let input = "DataSourceExec: file_groups={16 groups: [[/data/orders/<CAYENNE_PATH>.vortex:<RANGE>, /data/orders/<CAYENNE_PATH>.vortex:<RANGE>], [/data/orders/<CAYENNE_PATH>.vortex:<RANGE>], ...]}, projection=[o_orderkey]";
        let expected =
            "DataSourceExec: file_groups={16 groups: [<redacted>]}, projection=[o_orderkey]";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Single group (singular "group") with single vortex file
        let input = "DataSourceExec: file_groups={1 group: [[/data/nation/<CAYENNE_PATH>.vortex]]}, projection=[n_nationkey]";
        let expected =
            "DataSourceExec: file_groups={1 group: [<redacted>]}, projection=[n_nationkey]";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Single group with parquet file
        let input = "DataSourceExec: file_groups={1 group: [[tpcds_sf1/item.parquet]]}, projection=[i_manufact], file_type=parquet";
        let expected = "DataSourceExec: file_groups={1 group: [<redacted>]}, projection=[i_manufact], file_type=parquet";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Multiple groups with parquet byte ranges and trailing `...`
        let input = "ParquetExec: file_groups={24 groups: [[/data/orders.parquet:0..2311466], [/data/orders.parquet:2311466..4622932], [/data/orders.parquet:4622932..6934398], ...]}, projection=[o_orderkey], limit=10";
        let expected =
            "ParquetExec: file_groups={24 groups: [<redacted>]}, projection=[o_orderkey], limit=10";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Single group with temp path
        let input = "DataSourceExec: file_groups={1 group: [[<TEMP_PATH>/.vortex]]}, projection=[id, name], file_type=vortex";
        let expected = "DataSourceExec: file_groups={1 group: [<redacted>]}, projection=[id, name], file_type=vortex";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // Multiple file_groups on one line (two DataSourceExec nodes in plan output)
        let input = "DataSourceExec: file_groups={4 groups: [[a], [b], [c], [d]]}, x=1 ... DataSourceExec: file_groups={2 groups: [[e], [f]]}, y=2";
        let expected = "DataSourceExec: file_groups={4 groups: [<redacted>]}, x=1 ... DataSourceExec: file_groups={2 groups: [<redacted>]}, y=2";
        assert_eq!(regex.replace_all(input, replacement), expected);

        // No file_groups — input unchanged
        let input = "SortExec: expr=[revenue@1 DESC]";
        assert_eq!(regex.replace_all(input, replacement), input);

        Ok(())
    }
}
