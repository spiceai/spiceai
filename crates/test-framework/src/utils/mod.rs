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

use regex::Regex;
use std::{
    fs,
    future::Future,
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

use arrow::array::{AsArray, LargeStringArray, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;

use crate::process::{MemoryReading, MemoryReadingsHandle};

pub async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

pub fn hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Apply regex filters to all string columns in a list of `RecordBatch`es.
///
/// Filters use the same `(pattern, replacement)` tuple format as insta snapshot
/// filters. Each cell value in every `Utf8` column is run through the filters
/// in order. This should be applied **before** `pretty_format_batches` so that
/// table column widths are computed from the normalized (deterministic) values.
///
/// # Errors
///
/// Returns an error if a filter pattern is not a valid regex or if rebuilding
/// a `RecordBatch` fails.
pub fn sanitize_record_batches<P: AsRef<str>, R: AsRef<str>>(
    batches: &[RecordBatch],
    filters: &[(P, R)],
) -> anyhow::Result<Vec<RecordBatch>> {
    let compiled: Vec<(Regex, &str)> = filters
        .iter()
        .map(|(pattern, replacement)| {
            let pattern = pattern.as_ref();
            let regex = Regex::new(pattern)?;
            Ok((regex, replacement.as_ref()))
        })
        .collect::<Result<Vec<_>, regex::Error>>()?;
    batches
        .iter()
        .map(|batch| sanitize_batch(batch, &compiled).map_err(Into::into))
        .collect()
}

fn sanitize_batch(
    batch: &RecordBatch,
    filters: &[(Regex, &str)],
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    let new_columns: Vec<Arc<dyn arrow::array::Array>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let col = batch.column(i);
            match field.data_type() {
                DataType::Utf8 => {
                    let str_array = col.as_string::<i32>();
                    let sanitized: StringArray = str_array
                        .iter()
                        .map(|opt| opt.map(|v| apply_filters(v, filters)))
                        .collect();
                    Arc::new(sanitized) as Arc<dyn arrow::array::Array>
                }
                DataType::LargeUtf8 => {
                    let str_array = col.as_string::<i64>();
                    let sanitized: LargeStringArray = str_array
                        .iter()
                        .map(|opt| opt.map(|v| apply_filters(v, filters)))
                        .collect();
                    Arc::new(sanitized) as Arc<dyn arrow::array::Array>
                }
                _ => Arc::clone(col),
            }
        })
        .collect();

    RecordBatch::try_new(Arc::clone(&schema), new_columns)
}

/// Apply a list of regex filters to a string, returning the result.
fn apply_filters(value: &str, filters: &[(Regex, &str)]) -> String {
    let mut result = value.to_string();
    for (regex, replacement) in filters {
        result = regex.replace_all(&result, *replacement).into_owned();
    }
    result
}

// replace insta headers with an empty string
const INSTA_HEADER_REGEX: &str = r"^---\n(([\w\W]*\n)+)---\n";
static INSTA_HEADER_RE: LazyLock<Regex> = LazyLock::new(|| {
    #[expect(clippy::expect_used)] // the regex is valid
    Regex::new(INSTA_HEADER_REGEX).expect("Insta header replacement regex should build")
});

/// Compare two insta snapshots by hashing their contents.
/// Returns true if the snapshots are the same.
///
/// This doesn't use ``assert_snapshot!`` because:
/// - insta might update the snapshots which we don't want
/// - we want to return a boolean instead of any other kind of error/panic
#[must_use]
pub fn snapshots_are_equal(snapshot_a: &str, snapshot_b: &str) -> bool {
    // remove insta headers
    let snapshot_a = INSTA_HEADER_RE.replace(snapshot_a, "");
    let snapshot_b = INSTA_HEADER_RE.replace(snapshot_b, "");

    let hash_a = hash(&snapshot_a);
    let hash_b = hash(&snapshot_b);

    hash_a == hash_b
}

/// Recursively scan a directory for YAML files
pub fn scan_directory_for_yamls(path: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = vec![];

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            files.append(&mut scan_directory_for_yamls(&path)?);
        } else if path.is_file() && path.extension().is_some_and(|ext| ext == "yaml") {
            files.push(path);
        }
    }

    Ok(files)
}

/// From a list of memory readings, return the maximum observed memory usage
pub fn max_observed_memory(readings: &[MemoryReading]) -> f64 {
    readings
        .iter()
        .map(|reading| reading.memory_usage)
        .fold(0.0, f64::max)
}

/// From a list of memory readings, return the median observed memory usage
pub fn median_observed_memory(readings: &[MemoryReading]) -> anyhow::Result<f64> {
    let mut memory_usages: Vec<f64> = readings
        .iter()
        .map(|reading| reading.memory_usage)
        .collect();
    memory_usages.sort_by(f64::total_cmp);

    let len = memory_usages.len();
    if len.is_multiple_of(2) {
        Ok(f64::midpoint(
            memory_usages[len / 2],
            memory_usages[len / 2 - 1],
        ))
    } else {
        Ok(memory_usages[len / 2])
    }
}

/// Collect memory readings from a join handle, using a cancellation token to end the handle
/// Print the maximum and median memory usage, then return then in a tuple as floats
pub async fn observe_memory(
    cancellation_token: CancellationToken,
    memory_readings: MemoryReadingsHandle,
) -> anyhow::Result<(f64, f64)> {
    cancellation_token.cancel();
    let memory_readings = memory_readings.await??;
    let max_memory = max_observed_memory(&memory_readings);
    let median_memory = median_observed_memory(&memory_readings)?;
    println!("Max memory usage: {max_memory:.2} GB");
    println!("Median memory usage: {median_memory:.2} GB");
    Ok((max_memory, median_memory))
}

pub fn recursively_get_dir_size(dir: &PathBuf) -> anyhow::Result<usize> {
    let mut total_size = 0;
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                total_size += usize::try_from(entry.metadata()?.len())?;
            } else if entry.file_type()?.is_dir() {
                total_size += recursively_get_dir_size(&entry.path())?;
            }
        }
    }
    Ok(total_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(plan_types: &[&str], plans: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    plan_types
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    plans.iter().map(ToString::to_string).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("failed to create test batch")
    }

    #[test]
    fn test_sanitize_no_filters() {
        let batch = make_batch(&["logical_plan"], &["Scan /tmp/abc/data"]);
        let result = sanitize_record_batches(std::slice::from_ref(&batch), &[] as &[(&str, &str)])
            .expect("to sanitize");
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].column(1).as_string::<i32>().value(0),
            "Scan /tmp/abc/data"
        );
    }

    #[test]
    fn test_sanitize_single_filter() {
        let batch = make_batch(
            &["physical_plan"],
            &["Scan /tmp/sess123/.spice/data/table.parquet"],
        );
        let filters = vec![(r"/tmp/[^/]+/\.spice/data", "<DATA>")];
        let result = sanitize_record_batches(&[batch], &filters).expect("to sanitize");
        assert_eq!(
            result[0].column(1).as_string::<i32>().value(0),
            "Scan <DATA>/table.parquet"
        );
    }

    #[test]
    fn test_sanitize_multiple_filters_applied_in_order() {
        let batch = make_batch(
            &["physical_plan"],
            &["file.vortex:100..200 required_guarantees=[a, b]"],
        );
        let filters = vec![
            (r"(\.vortex):\d+\.\.\d+", "$1:<RANGE>"),
            (r"required_guarantees=\[[^\]]*\]", "required_guarantees=[N]"),
        ];
        let result = sanitize_record_batches(&[batch], &filters).expect("to sanitize");
        assert_eq!(
            result[0].column(1).as_string::<i32>().value(0),
            "file.vortex:<RANGE> required_guarantees=[N]"
        );
    }

    #[test]
    fn test_sanitize_preserves_non_string_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["abc123", "def456"])),
            ],
        )
        .expect("failed to create test batch");

        let filters: Vec<(&str, &str)> = vec![(r"\d+", "N")];
        let result = sanitize_record_batches(&[batch], &filters).expect("to sanitize");

        // Int column untouched
        let int_col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int col");
        assert_eq!(int_col.value(0), 1);
        assert_eq!(int_col.value(1), 2);

        // String column filtered
        let str_col = result[0].column(1).as_string::<i32>();
        assert_eq!(str_col.value(0), "abcN");
        assert_eq!(str_col.value(1), "defN");
    }

    #[test]
    fn test_sanitize_preserves_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("hello123"),
                None,
                Some("world456"),
            ]))],
        )
        .expect("failed to create test batch");

        let filters: Vec<(&str, &str)> = vec![(r"\d+", "N")];
        let result = sanitize_record_batches(&[batch], &filters).expect("to sanitize");
        let col = result[0].column(0).as_string::<i32>();
        assert_eq!(col.value(0), "helloN");
        assert!(col.is_null(1));
        assert_eq!(col.value(2), "worldN");
    }

    #[test]
    fn test_sanitize_multiple_batches() {
        let b1 = make_batch(&["logical_plan"], &["path /tmp/a/data"]);
        let b2 = make_batch(&["physical_plan"], &["path /tmp/b/data"]);
        let filters = vec![(r"/tmp/[a-z]/data", "<DIR>")];
        let result = sanitize_record_batches(&[b1, b2], &filters).expect("to sanitize");
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].column(1).as_string::<i32>().value(0),
            "path <DIR>"
        );
        assert_eq!(
            result[1].column(1).as_string::<i32>().value(0),
            "path <DIR>"
        );
    }
}
