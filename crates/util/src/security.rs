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

//! Security utilities for validating and sanitizing inputs to prevent common
//! security vulnerabilities.

use serde_json::Value;
use std::path::Path;

/// Maximum safe JSON nesting depth to prevent stack overflow attacks.
///
/// Deeply nested JSON can cause stack overflow during parsing or traversal.
/// This limit prevents such attacks while allowing reasonable nesting for
/// legitimate use cases.
///
/// # Security Note
///
/// A depth of 32 is considered safe for most practical applications while
/// preventing malicious payloads that could cause stack exhaustion.
pub const MAX_SAFE_JSON_DEPTH: usize = 32;

/// Sanitizes a filename by extracting only the file name component and removing
/// any directory path components.
///
/// This prevents path traversal attacks by ensuring that only the filename is used,
/// stripping any directory components (including `..` or absolute paths).
///
/// # Arguments
///
/// * `path` - The path to sanitize
///
/// # Returns
///
/// A sanitized filename with directory components removed. Returns the original
/// input if it cannot be parsed as a valid filename.
///
/// # Examples
///
/// ```
/// use util::security::sanitize_filename;
///
/// assert_eq!(sanitize_filename("../etc/passwd"), "passwd");
/// assert_eq!(sanitize_filename("/etc/passwd"), "passwd");
/// assert_eq!(sanitize_filename("file.txt"), "file.txt");
/// assert_eq!(sanitize_filename("dir/file.txt"), "file.txt");
/// ```
#[must_use]
pub fn sanitize_filename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path)
        .to_string()
}

/// Validates that a byte slice is not empty.
///
/// This is useful for ensuring that downloaded or received data contains actual
/// content and hasn't been truncated or is incomplete.
///
/// # Arguments
///
/// * `bytes` - The byte slice to validate
/// * `context` - A description of what is being validated (for error messages)
///
/// # Returns
///
/// `Ok(())` if the byte slice is non-empty, otherwise returns an error with
/// a descriptive message.
///
/// # Examples
///
/// ```
/// use util::security::validate_non_empty_bytes;
///
/// assert!(validate_non_empty_bytes(&[1, 2, 3], "test data").is_ok());
/// assert!(validate_non_empty_bytes(&[], "test data").is_err());
/// ```
///
/// # Errors
///
/// Returns an error if `bytes` is empty.
pub fn validate_non_empty_bytes(
    bytes: &[u8],
    context: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if bytes.is_empty() {
        return Err(format!("{context} is empty").into());
    }
    Ok(())
}

/// Quotes a table reference to prevent SQL injection attacks.
///
/// This function properly escapes table names by enclosing them in quotes and
/// escaping any existing quotes within the name. This prevents SQL injection
/// when table names are constructed from user input.
///
/// # Arguments
///
/// * `table_ref` - The table reference to quote
///
/// # Returns
///
/// A properly quoted table reference safe for use in SQL statements.
///
/// # Examples
///
/// ```
/// use util::security::quote_table_reference;
///
/// assert_eq!(quote_table_reference("users"), "\"users\"");
/// assert_eq!(quote_table_reference("my\"table"), "\"my\"\"table\"");
/// ```
#[must_use]
pub fn quote_table_reference(table_ref: &str) -> String {
    format!("\"{}\"", table_ref.replace('"', "\"\""))
}

/// Calculates the maximum nesting depth of a JSON value.
///
/// This function traverses a JSON structure and returns the maximum depth of
/// nesting. This is useful for preventing stack overflow attacks via deeply
/// nested JSON payloads.
///
/// # Arguments
///
/// * `value` - The JSON value to analyze
///
/// # Returns
///
/// The maximum nesting depth. A simple value (string, number, boolean, null)
/// has depth 0. An array or object has depth 1 + the maximum depth of its contents.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// use util::security::get_json_depth;
///
/// assert_eq!(get_json_depth(&json!("simple")), 0);
/// assert_eq!(get_json_depth(&json!({"a": "b"})), 1);
/// assert_eq!(get_json_depth(&json!({"a": {"b": "c"}})), 2);
/// assert_eq!(get_json_depth(&json!([1, [2, [3]]])), 3);
/// ```
#[must_use]
pub fn get_json_depth(value: &Value) -> usize {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                0
            } else {
                1 + map.values().map(get_json_depth).max().unwrap_or(0)
            }
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                0
            } else {
                1 + arr.iter().map(get_json_depth).max().unwrap_or(0)
            }
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sanitize_filename() {
        // Path traversal attempts
        assert_eq!(sanitize_filename("../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("/etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("/etc/../passwd"), "passwd");

        // Windows paths
        assert_eq!(sanitize_filename("C:\\Windows\\System32\\config"), "config");
        assert_eq!(sanitize_filename("..\\..\\Windows\\System32"), "System32");

        // Normal filenames
        assert_eq!(sanitize_filename("file.txt"), "file.txt");
        assert_eq!(sanitize_filename("my-file.pdf"), "my-file.pdf");

        // With directory components
        assert_eq!(sanitize_filename("dir/file.txt"), "file.txt");
        assert_eq!(sanitize_filename("path/to/file.txt"), "file.txt");

        // Edge cases
        assert_eq!(sanitize_filename(""), "");
        assert_eq!(sanitize_filename("."), ".");
        assert_eq!(sanitize_filename(".."), "..");
    }

    #[test]
    fn test_validate_non_empty_bytes() {
        assert!(validate_non_empty_bytes(&[1, 2, 3], "test").is_ok());
        assert!(validate_non_empty_bytes(&[0], "test").is_ok());

        let err = validate_non_empty_bytes(&[], "test data");
        assert!(err.is_err());
        assert!(
            err.expect_err("should be an error")
                .to_string()
                .contains("test data is empty")
        );
    }

    #[test]
    fn test_quote_table_reference() {
        assert_eq!(quote_table_reference("users"), "\"users\"");
        assert_eq!(quote_table_reference("my_table"), "\"my_table\"");

        // SQL injection attempts
        assert_eq!(
            quote_table_reference("users; DROP TABLE users;"),
            "\"users; DROP TABLE users;\""
        );

        // Quotes in table name
        assert_eq!(quote_table_reference("my\"table"), "\"my\"\"table\"");
        assert_eq!(
            quote_table_reference("test\"\"table"),
            "\"test\"\"\"\"table\""
        );

        // Edge cases
        assert_eq!(quote_table_reference(""), "\"\"");
        assert_eq!(quote_table_reference("\""), "\"\"\"\"");
    }

    #[test]
    fn test_get_json_depth_simple_values() {
        assert_eq!(get_json_depth(&json!(null)), 0);
        assert_eq!(get_json_depth(&json!(true)), 0);
        assert_eq!(get_json_depth(&json!(false)), 0);
        assert_eq!(get_json_depth(&json!(42)), 0);
        assert_eq!(get_json_depth(&json!(3.5)), 0);
        assert_eq!(get_json_depth(&json!("string")), 0);
    }

    #[test]
    fn test_get_json_depth_empty_collections() {
        assert_eq!(get_json_depth(&json!([])), 0);
        assert_eq!(get_json_depth(&json!({})), 0);
    }

    #[test]
    fn test_get_json_depth_arrays() {
        assert_eq!(get_json_depth(&json!([1, 2, 3])), 1);
        assert_eq!(get_json_depth(&json!([1, [2, 3]])), 2);
        assert_eq!(get_json_depth(&json!([1, [2, [3, [4]]]])), 4);
        assert_eq!(get_json_depth(&json!([[[[[[[[[[1]]]]]]]]]])), 10);
    }

    #[test]
    fn test_get_json_depth_objects() {
        assert_eq!(get_json_depth(&json!({"a": 1})), 1);
        assert_eq!(get_json_depth(&json!({"a": {"b": 2}})), 2);
        assert_eq!(get_json_depth(&json!({"a": {"b": {"c": 3}}})), 3);
    }

    #[test]
    fn test_get_json_depth_mixed() {
        assert_eq!(
            get_json_depth(&json!({
                "simple": "value",
                "nested": {
                    "array": [1, 2, {"deep": "value"}]
                }
            })),
            4
        );

        assert_eq!(
            get_json_depth(&json!([
                {"a": 1},
                {"b": {"c": [1, 2, 3]}}
            ])),
            4
        );
    }

    #[test]
    fn test_get_json_depth_max_safe() {
        // Create a deeply nested JSON at the safe limit
        let mut value = json!(0);
        for _ in 0..MAX_SAFE_JSON_DEPTH {
            value = json!([value]);
        }
        assert_eq!(get_json_depth(&value), MAX_SAFE_JSON_DEPTH);
    }

    #[test]
    fn test_get_json_depth_exceeds_safe() {
        // Create a deeply nested JSON exceeding the safe limit
        let mut value = json!(0);
        for _ in 0..(MAX_SAFE_JSON_DEPTH + 10) {
            value = json!([value]);
        }
        assert_eq!(get_json_depth(&value), MAX_SAFE_JSON_DEPTH + 10);
    }

    #[test]
    fn test_get_json_depth_wide_objects() {
        // Wide but shallow - should have low depth
        let wide_object = json!({
            "a": 1, "b": 2, "c": 3, "d": 4, "e": 5,
            "f": 6, "g": 7, "h": 8, "i": 9, "j": 10
        });
        assert_eq!(get_json_depth(&wide_object), 1);
    }

    #[test]
    fn test_get_json_depth_unbalanced_nesting() {
        // Different branches with different depths
        let value = json!({
            "shallow": 1,
            "medium": {"a": 2},
            "deep": {"b": {"c": {"d": 4}}}
        });
        assert_eq!(get_json_depth(&value), 4); // Takes the deepest branch
    }
}
