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

//! Security utilities for safe file operations and input validation.
//!
//! This module provides functions to prevent common security vulnerabilities:
//! - Path traversal attacks (e.g., `../../etc/passwd`)
//! - Empty file downloads that could cause runtime errors
//! - SQL injection via unquoted table identifiers

use datafusion::sql::TableReference;
use std::path::Path;

/// The maximum safe nesting depth for JSON values.
///
/// This limit prevents stack overflow attacks from deeply nested JSON structures.
/// A depth of 32 is sufficient for legitimate use cases while protecting against
/// malicious payloads designed to exhaust stack space.
pub const MAX_SAFE_JSON_DEPTH: usize = 32;

/// Sanitizes a filename by extracting only the file component, preventing path traversal.
///
/// This function is critical for security when accepting filenames from untrusted sources
/// (e.g., API parameters, user input, external configuration). It strips any directory
/// components, ensuring that paths like `../../etc/passwd` become just `passwd`.
///
/// # Security Guarantees
///
/// - Prevents path traversal attacks by removing all directory components
/// - Rejects invalid UTF-8 sequences in filenames
/// - Returns only the filename component without any path separators
/// - Does not sanitize special characters in the filename itself; callers should
///   apply additional validation if they need to restrict allowed characters
///
/// # Arguments
///
/// * `input` - The potentially unsafe filename from an untrusted source
///
/// # Returns
///
/// * `Ok(String)` - The sanitized filename containing only the file component
/// * `Err(String)` - An error message if the filename is invalid or contains path traversal attempts
///
/// # Errors
///
/// Returns an error if:
/// - The input contains invalid UTF-8 sequences
/// - The path cannot be parsed to extract a filename component
///
/// # Examples
///
/// ```
/// use util::security::sanitize_filename;
///
/// // Safe filename extraction
/// assert_eq!(sanitize_filename("model.onnx").unwrap(), "model.onnx");
///
/// // Path traversal attempts are neutralized
/// assert_eq!(sanitize_filename("../../etc/passwd").unwrap(), "passwd");
/// assert_eq!(sanitize_filename("/var/log/secrets.txt").unwrap(), "secrets.txt");
/// assert_eq!(sanitize_filename("subdir/model.bin").unwrap(), "model.bin");
/// ```
#[must_use = "sanitized filename must be used to prevent path traversal vulnerabilities"]
pub fn sanitize_filename(input: &str) -> Result<String, String> {
    Path::new(input)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("Invalid filename: {input}"))
}

/// Validates that a byte buffer is not empty, preventing silent failures.
///
/// This function is critical when downloading files or processing external data.
/// Empty files could indicate network failures, corrupted downloads, or malicious
/// responses that could cause runtime errors when loading models or configurations.
///
/// # Security Guarantees
///
/// - Prevents loading of empty/corrupted files that could cause runtime panics
/// - Provides clear error messages for debugging download failures
/// - Enforces minimum data validation before expensive operations
///
/// # Arguments
///
/// * `bytes` - The byte buffer to validate
/// * `context` - A description of what was being downloaded (e.g., "model file config.json")
///
/// # Returns
///
/// * `Ok(())` - The buffer contains data and is safe to use
/// * `Err(String)` - An error message indicating the buffer is empty
///
/// # Errors
///
/// Returns an error if the byte buffer is empty, indicating a failed download
/// or corrupted data that should not be processed further.
///
/// # Examples
///
/// ```
/// use util::security::validate_non_empty_bytes;
///
/// // Valid data passes
/// let data = b"model data";
/// assert!(validate_non_empty_bytes(data, "model.onnx").is_ok());
///
/// // Empty data is rejected
/// let empty = b"";
/// assert!(validate_non_empty_bytes(empty, "config.json").is_err());
/// ```
pub fn validate_non_empty_bytes(bytes: &[u8], context: &str) -> Result<(), String> {
    if bytes.is_empty() {
        Err(format!("Downloaded file {context} is empty"))
    } else {
        Ok(())
    }
}

/// Safely quotes a table reference for use in SQL queries, preventing SQL injection.
///
/// This function handles all forms of table references (bare, partial, full) and properly
/// quotes each component by wrapping them in double quotes and escaping any embedded quotes.
/// This prevents SQL injection attacks where malicious table names could break out of
/// identifier context.
///
/// # Security Guarantees
///
/// - Prevents SQL injection via malicious table/schema/catalog names
/// - Properly escapes embedded double quotes by doubling them (SQL standard)
/// - Handles multi-part identifiers (catalog.schema.table) correctly
///
/// # Arguments
///
/// * `tbl` - The table reference to quote (bare, partial, or full)
///
/// # Returns
///
/// A properly quoted SQL identifier string safe for use in queries.
///
/// # Examples
///
/// ```
/// use datafusion::sql::TableReference;
/// use util::security::quote_table_reference;
///
/// // Simple table name
/// let tbl = TableReference::bare("users");
/// assert_eq!(quote_table_reference(&tbl), "\"users\"");
///
/// // Schema-qualified table
/// let tbl = TableReference::partial("public", "users");
/// assert_eq!(quote_table_reference(&tbl), "\"public\".\"users\"");
///
/// // Fully-qualified table
/// let tbl = TableReference::full("catalog", "public", "users");
/// assert_eq!(quote_table_reference(&tbl), "\"catalog\".\"public\".\"users\"");
/// ```
#[must_use = "quoted table reference must be used in SQL queries to prevent injection"]
pub fn quote_table_reference(tbl: &TableReference) -> String {
    /// Quotes a single identifier with double quotes, escaping any embedded quotes.
    fn quote_part(s: &str) -> String {
        format!("\"{}\"", s.replace('"', "\"\""))
    }

    match tbl {
        TableReference::Bare { table } => quote_part(table.as_ref()),
        TableReference::Partial { schema, table } => {
            format!(
                "{}.{}",
                quote_part(schema.as_ref()),
                quote_part(table.as_ref())
            )
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            format!(
                "{}.{}.{}",
                quote_part(catalog.as_ref()),
                quote_part(schema.as_ref()),
                quote_part(table.as_ref())
            )
        }
    }
}

/// Calculates the maximum nesting depth of a JSON value.
///
/// This function is critical for preventing stack overflow attacks from maliciously
/// crafted JSON payloads with excessive nesting. Deep nesting can exhaust stack space
/// during parsing, serialization, or traversal operations.
///
/// # Security Guarantees
///
/// - Prevents stack overflow from deeply nested JSON structures
/// - Iteratively calculates depth without consuming call stack
/// - Works with both objects and arrays at any nesting level
///
/// # Performance
///
/// - Time Complexity: O(n) where n is the total number of values in the JSON
/// - Space Complexity: O(d) where d is the depth (explicit stack storage)
/// - Uses iterative traversal to avoid recursive stack consumption
///
/// # Arguments
///
/// * `value` - The JSON value to measure (from `serde_json::Value`)
///
/// # Returns
///
/// The maximum nesting depth as a `usize`. A simple value (string, number, bool, null)
/// has depth 1. Each level of nesting (array or object) adds 1 to the depth.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// use util::security::get_json_depth;
///
/// // Simple values have depth 1
/// assert_eq!(get_json_depth(&json!("string")), 1);
/// assert_eq!(get_json_depth(&json!(42)), 1);
/// assert_eq!(get_json_depth(&json!(true)), 1);
/// assert_eq!(get_json_depth(&json!(null)), 1);
///
/// // Empty containers have depth 1
/// assert_eq!(get_json_depth(&json!([])), 1);
/// assert_eq!(get_json_depth(&json!({})), 1);
///
/// // Nested structures add depth
/// assert_eq!(get_json_depth(&json!({"a": 1})), 2);
/// assert_eq!(get_json_depth(&json!([1, 2, 3])), 2);
/// assert_eq!(get_json_depth(&json!({"a": {"b": 1}})), 3);
/// assert_eq!(get_json_depth(&json!([[[1]]])), 4);
///
/// // Complex nested structure
/// let complex = json!({
///     "level1": {
///         "level2": {
///             "level3": [1, 2, {"level4": "deep"}]
///         }
///     }
/// });
/// assert_eq!(get_json_depth(&complex), 5);
/// ```
///
/// # Validation Example
///
/// ```
/// use serde_json::json;
/// use util::security::{get_json_depth, MAX_SAFE_JSON_DEPTH};
///
/// let user_input = json!({"a": {"b": {"c": "value"}}});
/// let depth = get_json_depth(&user_input);
///
/// if depth > MAX_SAFE_JSON_DEPTH {
///     panic!("JSON too deeply nested: {} levels (max: {})", depth, MAX_SAFE_JSON_DEPTH);
/// }
/// ```
#[must_use = "JSON depth must be validated to prevent stack overflow attacks"]
pub fn get_json_depth(value: &serde_json::Value) -> usize {
    // Iterative implementation using an explicit stack to avoid recursion.
    // Each stack entry is (current_value, current_depth).
    let mut max_depth = 1;
    let mut stack = Vec::new();
    stack.push((value, 1));

    while let Some((v, depth)) = stack.pop() {
        match v {
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    max_depth = max_depth.max(depth);
                } else {
                    for item in arr {
                        stack.push((item, depth + 1));
                    }
                }
            }
            serde_json::Value::Object(obj) => {
                if obj.is_empty() {
                    max_depth = max_depth.max(depth);
                } else {
                    for value in obj.values() {
                        stack.push((value, depth + 1));
                    }
                }
            }
            _ => {
                max_depth = max_depth.max(depth);
            }
        }
    }

    max_depth
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename_safe_names() {
        assert_eq!(
            sanitize_filename("model.onnx").expect("should sanitize model.onnx"),
            "model.onnx"
        );
        assert_eq!(
            sanitize_filename("config.json").expect("should sanitize config.json"),
            "config.json"
        );
        assert_eq!(
            sanitize_filename("my-model_v2.bin").expect("should sanitize my-model_v2.bin"),
            "my-model_v2.bin"
        );
    }

    #[test]
    fn test_sanitize_filename_path_traversal() {
        // Classic path traversal attempts
        assert_eq!(
            sanitize_filename("../../etc/passwd").expect("should sanitize ../../etc/passwd"),
            "passwd"
        );
        assert_eq!(
            sanitize_filename("../../../root/.ssh/id_rsa")
                .expect("should sanitize ../../../root/.ssh/id_rsa"),
            "id_rsa"
        );

        // Absolute paths
        assert_eq!(
            sanitize_filename("/etc/shadow").expect("should sanitize /etc/shadow"),
            "shadow"
        );
        assert_eq!(
            sanitize_filename("/var/log/secrets.txt")
                .expect("should sanitize /var/log/secrets.txt"),
            "secrets.txt"
        );

        // Relative paths with subdirectories
        assert_eq!(
            sanitize_filename("subdir/model.bin").expect("should sanitize subdir/model.bin"),
            "model.bin"
        );
        assert_eq!(
            sanitize_filename("a/b/c/file.txt").expect("should sanitize a/b/c/file.txt"),
            "file.txt"
        );
    }

    #[test]
    fn test_sanitize_filename_edge_cases() {
        // Current directory reference
        assert_eq!(
            sanitize_filename("./model.onnx").expect("should sanitize ./model.onnx"),
            "model.onnx"
        );

        // Just a filename, no path
        assert_eq!(
            sanitize_filename("model").expect("should sanitize model"),
            "model"
        );
    }

    #[test]
    fn test_validate_non_empty_bytes_valid() {
        let data = b"some model data";
        validate_non_empty_bytes(data, "model.onnx").expect("Should validate non-empty bytes");

        let single_byte = b"x";
        validate_non_empty_bytes(single_byte, "config.json")
            .expect("Should validate non-empty bytes");
    }

    #[test]
    fn test_validate_non_empty_bytes_empty() {
        let empty = b"";
        let result = validate_non_empty_bytes(empty, "model.bin");
        assert!(result.is_err());
        let error_msg = result.expect_err("should be error");
        assert!(error_msg.contains("model.bin"));
        assert!(error_msg.contains("empty"));
    }

    #[test]
    fn test_quote_table_reference_bare() {
        let tbl = TableReference::bare("users");
        assert_eq!(quote_table_reference(&tbl), r#""users""#);

        let tbl = TableReference::bare("my_table");
        assert_eq!(quote_table_reference(&tbl), r#""my_table""#);
    }

    #[test]
    fn test_quote_table_reference_partial() {
        let tbl = TableReference::partial("public", "users");
        assert_eq!(quote_table_reference(&tbl), r#""public"."users""#);

        let tbl = TableReference::partial("my_schema", "my_table");
        assert_eq!(quote_table_reference(&tbl), r#""my_schema"."my_table""#);
    }

    #[test]
    fn test_quote_table_reference_full() {
        let tbl = TableReference::full("catalog", "public", "users");
        assert_eq!(quote_table_reference(&tbl), r#""catalog"."public"."users""#);

        let tbl = TableReference::full("my_cat", "my_schema", "my_table");
        assert_eq!(
            quote_table_reference(&tbl),
            r#""my_cat"."my_schema"."my_table""#
        );
    }

    #[test]
    fn test_quote_table_reference_sql_injection() {
        // Table name with SQL injection attempt
        let tbl = TableReference::bare("users; DROP TABLE users--");
        let quoted = quote_table_reference(&tbl);
        // Should be safely quoted, preventing the injection
        assert!(quoted.contains("DROP TABLE"));
        assert!(quoted.starts_with('"'));
        assert!(quoted.ends_with('"'));
    }

    #[test]
    fn test_get_json_depth_primitives() {
        use serde_json::json;

        // All primitives have depth 1
        assert_eq!(get_json_depth(&json!("string")), 1);
        assert_eq!(get_json_depth(&json!(42)), 1);
        assert_eq!(get_json_depth(&json!(42.5)), 1);
        assert_eq!(get_json_depth(&json!(true)), 1);
        assert_eq!(get_json_depth(&json!(false)), 1);
        assert_eq!(get_json_depth(&json!(null)), 1);
    }

    #[test]
    fn test_get_json_depth_empty_containers() {
        use serde_json::json;

        // Empty containers have depth 1
        assert_eq!(get_json_depth(&json!([])), 1);
        assert_eq!(get_json_depth(&json!({})), 1);
    }

    #[test]
    fn test_get_json_depth_flat_containers() {
        use serde_json::json;

        // Flat arrays/objects have depth 2
        assert_eq!(get_json_depth(&json!([1, 2, 3])), 2);
        assert_eq!(get_json_depth(&json!(["a", "b", "c"])), 2);
        assert_eq!(get_json_depth(&json!({"a": 1, "b": 2})), 2);
        assert_eq!(get_json_depth(&json!({"key": "value"})), 2);
    }

    #[test]
    fn test_get_json_depth_nested_arrays() {
        use serde_json::json;

        assert_eq!(get_json_depth(&json!([[1, 2]])), 3);
        assert_eq!(get_json_depth(&json!([[[1]]])), 4);
        assert_eq!(get_json_depth(&json!([[[[1]]]])), 5);

        // Mixed nesting levels - should return max
        assert_eq!(get_json_depth(&json!([1, [2, [3]]])), 4);
        assert_eq!(get_json_depth(&json!([[1], 2, [[[3]]]])), 5);
    }

    #[test]
    fn test_get_json_depth_nested_objects() {
        use serde_json::json;

        assert_eq!(get_json_depth(&json!({"a": {"b": 1}})), 3);
        assert_eq!(get_json_depth(&json!({"a": {"b": {"c": 1}}})), 4);
        assert_eq!(get_json_depth(&json!({"a": {"b": {"c": {"d": 1}}}})), 5);

        // Multiple keys at same level
        assert_eq!(get_json_depth(&json!({"a": {"b": 1}, "c": {"d": 2}})), 3);
    }

    #[test]
    fn test_get_json_depth_mixed_structures() {
        use serde_json::json;

        // Object containing array
        assert_eq!(get_json_depth(&json!({"arr": [1, 2, 3]})), 3);

        // Array containing object
        assert_eq!(get_json_depth(&json!([{"key": "value"}])), 3);

        // Complex nested structure
        let complex = json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ],
            "metadata": {
                "version": 1,
                "nested": {
                    "deep": "value"
                }
            }
        });
        assert_eq!(get_json_depth(&complex), 4);
    }

    #[test]
    fn test_get_json_depth_maximum_safe_depth() {
        use serde_json::json;

        // Test at the boundary of MAX_SAFE_JSON_DEPTH
        let mut nested = json!(1);
        for _ in 0..31 {
            // 32 levels total
            nested = json!([nested]);
        }
        assert_eq!(get_json_depth(&nested), MAX_SAFE_JSON_DEPTH);

        // One level deeper should exceed the limit
        nested = json!([nested]);
        assert_eq!(get_json_depth(&nested), MAX_SAFE_JSON_DEPTH + 1);
    }

    #[test]
    fn test_get_json_depth_attack_scenario() {
        use serde_json::json;

        // Simulate a malicious deeply nested payload
        let mut attack_payload = json!({"end": "value"});
        for i in 0..100 {
            attack_payload = json!({format!("level{}", i): attack_payload});
        }

        let depth = get_json_depth(&attack_payload);
        assert_eq!(depth, 102); // 100 levels + 1 for outer + 1 for inner value
        assert!(depth > MAX_SAFE_JSON_DEPTH);
    }

    #[test]
    fn test_get_json_depth_realistic_api_payload() {
        use serde_json::json;

        // Realistic API request payload (should be well within limits)
        let api_payload = json!({
            "query": "SELECT * FROM users",
            "parameters": {
                "$1": 42,
                "$2": "test"
            },
            "options": {
                "timeout": 30,
                "format": "json"
            }
        });

        let depth = get_json_depth(&api_payload);
        assert_eq!(depth, 3);
        assert!(depth <= MAX_SAFE_JSON_DEPTH);
    }
}
