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
//! - Malicious filenames with special characters
//! - SQL injection via unquoted table identifiers

use datafusion::{common::utils::quote_identifier, sql::TableReference};
use std::path::Path;

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
/// quotes each component using `DataFusion`'s `quote_identifier`. This prevents SQL injection
/// attacks where malicious table names could break out of identifier context.
///
/// # Security Guarantees
///
/// - Prevents SQL injection via malicious table/schema/catalog names
/// - Properly escapes special characters and SQL keywords
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
    match tbl {
        TableReference::Bare { table } => quote_identifier(table.as_ref()).to_string(),
        TableReference::Partial { schema, table } => {
            format!(
                "{}.{}",
                quote_identifier(schema.as_ref()),
                quote_identifier(table.as_ref())
            )
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            format!(
                "{}.{}.{}",
                quote_identifier(catalog.as_ref()),
                quote_identifier(schema.as_ref()),
                quote_identifier(table.as_ref())
            )
        }
    }
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
        assert!(validate_non_empty_bytes(data, "model.onnx").is_ok());

        let single_byte = b"x";
        assert!(validate_non_empty_bytes(single_byte, "config.json").is_ok());
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
        assert_eq!(quote_table_reference(&tbl), "\"users\"");

        let tbl = TableReference::bare("my_table");
        assert_eq!(quote_table_reference(&tbl), "\"my_table\"");
    }

    #[test]
    fn test_quote_table_reference_partial() {
        let tbl = TableReference::partial("public", "users");
        assert_eq!(quote_table_reference(&tbl), "\"public\".\"users\"");

        let tbl = TableReference::partial("my_schema", "my_table");
        assert_eq!(quote_table_reference(&tbl), "\"my_schema\".\"my_table\"");
    }

    #[test]
    fn test_quote_table_reference_full() {
        let tbl = TableReference::full("catalog", "public", "users");
        assert_eq!(
            quote_table_reference(&tbl),
            "\"catalog\".\"public\".\"users\""
        );

        let tbl = TableReference::full("my_cat", "my_schema", "my_table");
        assert_eq!(
            quote_table_reference(&tbl),
            "\"my_cat\".\"my_schema\".\"my_table\""
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
}
