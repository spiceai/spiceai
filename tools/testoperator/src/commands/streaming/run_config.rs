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

//! Run configuration for isolated test runs.
//!
//! This module provides functionality to generate unique table name prefixes
//! and rewrite spicepods for parallel test execution without conflicts.

use std::path::{Path, PathBuf};

use test_framework::anyhow::{Context, Result};

/// Configuration for an isolated test run.
///
/// Generates unique table name prefixes to allow multiple test runs
/// to execute in parallel without conflicting on table names.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Unique run identifier (e.g., "a1b2c3")
    run_id: String,
    /// Path to the temporary spicepod file (if created)
    temp_spicepod_path: Option<PathBuf>,
}

impl RunConfig {
    /// Create a new run configuration with a unique ID.
    #[must_use]
    pub fn new() -> Self {
        let run_id = Self::generate_short_id();
        println!("Generated run ID: {run_id}");
        Self {
            run_id,
            temp_spicepod_path: None,
        }
    }

    /// Create a run configuration with a specific ID (for reproducibility).
    #[must_use]
    #[expect(dead_code)]
    pub fn with_id(run_id: String) -> Self {
        Self {
            run_id,
            temp_spicepod_path: None,
        }
    }

    /// Get the run ID.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Get the table name with the run prefix.
    #[must_use]
    #[expect(dead_code)]
    pub fn table_name(&self, base_name: &str) -> String {
        format!("{}_{}", self.run_id, base_name)
    }

    /// Generate a short unique ID (6 hex characters).
    fn generate_short_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        // Combine timestamp with some randomness from the lower bits
        let seed = now.as_nanos();
        format!("{:06x}", (seed & 0xFFFFFF) as u32)
    }

    /// Prepare a spicepod for this run by rewriting table references.
    ///
    /// Creates a temporary copy of the spicepod with all table names prefixed
    /// with the run ID.
    ///
    /// # Arguments
    /// * `original_path` - Path to the original spicepod.yaml
    /// * `table_names` - List of base table names to prefix (e.g., ["lineitem", "orders"])
    pub fn prepare_spicepod(
        &mut self,
        original_path: &Path,
        table_names: &[&str],
    ) -> Result<PathBuf> {
        let content = std::fs::read_to_string(original_path)
            .with_context(|| format!("Failed to read spicepod from {}", original_path.display()))?;

        let modified = self.rewrite_table_references(&content, table_names);

        // Create temp file with run_id in the name
        let temp_dir = std::env::temp_dir();
        let original_name = original_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("spicepod");
        let temp_path = temp_dir.join(format!("{}_{}.yaml", original_name, self.run_id));

        std::fs::write(&temp_path, &modified).with_context(|| {
            format!(
                "Failed to write temporary spicepod to {}",
                temp_path.display()
            )
        })?;

        println!(
            "Created temporary spicepod: {} (rewrote {} table references)",
            temp_path.display(),
            table_names.len()
        );

        self.temp_spicepod_path = Some(temp_path.clone());
        Ok(temp_path)
    }

    /// Rewrite table references in a spicepod content.
    ///
    /// Replaces patterns like:
    /// - `dynamodb:lineitem` → `dynamodb:a1b2c3_lineitem`
    /// - `from: dynamodb:lineitem` → `from: dynamodb:a1b2c3_lineitem`
    fn rewrite_table_references(&self, content: &str, table_names: &[&str]) -> String {
        let mut result = content.to_string();

        for table_name in table_names {
            // Replace "dynamodb:tablename" with "dynamodb:runid_tablename"
            let old_pattern = format!("dynamodb:{table_name}");
            let new_pattern = format!("dynamodb:{}_{}", self.run_id, table_name);
            result = result.replace(&old_pattern, &new_pattern);

            // Also handle the accelerated table name pattern (name: tablename)
            // This is trickier because "name: lineitem" could match other things
            // So we only replace if it's in a datasets context
            // For now, we rely on the dynamodb: pattern being sufficient
        }

        result
    }

    /// Clean up temporary files created by this run.
    pub fn cleanup(&self) -> Result<()> {
        if let Some(ref temp_path) = self.temp_spicepod_path {
            if temp_path.exists() {
                std::fs::remove_file(temp_path).with_context(|| {
                    format!(
                        "Failed to remove temporary spicepod: {}",
                        temp_path.display()
                    )
                })?;
                println!("Removed temporary spicepod: {}", temp_path.display());
            }
        }
        Ok(())
    }

    /// Get the temporary spicepod path if one was created.
    #[must_use]
    #[expect(dead_code)]
    pub fn temp_spicepod_path(&self) -> Option<&Path> {
        self.temp_spicepod_path.as_deref()
    }
}

impl Default for RunConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RunConfig {
    fn drop(&mut self) {
        // Best effort cleanup on drop
        if let Err(e) = self.cleanup() {
            eprintln!("Warning: Failed to cleanup run config: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_prefixing() {
        let config = RunConfig::with_id("abc123".to_string());
        assert_eq!(config.table_name("lineitem"), "abc123_lineitem");
        assert_eq!(config.table_name("orders"), "abc123_orders");
    }

    #[test]
    fn test_rewrite_table_references() {
        let config = RunConfig::with_id("test01".to_string());
        let content = r#"
datasets:
  - from: dynamodb:lineitem
    name: lineitem
  - from: dynamodb:orders
    name: orders
"#;
        let result = config.rewrite_table_references(content, &["lineitem", "orders"]);
        assert!(result.contains("dynamodb:test01_lineitem"));
        assert!(result.contains("dynamodb:test01_orders"));
        assert!(!result.contains("dynamodb:lineitem"));
        assert!(!result.contains("dynamodb:orders"));
    }
}
