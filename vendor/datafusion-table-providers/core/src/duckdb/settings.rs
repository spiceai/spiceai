//! # DuckDB Settings System
//!
//! This module provides a flexible, trait-based system for managing DuckDB database settings.
//! Library users can easily extend the system with their own custom settings without modifying
//! this crate.
//!
//! ## Basic Usage
//!
//! ```rust,no_run
//! use datafusion_table_providers::duckdb::{DuckDBSettingsRegistry, MemoryLimitSetting};
//! use std::collections::HashMap;
//!
//! // Create a registry with default settings
//! let mut registry = DuckDBSettingsRegistry::new();
//!
//! // Or create an empty registry and add settings manually
//! let mut empty_registry = DuckDBSettingsRegistry::empty();
//! empty_registry.register(Box::new(MemoryLimitSetting));
//!
//! // Use with options
//! let mut options = HashMap::new();
//! options.insert("memory_limit".to_string(), "2GB".to_string());
//!
//! // The registry will automatically apply settings when used with DuckDBTableProviderFactory
//! ```
//!
//! ## Creating Custom Settings
//!
//! Library users can create their own settings by implementing the `DuckDBSetting` trait:
//!
//! ```rust,no_run
//! use datafusion_table_providers::duckdb::{DuckDBSetting, DuckDBSettingScope};
//! use datafusion_table_providers::duckdb::Error;
//! use std::collections::HashMap;
//!
//! #[derive(Debug)]
//! struct CustomTimeoutSetting;
//!
//! impl DuckDBSetting for CustomTimeoutSetting {
//!     fn as_any(&self) -> &dyn std::any::Any {
//!         self
//!     }
//!
//!     fn setting_name(&self) -> &'static str {
//!         "query_timeout"
//!     }
//!
//!     fn get_value(&self, options: &HashMap<String, String>) -> Option<String> {
//!         options.get("query_timeout").cloned()
//!     }
//!
//!     fn scope(&self) -> DuckDBSettingScope {
//!         DuckDBSettingScope::Global
//!     }
//!
//!     fn validate(&self, value: &str) -> Result<(), Error> {
//!         // Validate that the timeout is a valid number
//!         value.parse::<u64>().map_err(|_| Error::DbConnectionError {
//!             source: format!("Invalid timeout value: {}", value).into(),
//!         })?;
//!         Ok(())
//!     }
//!
//!     fn format_sql_value(&self, value: &str) -> String {
//!         // No quotes needed for numeric values
//!         value.to_string()
//!     }
//! }
//! ```
//!
//! ## Unconditional Settings
//!
//! You can create settings that always apply regardless of the options:
//!
//! ```rust,no_run
//! use datafusion_table_providers::duckdb::{DuckDBSetting, DuckDBSettingScope};
//! use std::collections::HashMap;
//!
//! #[derive(Debug)]
//! struct AlwaysEnabledFeature;
//!
//! impl DuckDBSetting for AlwaysEnabledFeature {
//!     fn as_any(&self) -> &dyn std::any::Any { self }
//!     
//!     fn setting_name(&self) -> &'static str {
//!         "enable_feature"
//!     }
//!
//!     fn scope(&self) -> DuckDBSettingScope {
//!         DuckDBSettingScope::Global
//!     }
//!
//!     fn get_value(&self, _options: &HashMap<String, String>) -> Option<String> {
//!         // Always return a value, regardless of options
//!         Some("true".to_string())
//!     }
//! }
//! ```

use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDBSyncParameter;
use crate::{duckdb::Error, sql::db_connection_pool::dbconnection::SyncDbConnection};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use duckdb::DuckdbConnectionManager;
use r2d2::PooledConnection;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Indicates the scope of a DuckDB setting
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuckDBSettingScope {
    /// Global settings are applied once to the entire DuckDB instance
    Global,
    /// Local settings are applied to each connection
    Local,
}

/// Trait for DuckDB settings that can be applied to a connection
pub trait DuckDBSetting: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn std::any::Any;

    /// The name of the DuckDB setting, i.e. `SET <setting_name> = <value>`
    fn setting_name(&self) -> &'static str;

    /// The scope of the DuckDB setting, see [DuckDB documentation](https://duckdb.org/docs/stable/sql/statements/set.html#scopes)
    fn scope(&self) -> DuckDBSettingScope;

    /// Get the value for this setting from the options, if available
    fn get_value(&self, options: &HashMap<String, String>) -> Option<String>;

    /// Validate the value before applying it
    fn validate(&self, _value: &str) -> Result<(), Error> {
        Ok(())
    }

    /// Format the value for use in SQL (e.g., add quotes for strings)
    fn format_sql_value(&self, value: &str) -> String {
        value.to_string()
    }
}

/// Registry for managing DuckDB settings
#[derive(Debug)]
pub struct DuckDBSettingsRegistry {
    settings: Vec<Box<dyn DuckDBSetting>>,
}

impl Default for DuckDBSettingsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DuckDBSettingsRegistry {
    /// Create a new registry with default settings
    pub fn new() -> Self {
        let mut registry = Self {
            settings: Vec::new(),
        };

        // Register default settings
        registry.register(Box::new(MemoryLimitSetting));
        registry.register(Box::new(TempDirectorySetting));
        registry.register(Box::new(PreserveInsertionOrderSetting));

        registry
    }

    /// Create an empty registry without default settings
    pub fn empty() -> Self {
        Self {
            settings: Vec::new(),
        }
    }

    pub fn with_setting(mut self, setting: Box<dyn DuckDBSetting>) -> Self {
        self.register(setting);
        self
    }

    /// Register a new setting
    pub fn register(&mut self, setting: Box<dyn DuckDBSetting>) {
        self.settings.push(setting);
    }

    /// Apply all applicable settings to the connection pool
    pub fn apply_settings(
        &self,
        conn: &dyn SyncDbConnection<
            PooledConnection<DuckdbConnectionManager>,
            Box<dyn DuckDBSyncParameter>,
        >,
        options: &HashMap<String, String>,
        scope: DuckDBSettingScope,
    ) -> DataFusionResult<()> {
        for setting in &self.settings {
            if setting.scope() != scope {
                tracing::debug!(
                    "Skipping setting {} because it's not a {scope:?}",
                    setting.setting_name(),
                );
                continue;
            }

            if let Some(value) = setting.get_value(options) {
                setting
                    .validate(&value)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let set_statement = self.set_statement(setting.as_ref(), &value);
                tracing::debug!("DuckDB: {}", set_statement);
                conn.execute(&set_statement, &[])?;
            }
        }

        Ok(())
    }

    /// Returns a list of DuckDB SET statements for the given settings and options
    pub fn get_setting_statements(
        &self,
        options: &HashMap<String, String>,
        scope: DuckDBSettingScope,
    ) -> Vec<Arc<str>> {
        self.settings
            .iter()
            .filter(|s| s.scope() == scope)
            .filter_map(|s| {
                s.get_value(options)
                    .map(|value| self.set_statement(s.as_ref(), &value))
            })
            .map(|s| s.into())
            .collect()
    }

    fn set_statement(&self, setting: &dyn DuckDBSetting, value: &str) -> String {
        format!(
            "SET {} = {}",
            setting.setting_name(),
            setting.format_sql_value(value)
        )
    }
}

/// Memory limit setting implementation
#[derive(Debug)]
pub struct MemoryLimitSetting;

impl DuckDBSetting for MemoryLimitSetting {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn setting_name(&self) -> &'static str {
        "memory_limit"
    }

    fn scope(&self) -> DuckDBSettingScope {
        DuckDBSettingScope::Global
    }

    fn get_value(&self, options: &HashMap<String, String>) -> Option<String> {
        options.get("memory_limit").cloned()
    }

    fn validate(&self, value: &str) -> Result<(), Error> {
        byte_unit::Byte::parse_str(value, true).context(
            crate::duckdb::UnableToParseMemoryLimitSnafu {
                value: value.to_string(),
            },
        )?;
        Ok(())
    }

    fn format_sql_value(&self, value: &str) -> String {
        format!("'{}'", value)
    }
}

/// Temp directory setting implementation
#[derive(Debug)]
pub struct TempDirectorySetting;

impl DuckDBSetting for TempDirectorySetting {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn setting_name(&self) -> &'static str {
        "temp_directory"
    }

    fn scope(&self) -> DuckDBSettingScope {
        DuckDBSettingScope::Global
    }

    fn get_value(&self, options: &HashMap<String, String>) -> Option<String> {
        options.get("temp_directory").cloned()
    }

    fn format_sql_value(&self, value: &str) -> String {
        format!("'{}'", value)
    }
}

/// Preserve insertion order setting implementation
#[derive(Debug)]
pub struct PreserveInsertionOrderSetting;

impl DuckDBSetting for PreserveInsertionOrderSetting {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn setting_name(&self) -> &'static str {
        "preserve_insertion_order"
    }

    fn scope(&self) -> DuckDBSettingScope {
        DuckDBSettingScope::Global
    }

    fn get_value(&self, options: &HashMap<String, String>) -> Option<String> {
        options.get("preserve_insertion_order").cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Test implementation for unconditional settings
    #[derive(Debug)]
    struct TestUnconditionalSetting {
        name: &'static str,
        value: String,
    }

    impl TestUnconditionalSetting {
        fn new(name: &'static str, value: String) -> Self {
            Self { name, value }
        }
    }

    impl DuckDBSetting for TestUnconditionalSetting {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn setting_name(&self) -> &'static str {
            self.name
        }

        fn get_value(&self, _options: &HashMap<String, String>) -> Option<String> {
            // Always return the value regardless of options (unconditional)
            Some(self.value.clone())
        }

        fn scope(&self) -> DuckDBSettingScope {
            DuckDBSettingScope::Global
        }

        fn format_sql_value(&self, value: &str) -> String {
            format!("'{}'", value)
        }
    }

    /// Test implementation for custom validation
    #[derive(Debug)]
    struct TestValidatingSetting;

    impl DuckDBSetting for TestValidatingSetting {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn setting_name(&self) -> &'static str {
            "test_setting"
        }

        fn scope(&self) -> DuckDBSettingScope {
            DuckDBSettingScope::Global
        }

        fn get_value(&self, options: &HashMap<String, String>) -> Option<String> {
            options.get("test_setting").cloned()
        }

        fn validate(&self, value: &str) -> Result<(), Error> {
            if value == "invalid" {
                return Err(Error::DbConnectionError {
                    source: "Test validation error".into(),
                });
            }
            Ok(())
        }
    }

    #[test]
    fn test_memory_limit_setting() {
        let setting = MemoryLimitSetting;

        // Test setting name
        assert_eq!(setting.setting_name(), "memory_limit");

        // Test get_value with present key
        let mut options = HashMap::new();
        options.insert("memory_limit".to_string(), "1GB".to_string());
        assert_eq!(setting.get_value(&options), Some("1GB".to_string()));

        // Test get_value with missing key
        let empty_options = HashMap::new();
        assert_eq!(setting.get_value(&empty_options), None);

        // Test format_sql_value
        assert_eq!(setting.format_sql_value("1GB"), "'1GB'");

        // Test validate with valid value
        assert!(setting.validate("1GB").is_ok());
        assert!(setting.validate("512MiB").is_ok());

        // Test validate with invalid value
        assert!(setting.validate("invalid").is_err());
    }

    #[test]
    fn test_temp_directory_setting() {
        let setting = TempDirectorySetting;

        // Test setting name
        assert_eq!(setting.setting_name(), "temp_directory");

        // Test get_value with present key
        let mut options = HashMap::new();
        options.insert("temp_directory".to_string(), "/tmp/test".to_string());
        assert_eq!(setting.get_value(&options), Some("/tmp/test".to_string()));

        // Test get_value with missing key
        let empty_options = HashMap::new();
        assert_eq!(setting.get_value(&empty_options), None);

        // Test format_sql_value
        assert_eq!(setting.format_sql_value("/tmp/test"), "'/tmp/test'");

        // Test validate (should always pass)
        assert!(setting.validate("/tmp/test").is_ok());
        assert!(setting.validate("any_value").is_ok());
    }

    #[test]
    fn test_preserve_insertion_order_setting() {
        let setting = PreserveInsertionOrderSetting;

        // Test setting name
        assert_eq!(setting.setting_name(), "preserve_insertion_order");

        // Test get_value with present key
        let mut options = HashMap::new();
        options.insert("preserve_insertion_order".to_string(), "true".to_string());
        assert_eq!(setting.get_value(&options), Some("true".to_string()));

        // Test get_value with missing key
        let empty_options = HashMap::new();
        assert_eq!(setting.get_value(&empty_options), None);

        // Test format_sql_value (default implementation - no quotes)
        assert_eq!(setting.format_sql_value("true"), "true");
        assert_eq!(setting.format_sql_value("false"), "false");

        // Test validate (should always pass with default implementation)
        assert!(setting.validate("true").is_ok());
        assert!(setting.validate("false").is_ok());
    }

    #[test]
    fn test_settings_registry_new() {
        let registry = DuckDBSettingsRegistry::new();

        // Registry should have 3 default settings
        assert_eq!(registry.settings.len(), 3);

        // Check that default settings are present by testing their names
        let setting_names: Vec<&'static str> =
            registry.settings.iter().map(|s| s.setting_name()).collect();

        assert!(setting_names.contains(&"memory_limit"));
        assert!(setting_names.contains(&"temp_directory"));
        assert!(setting_names.contains(&"preserve_insertion_order"));
    }

    #[test]
    fn test_settings_registry_empty() {
        let registry = DuckDBSettingsRegistry::empty();

        // Empty registry should have no settings
        assert_eq!(registry.settings.len(), 0);
    }

    #[test]
    fn test_settings_registry_default() {
        let registry = DuckDBSettingsRegistry::default();

        // Default should be the same as new()
        assert_eq!(registry.settings.len(), 3);
    }

    #[test]
    fn test_settings_registry_register() {
        let mut registry = DuckDBSettingsRegistry::empty();

        // Start with empty registry
        assert_eq!(registry.settings.len(), 0);

        // Register a test setting
        registry.register(Box::new(TestUnconditionalSetting::new(
            "test_setting",
            "test_value".to_string(),
        )));

        // Should now have 1 setting
        assert_eq!(registry.settings.len(), 1);
        assert_eq!(registry.settings[0].setting_name(), "test_setting");

        // Register another setting
        registry.register(Box::new(MemoryLimitSetting));

        // Should now have 2 settings
        assert_eq!(registry.settings.len(), 2);
    }

    #[test]
    fn test_unconditional_setting() {
        let setting =
            TestUnconditionalSetting::new("test_unconditional", "always_this_value".to_string());

        // Test setting name
        assert_eq!(setting.setting_name(), "test_unconditional");

        // Test that it always returns a value regardless of options
        let empty_options = HashMap::new();
        assert_eq!(
            setting.get_value(&empty_options),
            Some("always_this_value".to_string())
        );

        let mut options_with_other_keys = HashMap::new();
        options_with_other_keys.insert("some_other_key".to_string(), "some_value".to_string());
        assert_eq!(
            setting.get_value(&options_with_other_keys),
            Some("always_this_value".to_string())
        );

        // Test format_sql_value
        assert_eq!(setting.format_sql_value("test"), "'test'");
    }

    #[test]
    fn test_custom_validation() {
        let setting = TestValidatingSetting;

        // Test setting name
        assert_eq!(setting.setting_name(), "test_setting");

        // Test get_value
        let mut options = HashMap::new();
        options.insert("test_setting".to_string(), "valid_value".to_string());
        assert_eq!(setting.get_value(&options), Some("valid_value".to_string()));

        // Test validation with valid value
        assert!(setting.validate("valid_value").is_ok());

        // Test validation with invalid value
        assert!(setting.validate("invalid").is_err());
    }

    #[test]
    fn test_trait_default_implementations() {
        let setting = TestUnconditionalSetting::new("test", "value".to_string());

        // Test default validate implementation (should always return Ok)
        assert!(setting.validate("any_value").is_ok());

        // Test default format_sql_value implementation (should return value as-is)
        let setting_with_defaults = TestValidatingSetting;
        assert_eq!(setting_with_defaults.format_sql_value("test"), "test");
    }

    #[test]
    fn test_as_any_functionality() {
        let memory_setting = MemoryLimitSetting;
        let boxed_setting: Box<dyn DuckDBSetting> = Box::new(memory_setting);

        // Test that we can downcast using as_any
        let any_ref = boxed_setting.as_any();
        assert!(any_ref.is::<MemoryLimitSetting>());

        // Test downcasting
        let downcasted = any_ref.downcast_ref::<MemoryLimitSetting>();
        assert!(downcasted.is_some());

        // Test that incorrect downcast fails
        assert!(any_ref.downcast_ref::<TempDirectorySetting>().is_none());
    }

    #[test]
    fn test_memory_limit_validation_edge_cases() {
        let setting = MemoryLimitSetting;

        // Test various valid memory limit formats
        assert!(setting.validate("1KB").is_ok());
        assert!(setting.validate("1MB").is_ok());
        assert!(setting.validate("1GB").is_ok());
        assert!(setting.validate("1TB").is_ok());
        assert!(setting.validate("1KiB").is_ok());
        assert!(setting.validate("1MiB").is_ok());
        assert!(setting.validate("1GiB").is_ok());
        assert!(setting.validate("1TiB").is_ok());
        assert!(setting.validate("512.5MB").is_ok());
        assert!(setting.validate("123").is_ok()); // Plain number (defaults to bytes)

        // Test invalid formats
        assert!(setting.validate("").is_err());
        assert!(setting.validate("not_a_number").is_err());
        assert!(setting.validate("123XB").is_err()); // Invalid unit
        assert!(setting.validate("abc123MB").is_err()); // Invalid number format
    }

    #[test]
    fn test_settings_ordering_in_registry() {
        let mut registry = DuckDBSettingsRegistry::empty();

        // Add settings in a specific order
        registry.register(Box::new(TestUnconditionalSetting::new(
            "first",
            "1".to_string(),
        )));
        registry.register(Box::new(TestUnconditionalSetting::new(
            "second",
            "2".to_string(),
        )));
        registry.register(Box::new(TestUnconditionalSetting::new(
            "third",
            "3".to_string(),
        )));

        // Verify they're stored in the order they were added
        assert_eq!(registry.settings[0].setting_name(), "first");
        assert_eq!(registry.settings[1].setting_name(), "second");
        assert_eq!(registry.settings[2].setting_name(), "third");
    }

    #[test]
    fn test_multiple_settings_with_same_option_key() {
        // Test case where multiple settings might look for the same option key
        // (though this would be unusual in practice)

        #[derive(Debug)]
        struct TestSetting1;

        #[derive(Debug)]
        struct TestSetting2;

        impl DuckDBSetting for TestSetting1 {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn setting_name(&self) -> &'static str {
                "setting1"
            }
            fn scope(&self) -> DuckDBSettingScope {
                DuckDBSettingScope::Global
            }
            fn get_value(&self, options: &HashMap<String, String>) -> Option<String> {
                options.get("shared_key").cloned()
            }
        }

        impl DuckDBSetting for TestSetting2 {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn setting_name(&self) -> &'static str {
                "setting2"
            }
            fn scope(&self) -> DuckDBSettingScope {
                DuckDBSettingScope::Global
            }
            fn get_value(&self, options: &HashMap<String, String>) -> Option<String> {
                options.get("shared_key").cloned()
            }
        }

        let setting1 = TestSetting1;
        let setting2 = TestSetting2;

        let mut options = HashMap::new();
        options.insert("shared_key".to_string(), "shared_value".to_string());

        // Both settings should get the same value
        assert_eq!(
            setting1.get_value(&options),
            Some("shared_value".to_string())
        );
        assert_eq!(
            setting2.get_value(&options),
            Some("shared_value".to_string())
        );
    }
}
