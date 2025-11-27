use std::collections::HashMap;

/// Configuration settings for DuckDB write operations
#[derive(Debug, Clone)]
pub struct DuckDBWriteSettings {
    /// Whether to execute ANALYZE statements after data write operations
    /// to update table statistics for query optimization
    pub recompute_statistics_on_write: bool,
}

impl Default for DuckDBWriteSettings {
    fn default() -> Self {
        Self {
            recompute_statistics_on_write: true, // Enabled by default for better query performance
        }
    }
}

impl DuckDBWriteSettings {
    /// Create a new `DuckDBWriteSettings` with default values
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to recompute statistics on write
    #[must_use]
    pub fn with_recompute_statistics_on_write(mut self, enabled: bool) -> Self {
        self.recompute_statistics_on_write = enabled;
        self
    }

    /// Parse settings from  table creation parameters
    #[must_use]
    pub fn from_params(params: &HashMap<String, String>) -> Self {
        let mut settings = Self::default();

        if let Some(value) = params.get("recompute_statistics_on_write") {
            settings.recompute_statistics_on_write = match value.to_lowercase().as_str() {
                "true" | "enabled" => true,
                "false" | "disabled" => false,
                _ => {
                    tracing::warn!(
                "Invalid value for recompute statistics on write parameter: '{value}'. Expected 'enabled' or 'disabled'. Using default: {}",
                settings.recompute_statistics_on_write
                );
                    settings.recompute_statistics_on_write
                }
            };
        }

        settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_settings() {
        let settings = DuckDBWriteSettings::default();
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_new_settings() {
        let settings = DuckDBWriteSettings::new();
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_with_recompute_statistics_on_write() {
        let settings = DuckDBWriteSettings::new().with_recompute_statistics_on_write(false);
        assert!(!settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_valid_enabled() {
        let mut params = HashMap::new();
        params.insert(
            "recompute_statistics_on_write".to_string(),
            "enabled".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_valid_disabled() {
        let mut params = HashMap::new();
        params.insert(
            "recompute_statistics_on_write".to_string(),
            "disabled".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        assert!(!settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_invalid_value() {
        let mut params = HashMap::new();
        params.insert(
            "recompute_statistics_on_write".to_string(),
            "invalid".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        // Should fall back to default (true) and log a warning
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_missing_param() {
        let params = HashMap::new();

        let settings = DuckDBWriteSettings::from_params(&params);
        // Should use default value
        assert!(settings.recompute_statistics_on_write);
    }
}
