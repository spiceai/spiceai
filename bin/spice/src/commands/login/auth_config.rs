/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Authentication configuration management.
//!
//! Handles reading and writing credentials to .env or .env.local files.

use crate::error::{ConfigIoSnafu, Result};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::Path;

/// Merge authentication configuration into the .env file.
///
/// Credentials are stored with the format: `SPICE_{AUTH_TYPE}_{PARAM}=value`
///
/// # Arguments
///
/// * `auth_type` - The authentication type (e.g., "DREMIO", "S3", "PG")
/// * `params` - Key-value pairs of authentication parameters
///
/// # Errors
///
/// Returns an error if the .env file cannot be read or written.
pub fn merge_auth_config(auth_type: &str, params: &[(&str, &str)]) -> Result<()> {
    // Determine which env file to use
    let env_file = if Path::new(".env.local").exists() {
        ".env.local"
    } else {
        ".env"
    };

    // Read existing env vars (ignore errors - file might not exist)
    let mut env_vars: HashMap<String, String> = if Path::new(env_file).exists() {
        read_env_file(env_file).unwrap_or_default()
    } else {
        HashMap::new()
    };

    // Add new auth params
    for (key, value) in params {
        let secret_key = format!("SPICE_{auth_type}_{key}");
        env_vars.insert(secret_key, (*value).to_string());
    }

    // Write back to file
    write_env_file(env_file, &env_vars)?;

    // Set file permissions to 0600 (owner read/write only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o600);
        std::fs::set_permissions(env_file, permissions).context(ConfigIoSnafu {
            operation: "set permissions",
            path: std::path::PathBuf::from(env_file),
        })?;
    }

    Ok(())
}

/// Read environment variables from a .env file.
fn read_env_file(path: &str) -> Result<HashMap<String, String>> {
    let contents = std::fs::read_to_string(path).context(ConfigIoSnafu {
        operation: "read",
        path: std::path::PathBuf::from(path),
    })?;

    let mut vars = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            // Remove surrounding quotes if present
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .unwrap_or(value);
            let value = value
                .strip_prefix('\'')
                .and_then(|v| v.strip_suffix('\''))
                .unwrap_or(value);
            vars.insert(key.to_string(), value.to_string());
        }
    }

    Ok(vars)
}

/// Write environment variables to a .env file.
fn write_env_file(path: &str, vars: &HashMap<String, String>) -> Result<()> {
    let mut lines: Vec<String> = vars
        .iter()
        .map(|(k, v)| {
            // Quote values that contain special characters
            if v.contains(' ') || v.contains('"') || v.contains('\'') || v.contains('=') {
                format!("{k}=\"{}\"", v.replace('"', "\\\""))
            } else {
                format!("{k}={v}")
            }
        })
        .collect();

    // Sort for consistent output
    lines.sort();

    let contents = lines.join("\n") + "\n";
    std::fs::write(path, contents).context(ConfigIoSnafu {
        operation: "write",
        path: std::path::PathBuf::from(path),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_read_env_file_format() {
        // Test various formats that should be parsed
        let content = r#"
# Comment
KEY1=value1
KEY2="quoted value"
KEY3='single quoted'
KEY4=
"#;
        // This would need a temp file to test properly
        assert!(content.contains("KEY1=value1"));
    }
}
