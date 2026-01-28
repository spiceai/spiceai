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

//! Dataset command for configuring individual datasets.

use crate::Result;
use crate::error::{ConfigIoSnafu, CreateDirectorySnafu, InvalidArgumentSnafu};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use spicepod::component::{ComponentOrReference, ComponentReference};
use spicepod::spec::SpicepodDefinition;
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

/// Known data source prefixes that require special handling.
mod data_source {
    pub const DREMIO: &str = "dremio";
    pub const DATABRICKS: &str = "databricks";
    pub const S3: &str = "s3";
    pub const FTP: &str = "ftp";
    pub const SFTP: &str = "sftp";
}

/// Arguments for the dataset command.
#[derive(Args, Debug)]
pub struct DatasetArgs {
    #[command(subcommand)]
    pub command: DatasetCommands,
}

/// Dataset subcommands.
#[derive(Subcommand, Debug)]
pub enum DatasetCommands {
    /// Configure a new dataset interactively
    Configure,
}

/// Execute the dataset command.
///
/// # Errors
///
/// Returns an error if the dataset configuration fails.
pub fn execute(args: &DatasetArgs) -> Result<()> {
    match args.command {
        DatasetCommands::Configure => configure_dataset(),
    }
}

/// Dataset specification for YAML output.
#[derive(Debug, Serialize, Deserialize)]
struct DatasetSpec {
    from: String,
    name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    description: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    params: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acceleration: Option<AccelerationSpec>,
}

/// Acceleration specification for YAML output.
#[derive(Debug, Serialize, Deserialize)]
struct AccelerationSpec {
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_check_interval: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_mode: Option<String>,
}

/// Interactive dataset configuration.
fn configure_dataset() -> Result<()> {
    // Check that spicepod.yaml exists
    let spicepod_path = Path::new("spicepod.yaml");
    if !spicepod_path.exists() {
        return Err(crate::error::Error::InvalidArgument {
            message: "No spicepod.yaml found. Run 'spice init <app>' first.".to_string(),
        });
    }

    let stdin = io::stdin();
    let mut reader = stdin.lock();

    // Get dataset name with default from current directory
    let cwd = std::env::current_dir().context(ConfigIoSnafu {
        operation: "read",
        path: PathBuf::from("."),
    })?;
    let default_name = cwd
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("dataset")
        .to_string();

    let dataset_name = prompt_with_default(&mut reader, "dataset name", &default_name)?;

    // Validate dataset name
    if !is_valid_dataset_name(&dataset_name) {
        return InvalidArgumentSnafu {
            message: "Dataset name can only contain letters, numbers, underscores, and hyphens",
        }
        .fail();
    }

    // Warn about hyphens in dataset name
    if dataset_name.contains('-') {
        println!(
            "\x1b[33mDataset names containing hyphens (-) are deprecated and will no longer be supported starting with version 2.0.\nDataset names with hyphens should be quoted in queries:\ni.e. SELECT * FROM \"{dataset_name}\"\x1b[0m"
        );
    }

    // Get description
    let description = prompt(&mut reader, "description")?;

    // Get 'from' source
    let from = prompt(&mut reader, "from")?;

    // Collect additional params based on data source type
    let mut params = HashMap::new();
    let data_source_prefix = from.split(':').next().unwrap_or("");

    // Handle endpoint for dremio/databricks
    if data_source_prefix == data_source::DREMIO || data_source_prefix == data_source::DATABRICKS {
        let endpoint = prompt(&mut reader, "endpoint")?;
        if !endpoint.is_empty() {
            params.insert(format!("{data_source_prefix}_endpoint"), endpoint);
        }
    }

    // Handle file_format for s3/ftp/sftp
    if data_source_prefix == data_source::S3
        || data_source_prefix == data_source::FTP
        || data_source_prefix == data_source::SFTP
    {
        let from_path = std::path::Path::new(&from);
        let has_known_ext = from_path.extension().is_some_and(|ext| {
            ext.eq_ignore_ascii_case("csv") || ext.eq_ignore_ascii_case("parquet")
        });
        if !has_known_ext {
            let file_format =
                prompt_with_default(&mut reader, "file_format (parquet/csv)", "parquet")?;
            if file_format != "parquet" && file_format != "csv" {
                return InvalidArgumentSnafu {
                    message: "file_format must be either 'parquet' or 'csv'",
                }
                .fail();
            }
            params.insert("file_format".to_string(), file_format);
        }
    }

    // Ask about local acceleration
    let accelerate_str = prompt_with_default(&mut reader, "locally accelerate (y/n)?", "y")?;
    let accelerate = accelerate_str.is_empty() || accelerate_str.to_lowercase() == "y";

    // Build dataset spec
    let dataset = DatasetSpec {
        from,
        name: dataset_name.clone(),
        description,
        params,
        acceleration: if accelerate {
            Some(AccelerationSpec {
                enabled: true,
                refresh_check_interval: Some("10s".to_string()),
                refresh_mode: Some("full".to_string()),
            })
        } else {
            None
        },
    };

    // Serialize to YAML
    let dataset_yaml =
        serde_yaml::to_string(&dataset).map_err(|e| crate::error::Error::ConfigParse {
            message: format!("Failed to serialize dataset to YAML: {e}"),
        })?;

    // Create dataset directory with secure permissions (0700)
    let dir_path = PathBuf::from("datasets").join(&dataset_name);
    fs::create_dir_all(&dir_path).context(CreateDirectorySnafu {
        path: dir_path.clone(),
    })?;

    // Set directory permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o700);
        fs::set_permissions(&dir_path, permissions).context(ConfigIoSnafu {
            operation: "set permissions on",
            path: dir_path.clone(),
        })?;
    }

    // Write dataset.yaml
    let file_path = dir_path.join("dataset.yaml");
    write_secure_file(&file_path, dataset_yaml.as_bytes())?;

    // Update spicepod.yaml to reference the dataset
    update_spicepod_with_dataset(&dir_path)?;

    println!("\x1b[32mSaved {}\x1b[0m", file_path.display());

    Ok(())
}

/// Prompt the user for input.
fn prompt<R: BufRead>(reader: &mut R, prompt_text: &str) -> Result<String> {
    print!("{prompt_text}: ");
    io::stdout().flush().context(ConfigIoSnafu {
        operation: "write",
        path: PathBuf::from("stdout"),
    })?;

    let mut input = String::new();
    reader.read_line(&mut input).context(ConfigIoSnafu {
        operation: "read",
        path: PathBuf::from("stdin"),
    })?;

    Ok(input.trim().to_string())
}

/// Prompt the user for input with a default value.
fn prompt_with_default<R: BufRead>(
    reader: &mut R,
    prompt_text: &str,
    default: &str,
) -> Result<String> {
    print!("{prompt_text}: ({default}) ");
    io::stdout().flush().context(ConfigIoSnafu {
        operation: "write",
        path: PathBuf::from("stdout"),
    })?;

    let mut input = String::new();
    reader.read_line(&mut input).context(ConfigIoSnafu {
        operation: "read",
        path: PathBuf::from("stdin"),
    })?;

    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

/// Validate dataset name - only letters, numbers, underscores, and hyphens.
fn is_valid_dataset_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Write a file with secure permissions (0600 on Unix).
fn write_secure_file(path: &Path, contents: &[u8]) -> Result<()> {
    fs::write(path, contents).context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, permissions).context(ConfigIoSnafu {
            operation: "set permissions on",
            path: path.to_path_buf(),
        })?;
    }

    Ok(())
}

/// Update spicepod.yaml to include a reference to the dataset.
fn update_spicepod_with_dataset(dataset_dir: &Path) -> Result<()> {
    let spicepod_path = Path::new("spicepod.yaml");

    // Read existing spicepod.yaml
    let content = fs::read_to_string(spicepod_path).context(ConfigIoSnafu {
        operation: "read",
        path: spicepod_path.to_path_buf(),
    })?;

    let mut spicepod: SpicepodDefinition =
        serde_yaml::from_str(&content).map_err(|e| crate::error::Error::ConfigParse {
            message: format!("Failed to parse spicepod.yaml: {e}"),
        })?;

    // Check if dataset is already referenced
    let dataset_ref_path = dataset_dir.to_string_lossy().to_string();
    let already_referenced = spicepod.datasets.iter().any(|d| match d {
        ComponentOrReference::Reference(r) => r.r#ref == dataset_ref_path,
        ComponentOrReference::Component(_) => false,
    });

    if !already_referenced {
        // Add the dataset reference
        spicepod
            .datasets
            .push(ComponentOrReference::Reference(ComponentReference {
                r#ref: dataset_ref_path,
                depends_on: Vec::new(),
            }));

        // Write back to spicepod.yaml
        let updated_yaml =
            serde_yaml::to_string(&spicepod).map_err(|e| crate::error::Error::ConfigParse {
                message: format!("Failed to serialize spicepod.yaml: {e}"),
            })?;

        write_secure_file(spicepod_path, updated_yaml.as_bytes())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_dataset_name() {
        assert!(is_valid_dataset_name("my_dataset"));
        assert!(is_valid_dataset_name("my-dataset"));
        assert!(is_valid_dataset_name("MyDataset123"));
        assert!(is_valid_dataset_name("dataset_v2"));

        assert!(!is_valid_dataset_name(""));
        assert!(!is_valid_dataset_name("my dataset")); // spaces
        assert!(!is_valid_dataset_name("my.dataset")); // dots
        assert!(!is_valid_dataset_name("my/dataset")); // slashes
    }

    #[test]
    fn test_prompt_with_default() {
        let input = b"\n";
        let mut reader = &input[..];
        let result =
            prompt_with_default(&mut reader, "test", "default").expect("prompt should succeed");
        assert_eq!(result, "default");

        let input = b"custom\n";
        let mut reader = &input[..];
        let result =
            prompt_with_default(&mut reader, "test", "default").expect("prompt should succeed");
        assert_eq!(result, "custom");
    }

    #[test]
    fn test_component_reference() {
        let ref_dataset: ComponentOrReference<spicepod::component::dataset::Dataset> =
            ComponentOrReference::Reference(ComponentReference {
                r#ref: "datasets/test".to_string(),
                depends_on: Vec::new(),
            });
        match &ref_dataset {
            ComponentOrReference::Reference(r) => assert_eq!(r.r#ref, "datasets/test"),
            ComponentOrReference::Component(_) => panic!("expected reference"),
        }
    }
}
