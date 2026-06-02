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
use crate::commands::component::{self, ComponentSection};
use crate::error::{ConfigIoSnafu, CreateDirectorySnafu, InvalidArgumentSnafu};
use crate::manifest;
use ansi_colors::Color;
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
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
#[command(
    about = "Add or configure dataset entries in spicepod.yaml",
    long_about = r#"Add or configure dataset entries in `spicepod.yaml`.

USAGE
  spice dataset add <name>       [body flags]   # add a new dataset; fails if it exists
  spice dataset configure <name> [body flags]   # add or update a dataset in place
  spice dataset configure                       # interactive prompts (no flags)

BODY FLAGS (same as `spice <component>` editors)
  --from <SOURCE>           Provider/URI (e.g. `s3://bucket/key`, `databricks:catalog.schema.table`)
  --description <TEXT>      Human-readable description
  --param KEY=VALUE         Add a `params:` entry (string by default; prefix `yaml:` for typed)
  --set PATH=VALUE          Set any schema field by dotted path; VALUE is parsed as YAML
  --depends-on NAME         Append to `dependsOn:`
  --enable | --disable      Set `enabled: true` / `enabled: false`
  --file <PATH> | --stdin   Read the dataset body from a YAML/JSON file or stdin
  --manifest <PATH>         Edit a non-default Spicepod file

EXAMPLES
  # Add a Parquet dataset on S3
  spice dataset add taxi_trips --from s3://my-bucket/trips.parquet \
      --param file_format=parquet

  # Enable acceleration on an existing dataset
  spice dataset configure taxi_trips --set acceleration.enabled=true

  # Run interactive prompts (great for first-time setup)
  spice dataset configure

Docs: https://spiceai.org/docs"#
)]
pub struct DatasetArgs {
    #[command(subcommand)]
    pub command: DatasetCommands,
}

/// Dataset subcommands.
#[derive(Subcommand, Debug)]
pub enum DatasetCommands {
    /// Add a dataset to spicepod.yaml
    Add(component::ComponentAddArgs),

    /// Create or update a dataset in spicepod.yaml, or run interactively with no arguments
    Configure(component::ComponentConfigureArgs),
}

/// Execute the dataset command.
///
/// # Errors
///
/// Returns an error if the dataset configuration fails.
pub fn execute(args: &DatasetArgs) -> Result<()> {
    match &args.command {
        DatasetCommands::Add(add_args) => {
            component::add_component(ComponentSection::Dataset, add_args)
        }
        DatasetCommands::Configure(configure_args) => {
            if configure_args.has_manifest_edits() {
                component::configure_component(ComponentSection::Dataset, configure_args)
            } else {
                configure_dataset()
            }
        }
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
    let Some(spicepod_path) = manifest::existing_spicepod_path(Path::new(".")) else {
        return Err(crate::error::Error::InvalidArgument {
            message: "No spicepod.yaml or spicepod.yml found. Run 'spice init <app>' first."
                .to_string(),
        });
    };

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
        let warning = format!(
            "Dataset names containing hyphens (-) are deprecated and will no longer be supported starting with version 2.0.\nDataset names with hyphens should be quoted in queries:\ni.e. SELECT * FROM \"{dataset_name}\""
        );
        println!("{}", Color::Yellow.paint(warning));
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
    let dataset_yaml = yaml::to_string(&dataset).map_err(|e| crate::error::Error::ConfigParse {
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
    manifest::write_secure_file(&file_path, dataset_yaml.as_bytes())?;

    // Update the Spicepod manifest to reference the dataset.
    update_spicepod_with_dataset(&spicepod_path, &dir_path)?;

    println!(
        "{}",
        Color::Green.paint(format!("Saved {}", file_path.display()))
    );

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

/// Update the Spicepod manifest to include a reference to the dataset.
fn update_spicepod_with_dataset(spicepod_path: &Path, dataset_dir: &Path) -> Result<()> {
    let mut spicepod = manifest::read_spicepod_value(spicepod_path)?;
    let dataset_ref_path = manifest::path_to_spicepod_ref(dataset_dir);

    if manifest::ensure_component_reference(&mut spicepod, "datasets", &dataset_ref_path)? {
        manifest::write_spicepod_value(spicepod_path, &spicepod)?;
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
    fn test_update_spicepod_with_dataset_uses_existing_yml() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let spicepod_path = temp_dir.path().join(manifest::SPICEPOD_YML);
        std::fs::write(
            &spicepod_path,
            "version: v2\nkind: Spicepod\nname: test_app\nmodels: []\nembeddings: []\nworkers: []\n",
        )
        .expect("spicepod.yml should be written");

        update_spicepod_with_dataset(&spicepod_path, Path::new("datasets/test"))
            .expect("spicepod.yml should be updated");

        let updated =
            std::fs::read_to_string(&spicepod_path).expect("spicepod.yml should be readable");
        assert!(updated.contains("models:"), "models should be preserved");
        assert!(
            updated.contains("embeddings:"),
            "embeddings should be preserved"
        );
        assert!(updated.contains("workers:"), "workers should be preserved");
        assert!(
            updated.contains("ref: datasets/test"),
            "dataset reference should be added"
        );
    }
}
