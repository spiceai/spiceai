use anyhow::{anyhow, Context, Result};
use path_clean::PathClean;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// YAML representation of an eval specification file.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EvalSpecification {
    #[serde(flatten)]
    pub entries: HashMap<String, EvalEntry>,
}

/// Valid Eval from a specification YAML file.
pub struct Eval {
    pub id: String,
    pub description: Option<String>,
    pub metrics: Option<Vec<String>>,

    pub class: String,
    pub args: HashMap<String, Value>,
}
pub type Class = String;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum EvalEntry {
    Metadata(EvalMetadata),
    Definition(EvalDefinition),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EvalDefinition {
    pub class: String,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub args: HashMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EvalMetadata {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    disclaimer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<Vec<String>>,
}

impl EvalSpecification {
    pub fn validate_from_file(file: &Path, data_dir: &Path) -> Result<Eval> {
        let file_contents = std::fs::read_to_string(file)
            .context(anyhow!("Failed to read YAML file '{}'", file.display()))?;

        let spec: EvalSpecification = serde_yaml::from_str(&file_contents)
            .context(anyhow!("Failed to parse YAML file '{}'", file.display()))?;

        spec.validate(data_dir)
    }

    /// Validate the specification from a YAML file, and converts it into an internal representation.
    pub fn validate(&self, data_dir: &Path) -> Result<Eval> {
        let Some((
            name,
            EvalMetadata {
                id,
                description,
                metrics,
                ..
            },
        )) = self.get_metadata()
        else {
            return Err(anyhow!("Metadata entry not found"));
        };

        let defs = self.get_definition_entries();
        let Some((_, def)) = defs.iter().find(|(n, _)| *n == id.as_str()) else {
            return Err(anyhow!("For {name}, expected '{id}' entry"));
        };

        self.resolve_file_paths(def, data_dir)?;

        Ok(Eval {
            id: id.clone(),
            description: description.clone(),
            metrics: metrics.clone(),
            class: def.class.clone(),
            args: def.args.clone(),
        })
    }

    pub fn resolve_file_paths(&self, def: &EvalDefinition, data_dir: &Path) -> Result<()> {
        for (key, value) in &def.args {
            let Value::String(s) = value else {
                continue;
            };
            if is_potential_file_key(key.as_str()) && !data_dir.join(s).exists() {
                return Err(anyhow!(
                    "Value in `{key}: {}` should be a file, but does not exist.",
                    data_dir.join(s).display()
                ));
            }
        }
        Ok(())
    }

    pub fn get_metadata(&self) -> Option<(&str, &EvalMetadata)> {
        self.entries.iter().find_map(|(name, entry)| match entry {
            EvalEntry::Metadata(metadata) => Some((name.as_str(), metadata)),
            _ => None,
        })
    }

    /// Get all definition entries
    pub fn get_definition_entries(&self) -> Vec<(&str, &EvalDefinition)> {
        self.entries
            .iter()
            .filter_map(|(name, entry)| match entry {
                EvalEntry::Definition(definition) => Some((name.as_str(), definition)),
                _ => None,
            })
            .collect()
    }
}

fn is_potential_file_key(value: &str) -> bool {
    match value {
        "samples_jsonl" => true,
        _ => false,
    }
}
