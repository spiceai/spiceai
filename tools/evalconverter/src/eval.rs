use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use spicepod::component::{dataset::Dataset as DatasetComponent, eval::Eval as EvalComponent};
use std::collections::HashMap;
use std::path::Path;

/// YAML representation of an eval specification file.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EvalSpecification {
    #[serde(flatten)]
    pub entries: HashMap<String, EvalEntry>,
}

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

/// Valid Eval from a specification YAML file.
pub struct Eval {
    // Name of eval
    pub name: String,

    // Id of [`EvalDefinition`] used.
    pub id: String,

    pub class: Class,
    pub args: HashMap<String, Value>,
}
pub type Class = String;

fn class_to_scorer(c: &Class) -> Option<String> {
    match c.as_str() {
        "evals.elsuite.basic.match:Match" => Some("match".to_string()),
        _ => None,
    }
}

/// Converts an [`Eval`] into the spice components needed to run the eval in spice.
///
/// [`DatasetComponent`] is non-optional because, currently, every eval needs a dataset.
pub(super) fn spice_components(
    eval: &Eval,
    data_dir: &Path,
) -> Result<(EvalComponent, DatasetComponent)> {
    let Some(scorer) = class_to_scorer(&eval.class) else {
        return Err(anyhow!("Unsupported class: {}", eval.class));
    };

    let Some(dataset) = dataset_needed(eval, data_dir) else {
        return Err(anyhow!("Need dataset for eval '{}'", eval.name));
    };

    Ok((
        EvalComponent {
            name: eval.name.clone(),
            scorers: vec![scorer],
            dataset: dataset.name.clone(),
            description: None,
            depends_on: vec![],
        },
        dataset,
    ))
}

/// Construct the associated [`DatasetComponent`] for the given eval, as a local file dataset.
fn dataset_needed(eval: &Eval, data_dir: &Path) -> Option<DatasetComponent> {
    eval.args.iter().find_map(|(key, value)| {
        let Value::String(s) = value else { return None };

        if is_potential_file_key(key) {
            return Some(DatasetComponent::new(
                format!("file:{}", data_dir.join(s).display()),
                normalise_table_name(eval.id.as_str()),
            ));
        }
        None
    })
}

/// Normalise a table name to a valid identifier.
fn normalise_table_name(x: &str) -> String {
    x.to_lowercase().replace('-', "_").replace('.', "__")
}

impl EvalSpecification {
    pub fn validate_from_file(file: &Path, data_dir: &Path) -> Result<Vec<Eval>> {
        let file_contents = std::fs::read_to_string(file)
            .context(anyhow!("Failed to read YAML file '{}'", file.display()))?;

        let spec: EvalSpecification = serde_yaml::from_str(&file_contents)
            .context(anyhow!("Failed to parse YAML file '{}'", file.display()))?;

        // Break into sub-[`EvalSpecification`]. Each [`EvalSpecification`] should have one [`EvalMetadata`] followed by >=1 [`EvalDefinition`].

        spec.validate(data_dir)
    }

    /// Validate the specification from a YAML file, and converts it into an internal representation.
    pub fn validate(&self, data_dir: &Path) -> Result<Vec<Eval>> {
        let metadata: Vec<(String, &EvalMetadata)> = self
            .entries
            .iter()
            .filter_map(|(n, e)| match e {
                EvalEntry::Metadata(m) => Some((n.clone(), m)),
                EvalEntry::Definition(_) => None,
            })
            .collect();

        if metadata.is_empty() {
            return Err(anyhow!("Metadata entry not found"));
        }

        let pairs = metadata
            .iter()
            .map(|(name, m)| {
                let Some(EvalEntry::Definition(def)) = self.entries.get(&m.id) else {
                    return Err(anyhow!("For {}, expected '{}' entry", name.clone(), m.id));
                };
                Ok((name.clone(), *m, def))
            })
            .collect::<Result<Vec<(String, &EvalMetadata, &EvalDefinition)>>>()?;

        pairs
            .iter()
            .map(|(name, EvalMetadata { id, .. }, def)| {
                Self::resolve_file_paths(def, data_dir)?;

                Ok(Eval {
                    id: id.clone(),
                    name: name.to_string(),
                    class: def.class.clone(),
                    args: def.args.clone(),
                })
            })
            .collect::<Result<Vec<Eval>>>()
    }

    pub fn resolve_file_paths(def: &EvalDefinition, data_dir: &Path) -> Result<()> {
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
}

fn is_potential_file_key(value: &str) -> bool {
    matches!(value, "samples_jsonl")
}
