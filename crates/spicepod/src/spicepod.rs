use std::{fs::{ReadDir, File}, error::Error};

use serde::Deserialize;
use serde_yaml;

#[derive(Debug, Deserialize)]
pub struct SpicepodDefinition {
  pub name: String,

  #[serde(with = "spicepod_kind")]
  pub kind: SpicepodKind,
}

#[derive(Debug)]
pub enum SpicepodKind {
  Spicepod,
}

mod spicepod_kind {
  use super::SpicepodKind;
  use serde::Deserialize;

  pub fn deserialize<'de, D>(deserializer: D) -> Result<SpicepodKind, D::Error>
  where
      D: serde::Deserializer<'de>,
  {
      let kind: String = Deserialize::deserialize(deserializer)?;
      match kind.as_str() {
          "Spicepod" => Ok(SpicepodKind::Spicepod),
          _ => Err(serde::de::Error::custom(format!("unknown kind: {}", kind))),
      }
  }
}

pub fn load(path: &str) -> Result<SpicepodDefinition, Box<dyn Error>> {
  let dir: ReadDir = std::fs::read_dir(path)?;

  for entry in dir {
    let entry = entry?;
    let file_name = entry.file_name();
    let entry_path = entry.path();
    if entry_path.is_file() {
      if file_name == "spicepod.yaml" || file_name == "spicepod.yml" {
        let spicepod_definition_yaml = File::open(entry_path)?;

        let spicepod_definition: SpicepodDefinition = serde_yaml::from_reader(spicepod_definition_yaml)?;

        return Ok(spicepod_definition);
      }
    }
  }
  
  Err(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "spicepod.yaml not found")))
}