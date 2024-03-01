use super::WithDependsOn;
use serde::{Deserialize, Serialize};

use super::secret::{resolve_secrets, Secret, SecretWithValueFrom, WithGetSecrets};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Model {
    pub from: String,
    pub name: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "datasets", default)]
    pub datasets: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<Secret>,
}

impl WithDependsOn<Model> for Model {
    fn depends_on(&self, depends_on: &[String]) -> Model {
        Model {
            from: self.from.clone(),
            name: self.name.clone(),
            datasets: depends_on.to_vec(),
            secrets: Vec::default(),
        }
    }
}

impl WithGetSecrets for Model {
    fn get_secrets(&self, default_from: Option<String>) -> Vec<SecretWithValueFrom> {
        resolve_secrets(&self.secrets, default_from)
    }
}
