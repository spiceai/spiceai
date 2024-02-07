use super::WithDependsOn;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub from: String,
    pub name: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "datasets", default)]
    pub datasets: Vec<String>,
}

impl WithDependsOn<Model> for Model {
    fn depends_on(&self, depends_on: &[String]) -> Model {
        Model {
            from: self.from.clone(),
            name: self.name.clone(),
            datasets: depends_on.to_vec(),
        }
    }
}

impl Model {
    #[must_use]
    pub fn source(&self) -> String {
        let from = self.from.clone();

        // teset if from contains file:/
        if from.starts_with("file:/") {
            return "local".to_string();
        } else if from.starts_with("spice.ai") {
            return "spice.ai".to_string();
        } else {
            return "debug".to_string();
        }
    }
}
