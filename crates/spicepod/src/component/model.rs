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

        if from.starts_with("file:/") {
            return "localhost".to_string();
        } else if from.starts_with("spice.ai") {
            return "spiceai".to_string();
        }

        "debug".to_string()
    }
}
