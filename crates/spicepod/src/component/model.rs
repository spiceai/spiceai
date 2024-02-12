use super::WithDependsOn;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub from: String,
    pub name: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
