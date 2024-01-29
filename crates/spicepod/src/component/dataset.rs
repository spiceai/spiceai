use serde::{Deserialize, Serialize};

use super::WithDependsOn;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatasetType {
    Append,
    Replace,
    View,
    Blockchain,
    Overwrite,
    Mutable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub name: String,

    pub r#type: DatasetType,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    // Intentionally not making this an enum, as we want to support extensible sources
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<acceleration::Acceleration>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl WithDependsOn<Dataset> for Dataset {
    fn depends_on(&self, depends_on: &[String]) -> Dataset {
        Dataset {
            name: self.name.clone(),
            r#type: self.r#type.clone(),
            description: self.description.clone(),
            source: self.source.clone(),
            acceleration: self.acceleration.clone(),

            depends_on: depends_on.to_vec(),
        }
    }
}

pub mod acceleration {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Acceleration {
        #[serde(default)]
        pub enabled: bool,
    }
}
