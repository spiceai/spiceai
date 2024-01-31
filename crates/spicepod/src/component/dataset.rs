use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::WithDependsOn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub from: String,

    pub name: String,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<acceleration::Acceleration>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl WithDependsOn<Dataset> for Dataset {
    fn depends_on(&self, depends_on: &[String]) -> Dataset {
        Dataset {
            from: self.from.clone(),
            name: self.name.clone(),
            params: self.params.clone(),
            acceleration: self.acceleration.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}

pub mod acceleration {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum RefreshMode {
        Full,
        Append,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Acceleration {
        #[serde(default)]
        pub enabled: bool,

        #[serde(default, skip_serializing_if = "String::is_empty")]
        pub mode: String,

        #[serde(default, skip_serializing_if = "String::is_empty")]
        pub engine: String,

        #[serde(default, skip_serializing_if = "String::is_empty")]
        pub refresh_interval: String,

        pub refresh_mode: RefreshMode,

        #[serde(default, skip_serializing_if = "String::is_empty")]
        pub retention: String,
    }
}
