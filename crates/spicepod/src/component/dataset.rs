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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firecache: Option<firecache::Firecache>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl WithDependsOn<Dataset> for Dataset {
    fn new(&self, depends_on: Vec<String>) -> Dataset {
        Dataset {
            name: self.name.clone(),
            r#type: self.r#type.clone(),
            description: self.description.clone(),
            firecache: self.firecache.clone(),

            depends_on: depends_on.clone(),
        }
    }
}

pub mod firecache {
  use serde::{Deserialize, Serialize};

  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct Firecache {
      pub enabled: bool,
      pub trigger: Trigger,
      pub time_column: TimeColumn,
  }
  
  #[derive(Debug, Clone, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum Trigger {
      BlockNumber,
      BlockSlot,
      Number,
      Height,
      BlockHeight,
  }

  #[derive(Debug, Clone, Serialize, Deserialize)]
  #[serde(rename_all = "snake_case")]
  pub enum TimeColumn {
      BlockTimestamp,
      Timestamp,
  }
}
