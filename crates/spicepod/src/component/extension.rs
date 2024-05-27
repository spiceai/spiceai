use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Extension {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,
}

impl Default for Extension {
    fn default() -> Self {
        Self {
            enabled: true,
            params: HashMap::new(),
        }
    }
}

const fn default_true() -> bool {
    true
}
