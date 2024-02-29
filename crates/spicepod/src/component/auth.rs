use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Auth {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_store: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_key: String,
}
