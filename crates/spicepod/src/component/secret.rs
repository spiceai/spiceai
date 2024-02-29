use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Secret {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub store: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub store_key: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data_key: String,
}
