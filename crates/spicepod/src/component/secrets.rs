use serde::{Deserialize, Serialize};

/// The secrets configuration for a Spicepod.
///
/// Example:
/// ```yaml
/// secrets:
///   store: file
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Secrets {
    pub store: SpiceSecretStore,
}

impl Default for Secrets {
    fn default() -> Self {
        Self {
            store: SpiceSecretStore::File,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SpiceSecretStore {
    File,
    Env,
}
