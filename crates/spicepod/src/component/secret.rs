use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Secret {
    SecretBase(SecretBase),
    SecretWithValueFrom(SecretWithValueFrom),
}

/// SecretBase is a shorthand for a secret that is stored in a file
///
/// ```yaml
/// secrets:
///   - name: my_secret
///     key: my_secret_key
/// ```
///
/// is equivalent to
///
/// ```yaml
/// secrets:
///   - name: my_secret
///     valueFrom:
///       file:
///         name: my_secret
///         key: my_secret_key
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecretBase {
    pub name: String,
    pub key: String,
}

/// SecretWithValueFrom is a secret that is resolved from a secret store
///
/// ```yaml
/// secrets:
///   - name: my_secret
///     valueFrom:
///       file:
///         name: my_secret
///         key: my_secret_key
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecretWithValueFrom {
    pub name: String,
    pub value_from: HashMap<String, SecretReference>,
}

/// SecretReference is a reference to a secret value in a secret store
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SecretReference {
    pub name: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub key: String,
}

/// WithGetSecrets is a trait for components that have secrets
pub trait WithGetSecrets {
    fn get_secrets(&self, default_from: Option<String>) -> Vec<SecretWithValueFrom>;
}

/// resolve_secrets resolves secrets into SecretWithValueFrom
///
/// Example:
///
/// ```rust
/// use spicepod::component::secret::{resolve_secrets, Secret};
/// use std::collections::HashMap;
///
/// let secrets = resolve_secrets(secrets, None); // resolve secrets with a default file secret store
///
/// let secrets = resolve_secrets(secrets, Some("keyring".to_string())); // resolve secrets from keyring store
/// ````
pub fn resolve_secrets(
    secrets: &[Secret],
    default_from: Option<String>,
) -> Vec<SecretWithValueFrom> {
    secrets
        .iter()
        .map(|s| match s {
            Secret::SecretWithValueFrom(s) => s.clone(),

            // default spice secret store is file
            Secret::SecretBase(s) => {
                let mut value_from: HashMap<String, SecretReference> = HashMap::new();

                let from = default_from.clone().unwrap_or_else(|| "file".to_string());

                value_from.insert(
                    from,
                    SecretReference {
                        name: s.name.clone(),
                        key: s.key.clone(),
                    },
                );

                SecretWithValueFrom {
                    name: s.name.clone(),
                    value_from,
                }
            }
        })
        .collect()
}
