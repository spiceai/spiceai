use std::collections::HashMap;

use secrecy::SecretString;

pub fn convert_params_for_validation(
    params: &HashMap<String, SecretString>,
) -> Vec<(String, SecretString)> {
    params.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

#[macro_export]
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}
