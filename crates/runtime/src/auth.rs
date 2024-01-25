use dirs;
use std::fs::File;
use std::io::Read;

use serde::Deserialize;

pub trait Auth {
    fn get_token(&self) -> String;
}

#[allow(clippy::module_name_repetitions)]
#[derive(Deserialize)]
pub struct AuthConfig {
    pub auth: AuthProvider,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Deserialize)]
pub struct AuthProvider {
    pub provider_type: String,
    pub key: String,
}

pub mod none;
pub mod spiceai;

#[must_use]
pub fn get() -> Box<dyn Auth> {
    let auth_path = match dirs::home_dir() {
        Some(mut path) => {
            path.push(".spice/auth");
            path
        }
        None => return Box::new(none::NoneAuth::new()),
    };

    let Ok(mut auth_file) = File::open(auth_path) else {
        return Box::new(none::NoneAuth::new());
    };

    let mut auth_contents = String::new();
    match auth_file.read_to_string(&mut auth_contents) {
        Ok(_) => (),
        Err(_) => return Box::new(none::NoneAuth::new()),
    }

    let Ok(auth_config) = toml::from_str::<AuthConfig>(&auth_contents) else {
        return Box::new(none::NoneAuth::new());
    };

    match auth_config.auth.provider_type.as_str() {
        "spice.ai" => Box::new(spiceai::SpiceAuth::new(auth_config.auth.key)),
        _ => Box::new(none::NoneAuth::new()),
    }
}
