use dirs;
use snafu::{OptionExt, Snafu};
use std::fs::File;
use std::io::{Read, Result};
use std::path::PathBuf;

pub trait Auth {
    fn get_token(&self) -> String;
}

#[allow(clippy::module_name_repetitions)]
pub struct AuthProviders {}

pub mod none;
pub mod spiceai;

impl AuthProviders {
    #[must_use]
    pub fn get_auth() -> crate::Result<Box<dyn Auth>> {
        let home_dir = dirs::home_dir().context(crate::NoHomeDirectorySnafu)?;

        match name {
            "spiceai" => {
                todo!("SpiceAI auth not implemented yet")
            }
            _ => Box::new(none::NoneAuth::new()),
        }
    }
}
