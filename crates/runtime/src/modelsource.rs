use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub mod local;

#[derive(Debug, Snafu)]
pub enum Error {
    UnableToCreateModelSource {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to find home directory"))]
    UnableToFindHomeDir {},

    #[snafu(display("Unable to create model path"))]
    UnableToCreateModelPath { source: std::io::Error },

    #[snafu(display("Unable to create model path"))]
    UnableToLoadConfig {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait ModelSource {
    fn pull(&self, params: Arc<Option<HashMap<String, String>>>) -> Result<String>;
}

pub fn ensure_model_path(name: &str) -> Result<String> {
    let mut model_path = dirs::home_dir().context(UnableToFindHomeDirSnafu)?;
    model_path.push(".spice/models");
    model_path.push(name);

    if !model_path.exists() {
        std::fs::create_dir_all(&model_path).context(UnableToCreateModelPathSnafu)?;
    }

    let Some(model_path) = model_path.to_str() else {
        return Err(Error::UnableToCreateModelSource {
            source: "Unable to create model path".into(),
        });
    };

    Ok(model_path.to_string())
}
