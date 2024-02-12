pub struct Local {}

use super::ModelSource;
use std::collections::HashMap;
use std::sync::Arc;

impl ModelSource for Local {
    fn pull(&self, params: Arc<Option<HashMap<String, String>>>) -> super::Result<String> {
        tracing::debug!("ModelSource::pull, {:?}", params);

        // fetch name from params
        let name = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("name"))
            .map(std::string::ToString::to_string);

        let Some(name) = name else {
            return Err(super::Error::UnableToCreateModelPath {
                source: std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Unable to create model path",
                ),
            });
        };

        let _ = super::ensure_model_path(name.as_str());

        let path = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("from"))
            .map(std::string::ToString::to_string);

        let Some(path) = path else {
            return Err(super::Error::UnableToCreateModelPath {
                source: std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Unable to create model path",
                ),
            });
        };

        Ok(path.trim_start_matches("file:").to_string())
    }
}
