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
            .map(|n| n.to_string());

        let _ = super::ensure_model_path(name.unwrap().as_str());

        let path = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("from"))
            .map(|n| n.to_string());

        // trim local path
        let path = path.map(|p| p.trim_start_matches("file:").to_string());

        return Ok(path.unwrap());
    }
}
