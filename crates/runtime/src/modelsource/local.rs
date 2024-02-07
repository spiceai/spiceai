pub struct Local {}
use std::collections::HashMap;
use std::sync::Arc;

impl super::ModelSource for Local {
    fn pull(&self, params: Arc<Option<HashMap<String, String>>>) -> bool {
        tracing::debug!("ModelSource::pull, {:?}", params);

        // fetch name from params
        let name = params.as_ref()
            .as_ref()
            .and_then(|p| p.get("name"))
            .map(|n| n.to_string());

        tracing::debug!("{:?}", super::ensure_model_path(name.unwrap().as_str()));
        return false;
    }
}
