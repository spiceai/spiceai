use super::ModelSource;
use crate::auth::AuthProvider;
use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;

pub struct Local {}
impl ModelSource for Local {
    fn pull(&self, _: AuthProvider, params: Arc<Option<HashMap<String, String>>>) -> super::Result<String> {
        let name = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("name"))
            .map(ToString::to_string);

        let Some(name) = name else {
            return Err(super::UnableToLoadConfigSnafu {}.build());
        };

        // it is not copying local model into .spice folder
        let _ = super::ensure_model_path(name.as_str())?;

        let path = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("from"))
            .map(ToString::to_string);

        let Some(path) = path else {
            return Err(super::UnableToLoadConfigSnafu {}.build());
        };

        Ok(path.trim_start_matches("file:").to_string())
    }
}
