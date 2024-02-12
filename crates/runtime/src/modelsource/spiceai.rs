pub struct SpiceAI {}

use super::ModelSource;
use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;

use crate::auth::AuthProvider;
use regex::Regex;

impl ModelSource for SpiceAI {
    fn pull(
        &self,
        auth_provider: AuthProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> super::Result<String> {
        tracing::info!("pulling model here");
        let name = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("name"))
            .map(ToString::to_string);

        let Some(name) = name else {
            return Err(super::UnableToLoadConfigSnafu {}.build());
        };

        // it is not copying local model into .spice folder
        let path = super::ensure_model_path(name.as_str())?;

        let remote = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("from"))
            .map(ToString::to_string);

        let Some(remote) = remote else {
            return Err(super::UnableToLoadConfigSnafu {}.build());
        };

        let re = Regex::new(r"\Aspice\.ai\/(?<org>[\w\-]+)\/(?<app>[\w\-]+)\/(?<model>[\w\-]+)\z")
            .unwrap();
        let Some(caps) = re.captures(remote.as_str()) else {
            return Err(super::UnableToLoadConfigSnafu {}.build());
        };

        let default_url = if cfg!(feature = "dev") {
            "https://spice.ai".to_string()
        } else {
            "https://dev.spice.xyz".to_string()
        };

        let url = format!(
            "{}/api/orgs/{}/apps/{}/models/{}",
            default_url,
            caps["org"].to_string(),
            caps["app"].to_string(),
            caps["model"].to_string()
        );

        tracing::debug!("pulling model here {:?}", url);

        todo!();
    }
}
