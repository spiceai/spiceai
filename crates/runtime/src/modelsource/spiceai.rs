pub struct SpiceAI {}

use super::ModelSource;
use async_trait::async_trait;
use snafu::prelude::*;
use std::collections::HashMap;
use std::io::Cursor;
use std::string::ToString;
use std::sync::Arc;

use crate::secrets::Secret;
use regex::Regex;

#[async_trait]
impl ModelSource for SpiceAI {
    async fn pull(
        &self,
        secret: Secret,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> super::Result<String> {
        let name = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("name"))
            .map(ToString::to_string);

        let Some(name) = name else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "Name is required",
            }
            .build());
        };

        // it is not copying local model into .spice folder
        let local_path = super::ensure_model_path(name.as_str())?;

        let remote_path = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("path"))
            .map(ToString::to_string);

        let Some(remote_path) = remote_path else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "From is required",
            }
            .build());
        };

        let Ok(re) = Regex::new(
            r"\A(?:spice\.ai\/)?(?<org>[\w\-]+)\/(?<app>[\w\-]+)(?:\/models)?\/(?<model>[\w\-]+):(?<version>[\w\d\-\.]+)\z",
        ) else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "Invalid regex",
            }
            .build());
        };
        let Some(caps) = re.captures(remote_path.as_str()) else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: format!("from is invalid for spice.ai source: {remote_path}"),
            }
            .build());
        };

        let default_url = if cfg!(feature = "dev") {
            "https://dev.spice.xyz".to_string()
        } else {
            "https://spice.ai".to_string()
        };

        let mut url = format!(
            "{}/api/orgs/{}/apps/{}/models/{}",
            default_url,
            caps["org"].to_owned(),
            caps["app"].to_owned(),
            caps["model"].to_owned(),
        );

        let version = match caps["version"].to_owned() {
            s if s.is_empty() => "latest".to_string(),
            _ => caps["version"].to_string(),
        };

        match version.as_str() {
            "latest" => {}
            _ => {
                url.push_str(&format!("?training_run_id={version}"));
            }
        }

        let client = reqwest::Client::new();
        let data = client
            .get(url.clone())
            .bearer_auth(secret.get("token").unwrap_or_default())
            .send()
            .await
            .context(super::UnableToFetchModelSnafu)?
            .json::<HashMap<String, serde_json::value::Value>>()
            .await
            .context(super::UnableToFetchModelSnafu)?;

        // Given we are still actively developing the model response, we'll only fetch the frist
        // export url for now.
        // In future, we can use a proper static model response format to parse the body
        let download_url = data
            .get("artifacts")
            .ok_or(super::UnableToParseMetadataSnafu {}.build())?
            .as_array()
            .ok_or(super::UnableToParseMetadataSnafu {}.build())?
            .first()
            .ok_or(super::UnableToParseMetadataSnafu {}.build())?
            .as_object()
            .ok_or(super::UnableToParseMetadataSnafu {}.build())?
            .get("export_url")
            .ok_or(super::UnableToParseMetadataSnafu {}.build())?
            .as_str()
            .ok_or(super::UnableToParseMetadataSnafu {}.build())?;

        let versioned_path = format!("{local_path}/{version}");
        let file_name = format!("{versioned_path}/model.onnx");

        let response = client
            .get(download_url)
            .send()
            .await
            .context(super::UnableToFetchModelSnafu {})?;

        std::fs::create_dir_all(versioned_path).context(super::UnableToCreateModelPathSnafu {})?;
        let mut file = std::fs::File::create(file_name.clone())
            .context(super::UnableToCreateModelPathSnafu {})?;
        let mut content = Cursor::new(response.bytes().await.unwrap_or_default());
        std::io::copy(&mut content, &mut file).context(super::UnableToCreateModelPathSnafu {})?;

        Ok(file_name)
    }
}
