use super::ModelSource;
use crate::auth::AuthProvider;
use async_trait::async_trait;
use regex::Regex;
use snafu::prelude::*;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

pub struct Huggingface {}

#[async_trait]
impl ModelSource for Huggingface {
    async fn pull(
        &self,
        _: AuthProvider,
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

        let files_param = params
            .as_ref()
            .as_ref()
            .and_then(|p| p.get("files"))
            .map(ToString::to_string);

        let files = match files_param {
            Some(files) => files.split(',').map(ToString::to_string).collect(),
            None => vec![],
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
            r"\A(huggingface:)(huggingface\.co\/)?(?<org>[\w\-]+)\/(?<model>[\w\-]+)(:(?<revision>[\w\d\-\.]+))?\z",
        ) else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: "Invalid regex",
            }
            .build());
        };
        let Some(caps) = re.captures(remote_path.as_str()) else {
            return Err(super::UnableToLoadConfigSnafu {
                reason: format!("from is invalid for huggingface source: {remote_path}"),
            }
            .build());
        };

        let revision = match caps["revision"].to_owned() {
            s if s.is_empty() => "main".to_string(),
            s if s == "latest" => "main".to_string(),
            _ => caps["revision"].to_string(),
        };

        let versioned_path = format!("{local_path}/{revision}");

        let mut onnx_file_name = "".to_string();

        std::fs::create_dir_all(versioned_path.clone())
            .context(super::UnableToCreateModelPathSnafu {})?;

        // replace lfs reference with file downloaded from huggingface api
        let p = versioned_path.clone();
        for file in files {
            let file_name = format!("{p}/{file}");

            if std::fs::metadata(file_name.clone()).is_ok() {
                println!("File already exists: {file_name}, skipping download");
                continue;
            }

            println!("Downloading {file_name}...");

            let download_url = format!(
                "https://huggingface.co/{}/{}/resolve/{}/{}",
                caps["org"].to_owned(),
                caps["model"].to_owned(),
                revision,
                file,
            );

            if file.ends_with(".onnx") {
                onnx_file_name = file_name.clone();
            }

            let client = reqwest::Client::new();
            let response = client
                .get(download_url)
                .send()
                .await
                .context(super::UnableToFetchModelSnafu {})?;

            let mut file = std::fs::File::create(file_name.clone())
                .context(super::UnableToCreateModelPathSnafu {})?;
            let mut content = Cursor::new(response.bytes().await.unwrap_or_default());
            std::io::copy(&mut content, &mut file)
                .context(super::UnableToCreateModelPathSnafu {})?;

            println!("Downloaded {file_name}");
        }

        Ok(onnx_file_name)
    }
}
