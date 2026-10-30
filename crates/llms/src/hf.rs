/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! What every local model, embedding and reranker shares for reading the Hugging Face Hub:
//! the Hub settings a component names, and opening a repository's snapshot with them.

use huggingface_hub::{DownloadError, ParamToken, RepoId, Settings, Snapshot};
pub use huggingface_hub::{RepoType, Requester};
use secrecy::SecretString;

/// The Hugging Face Hub settings a component names: its `hf_token` and `hf_endpoint`, which the
/// environment's settings fill in (see [`huggingface_hub::settings`]).
#[derive(Debug, Default, Clone, Copy)]
pub struct HubParams<'a> {
    pub token: Option<&'a SecretString>,
    /// Whether the Spicepod sets `token`, rather than Spice loading it from a secret store. A
    /// loaded token is never sent to an `hf_endpoint` the Spicepod sets (see [`ParamToken`]).
    pub token_in_spicepod: bool,
    pub endpoint: Option<&'a str>,
}

/// Opens `model_id` (`org/name`) at `revision` for `requester`, with `hub`'s settings.
///
/// # Errors
///
/// Returns an error if the repository id or a setting is not valid, or the repository or
/// revision cannot be read and the cache holds no snapshot of it.
pub async fn open_model(
    requester: Requester,
    model_id: &str,
    revision: Option<&str>,
    hub: HubParams<'_>,
) -> Result<Snapshot, DownloadError> {
    let repo = DownloadError::parse_repo(&requester, RepoType::Model, model_id, revision)?;
    let settings = settings(&requester, &repo, revision, hub).await?;
    Snapshot::open(settings, requester, repo, revision).await
}

/// Opens `paths` of `repo` (`org/name`), a repository of `kind`, at `revision` for
/// `requester`, with `hub`'s settings. Only those paths are kept, and a warm load costs at most
/// one request (see [`Snapshot::open_paths`]).
///
/// # Errors
///
/// Returns an error if the repository id or a setting is not valid, or the repository or
/// revision cannot be read and the cache holds no snapshot of it.
pub async fn open_paths(
    requester: Requester,
    kind: RepoType,
    repo: &str,
    revision: Option<&str>,
    hub: HubParams<'_>,
    paths: &[&str],
) -> Result<Snapshot, DownloadError> {
    let repo = DownloadError::parse_repo(&requester, kind, repo, revision)?;
    let settings = settings(&requester, &repo, revision, hub).await?;
    Snapshot::open_paths(settings, requester, repo, revision, paths).await
}

async fn settings(
    requester: &Requester,
    repo: &RepoId,
    revision: Option<&str>,
    hub: HubParams<'_>,
) -> Result<Settings, DownloadError> {
    let token = hub.token.cloned().map(|secret| {
        if hub.token_in_spicepod {
            ParamToken::Spicepod(secret)
        } else {
            ParamToken::SecretStore(secret)
        }
    });
    Settings::resolve(token, hub.endpoint)
        .await
        .map_err(|source| DownloadError::settings(requester, repo, revision, source))
}

/// The safetensors index of a sharded model, `model.safetensors.index.json`.
pub const SAFETENSORS_INDEX: &str = "model.safetensors.index.json";

/// The shard files a safetensors index names, sorted, each of which `listed` must accept.
///
/// # Errors
///
/// Returns why the index cannot be used, worded to follow its file name.
pub fn safetensors_shards(
    index: &str,
    listed: impl Fn(&str) -> bool,
) -> Result<Vec<String>, String> {
    let index: serde_json::Value =
        serde_json::from_str(index).map_err(|_| "is not valid JSON".to_string())?;
    let map = index
        .get("weight_map")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "has no `weight_map`".to_string())?;
    let mut shards: Vec<String> = map
        .values()
        .filter_map(serde_json::Value::as_str)
        .map(ToString::to_string)
        .collect();
    shards.sort();
    shards.dedup();
    if shards.is_empty() {
        return Err("names no weight files".to_string());
    }
    if let Some(missing) = shards.iter().find(|shard| !listed(shard)) {
        return Err(format!("names '{missing}', which is not in the repo"));
    }
    Ok(shards)
}

/// The shards a downloaded safetensors index names, read from the snapshot.
///
/// # Errors
///
/// Returns an error if the index cannot be downloaded or read, or names a file the repository
/// does not have.
pub async fn read_shards(snapshot: &Snapshot) -> Result<Vec<String>, DownloadError> {
    let index = snapshot.fetch_quietly(&[SAFETENSORS_INDEX]).await?;
    let index = index.first().cloned().unwrap_or_default();
    let contents = tokio::fs::read_to_string(&index).await.map_err(|error| {
        DownloadError::unusable(
            snapshot,
            format!("'{SAFETENSORS_INDEX}' could not be read ({error})"),
        )
    })?;
    safetensors_shards(&contents, |shard| snapshot.file(shard).is_some())
        .map_err(|why| DownloadError::unusable(snapshot, format!("'{SAFETENSORS_INDEX}' {why}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn an_index_names_its_shards() {
        let index = r#"{"metadata":{},"weight_map":{"a":"model-00001-of-00002.safetensors","b":"model-00002-of-00002.safetensors","c":"model-00001-of-00002.safetensors"}}"#;
        assert_eq!(
            safetensors_shards(index, |_| true),
            Ok(owned(&[
                "model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors"
            ]))
        );
        assert_eq!(
            safetensors_shards(index, |shard| shard != "model-00002-of-00002.safetensors"),
            Err("names 'model-00002-of-00002.safetensors', which is not in the repo".to_string())
        );
        assert_eq!(
            safetensors_shards("{}", |_| true),
            Err("has no `weight_map`".to_string())
        );
        assert_eq!(
            safetensors_shards(r#"{"weight_map":{}}"#, |_| true),
            Err("names no weight files".to_string())
        );
        assert_eq!(
            safetensors_shards("not json", |_| true),
            Err("is not valid JSON".to_string())
        );
    }
}
