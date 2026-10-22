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

//! Downloads files of a Hugging Face repository into the Hub's cache, the way a model,
//! embedding or reranker does, and prints where each one landed. Settings come from the
//! environment (`HF_HOME`, `HF_TOKEN`, `HF_HUB_OFFLINE`, …).
//!
//! ```text
//! cargo run -p huggingface-hub --example fetch -- <model|dataset|space> <owner>/<name> <revision> <path>...
//! ```
//!
//! It is how the cache's compatibility with the `hf` CLI is checked: download the same files
//! with both, compare their bytes, and read each cache with the other tool offline.

use huggingface_hub::{RepoId, RepoType, Requester, Settings, Snapshot};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [kind, repo, revision, paths @ ..] = args.as_slice() else {
        return Err(
            "usage: fetch <model|dataset|space> <owner>/<name> <revision> <path>...".into(),
        );
    };
    let kind = match kind.as_str() {
        "model" => RepoType::Model,
        "dataset" => RepoType::Dataset,
        "space" => RepoType::Space,
        other => return Err(format!("unknown repository type '{other}'").into()),
    };
    let (owner, name) = repo
        .split_once('/')
        .ok_or("the repository must be <owner>/<name>")?;
    let repo = RepoId::new(kind, owner, name)?;
    let settings = Settings::resolve(None, None).await?;
    let requester = Requester {
        kind: "example",
        name: "fetch".to_string(),
        docs_url: "https://spiceai.org/docs/components/models/huggingface",
        retried: false,
    };
    let snapshot = Snapshot::open(settings, requester, repo, Some(revision)).await?;
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    for path in snapshot.download(&paths).await? {
        println!("{}", path.display());
    }
    Ok(())
}
