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

//! Policy provider that wraps [`InMemoryPolicyProvider`] and reloads named
//! policy sets when watched files change.

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use cedar_policy::PolicySet;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use snafu::Snafu;
use tokio::sync::RwLock;

use super::{PolicyProvider, memory::InMemoryPolicyProvider};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read policy file '{path}': {source}"))]
    FileRead {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to parse Cedar policies from file: {source}"))]
    PolicyParse { source: crate::error::Error },

    #[snafu(display("Policy provider lock poisoned"))]
    LockPoisoned,

    #[snafu(display("Failed to create file watcher: {source}"))]
    WatcherCreate { source: notify::Error },

    #[snafu(display("Failed to register file watcher for '{path}': {source}"))]
    WatcherRegister { path: String, source: notify::Error },
}

/// Wraps [`InMemoryPolicyProvider`] and reloads named policy sets from watched
/// files when configured.
pub struct WatchedPolicyProvider {
    inner: Arc<RwLock<InMemoryPolicyProvider>>,
    watchers: HashMap<String, RecommendedWatcher>,
}

impl WatchedPolicyProvider {
    /// # Errors
    ///
    /// Returns an error if the default empty policy set cannot be created.
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            inner: Arc::new(RwLock::new(
                InMemoryPolicyProvider::try_new("default", vec![])
                    .map_err(|source| Error::PolicyParse { source })?,
            )),
            watchers: HashMap::new(),
        })
    }

    /// Add a named inline policy set.
    ///
    /// # Errors
    ///
    /// Returns an error if the Cedar policy text cannot be parsed.
    pub fn add(
        &mut self,
        name: impl Into<String>,
        policies: Vec<String>,
    ) -> Result<&mut Self, Error> {
        self.inner
            .write()
            .map_err(|_| Error::LockPoisoned)?
            .update_policy(name, policies)
            .map_err(|source| Error::PolicyParse { source })?;
        Ok(self)
    }

    /// Add a named policy set backed by a file, and watch that file for changes.
    ///
    /// When the file changes the named policy set is replaced with a fresh load.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, the Cedar text cannot be
    /// parsed, or the watcher cannot be registered.
    pub fn watch(&mut self, name: impl Into<String>, path: PathBuf) -> Result<&mut Self, Error> {
        let name = name.into();
        let initial = load_from_file(&path)?;

        self.inner
            .write()
            .map_err(|_| Error::LockPoisoned)?
            .update_policy(&name, initial)
            .map_err(|source| Error::PolicyParse { source })?;

        let provider = Arc::clone(&self.inner);

        let mut watcher =
            notify::recommended_watcher(make_reload_handler(name.clone(), path.clone(), provider))
                .map_err(|source| Error::WatcherCreate { source })?;

        watcher
            .watch(&path, RecursiveMode::NonRecursive)
            .map_err(|source| Error::WatcherRegister {
                path: path.display().to_string(),
                source,
            })?;

        self.watchers.insert(name, watcher);
        Ok(self)
    }

    #[must_use]
    pub fn watcher_count(&self) -> usize {
        self.watchers.len()
    }
}

#[async_trait::async_trait]
impl PolicyProvider for WatchedPolicyProvider {
    async fn fetch_policies(&self) -> Result<PolicySet, crate::error::Error> {
        self.inner
            .read()
            .map_err(|_| crate::error::Error::Provider {
                reason: "watched policy provider lock poisoned".to_string(),
            })?
            .fetch_policies()
            .await
    }
}

fn load_from_file(path: &PathBuf) -> Result<Vec<String>, Error> {
    let content = std::fs::read_to_string(path).map_err(|source| Error::FileRead {
        path: path.display().to_string(),
        source,
    })?;
    Ok(vec![content])
}

fn make_reload_handler(
    name: String,
    path: PathBuf,
    provider: Arc<RwLock<InMemoryPolicyProvider>>,
) -> impl Fn(notify::Result<notify::Event>) {
    move |res| match res {
        Ok(event) => {
            if !is_policy_reload_event(&event.kind) {
                return;
            }
            match load_from_file(&path) {
                Ok(reloaded) => match provider.write() {
                    Ok(mut p) => {
                        if let Err(e) = p.update_policy(&name, reloaded) {
                            tracing::error!(name = %name, "Failed to update watched policy set: {e}");
                        }
                    }
                    Err(e) => tracing::error!(name = %name, "Failed to acquire lock: {e}"),
                },
                Err(e) => tracing::error!(name = %name, "Failed to reload policy file: {e}"),
            }
        }
        Err(e) => tracing::error!("Watched policy provider error: {e}"),
    }
}

fn is_policy_reload_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Any | EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_only_watched_policy_sets_create_watchers() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let watched_file = temp_dir.path().join("policy.cedar");
        std::fs::write(
            &watched_file,
            r#"permit(principal, action == Spice::Action::"query", resource);"#,
        )
        .expect("watched policy file should be written");

        let mut provider = WatchedPolicyProvider::new().expect("provider should build");
        provider
            .add(
                "inline-only",
                vec![
                    r#"permit(principal, action == Spice::Action::"read", resource);"#.to_string(),
                ],
            )
            .expect("inline policy should be added");
        provider
            .watch("watched", watched_file)
            .expect("watched policy should be added");

        assert_eq!(provider.watcher_count(), 1);
    }

    #[tokio::test]
    async fn test_watched_policy_file_reload_updates_inner_provider() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let watched_file = temp_dir.path().join("policy.cedar");
        std::fs::write(
            &watched_file,
            r#"permit(principal, action == Spice::Action::"query", resource);"#,
        )
        .expect("watched policy file should be written");

        let mut provider = WatchedPolicyProvider::new().expect("provider should build");
        provider
            .watch("watched", watched_file.clone())
            .expect("watched policy should be added");

        assert_eq!(
            provider
                .fetch_policies()
                .await
                .expect("policies should parse")
                .policies()
                .count(),
            1
        );

        std::fs::write(
            &watched_file,
            r#"
            permit(principal, action == Spice::Action::"query", resource);
            forbid(principal, action == Spice::Action::"read", resource);
            "#,
        )
        .expect("watched policy file should be updated");

        let updated = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let policy_count = provider
                    .fetch_policies()
                    .await
                    .expect("policies should parse")
                    .policies()
                    .count();
                if policy_count == 2 {
                    break policy_count;
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for watched policy reload");

        assert_eq!(updated, 2);
    }
}
