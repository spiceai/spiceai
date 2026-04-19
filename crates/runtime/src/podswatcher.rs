/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use notify::{EventKind, RecursiveMode, Watcher};
use parking_lot::RwLock;
use spicepod::component::ComponentOrReference;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    runtime::Handle,
    sync::mpsc::{Receiver, channel},
};

pub struct PodsWatcher {
    root_path: PathBuf,
    watcher: Option<notify::RecommendedWatcher>,
}

impl PodsWatcher {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            root_path: path.into(),
            watcher: None,
        }
    }

    pub async fn watch(&mut self) -> notify::Result<Receiver<PathBuf>> {
        let root_path = self.root_path.clone();
        let runtime_handle = Handle::current();

        let (tx, rx) = channel(100);

        let root_spicepod_path = [
            root_path.join("spicepod.yaml"),
            root_path.join("spicepod.yml"),
        ];

        let watch_paths = Arc::new(RwLock::new(get_watch_paths(&root_path).await));
        let refresh_generation = Arc::new(AtomicU64::new(0));

        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(event) => {
                    if is_root_spicepod_event(&root_spicepod_path, &event)
                        && is_relevant_spicepods_event_kind(event.kind)
                    {
                        refresh_watch_paths(
                            &runtime_handle,
                            &watch_paths,
                            &refresh_generation,
                            &root_path,
                        );
                    }

                    let current_watch_paths = watch_paths.read();

                    if !is_spicepods_modification_event(&current_watch_paths, &event) {
                        return;
                    }

                    tracing::debug!("Detected pods content changes: {:?}", event);

                    let _ = tx.try_send(root_path.clone());
                }
                Err(e) => tracing::error!("Pods content watcher error: {e}"),
            },
        )?;

        watcher.watch(&self.root_path, RecursiveMode::Recursive)?;

        self.watcher = Some(watcher);

        Ok(rx)
    }
}

fn is_relevant_spicepods_event_kind(event_kind: EventKind) -> bool {
    matches!(
        event_kind,
        EventKind::Any | EventKind::Create(_) | EventKind::Remove(_) | EventKind::Modify(_)
    )
}

fn refresh_watch_paths(
    runtime_handle: &Handle,
    watch_paths: &Arc<RwLock<Vec<PathBuf>>>,
    refresh_generation: &Arc<AtomicU64>,
    root_path: &Path,
) {
    let runtime_handle = runtime_handle.clone();
    let watch_paths = Arc::clone(watch_paths);
    let refresh_generation = Arc::clone(refresh_generation);
    let root_path = root_path.to_path_buf();
    let refresh_id = refresh_generation.fetch_add(1, Ordering::Relaxed) + 1;

    runtime_handle.spawn(async move {
        if apply_refreshed_watch_paths(
            &watch_paths,
            &refresh_generation,
            refresh_id,
            try_get_watch_paths(&root_path).await,
        ) {
            tracing::debug!("Refreshed watched pods paths after main spicepod change");
        }
    });
}

fn apply_refreshed_watch_paths(
    watch_paths: &RwLock<Vec<PathBuf>>,
    refresh_generation: &AtomicU64,
    refresh_id: u64,
    refreshed_paths: spicepod::Result<Vec<PathBuf>>,
) -> bool {
    match refreshed_paths {
        Ok(refreshed_paths) => {
            if refresh_generation.load(Ordering::Relaxed) != refresh_id {
                tracing::debug!("Ignoring stale watched pods path refresh result");
                return false;
            }

            *watch_paths.write() = refreshed_paths;
            true
        }
        Err(err) => {
            tracing::warn!(
                "Failed to refresh watched pods paths after main spicepod change: {err}"
            );
            false
        }
    }
}

macro_rules! enable_watch_for_component {
    ($items:expr, $dirs:expr, $root_dir:expr) => {
        for item in $items {
            match item {
                ComponentOrReference::Reference(reference) => {
                    $dirs.push($root_dir.join(&$root_dir.join(&reference.r#ref)));
                }
                ComponentOrReference::Component(_) => { /* ignore component */ }
            }
        }
    };
}

async fn get_watch_paths(app_path: impl Into<PathBuf>) -> Vec<PathBuf> {
    let root_dir: PathBuf = app_path.into();

    try_get_watch_paths(root_dir.clone())
        .await
        .unwrap_or_else(|_| root_spicepod_paths(&root_dir))
}

fn root_spicepod_paths(root_dir: &Path) -> Vec<PathBuf> {
    vec![
        root_dir.join("spicepod.yaml"),
        root_dir.join("spicepod.yml"),
    ]
}

async fn try_get_watch_paths(app_path: impl Into<PathBuf>) -> spicepod::Result<Vec<PathBuf>> {
    let root_dir: PathBuf = app_path.into();

    let mut dirs = root_spicepod_paths(&root_dir);

    let spicepod = spicepod::Spicepod::load_definition(&root_dir).await?;

    for dep in spicepod.dependencies {
        let dep_path = root_dir.join("spicepods").join(dep);
        dirs.push(dep_path);
    }

    enable_watch_for_component!(spicepod.datasets, dirs, root_dir);
    enable_watch_for_component!(spicepod.models, dirs, root_dir);
    enable_watch_for_component!(spicepod.catalogs, dirs, root_dir);
    enable_watch_for_component!(spicepod.views, dirs, root_dir);

    Ok(dirs)
}

fn is_spicepods_modification_event(spicepod_paths: &[PathBuf], event: &notify::Event) -> bool {
    if !is_relevant_spicepods_event_kind(event.kind) {
        return false;
    }

    for event_path in &event.paths {
        if spicepod_paths.iter().any(|dir| event_path.starts_with(dir)) {
            return true;
        }
    }

    false
}

fn is_root_spicepod_event(root_spicepod_paths: &[PathBuf; 2], event: &notify::Event) -> bool {
    event
        .paths
        .iter()
        .any(|event_path| root_spicepod_paths.iter().any(|path| event_path == path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, DataChange, EventAttributes, ModifyKind, RenameMode};

    fn watcher_event(kind: EventKind, path: &str) -> notify::Event {
        notify::Event {
            kind,
            paths: vec![PathBuf::from(path)],
            attrs: EventAttributes::default(),
        }
    }

    #[test]
    fn test_is_spicepods_modification_event_accepts_modify_any() {
        let watch_paths = vec![PathBuf::from("/tmp/app/spicepod.yaml")];
        let event = watcher_event(EventKind::Modify(ModifyKind::Any), "/tmp/app/spicepod.yaml");

        assert!(is_spicepods_modification_event(&watch_paths, &event));
    }

    #[test]
    fn test_is_spicepods_modification_event_accepts_modify_name() {
        let watch_paths = vec![PathBuf::from("/tmp/app/spicepod.yaml")];
        let event = watcher_event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
            "/tmp/app/spicepod.yaml",
        );

        assert!(is_spicepods_modification_event(&watch_paths, &event));
    }

    #[test]
    fn test_is_spicepods_modification_event_accepts_modify_data() {
        let watch_paths = vec![PathBuf::from("/tmp/app/spicepod.yaml")];
        let event = watcher_event(
            EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            "/tmp/app/spicepod.yaml",
        );

        assert!(is_spicepods_modification_event(&watch_paths, &event));
    }

    #[test]
    fn test_is_spicepods_modification_event_rejects_access() {
        let watch_paths = vec![PathBuf::from("/tmp/app/spicepod.yaml")];
        let event = watcher_event(EventKind::Access(AccessKind::Any), "/tmp/app/spicepod.yaml");

        assert!(!is_spicepods_modification_event(&watch_paths, &event));
    }

    #[test]
    fn test_is_root_spicepod_event_detects_main_spicepod() {
        let root_spicepod_paths = [
            PathBuf::from("/tmp/app/spicepod.yaml"),
            PathBuf::from("/tmp/app/spicepod.yml"),
        ];
        let event = watcher_event(EventKind::Modify(ModifyKind::Any), "/tmp/app/spicepod.yaml");

        assert!(is_root_spicepod_event(&root_spicepod_paths, &event));
    }

    #[test]
    fn test_apply_refreshed_watch_paths_updates_current_success() {
        let expected = vec![PathBuf::from("/tmp/app/spicepods/views/orders.sql")];
        let watch_paths = RwLock::new(vec![PathBuf::from("/tmp/app/spicepod.yaml")]);
        let refresh_generation = AtomicU64::new(1);

        let updated =
            apply_refreshed_watch_paths(&watch_paths, &refresh_generation, 1, Ok(expected.clone()));

        assert!(updated);
        assert_eq!(*watch_paths.read(), expected);
    }

    #[test]
    fn test_apply_refreshed_watch_paths_rejects_stale_refresh() {
        let current = vec![PathBuf::from("/tmp/app/spicepod.yaml")];
        let watch_paths = RwLock::new(current.clone());
        let refresh_generation = AtomicU64::new(2);

        let updated = apply_refreshed_watch_paths(
            &watch_paths,
            &refresh_generation,
            1,
            Ok(vec![PathBuf::from("/tmp/app/spicepods/views/orders.sql")]),
        );

        assert!(!updated);
        assert_eq!(*watch_paths.read(), current);
    }

    #[test]
    fn test_apply_refreshed_watch_paths_keeps_last_known_good_on_failure() {
        let current = vec![PathBuf::from("/tmp/app/spicepods/views/orders.sql")];
        let watch_paths = RwLock::new(current.clone());
        let refresh_generation = AtomicU64::new(1);

        let updated = apply_refreshed_watch_paths(
            &watch_paths,
            &refresh_generation,
            1,
            Err(spicepod::Error::SpicepodNotFound {
                path: PathBuf::from("/tmp/app"),
            }),
        );

        assert!(!updated);
        assert_eq!(*watch_paths.read(), current);
    }
}
