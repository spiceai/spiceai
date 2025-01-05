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

use notify::{
    event::{CreateKind, ModifyKind, RemoveKind},
    EventKind, RecursiveMode, Watcher,
};
use spicepod::component::ComponentOrReference;
use std::path::PathBuf;
use tokio::sync::mpsc::{channel, Receiver};

use app::{App, AppBuilder};

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

    pub fn watch(&mut self) -> notify::Result<Receiver<App>> {
        let root_path = self.root_path.clone();

        let (tx, rx) = channel(100);

        let root_spicepod_path = [
            root_path.join("spicepod.yaml"),
            root_path.join("spicepod.yml"),
        ];

        let mut watch_paths = get_watch_paths(&root_path);

        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| {
                match res {
                    Ok(event) => {
                        if !is_spicepods_modification_event(&watch_paths, &event) {
                            return;
                        }
                        tracing::debug!("Detected pods content changes: {:?}", event);

                        // check if main spicepod file has been modified to update watching paths
                        for event_path in &event.paths {
                            if root_spicepod_path.iter().any(|dir| event_path.eq(dir)) {
                                watch_paths = get_watch_paths(&root_path);
                            }
                        }

                        match AppBuilder::build_from_filesystem_path(root_path.clone()) {
                            Ok(app) => {
                                if let Err(e) = tx.blocking_send(app) {
                                    tracing::error!("Pods content watcher is unable to notify detected state change: {e}");
                                }
                            }
                            Err(e) => tracing::warn!(
                                "Invalid app state detected, unable to load pods information: {e}"
                            ),
                        }
                    }
                    Err(e) => tracing::error!("Pods content watcher error: {e}"),
                }
            },
        )?;

        watcher.watch(&self.root_path, RecursiveMode::Recursive)?;

        self.watcher = Some(watcher);

        Ok(rx)
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

fn get_watch_paths(app_path: impl Into<PathBuf>) -> Vec<PathBuf> {
    let root_dir: PathBuf = app_path.into();

    let mut dirs = vec![
        root_dir.join("spicepod.yaml"),
        root_dir.join("spicepod.yml"),
    ];

    if let Ok(spicepod) = spicepod::Spicepod::load_definition(&root_dir) {
        for dep in spicepod.dependencies {
            let dep_path = root_dir.join("spicepods").join(dep);
            dirs.push(dep_path);
        }

        enable_watch_for_component!(spicepod.datasets, dirs, root_dir);
        enable_watch_for_component!(spicepod.models, dirs, root_dir);
        enable_watch_for_component!(spicepod.catalogs, dirs, root_dir);
        enable_watch_for_component!(spicepod.views, dirs, root_dir);
    }

    dirs
}

fn is_spicepods_modification_event(spicepod_paths: &[PathBuf], event: &notify::Event) -> bool {
    match event.kind {
        EventKind::Create(CreateKind::File)
        | EventKind::Remove(RemoveKind::File)
        | EventKind::Modify(ModifyKind::Data(_)) => {
            for event_path in &event.paths {
                if spicepod_paths.iter().any(|dir| event_path.starts_with(dir)) {
                    return true;
                }
            }
        }
        _ => { /*  ignore meta events and other changes */ }
    }

    false
}
