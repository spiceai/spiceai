use notify::{
    event::{CreateKind, ModifyKind, RemoveKind},
    EventKind, RecursiveMode, Watcher,
};
use spicepod::component::ComponentOrReference;
use std::path::PathBuf;

use app::App;

#[derive(Debug)]
pub enum PodsWatcherEvent {
    PodsUpdated(App),
}

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

    pub fn watch<F>(&mut self, mut event_handler: F) -> notify::Result<()>
    where
        F: FnMut(PodsWatcherEvent) + 'static + Send,
    {
        let root_path = self.root_path.clone();

        let root_spicepod_path = vec![
            root_path.join("spicepod.yaml"),
            root_path.join("spicepod.yml"),
        ];

        let mut watch_paths = get_watch_paths(&root_path);

        let mut watcher =
            notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
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

                        if let Ok(app) = App::new(root_path.clone()) {
                            event_handler(PodsWatcherEvent::PodsUpdated(app));
                        } else {
                            tracing::debug!("Invalid app state detected, ignoring changes.");
                        }
                    }
                    Err(e) => tracing::error!("Pods content watcher error: {:?}", e),
                }
            })?;

        watcher.watch(&self.root_path, RecursiveMode::Recursive)?;

        self.watcher = Some(watcher);

        Ok(())
    }
}

fn get_watch_paths(app_path: impl Into<PathBuf>) -> Vec<PathBuf> {
    let root_dir: PathBuf = app_path.into();

    let mut dirs = vec![
        // self.root_path.join("spicepods/"),
        root_dir.join("spicepod.yaml"),
        root_dir.join("spicepod.yml"),
    ];

    // attempt to open root spicepod definition file
    if let Ok(spicepod) = spicepod::Spicepod::load_definition(&root_dir) {
        // watch dependencies
        for dep in spicepod.dependencies {
            let dep_path = root_dir.join("spicepods").join(dep);
            dirs.push(dep_path);
        }

        // watch ref datasets folders
        for dataset in spicepod.datasets {
            match dataset {
                ComponentOrReference::Reference(reference) => {
                    let ref_path = root_dir.join(&reference.r#ref);
                    dirs.push(root_dir.join(ref_path));
                }
                ComponentOrReference::Component(_) => { /* ignore component */ }
            }
        }

        // watch ref datasets folders
        for model in spicepod.models {
            match model {
                ComponentOrReference::Reference(reference) => {
                    let ref_path = root_dir.join(&reference.r#ref);
                    dirs.push(root_dir.join(ref_path));
                }
                ComponentOrReference::Component(_) => { /* ignore component */ }
            }
        }
    }

    dirs
}

fn is_spicepods_modification_event(spicepod_paths: &[PathBuf], event: &notify::Event) -> bool {
    match event.kind {
        EventKind::Create(CreateKind::File)
        | EventKind::Remove(RemoveKind::File)
        | EventKind::Modify(ModifyKind::Data(_)) => {
            // iterate through all paths in the event and
            // check if relative path is within the `spicepods` dir
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
