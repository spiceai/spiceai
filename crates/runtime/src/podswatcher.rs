use notify::{event::{CreateKind, ModifyKind, RemoveKind}, EventKind, RecursiveMode, Watcher};
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
            watcher: None
        }
    }

    pub fn watch<F>(&mut self, mut event_handler: F) -> notify::Result<()> 
    where
        F: FnMut(PodsWatcherEvent) + 'static + Send,
    {
        let root_path = self.root_path.clone();

        let mut watcher = notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
            match res {
                Ok(event) => {
                    tracing::debug!("detected content changes: {:?}", event);

                    match event.kind {
                        EventKind::Create(CreateKind::File) |
                        EventKind::Remove(RemoveKind::File) |
                        EventKind::Modify(ModifyKind::Data(_)) => {

                            if let Ok(app) = App::new(root_path.clone()) {
                                event_handler(PodsWatcherEvent::PodsUpdated(app));
                            } else {
                                tracing::debug!("invalid app state detected, ignoring changes.");
                            }
                        }
                        _ => { /*  ignore meta events and other changes */ }
                    }
                },
                Err(e) => tracing::error!("pod content watcher error: {:?}", e),
            }
        })?;

        // root pod file
        let root_yaml_files = vec![format!("spicepod.yaml"), format!("spicepod.yml")];
        for yaml_file in root_yaml_files {
            let yaml_path = self.root_path.clone().join(&yaml_file);
            // check that file exists before watching
            if yaml_path.exists() {
                watcher.watch(&yaml_path, RecursiveMode::NonRecursive)?;
            }
        }
        
        // spicepods dir is required to start watcher; create it if it does not exist
        let spicepods_dir = self.root_path.clone().join("spicepods");
        if !spicepods_dir.exists() {
            std::fs::create_dir(&spicepods_dir)?;
        }
        watcher.watch(&spicepods_dir, RecursiveMode::Recursive)?;

        self.watcher = Some(watcher);

        Ok(())
    }
}

