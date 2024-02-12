use notify::{event::{CreateKind, ModifyKind, RemoveKind}, EventKind, RecursiveMode, Watcher};
use std::path::{PathBuf};

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
                    if !is_spicepods_modification_event(&root_path, &event) {
                        return;
                    }
                    tracing::debug!("Detected pods content changes: {:?}", event);

                    if let Ok(app) = App::new(root_path.clone()) {
                        event_handler(PodsWatcherEvent::PodsUpdated(app));
                    } else {
                        tracing::debug!("Invalid app state detected, ignoring changes.");
                    }
                },
                Err(e) => tracing::error!("Pods content watcher error: {:?}", e),
            }
        })?;

        watcher.watch(&self.root_path, RecursiveMode::Recursive)?;

        self.watcher = Some(watcher);

        Ok(())
    }
}

fn is_spicepods_modification_event(app_path: impl Into<PathBuf>, event: &notify::Event) -> bool {
   let root_dir = app_path.into();
    
    let spicepod_paths = vec![
        root_dir.join("spicepods/"),
        root_dir.join("spicepod.yaml"),
        root_dir.join("spicepod.yml"),
    ];
    
    match event.kind {
        EventKind::Create(CreateKind::File) |
        EventKind::Remove(RemoveKind::File) |
        EventKind::Modify(ModifyKind::Data(_)) => {
            // iterate through all paths in the event and 
            // check if relative path is within the `spicepods` dir
            for event_path in event.paths.iter() {
                if spicepod_paths.iter().any(|dir| event_path.starts_with(dir)) {
                    return true;
                }
            }

        }
        _ => { /*  ignore meta events and other changes */ }
    }

    false
}

