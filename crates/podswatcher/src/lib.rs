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

        watch_root(&mut watcher, &self.root_path)?;

        self.watcher = Some(watcher);

        Ok(rx)
    }
}

/// Watch `root_path` for Spicepod changes, covering the directory alone when its subtree
/// cannot be walked.
///
/// A recursive watch registers every directory beneath `root_path`, which is the working
/// directory the runtime was started in — not a Spice-owned tree. Anything unreadable under it
/// (another user's files) or a tree large enough to exhaust the kernel's watch limit fails the
/// whole registration, and the pods watcher runs as a runtime task whose failure stops the
/// process. Serving a valid Spicepod must not depend on every unrelated neighbour of it being
/// readable, so those two environmental failures fall back to watching `root_path` itself,
/// which is where `spicepod.yaml` lives. Any other failure — including an unreadable
/// `root_path` — still propagates, because then there is nothing left to watch.
fn watch_root(watcher: &mut notify::RecommendedWatcher, root_path: &Path) -> notify::Result<()> {
    let Err(err) = watcher.watch(root_path, RecursiveMode::Recursive) else {
        return Ok(());
    };

    if !is_untraversable_subtree(&err) {
        return Err(err);
    }

    unwatch_partial_registration(watcher, root_path)?;
    watcher.watch(root_path, RecursiveMode::NonRecursive)?;

    tracing::warn!(
        "Watching {} for Spicepod changes, but not the directories below it: {err}. Edits to spicepod.yaml still reload; edits to files it references from subdirectories do not. Run Spice from a directory it can read in full, or raise the system's file-watch limit, to restore them.",
        root_path.display()
    );

    Ok(())
}

/// Drop whatever the failed recursive registration left behind, so the fallback starts from
/// nothing.
///
/// A recursive `watch` is not atomic. `notify`'s inotify backend walks the tree and registers a
/// watch per directory as it goes, propagating the first failure without unwinding, so every
/// directory it reached before the error is still registered — with the kernel as well as in the
/// backend's own map. Re-watching `root_path` non-recursively would only rewrite the root's
/// entry, leaving those descendants in place: the `MaxFilesWatch` fallback would sit at the
/// watch limit it was supposed to retreat from, and the permission-denied fallback would be a
/// partially recursive watcher whose coverage nobody can predict — while the warning below says
/// the directories underneath are not watched.
///
/// Unwatching the root is enough to clear them, because the backend recorded that entry as
/// recursive and so removes every watch beneath it too. `WatchNotFound` is the expected answer
/// when the walk failed on `root_path` itself, and on the backends that register a subtree in
/// one operation and so leave nothing partial behind; neither is an error here. Anything else is
/// the watcher refusing to give a registration back, which the fallback cannot paper over.
fn unwatch_partial_registration(
    watcher: &mut notify::RecommendedWatcher,
    root_path: &Path,
) -> notify::Result<()> {
    match watcher.unwatch(root_path) {
        Ok(()) => Ok(()),
        Err(err) if matches!(err.kind, notify::ErrorKind::WatchNotFound) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Whether `err` reports that the subtree below the watched directory could not be walked,
/// rather than that the directory itself cannot be watched.
fn is_untraversable_subtree(err: &notify::Error) -> bool {
    match &err.kind {
        // A directory beneath the root that this process may not read.
        notify::ErrorKind::Io(err) => err.kind() == std::io::ErrorKind::PermissionDenied,
        // More directories beneath the root than the kernel will watch.
        notify::ErrorKind::MaxFilesWatch => true,
        _ => false,
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
    fn an_unreadable_directory_in_the_subtree_is_untraversable() {
        let err = notify::Error::new(notify::ErrorKind::Io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )));

        assert!(is_untraversable_subtree(&err));
    }

    #[test]
    fn exhausting_the_watch_limit_is_untraversable() {
        let err = notify::Error::new(notify::ErrorKind::MaxFilesWatch);

        assert!(is_untraversable_subtree(&err));
    }

    /// The fallback narrows what is watched, so it must not stand in for a root that cannot be
    /// watched at all — there is nothing left to degrade to.
    #[test]
    fn a_missing_or_unreadable_root_is_not_untraversable() {
        for kind in [
            notify::ErrorKind::PathNotFound,
            notify::ErrorKind::Io(std::io::Error::from(std::io::ErrorKind::NotFound)),
            notify::ErrorKind::Generic("something else".to_string()),
        ] {
            let err = notify::Error::new(kind);

            assert!(
                !is_untraversable_subtree(&err),
                "{err:?} must propagate rather than fall back"
            );
        }
    }

    /// A root that does not exist has to keep failing: the fallback watch is on the same path,
    /// so swallowing this would leave a watcher that reports nothing, forever.
    #[tokio::test]
    async fn a_root_that_does_not_exist_still_fails() {
        let root = tempfile::tempdir().expect("failed to create temp dir");
        let missing = root.path().join("no-such-directory");

        let err = PodsWatcher::new(&missing)
            .watch()
            .await
            .expect_err("watching a non-existent directory must fail");

        assert!(
            !is_untraversable_subtree(&err),
            "a missing root must not be treated as a walkable-subtree failure: {err:?}"
        );
    }

    /// The reported bug: Spice started in a directory that merely *contains* something the
    /// process cannot read (`~` with another application's files under it) exited, because the
    /// recursive registration failed and the pods watcher is a runtime task.
    ///
    /// On Linux the recursive walk is what fails, so this exercises the fallback directly. On
    /// macOS `FSEvents` does not walk the tree, so the recursive watch succeeds and this asserts
    /// the same end state by the ordinary path — either way an unreadable neighbour must not
    /// stop the watcher, and edits to `spicepod.yaml` must still arrive.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_unreadable_neighbour_directory_does_not_stop_the_watcher() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().expect("failed to create temp dir");
        // The watcher matches event paths against the paths it was given, and the platform
        // backend reports resolved ones — on macOS the temp dir is `/var/…`, a symlink to
        // `/private/var/…`, so an unresolved root would match nothing and time out below.
        let root_path = root
            .path()
            .canonicalize()
            .expect("failed to resolve temp dir");

        let spicepod = root_path.join("spicepod.yaml");
        std::fs::write(&spicepod, "version: v1\nkind: Spicepod\nname: test\n")
            .expect("failed to write spicepod");

        let locked = root_path.join("unreadable");
        std::fs::create_dir(&locked).expect("failed to create directory");
        std::fs::create_dir(locked.join("nested")).expect("failed to create nested directory");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000))
            .expect("failed to drop permissions");

        // Root ignores the mode bits, which would make the fixture a no-op and the assertions
        // below vacuous.
        if std::fs::read_dir(&locked).is_ok() {
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))
                .expect("failed to restore permissions");
            return;
        }

        let mut watcher = PodsWatcher::new(&root_path);
        let mut rx = watcher
            .watch()
            .await
            .expect("an unreadable neighbour must not stop the pods watcher");

        std::fs::write(&spicepod, "version: v1\nkind: Spicepod\nname: changed\n")
            .expect("failed to modify spicepod");

        let changed = tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv()).await;

        // Restore before asserting so a failure still leaves the tree removable.
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))
            .expect("failed to restore permissions");

        assert_eq!(
            changed.expect("timed out waiting for the spicepod change"),
            Some(root_path.clone()),
            "a spicepod edit must still be reported after an unreadable neighbour is skipped"
        );
    }

    /// The fallback narrows the watch to `root_path`, which only means anything if the
    /// registrations the failed recursive walk left behind are gone first — otherwise
    /// `MaxFilesWatch` retreats to the limit it just hit, and the warning's claim that the
    /// directories below are unwatched is false.
    ///
    /// Asserted by removing a registration that is known to exist and then removing it again:
    /// the second call can only be `WatchNotFound`, so a version of this that quietly skipped
    /// the `unwatch` would leave the registration in place and fail here.
    #[test]
    fn clearing_a_partial_registration_removes_it_and_tolerates_its_absence() {
        let root = tempfile::tempdir().expect("failed to create temp dir");
        std::fs::create_dir(root.path().join("nested")).expect("failed to create nested directory");

        let mut watcher = notify::recommended_watcher(|_: notify::Result<notify::Event>| {})
            .expect("failed to construct a platform watcher");
        watcher
            .watch(root.path(), RecursiveMode::Recursive)
            .expect("failed to register the watch this test then clears");

        unwatch_partial_registration(&mut watcher, root.path())
            .expect("clearing a registered watch must succeed");
        unwatch_partial_registration(&mut watcher, root.path())
            .expect("clearing an already-cleared watch must be tolerated, not an error");
    }

    /// A walk that failed on `root_path` itself registered nothing, and the backends that
    /// register a subtree in one operation leave nothing partial behind either. Neither is a
    /// reason to refuse the fallback.
    #[test]
    fn clearing_a_registration_that_was_never_made_is_not_an_error() {
        let root = tempfile::tempdir().expect("failed to create temp dir");

        let mut watcher = notify::recommended_watcher(|_: notify::Result<notify::Event>| {})
            .expect("failed to construct a platform watcher");

        unwatch_partial_registration(&mut watcher, root.path())
            .expect("an unwatched path must not stop the fallback");
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
