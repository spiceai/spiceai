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

use crate::component::dataset::DatasetSpec;
use crate::dataconnector::ConnectorComponent;
use crate::dataconnector::ConnectorContext;
use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;
use async_trait::async_trait;
use data_connector_api::accelerated::RegisteredAcceleratedTable;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use snafu::prelude::*;
use std::ffi::OsString;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{any::Any, env};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use url::Url;

use super::ConnectorParams;
use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, InvalidConfigurationSnafu,
    ParameterSpec, Parameters, listing::ListingTableConnector,
};

/// Connector name for the local file connector, as it appears in a dataset's `from: file:...`.
pub const FILE_DATACONNECTOR: &str = "file";

#[derive(Debug)]
pub struct File {
    params: Parameters,
    tokio_io_runtime: Handle,
}

impl std::fmt::Display for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file")
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct FileFactory {}

impl FileFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for FileFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        _context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send + 'a>> {
        Box::pin(async move {
            Ok(Arc::new(File {
                params: params.parameters,
                tokio_io_runtime: params.io_runtime,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        FILE_DATACONNECTOR
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        LISTING_TABLE_PARAMETERS
    }
}

#[async_trait]
impl ListingTableConnector for File {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> Handle {
        self.tokio_io_runtime.clone()
    }

    /// Creates a valid file [`url::Url`], from the dataset, supporting both
    ///   1. Relative paths
    ///   2. Datasets prefixed with `file://` (not just `file:/`). This is to mirror the UX of [`Url::parse`].
    fn get_object_store_url(
        &self,
        dataset: &DatasetSpec,
        path: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let path = match path {
            Some(p) => PathBuf::from(p.trim_start_matches("file:"))
                .to_string_lossy()
                .into_owned(),
            None => get_path(dataset).to_string_lossy().into_owned(),
        };
        // Convert relative path to absolute path
        let url_str = if path.starts_with('/') {
            format!("file:{path}")
        } else {
            let absolute_path = env::current_dir()
                .boxed()
                .context(InvalidConfigurationSnafu {
                    dataconnector: "file".to_string(),
                    message: "Could not identify current directory for a relative file path. Does the running user have the right filesystem permissions?".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                })?
                .join(&path)
                .to_string_lossy()
                .to_string();

            format!("file:{absolute_path}")
        };

        Url::parse(&url_str)
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "file".to_string(),
                message: format!("The specified file path {path} created an invalid URL. Check your file path and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/file"),
                connector_component: ConnectorComponent::from(dataset),
            })
    }

    /// Set up a file watcher to refresh the accelerated table when the file is updated.
    ///
    /// Spawns an async top-level Tokio task to watch the file(s) and attaches it to the
    /// accelerated table, so the watcher is aborted when that table is dropped.
    async fn on_accelerated_table_registration(
        &self,
        dataset: &DatasetSpec,
        accelerated_table: &mut dyn RegisteredAcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Only enable the file watcher if the acceleration has the file_watcher parameter set to "enabled"
        let enabled = dataset.acceleration.as_ref().is_some_and(|acceleration| {
            acceleration
                .params
                .get("file_watcher")
                .is_some_and(|v| v == "enabled")
        });

        if !enabled {
            tracing::debug!("File watcher disabled for dataset {}", dataset.name);
            return Ok(());
        }

        let path = get_path(dataset);
        // Canonical, so it compares equal to the root a resolved symlink target
        // shares with it: a relative `.` and its absolute form are one inotify
        // watch, and dropping the target's root must not drop this one.
        let watch_path = watch_root(&path);
        let watch_path = watch_path.canonicalize().unwrap_or(watch_path);
        let directory_dataset = path.is_dir();
        let mode = if directory_dataset {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        let watched_names = Arc::new(Mutex::new(if directory_dataset {
            Vec::new()
        } else {
            watched_file_names(&path)
        }));
        let (tx, mut rx) = mpsc::channel(100);
        let Some(refresh_requester) = accelerated_table.refresh_requester() else {
            return Ok(());
        };

        tracing::info!(
            "Watching changes to {} (via {})",
            path.display(),
            watch_path.display()
        );

        let names_for_callback = Arc::clone(&watched_names);
        let watcher_task = tokio::spawn(async move {
            let mut watcher: RecommendedWatcher = match notify::recommended_watcher(
                move |res: Result<notify::Event, notify::Error>| match res {
                    Ok(event) => {
                        let relevant = if directory_dataset {
                            file_watch_event_is_relevant(&event, None)
                        } else {
                            let names = names_for_callback
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            file_watch_event_is_relevant(&event, Some(names.as_slice()))
                        };
                        if relevant {
                            let _ = tx.blocking_send(());
                        }
                    }
                    Err(e) => {
                        tracing::debug!("File watcher event error: {e}");
                    }
                },
            ) {
                Ok(watcher) => watcher,
                Err(e) => {
                    tracing::error!("Failed to create file watcher: {e}");
                    return;
                }
            };

            match watcher.watch(&watch_path, mode) {
                Ok(()) => (),
                Err(e) => {
                    tracing::error!("Failed to watch file: {e}");
                    return;
                }
            }

            let mut extra_roots = Vec::new();
            let mut file_watch = None;
            refresh_resolved_file_watch(
                &mut watcher,
                &path,
                &watch_path,
                &mut extra_roots,
                &mut file_watch,
                &watched_names,
            );

            // Trailing-edge debounce: wait until events stop arriving, then
            // refresh. Events that land while a refresh is in flight keep a
            // dirty flag so another refresh runs after it.
            let mut pending = false;
            let mut refresh_in_flight = false;
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(()) => {
                                refresh_resolved_file_watch(
                                    &mut watcher,
                                    &path,
                                    &watch_path,
                                    &mut extra_roots,
                                    &mut file_watch,
                                    &watched_names,
                                );
                                pending = true;
                            }
                            None => break,
                        }
                    }
                    () = tokio::time::sleep(FILE_WATCHER_DEBOUNCE), if pending && !refresh_in_flight => {
                        pending = false;
                        refresh_in_flight = true;
                    }
                    result = refresh_requester.request_refresh(), if refresh_in_flight => {
                        if let Err(e) = result {
                            tracing::error!("Failed to trigger refresh: {e}");
                        } else {
                            tracing::debug!("Triggered refresh for file {}", path.display());
                        }
                        refresh_in_flight = false;
                    }
                    else => break,
                }
            }
        });

        accelerated_table.attach_task(watcher_task);

        Ok(())
    }
}

data_connector_api::register_data_connector!("file", FileFactory);

/// How long to wait after the last relevant filesystem event before refreshing.
/// Long enough that a truncate-then-write or a burst of appends is seen as one
/// change, short enough that an interactive edit still lands promptly.
const FILE_WATCHER_DEBOUNCE: Duration = Duration::from_millis(400);

fn watch_root(path: &Path) -> PathBuf {
    if path.is_dir() {
        return path.to_path_buf();
    }
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn watched_file_names(path: &Path) -> Vec<OsString> {
    let mut names = Vec::new();
    if let Some(name) = path.file_name() {
        names.push(name.to_owned());
    }
    if let Ok(resolved) = path.canonicalize()
        && let Some(name) = resolved.file_name()
        && !names.iter().any(|existing| existing.as_os_str() == name)
    {
        names.push(name.to_owned());
    }
    names
}

/// What to watch for `path`, read from where it resolves now. `None` while
/// `path` does not resolve, so the caller keeps what it last watched.
struct ResolvedFileWatch {
    /// File names whose events trigger a refresh.
    names: Vec<OsString>,
    /// Directories to watch besides the configured root.
    roots: Vec<PathBuf>,
    /// The file `path` resolves to, watched itself so writes through a hard
    /// link in another directory are seen.
    file: PathBuf,
}

fn resolved_file_watch(path: &Path, configured_root: &Path) -> Option<ResolvedFileWatch> {
    let resolved = path.canonicalize().ok()?;
    let mut names = Vec::new();
    if let Some(name) = path.file_name() {
        names.push(name.to_owned());
    }
    if let Some(name) = resolved.file_name()
        && !names.iter().any(|existing| existing.as_os_str() == name)
    {
        names.push(name.to_owned());
    }
    let root = watch_root(&resolved);
    let roots = if root.as_path() == configured_root {
        Vec::new()
    } else {
        vec![root]
    };
    Some(ResolvedFileWatch {
        names,
        roots,
        file: resolved,
    })
}

fn refresh_resolved_file_watch(
    watcher: &mut RecommendedWatcher,
    path: &Path,
    configured_root: &Path,
    extra_roots: &mut Vec<PathBuf>,
    file_watch: &mut Option<PathBuf>,
    names: &Mutex<Vec<OsString>>,
) {
    if path.is_dir() {
        return;
    }
    // While the path does not resolve (a symlink whose target was deleted ahead
    // of its recreation), keep the last resolved name and parent watch, so the
    // recreation and the writes after it still trigger a refresh.
    let Some(ResolvedFileWatch {
        names: resolved_names,
        roots: desired,
        file,
    }) = resolved_file_watch(path, configured_root)
    else {
        return;
    };
    let mut guard = names
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = resolved_names;
    drop(guard);

    for old in extra_roots.iter() {
        if !desired.contains(old) {
            let _ = watcher.unwatch(old);
        }
    }
    for new_root in &desired {
        if !extra_roots.contains(new_root)
            && let Err(e) = watcher.watch(new_root, RecursiveMode::NonRecursive)
        {
            tracing::debug!(
                "Failed to watch resolved file parent {}: {e}",
                new_root.display()
            );
        }
    }
    *extra_roots = desired;

    // Also watch the file itself, as the watcher did before it watched
    // directories: a write through a hard link in another directory reaches
    // only the inode. Re-arm it on every refresh, because a replace leaves the
    // old watch on the replaced inode.
    if let Some(old) = file_watch.take() {
        let _ = watcher.unwatch(&old);
    }
    match watcher.watch(&file, RecursiveMode::NonRecursive) {
        Ok(()) => *file_watch = Some(file),
        Err(e) => tracing::debug!("Failed to watch resolved file {}: {e}", file.display()),
    }
}

fn file_watch_event_is_relevant(event: &notify::Event, watched_names: Option<&[OsString]>) -> bool {
    if !(event.kind.is_create() || event.kind.is_modify() || event.kind.is_remove()) {
        return false;
    }
    match watched_names {
        None => true,
        Some(names) => event.paths.iter().any(|path| {
            path.file_name()
                .is_some_and(|name| names.iter().any(|watched| watched.as_os_str() == name))
        }),
    }
}

fn get_path(dataset: &DatasetSpec) -> PathBuf {
    PathBuf::from(dataset.path())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::builder::DatasetBuilder;

    #[tokio::test]
    async fn test_get_path() {
        let test_cases = vec![
            ("file:/path/to/file.csv", PathBuf::from("/path/to/file.csv")),
            ("file://path/to/file.csv", PathBuf::from("path/to/file.csv")),
            (
                "file:///path/to/file.csv",
                PathBuf::from("/path/to/file.csv"),
            ),
            (
                "file:relative/path/to/file.csv",
                PathBuf::from("relative/path/to/file.csv"),
            ),
        ];

        for (input, expected) in test_cases {
            let app = app::AppBuilder::new("test").build();
            let rt = crate::Runtime::builder().build().await;

            let dataset = DatasetBuilder::try_new(input.to_string(), "foo")
                .expect("Failed to create builder")
                .with_app(Arc::new(app))
                .with_runtime(Arc::new(rt))
                .build()
                .expect("Failed to build dataset");

            let result = get_path(&dataset);
            assert_eq!(result, expected, "Failed for input: {input}");
        }
    }

    #[tokio::test]
    async fn test_get_object_store_url() {
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let dataset = DatasetBuilder::try_new("file:/tmp/".into(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        let connector = File {
            params: Parameters::new(([]).to_vec(), "test", &[]),
            tokio_io_runtime: Handle::current(),
        };

        let url = connector
            .get_object_store_url(&dataset, None)
            .expect("should get a valid URL");
        assert_eq!(url.as_str(), "file:///tmp/");

        // object store override path with `file:` prefix
        let url = connector
            .get_object_store_url(&dataset, Some("file:/tmp/1/"))
            .expect("should get a valid URL");
        assert_eq!(url.as_str(), "file:///tmp/1/");

        // object store override without `file:` prefix
        let url = connector
            .get_object_store_url(&dataset, Some("/tmp/2/"))
            .expect("should get a valid URL");
        assert_eq!(url.as_str(), "file:///tmp/2/");
    }

    #[test]
    fn watch_root_uses_parent_for_a_file() {
        assert_eq!(
            watch_root(Path::new("/data/events.csv")),
            PathBuf::from("/data")
        );
        assert_eq!(watch_root(Path::new("events.csv")), PathBuf::from("."));
    }

    #[test]
    fn file_watch_event_filters_unrelated_names_and_accesses() {
        let target = [OsString::from("events.csv")];
        let create = notify::Event::new(notify::EventKind::Create(notify::event::CreateKind::File))
            .add_path(PathBuf::from("/data/events.csv"));
        let other = notify::Event::new(notify::EventKind::Create(notify::event::CreateKind::File))
            .add_path(PathBuf::from("/data/events.csv.tmp"));
        let access = notify::Event::new(notify::EventKind::Access(notify::event::AccessKind::Any))
            .add_path(PathBuf::from("/data/events.csv"));
        assert!(file_watch_event_is_relevant(
            &create,
            Some(target.as_slice())
        ));
        assert!(!file_watch_event_is_relevant(
            &other,
            Some(target.as_slice())
        ));
        assert!(!file_watch_event_is_relevant(
            &access,
            Some(target.as_slice())
        ));
        assert!(file_watch_event_is_relevant(&other, None));
    }

    #[test]
    fn file_watch_event_accepts_resolved_target_name() {
        let names = [OsString::from("names.csv"), OsString::from("actual.csv")];
        let target = notify::Event::new(notify::EventKind::Modify(notify::event::ModifyKind::Any))
            .add_path(PathBuf::from("/other/actual.csv"));
        assert!(file_watch_event_is_relevant(
            &target,
            Some(names.as_slice())
        ));
    }

    #[cfg(unix)]
    #[test]
    fn watched_file_names_includes_symlink_target() {
        let root =
            std::env::temp_dir().join(format!("spice-file-watch-names-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("real")).expect("create real dir");
        let real = root.join("real").join("actual.csv");
        std::fs::write(&real, b"x").expect("write target");
        let alias = root.join("names.csv");
        std::os::unix::fs::symlink(&real, &alias).expect("symlink");
        let names = watched_file_names(&alias);
        let _ = std::fs::remove_dir_all(&root);
        assert!(names.iter().any(|name| name.as_os_str() == "names.csv"));
        assert!(names.iter().any(|name| name.as_os_str() == "actual.csv"));
    }

    #[cfg(unix)]
    #[test]
    fn resolved_file_watch_is_none_while_the_symlink_target_is_gone() {
        let root =
            std::env::temp_dir().join(format!("spice-file-watch-resolve-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("real")).expect("create real dir");
        let real = root.join("real").join("actual.csv");
        std::fs::write(&real, b"x").expect("write target");
        let alias = root.join("names.csv");
        std::os::unix::fs::symlink(&real, &alias).expect("symlink");
        let real_dir = root
            .join("real")
            .canonicalize()
            .expect("canonical real dir");

        let resolved = resolved_file_watch(&alias, &root);
        std::fs::remove_file(&real).expect("delete target");
        let while_gone = resolved_file_watch(&alias, &root);
        std::fs::write(&real, b"y").expect("recreate target");
        let recreated = resolved_file_watch(&alias, &root);
        let _ = std::fs::remove_dir_all(&root);

        let watch = resolved.expect("resolves before the delete");
        assert!(
            watch
                .names
                .iter()
                .any(|name| name.as_os_str() == "actual.csv")
        );
        assert_eq!(watch.roots, vec![real_dir.clone()]);
        assert_eq!(watch.file, real_dir.join("actual.csv"));
        assert!(
            while_gone.is_none(),
            "a dangling symlink must not replace the last resolved watch"
        );
        let watch = recreated.expect("resolves after the recreate");
        assert!(
            watch
                .names
                .iter()
                .any(|name| name.as_os_str() == "actual.csv")
        );
        assert_eq!(watch.roots, vec![real_dir]);
    }

    /// Whether an event on `rx` within two seconds is both accepted by the
    /// watch and of a kind matching `kind`. Draining with `try_recv` is not a
    /// barrier, so a delayed event from an earlier step (e.g. the `IN_ATTRIB`
    /// a `link()` raises) can still arrive; `kind` keeps it from passing.
    #[cfg(target_os = "linux")]
    fn accepts_an_event(
        rx: &std::sync::mpsc::Receiver<notify::Event>,
        names: &Mutex<Vec<OsString>>,
        kind: fn(&notify::EventKind) -> bool,
    ) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) {
            let Ok(event) = rx.recv_timeout(left) else {
                return false;
            };
            let names = names
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if kind(&event.kind) && file_watch_event_is_relevant(&event, Some(names.as_slice())) {
                return true;
            }
        }
        false
    }

    /// A write to file contents.
    #[cfg(target_os = "linux")]
    fn is_data_modify(kind: &notify::EventKind) -> bool {
        matches!(
            kind,
            notify::EventKind::Modify(notify::event::ModifyKind::Data(_))
        )
    }

    /// A rename onto or away from a watched name.
    #[cfg(target_os = "linux")]
    fn is_rename(kind: &notify::EventKind) -> bool {
        matches!(
            kind,
            notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
        )
    }

    /// A write through a hard link reaches only the dataset's inode: from
    /// another directory it makes no event on the watched parent, and from the
    /// same directory it carries the other name. The file watch sees both, and
    /// is re-armed on the new inode after an atomic replace.
    #[cfg(target_os = "linux")]
    #[test]
    fn writes_through_hard_links_reach_the_file_watch() {
        use std::io::Write;
        let append = |path: &Path| {
            std::fs::OpenOptions::new()
                .append(true)
                .open(path)
                .expect("open for append")
                .write_all(b"2\n")
                .expect("append");
        };
        let root =
            std::env::temp_dir().join(format!("spice-file-watch-links-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let data = root.join("data");
        let other = root.join("other");
        std::fs::create_dir_all(&data).expect("create data dir");
        std::fs::create_dir_all(&other).expect("create other dir");
        let path = data.join("names.csv");
        std::fs::write(&path, b"id\n1\n").expect("write dataset");

        let names = Mutex::new(Vec::new());
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher =
            notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    let _ = tx.send(event);
                }
            })
            .expect("create watcher");
        let configured_root = watch_root(&path).canonicalize().expect("canonical root");
        watcher
            .watch(&configured_root, RecursiveMode::NonRecursive)
            .expect("watch root");
        let mut extra_roots = Vec::new();
        let mut file_watch = None;
        refresh_resolved_file_watch(
            &mut watcher,
            &path,
            &configured_root,
            &mut extra_roots,
            &mut file_watch,
            &names,
        );

        std::fs::hard_link(&path, other.join("alias.csv")).expect("link from another dir");
        std::fs::hard_link(&path, data.join("writer.csv")).expect("link in the same dir");
        while rx.try_recv().is_ok() {}
        append(&other.join("alias.csv"));
        let from_other_dir = accepts_an_event(&rx, &names, is_data_modify);
        while rx.try_recv().is_ok() {}
        append(&data.join("writer.csv"));
        let from_same_dir = accepts_an_event(&rx, &names, is_data_modify);

        let tmp = data.join("names.csv.tmp");
        std::fs::write(&tmp, b"id\n1\n2\n3\n").expect("write replacement");
        std::fs::rename(&tmp, &path).expect("atomic replace");
        let replace_seen = accepts_an_event(&rx, &names, is_rename);
        refresh_resolved_file_watch(
            &mut watcher,
            &path,
            &configured_root,
            &mut extra_roots,
            &mut file_watch,
            &names,
        );
        std::fs::hard_link(&path, other.join("alias-2.csv")).expect("link the new inode");
        while rx.try_recv().is_ok() {}
        append(&other.join("alias-2.csv"));
        let after_replace = accepts_an_event(&rx, &names, is_data_modify);

        drop(watcher);
        let _ = std::fs::remove_dir_all(&root);
        assert!(from_other_dir, "a hard link in another directory");
        assert!(from_same_dir, "a hard link in the same directory");
        assert!(replace_seen, "the atomic replace");
        assert!(after_replace, "a hard link to the replacing inode");
    }
}
