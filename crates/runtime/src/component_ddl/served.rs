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

//! What this instance served under each name SQL defines, kept in its
//! instance directory beside the acceleration data it loaded. While it is
//! stopped, another instance can drop such a dataset or point it at another
//! source, and a Spicepod edit can start or stop declaring the name; at its
//! next start, this instance compares what it served with what it is about to
//! serve, and deletes the acceleration data an earlier definition left before
//! anything loads it.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use spicepod::component::dataset::Dataset;

use super::SqlDefinitions;

/// The folder the definitions are kept in, in the instance directory as in a
/// `runtime.state` location.
const FOLDER: &str = "sql-definitions";

/// The file, in the instance directory's definitions folder.
const SERVED_FILE: &str = "served.json";

/// The definitions defined with SQL; the definition this instance served
/// under each of their names, its own or a Spicepod's; and the earlier
/// definitions of the datasets it withheld until their acceleration data is
/// deleted.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Record {
    pub(crate) definitions: SqlDefinitions,
    #[serde(default)]
    pub(crate) served: Vec<Dataset>,
    #[serde(default)]
    pub(crate) withheld: Vec<Dataset>,
}

/// Where `instance_dir` keeps what it served.
pub(crate) fn path(instance_dir: &Path) -> PathBuf {
    instance_dir.join(FOLDER).join(SERVED_FILE)
}

/// What `instance_dir` recorded it served, or `None` when it recorded nothing
/// or the record cannot be read.
pub(crate) async fn read(instance_dir: &Path) -> Option<Record> {
    let bytes = tokio::fs::read(path(instance_dir)).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Record `served` in `instance_dir`, replacing the earlier record whole: a
/// reader finds the earlier record or this one, never part of either. The
/// record and its folder are readable by their owner only: the definitions
/// keep their values as written, which can be credentials.
pub(crate) async fn write(instance_dir: &Path, served: &Record) -> std::io::Result<()> {
    let path = path(instance_dir);
    let Some(folder) = path.parent() else {
        return Ok(());
    };
    tokio::fs::create_dir_all(folder).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(folder, std::fs::Permissions::from_mode(0o700)).await?;
    }
    let staged = path.with_extension("json.tmp");
    tokio::fs::write(&staged, serde_json::to_vec(served)?).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600)).await?;
    }
    tokio::fs::rename(&staged, &path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A record is read back as written, and its absence reads as none.
    #[tokio::test]
    async fn what_was_served_is_read_back() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert_eq!(read(dir.path()).await, None);
        let served = Record {
            definitions: SqlDefinitions {
                datasets: vec![Dataset::new("file:y.csv", "s1")],
                ..SqlDefinitions::default()
            },
            served: vec![Dataset::new("file:z.csv", "s1")],
            withheld: vec![Dataset::new("file:x.csv", "s1")],
        };
        write(dir.path(), &served).await.expect("write");
        assert_eq!(read(dir.path()).await, Some(served));
        assert!(!path(dir.path()).with_extension("json.tmp").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |path: &Path| {
                std::fs::metadata(path)
                    .expect("metadata")
                    .permissions()
                    .mode()
                    & 0o777
            };
            assert_eq!(mode(&path(dir.path())), 0o600);
            assert_eq!(mode(path(dir.path()).parent().expect("folder")), 0o700);
        }
    }
}
