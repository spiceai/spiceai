/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#![cfg(unix)]

use std::io::Read;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;

use runtime_acceleration::snapshot::directory_archive::archive_directories_to_file_with_plan;

/// A rebuildable run replaced by a socket must not prevent archiving real data.
#[tokio::test]
async fn optional_unix_socket_does_not_fail_snapshot() {
    // Unix socket addresses have a short length limit; use a short fixture root.
    let fixture = tempfile::Builder::new()
        .prefix("optional-run-")
        .tempdir_in("/tmp")
        .expect("short fixture root");
    let data = fixture.path().join("data");
    let indexes = data.join("_lookup_index");
    std::fs::create_dir_all(&indexes).expect("create index directory");
    std::fs::write(data.join("rows.vortex"), b"authoritative row bytes")
        .expect("write authoritative data");
    let socket = indexes.join("socket.run");
    let _listener = UnixListener::bind(&socket).expect("bind optional run socket");
    let kept = indexes.join("kept.run");
    std::fs::write(&kept, b"regular index bytes").expect("write regular run");
    let destination = fixture.path().join("snapshot.tar");
    let result = archive_directories_to_file_with_plan(
        &[(data, "data/".to_string())],
        &destination,
        &[PathBuf::from("_lookup_index")],
        &[],
        &[
            (socket, "data/_lookup_index/socket.run".to_string()),
            (kept, "data/_lookup_index/kept.run".to_string()),
        ],
    )
    .await;
    println!("snapshot result: {result:?}");
    result.expect("a non-regular optional run must be omitted");
    let bytes = std::fs::read(destination).expect("read snapshot");
    let mut entries: Vec<_> = tar::Archive::new(bytes.as_slice())
        .entries()
        .expect("snapshot entries")
        .map(|entry| {
            let mut entry = entry.expect("snapshot entry");
            let path = entry.path().expect("entry path").into_owned();
            let mut contents = Vec::new();
            entry.read_to_end(&mut contents).expect("entry contents");
            (path, contents)
        })
        .collect();
    entries.sort();
    println!("snapshot entries: {entries:?}");
    assert_eq!(
        entries,
        vec![
            (
                PathBuf::from("data/_lookup_index/kept.run"),
                b"regular index bytes".to_vec()
            ),
            (
                PathBuf::from("data/rows.vortex"),
                b"authoritative row bytes".to_vec()
            ),
        ]
    );
}
