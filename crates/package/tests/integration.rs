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

//! `make_zip` against an object store that counts the downloaded bytes still held, so the tests
//! see how much of a package's content is in memory at once.

#![expect(
    clippy::expect_used,
    reason = "test helpers fail the test on setup errors"
)]

use std::fmt;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Downloaded bytes that are still referenced, and the most there were at once.
#[derive(Debug, Default)]
struct Held {
    now: AtomicUsize,
    peak: AtomicUsize,
}

/// The body of one download. It counts as held until the last `Bytes` sharing it is dropped.
struct Download {
    data: Vec<u8>,
    held: Arc<Held>,
}

impl Download {
    fn new(data: Vec<u8>, held: &Arc<Held>) -> Self {
        let now = held.now.fetch_add(data.len(), Ordering::SeqCst) + data.len();
        held.peak.fetch_max(now, Ordering::SeqCst);
        Self {
            data,
            held: Arc::clone(held),
        }
    }
}

impl AsRef<[u8]> for Download {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for Download {
    fn drop(&mut self) {
        self.held.now.fetch_sub(self.data.len(), Ordering::SeqCst);
    }
}

/// An in-memory store whose `get` copies each object, as a download from a remote store does.
#[derive(Debug)]
struct DownloadingStore {
    inner: InMemory,
    held: Arc<Held>,
}

impl fmt::Display for DownloadingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DownloadingStore")
    }
}

#[async_trait]
impl ObjectStore for DownloadingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let body = Bytes::from_owner(Download::new(result.bytes().await?.to_vec(), &self.held));
        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::once(async { Ok(body) }).boxed()),
            meta,
            range,
            attributes,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

const FILE_BYTES: usize = 256 * 1024;
const FILES: usize = 8;

/// Bytes that do not compress, so the archive is as large as its content.
fn incompressible(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

async fn store_with(spicepod: &str, files: &[(&str, Vec<u8>)]) -> DownloadingStore {
    let store = DownloadingStore {
        inner: InMemory::new(),
        held: Arc::new(Held::default()),
    };
    store
        .put(
            &Path::from("spicepod.yaml"),
            spicepod.as_bytes().to_vec().into(),
        )
        .await
        .expect("spicepod.yaml is stored");
    for (path, data) in files {
        store
            .put(&Path::from(*path), data.clone().into())
            .await
            .expect("file is stored");
    }
    store
}

fn archive_entries(zip: &Bytes) -> Vec<(String, Vec<u8>)> {
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(zip.as_ref())).expect("a valid zip archive");
    let mut entries = Vec::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).expect("archive entry");
        if file.is_dir() {
            continue;
        }
        let mut data = Vec::new();
        file.read_to_end(&mut data).expect("entry content");
        let name = file.name().expect("entry name is UTF-8").to_string();
        entries.push((name, data));
    }
    entries.sort();
    entries
}

/// A package holds one downloaded file at a time: a file is written to the archive and dropped
/// before the next is downloaded, so the content is never in memory twice (as downloads and in
/// the archive).
#[tokio::test]
async fn a_package_holds_one_downloaded_file_at_a_time() {
    let files: Vec<(String, Vec<u8>)> = (0..FILES as u64)
        .map(|i| (format!("data/part-{i}.bin"), incompressible(i, FILE_BYTES)))
        .collect();
    let file_refs: Vec<(&str, Vec<u8>)> = files
        .iter()
        .map(|(path, data)| (path.as_str(), data.clone()))
        .collect();
    let spicepod =
        "version: v1\nkind: Spicepod\nname: pkg\ndatasets:\n  - from: file:data\n    name: parts\n";
    let store = store_with(spicepod, &file_refs).await;

    let zip = package::make_zip(&store, &Path::from("spicepod.yaml"))
        .await
        .expect("the package is built");

    let mut expected: Vec<(String, Vec<u8>)> = files;
    expected.push(("spicepod.yaml".to_string(), spicepod.as_bytes().to_vec()));
    expected.sort();
    assert_eq!(archive_entries(&zip), expected);

    // The root spicepod.yaml is held until it is packaged last.
    let peak = store.held.peak.load(Ordering::SeqCst);
    assert_eq!(
        peak,
        FILE_BYTES + spicepod.len(),
        "at most one downloaded file may be held at once; {peak} bytes of {} were held together",
        FILES * FILE_BYTES + spicepod.len()
    );
    assert_eq!(store.held.now.load(Ordering::SeqCst), 0);
}

/// A file referenced twice, by two datasets and by the folder it is in, is downloaded once and
/// packaged once.
#[tokio::test]
async fn a_file_referenced_more_than_once_is_packaged_once() {
    let data = incompressible(7, FILE_BYTES);
    let spicepod = "version: v1\nkind: Spicepod\nname: pkg\ndatasets:\n  - from: file:data/a.csv\n    name: one\n  - from: file://data/a.csv\n    name: two\n  - from: file:data\n    name: all\n";
    let store = store_with(spicepod, &[("data/a.csv", data.clone())]).await;

    let zip = package::make_zip(&store, &Path::from("spicepod.yaml"))
        .await
        .expect("a file referenced more than once is packaged");

    assert_eq!(
        archive_entries(&zip),
        vec![
            ("data/a.csv".to_string(), data),
            ("spicepod.yaml".to_string(), spicepod.as_bytes().to_vec()),
        ]
    );
}
