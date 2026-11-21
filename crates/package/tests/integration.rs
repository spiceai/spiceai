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
/// It counts the objects downloaded, and can list every object with a size of 0, as a store whose
/// listing does not know the sizes does.
#[derive(Debug)]
struct DownloadingStore {
    inner: InMemory,
    held: Arc<Held>,
    gets: AtomicUsize,
    list_sizes: bool,
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
        self.gets.fetch_add(1, Ordering::SeqCst);
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
        let list_sizes = self.list_sizes;
        self.inner
            .list(prefix)
            .map(move |meta| {
                meta.map(|mut meta| {
                    if !list_sizes {
                        meta.size = 0;
                    }
                    meta
                })
            })
            .boxed()
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
        gets: AtomicUsize::new(0),
        list_sizes: true,
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

    // The root spicepod.yaml is packaged first, and is not held while the folder's files are.
    let peak = store.held.peak.load(Ordering::SeqCst);
    assert_eq!(
        peak,
        FILE_BYTES,
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

/// A folder that holds a component file, listed before the component that references the file,
/// counts that file once: it was downloaded and counted to find the files it references, and
/// the folder does not download or count it again. A package of exactly the files and bytes its
/// budget allows is built, and one with a file or a byte fewer is refused.
#[tokio::test]
async fn a_folder_holding_a_component_file_counts_it_once() {
    let spicepod = "version: v1\nkind: Spicepod\nname: pkg\ndatasets:\n  - from: file:data\n    name: all\n  - ref: data/one\n";
    let component = "from: file:data/a.csv\nname: one\n";
    let files = [
        ("data/a.csv", incompressible(11, FILE_BYTES)),
        ("data/b.csv", incompressible(12, FILE_BYTES)),
        ("data/one/dataset.yaml", component.as_bytes().to_vec()),
    ];
    let store = store_with(spicepod, &files).await;
    let mut expected: Vec<(String, Vec<u8>)> = files
        .iter()
        .map(|(path, data)| ((*path).to_string(), data.clone()))
        .collect();
    expected.push(("spicepod.yaml".to_string(), spicepod.as_bytes().to_vec()));
    expected.sort();
    let size = |len: usize| u64::try_from(len).expect("a test file's size fits in u64");
    let package_files = expected.len();
    let package_bytes: u64 = expected.iter().map(|(_, data)| size(data.len())).sum();
    assert_eq!(package_files, 4);

    store.gets.store(0, Ordering::SeqCst);
    let zip = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(package_files, package_bytes),
    )
    .await
    .expect("a package of exactly the files and bytes of its budget is built");
    assert_eq!(archive_entries(&zip), expected);
    assert_eq!(
        store.gets.load(Ordering::SeqCst),
        package_files,
        "each file is downloaded once"
    );

    let error = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(package_files - 1, package_bytes),
    )
    .await
    .expect_err("four files are over a three-file budget");
    assert_eq!(
        error.to_string(),
        "it references more than 3 files, the most a package may hold, and dataset 'all' ('file:data') alone lists 2. Reference fewer files in that dataset. See https://spiceai.org/docs/api/http/packages"
    );

    let error = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(10_000, package_bytes - 1),
    )
    .await
    .expect_err("the package's bytes are over a budget one byte smaller");
    let package::Error::TooManyBytes {
        origin,
        bytes,
        max_bytes,
    } = error
    else {
        panic!("not a byte-budget error: {error}");
    };
    assert_eq!(
        (origin, bytes, max_bytes),
        (
            package::Origin::Dataset {
                name: "all".to_string(),
                from: "file:data".to_string(),
            },
            2 * size(FILE_BYTES),
            package_bytes - 1,
        )
    );
}

fn parts(count: u64) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|i| (format!("data/part-{i}.bin"), incompressible(i, FILE_BYTES)))
        .collect()
}

const PARTS_SPICEPOD: &str =
    "version: v1\nkind: Spicepod\nname: pkg\ndatasets:\n  - from: file:data\n    name: parts\n";

async fn parts_store(files: &[(String, Vec<u8>)], list_sizes: bool) -> DownloadingStore {
    let file_refs: Vec<(&str, Vec<u8>)> = files
        .iter()
        .map(|(path, data)| (path.as_str(), data.clone()))
        .collect();
    let mut store = store_with(PARTS_SPICEPOD, &file_refs).await;
    store.list_sizes = list_sizes;
    store.gets.store(0, Ordering::SeqCst);
    store
}

/// A folder with more files than the package may hold is refused before any of its files is
/// downloaded, naming the dataset and how many files it lists.
#[tokio::test]
async fn a_folder_over_the_file_limit_is_refused_before_its_files_are_fetched() {
    let store = parts_store(&parts(FILES as u64), true).await;
    let error = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(5, 1 << 30),
    )
    .await
    .expect_err("eight files are over a five-file budget");
    assert!(error.is_over_budget());
    assert_eq!(
        error.to_string(),
        "it references more than 5 files, the most a package may hold, and dataset 'parts' ('file:data') alone lists 8. Reference fewer files in that dataset. See https://spiceai.org/docs/api/http/packages"
    );
    assert_eq!(
        store.gets.load(Ordering::SeqCst),
        1,
        "only spicepod.yaml may be downloaded"
    );
}

/// A folder whose listed sizes are over the byte limit is refused before any of its files is
/// downloaded.
#[tokio::test]
async fn listed_sizes_over_the_byte_limit_are_refused_before_any_download() {
    let store = parts_store(&parts(FILES as u64), true).await;
    let error = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(10_000, 1 << 20),
    )
    .await
    .expect_err("2 MiB of files are over a 1 MiB budget");
    assert_eq!(
        error.to_string(),
        "its files total more than 1 MiB, the most a package may hold, and dataset 'parts' ('file:data') alone adds 2 MiB. Reference fewer or smaller files in that dataset. See https://spiceai.org/docs/api/http/packages"
    );
    assert_eq!(store.gets.load(Ordering::SeqCst), 1);
}

/// When the listing does not give sizes, the bytes are counted as they arrive: the download that
/// passes the limit stops, and no later file is fetched.
#[tokio::test]
async fn bytes_over_the_limit_stop_the_download_when_sizes_are_not_listed() {
    let store = parts_store(&parts(FILES as u64), false).await;
    let error = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(10_000, 1 << 20),
    )
    .await
    .expect_err("2 MiB of files are over a 1 MiB budget");
    // spicepod.yaml and three 256 KiB files fit; the fourth passes 1 MiB as it arrives.
    assert_eq!(
        error.to_string(),
        "its files total more than 1 MiB, the most a package may hold, and dataset 'parts' ('file:data') alone adds 1 MiB. Reference fewer or smaller files in that dataset. See https://spiceai.org/docs/api/http/packages"
    );
    assert_eq!(store.gets.load(Ordering::SeqCst), 1 + 4);
}

/// A package within its budget is unchanged by it.
#[tokio::test]
async fn a_package_within_its_budget_is_built() {
    let files = parts(FILES as u64);
    let store = parts_store(&files, false).await;
    let zip = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(9, 3 << 20),
    )
    .await
    .expect("nine files and 2 MiB fit");
    assert_eq!(archive_entries(&zip).len(), FILES + 1);
}

/// A component file over the byte budget is refused for its size, even when the file with the
/// other YAML extension does not exist.
#[tokio::test]
async fn a_component_file_over_the_budget_is_refused_for_its_size() {
    let spicepod = "version: v1\nkind: Spicepod\nname: pkg\ndatasets:\n  - ref: datasets/big\n";
    let store = store_with(
        spicepod,
        &[("datasets/big/dataset.yaml", incompressible(3, 2 << 20))],
    )
    .await;
    let error = package::make_zip_within(
        &store,
        &Path::from("spicepod.yaml"),
        package::PackageBudget::new(10_000, 1 << 20),
    )
    .await
    .expect_err("a 2 MiB component file is over a 1 MiB budget");
    assert!(error.is_over_budget(), "not a budget error: {error}");
    assert_eq!(
        error.to_string(),
        "its files total more than 1 MiB, the most a package may hold, and 'datasets/big/dataset.yaml' alone adds 2 MiB. Make that file smaller. See https://spiceai.org/docs/api/http/packages"
    );
}
