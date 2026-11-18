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

#![allow(clippy::missing_errors_doc)]

//! Responsible for fetching Spicepods from an object store and packaging them into a zip file.

use std::path::PathBuf;
use std::{collections::HashSet, io::Write};

use bytes::{Bytes, BytesMut};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use snafu::prelude::*;
use spicepod::component::ComponentOrReference;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::embeddings::Embeddings;
use spicepod::component::model::Model;
use spicepod::component::tool::Tool;
use spicepod::component::view::View;
use spicepod::spec::SpicepodDefinition;

use futures::StreamExt;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read object from object store. {}", source))]
    FailedToReadObject { source: object_store::Error },

    #[snafu(display("Unable to parse the provided Spicepod. {}", source))]
    FailedToParseSpicepod { source: yaml::Error },

    #[snafu(display("Failed to create zip archive. {}", source))]
    FailedToCreateZip { source: zip::result::ZipError },

    #[snafu(display("Failed to write to zip archive. {}", source))]
    FailedToWriteZipFile { source: std::io::Error },

    #[snafu(display(
        "A file referenced by the Spicepod ({}) could not be retrieved. {}",
        linked_file_path.display(),
        source
    ))]
    FailedToGetLinkedFile {
        linked_file_path: PathBuf,
        source: object_store::Error,
    },

    #[snafu(display("A file referenced by the Spicepod is not a valid path. {}", source))]
    LinkedFileNotAValidPath { source: object_store::path::Error },

    #[snafu(display("Failed to parse the provided Spicepod component. {}", source))]
    UnableToParseSpicepodComponent { source: yaml::Error },

    #[snafu(display("Failed to create zip archive. {}", source))]
    ZipTaskFailed { source: tokio::task::JoinError },

    #[snafu(display(
        "it references more than {} files, the most a package may hold, and {origin} alone lists {}. {} See https://spiceai.org/docs/api/http/packages",
        thousands(max_files),
        thousands(files),
        origin.fewer_files_fix()
    ))]
    TooManyFiles {
        origin: Origin,
        files: usize,
        max_files: usize,
    },

    #[snafu(display(
        "its files total more than {}, the most a package may hold, and {origin} alone adds {}. {} See https://spiceai.org/docs/api/http/packages",
        human_bytes(*max_bytes),
        human_bytes(*bytes),
        origin.smaller_files_fix()
    ))]
    TooManyBytes {
        origin: Origin,
        bytes: u64,
        max_bytes: u64,
    },
}

impl Error {
    /// Whether the package was refused for its size: more files or bytes than a package may hold.
    #[must_use]
    pub fn is_over_budget(&self) -> bool {
        matches!(self, Self::TooManyFiles { .. } | Self::TooManyBytes { .. })
    }
}

/// What made a file part of a package, named in an over-budget error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// A dataset's `from: file:…` path, a file or a folder.
    Dataset { name: String, from: String },
    /// A view's `sql_ref`.
    View { name: String, sql_ref: String },
    /// A Spicepod or component file.
    File { path: String },
}

impl Origin {
    fn fewer_files_fix(&self) -> &'static str {
        match self {
            Self::Dataset { .. } => "Reference fewer files in that dataset.",
            Self::View { .. } | Self::File { .. } => "Reference fewer files.",
        }
    }

    fn smaller_files_fix(&self) -> &'static str {
        match self {
            Self::Dataset { .. } => "Reference fewer or smaller files in that dataset.",
            Self::View { .. } | Self::File { .. } => "Make that file smaller.",
        }
    }
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dataset { name, from } => write!(f, "dataset '{name}' ('{from}')"),
            Self::View { name, sql_ref } => write!(f, "view '{name}' ('{sql_ref}')"),
            Self::File { path } => write!(f, "'{path}'"),
        }
    }
}

/// `n` with thousands separators: `12,400`.
fn thousands(n: impl std::fmt::Display) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// `bytes` in the largest binary unit it reaches, to one decimal place, dropping a trailing
/// `.0`: `1 GiB`, `1.4 GiB`, `512 MiB`, `300 bytes`.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 3] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (unit, size) in UNITS {
        if bytes >= size {
            let tenths = u128::from(bytes) * 10 / u128::from(size);
            let (whole, tenth) = (tenths / 10, tenths % 10);
            return if tenth == 0 {
                format!("{whole} {unit}")
            } else {
                format!("{whole}.{tenth} {unit}")
            };
        }
    }
    format!("{bytes} bytes")
}

/// The most files and bytes one package may hold, and what it holds so far. A file counts when
/// it is downloaded, and its bytes count as they arrive, so a listed size that is missing or
/// wrong cannot get a package past the budget.
#[derive(Debug, Clone)]
pub struct PackageBudget {
    max_files: usize,
    max_bytes: u64,
    files: usize,
    bytes: u64,
}

impl Default for PackageBudget {
    fn default() -> Self {
        Self::new(MAX_PACKAGE_FILES, MAX_PACKAGE_BYTES)
    }
}

/// The most files a package may hold.
pub const MAX_PACKAGE_FILES: usize = 10_000;
/// The most bytes a package may download in total (1 GiB).
pub const MAX_PACKAGE_BYTES: u64 = 1 << 30;

impl PackageBudget {
    /// A budget of `max_files` files and `max_bytes` bytes.
    #[must_use]
    pub fn new(max_files: usize, max_bytes: u64) -> Self {
        Self {
            max_files,
            max_bytes,
            files: 0,
            bytes: 0,
        }
    }

    /// Refuses `files` more files from `origin`, before any is downloaded, when they would pass
    /// the file limit.
    fn check_files(&self, origin: &Origin, files: usize) -> Result<()> {
        ensure!(
            self.files.saturating_add(files) <= self.max_files,
            TooManyFilesSnafu {
                origin: origin.clone(),
                files,
                max_files: self.max_files,
            }
        );
        Ok(())
    }

    /// Refuses `bytes` more listed bytes from `origin`, before they are downloaded, when they
    /// would pass the byte limit.
    fn check_bytes(&self, origin: &Origin, bytes: u64) -> Result<()> {
        ensure!(
            self.bytes.saturating_add(bytes) <= self.max_bytes,
            TooManyBytesSnafu {
                origin: origin.clone(),
                bytes,
                max_bytes: self.max_bytes,
            }
        );
        Ok(())
    }

    /// Counts one downloaded file from `origin`, which has listed `origin_files` files.
    fn count_file(&mut self, origin: &Origin, origin_files: usize) -> Result<()> {
        ensure!(
            self.files < self.max_files,
            TooManyFilesSnafu {
                origin: origin.clone(),
                files: origin_files,
                max_files: self.max_files,
            }
        );
        self.files += 1;
        Ok(())
    }

    /// Counts `bytes` received from `origin`, which has now added `origin_bytes` in all.
    fn count_bytes(&mut self, origin: &Origin, bytes: u64, origin_bytes: u64) -> Result<()> {
        let total = self.bytes.saturating_add(bytes);
        ensure!(
            total <= self.max_bytes,
            TooManyBytesSnafu {
                origin: origin.clone(),
                bytes: origin_bytes,
                max_bytes: self.max_bytes,
            }
        );
        self.bytes = total;
        Ok(())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

enum PathReference {
    Direct {
        path: Path,
        origin: Origin,
    },
    YmlOrYaml {
        base_path: PathBuf,
        base_name: &'static str,
    },
    Retrieved {
        file_path: PathBuf,
        file_bytes: Bytes,
    },
}

impl std::fmt::Debug for PathReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathReference::Direct { path, .. } => write!(f, "Direct({path})"),
            PathReference::YmlOrYaml {
                base_path,
                base_name,
            } => {
                write!(f, "YmlOrYaml({base_path:?}, {base_name})")
            }
            PathReference::Retrieved {
                file_path,
                file_bytes,
            } => {
                write!(f, "Retrieved({file_path:?}, {} bytes)", file_bytes.len())
            }
        }
    }
}

impl PathReference {
    /// What made this file part of the package.
    fn origin(&self) -> Origin {
        match self {
            PathReference::Direct { origin, .. } => origin.clone(),
            PathReference::YmlOrYaml {
                base_path,
                base_name,
            } => Origin::File {
                path: base_path
                    .join(format!("{base_name}.yaml"))
                    .to_string_lossy()
                    .to_string(),
            },
            PathReference::Retrieved { file_path, .. } => Origin::File {
                path: file_path.to_string_lossy().to_string(),
            },
        }
    }

    fn try_get_path(&self) -> Result<Path> {
        match self {
            PathReference::Direct { path, .. } => Ok(path.clone()),
            PathReference::YmlOrYaml {
                base_path,
                base_name,
            } => Path::parse(
                base_path
                    .join(format!("{base_name}.yaml"))
                    .to_string_lossy(),
            )
            .context(LinkedFileNotAValidPathSnafu),
            PathReference::Retrieved { file_path, .. } => {
                Path::parse(file_path.to_string_lossy()).context(LinkedFileNotAValidPathSnafu)
            }
        }
    }
}

/// Checks if the given path is a folder reference.
fn is_folder_reference(path: &PathReference) -> bool {
    match path {
        // A simplified approach to check if the path is a folder, but works well for our use case
        PathReference::Direct { path, .. } => path.extension().is_none(),
        PathReference::YmlOrYaml { .. } | PathReference::Retrieved { .. } => false,
    }
}

/// Creates a zip package from the given object store and path to a spicepod.yaml, within the
/// product [`PackageBudget`]: [`MAX_PACKAGE_FILES`] files and [`MAX_PACKAGE_BYTES`] bytes.
///
/// It will parse the spicepod and find all of the linked files, and add them to the returned zip archive.
///
/// A data file (a view's SQL, a dataset's file, or each file of a dataset's folder) is added to the
/// archive as soon as it is downloaded and is not kept after that, so besides the archive the
/// package holds the Spicepod and component files it parsed and at most one data file. A file
/// referenced more than once (by two datasets, or by a folder and a file in it) is downloaded and
/// added once.
pub async fn make_zip(store: &dyn ObjectStore, spicepod_path: &Path) -> Result<Bytes> {
    make_zip_within(store, spicepod_path, PackageBudget::default()).await
}

/// [`make_zip`] within `budget`. A package that would pass it is refused as soon as that is
/// known: a folder's listed file count and sizes are checked before its files are downloaded, and
/// every file's bytes are counted as they arrive.
pub async fn make_zip_within(
    store: &dyn ObjectStore,
    spicepod_path: &Path,
    mut budget: PackageBudget,
) -> Result<Bytes> {
    let (spicepod_references, spicepods) =
        load_spicepods(store, spicepod_path, &mut budget).await?;
    let mut linked_paths = find_linked_files(store, &spicepods, &mut budget).await?;
    linked_paths.extend(spicepod_references);

    tracing::debug!(
        "[zip package] Num linked references: {}",
        linked_paths.len()
    );

    let mut zip = ZipArchive::new();
    for linked_path in linked_paths {
        tracing::debug!("[zip package] Extracting content for linked path: {linked_path:?}");
        let origin = linked_path.origin();
        if is_folder_reference(&linked_path) {
            let folder = linked_path.try_get_path()?;
            let mut files = Vec::new();
            let mut files_stream = store.list(Some(&folder));
            while let Some(item) = files_stream.next().await {
                let meta = item.context(FailedToReadObjectSnafu)?;
                if !zip.contains(&meta.location) {
                    files.push(meta);
                }
            }
            drop(files_stream);
            budget.check_files(&origin, files.len())?;
            budget.check_bytes(&origin, files.iter().map(|meta| meta.size).sum())?;
            let listed = files.len();
            let mut origin_bytes = 0;
            for meta in files {
                let file_bytes = download(
                    store,
                    &meta.location,
                    &mut budget,
                    &origin,
                    listed,
                    &mut origin_bytes,
                )
                .await?;
                zip = zip.add(meta.location, file_bytes).await?;
            }
            tracing::debug!(
                "[zip package] Folder reference {:?} resolved in {listed} files",
                &linked_path,
            );
        } else {
            let file_path = linked_path.try_get_path()?;
            if zip.contains(&file_path) {
                continue;
            }
            let file_bytes =
                get_file_bytes_from_reference(store, &linked_path, &mut budget).await?;
            zip = zip.add(file_path, file_bytes).await?;
        }
    }

    tracing::info!(
        "[zip package] Packaged {} files, {} bytes total",
        zip.files.len(),
        zip.input_bytes
    );

    zip.finish().await
}

/// A zip archive built in memory. Compressing a file is CPU work, so each file is written on the
/// blocking thread pool rather than on the async runtime.
struct ZipArchive {
    writer: zip::ZipWriter<std::io::Cursor<Vec<u8>>>,
    /// The directories already added to the archive.
    directories: HashSet<String>,
    /// The paths of the files already added to the archive.
    files: HashSet<Path>,
    /// The bytes of the files added, before compression.
    input_bytes: usize,
}

impl ZipArchive {
    fn new() -> Self {
        Self {
            writer: zip::ZipWriter::new(std::io::Cursor::new(Vec::new())),
            directories: HashSet::new(),
            files: HashSet::new(),
            input_bytes: 0,
        }
    }

    fn contains(&self, file_path: &Path) -> bool {
        self.files.contains(file_path)
    }

    /// Adds `file_bytes` to the archive as `file_path`, and drops them.
    async fn add(mut self, file_path: Path, file_bytes: Bytes) -> Result<Self> {
        let input_bytes = file_bytes.len();
        let mut zip = tokio::task::spawn_blocking(move || {
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            add_file_to_zip(
                &mut self.writer,
                options,
                &mut self.directories,
                std::path::Path::new(file_path.as_ref()),
                &file_bytes,
            )
            .map(|()| {
                self.files.insert(file_path);
                self
            })
        })
        .await
        .context(ZipTaskFailedSnafu)??;
        zip.input_bytes = zip.input_bytes.saturating_add(input_bytes);
        Ok(zip)
    }

    async fn finish(self) -> Result<Bytes> {
        let archive = tokio::task::spawn_blocking(move || self.writer.finish())
            .await
            .context(ZipTaskFailedSnafu)?
            .context(FailedToCreateZipSnafu)?;
        Ok(Bytes::from(archive.into_inner()))
    }
}

async fn load_spicepods(
    store: &dyn ObjectStore,
    spicepod_path: &Path,
    budget: &mut PackageBudget,
) -> Result<(Vec<PathReference>, Vec<SpicepodDefinition>)> {
    let mut spicepod_references = Vec::new();
    let mut spicepods = Vec::new();

    let (root_spicepod_bytes, root_spicepod) = get_spicepod(store, spicepod_path, budget).await?;

    for dependency in &root_spicepod.dependencies {
        let dependency_pathbuf = PathBuf::from("spicepods")
            .join(dependency)
            .join("spicepod.yaml");
        let dependency_path = Path::parse(dependency_pathbuf.to_string_lossy())
            .context(LinkedFileNotAValidPathSnafu)?;
        let (dependency_spicepod_bytes, dependency_spicepod) =
            get_spicepod(store, &dependency_path, budget).await?;
        spicepod_references.push(PathReference::Retrieved {
            file_path: dependency_pathbuf,
            file_bytes: dependency_spicepod_bytes,
        });
        spicepods.push(dependency_spicepod);
    }
    spicepod_references.insert(
        0,
        PathReference::Retrieved {
            file_path: PathBuf::from("spicepod.yaml"),
            file_bytes: root_spicepod_bytes,
        },
    );
    spicepods.insert(0, root_spicepod);

    Ok((spicepod_references, spicepods))
}

async fn get_spicepod(
    store: &dyn ObjectStore,
    spicepod_path: &Path,
    budget: &mut PackageBudget,
) -> Result<(Bytes, SpicepodDefinition)> {
    let origin = Origin::File {
        path: spicepod_path.to_string(),
    };
    let spicepod_bytes = download(store, spicepod_path, budget, &origin, 1, &mut 0).await?;
    // A clone of `Bytes` is just incrementing a reference count, so it's cheap.
    let cursor = std::io::Cursor::new(spicepod_bytes.clone());
    Ok((
        spicepod_bytes,
        yaml::from_reader(cursor).context(FailedToParseSpicepodSnafu)?,
    ))
}

/// Finds all of the files that are referenced by the given Spicepod.
///
/// References currently include:
/// - `dependencies` to other Spicepods
/// - `ref` for component references
/// - `views.sql_ref` for references to SQL files
/// - `datasets.from: file://<path>` for references to local data files
async fn find_linked_files(
    store: &dyn ObjectStore,
    spicepods: &[SpicepodDefinition],
    budget: &mut PackageBudget,
) -> Result<Vec<PathReference>> {
    let mut linked_files = Vec::new();

    for spicepod in spicepods {
        add_linked_components(
            store,
            budget,
            &mut linked_files,
            &spicepod.catalogs,
            "catalog",
            None::<fn(&Catalog) -> Result<Vec<PathReference>>>,
        )
        .await?;
        add_linked_components(
            store,
            budget,
            &mut linked_files,
            &spicepod.datasets,
            "dataset",
            Some(extract_linked_files_from_dataset),
        )
        .await?;
        add_linked_components(
            store,
            budget,
            &mut linked_files,
            &spicepod.views,
            "view",
            Some(extract_linked_files_from_view),
        )
        .await?;
        add_linked_components(
            store,
            budget,
            &mut linked_files,
            &spicepod.models,
            "model",
            None::<fn(&Model) -> Result<Vec<PathReference>>>,
        )
        .await?;
        add_linked_components(
            store,
            budget,
            &mut linked_files,
            &spicepod.embeddings,
            "embeddings",
            None::<fn(&Embeddings) -> Result<Vec<PathReference>>>,
        )
        .await?;
        add_linked_components(
            store,
            budget,
            &mut linked_files,
            &spicepod.tools,
            "tool",
            None::<fn(&Tool) -> Result<Vec<PathReference>>>,
        )
        .await?;
    }

    Ok(linked_files)
}

async fn add_linked_components<ComponentType>(
    store: &dyn ObjectStore,
    budget: &mut PackageBudget,
    linked_files: &mut Vec<PathReference>,
    components: &Vec<ComponentOrReference<ComponentType>>,
    component_name: &'static str,
    extract_linked_files: Option<impl Fn(&ComponentType) -> Result<Vec<PathReference>>>,
) -> Result<()>
where
    ComponentType: serde::de::DeserializeOwned,
{
    for component in components {
        if let ComponentOrReference::Component(component) = component
            && let Some(ref extract_linked_files) = extract_linked_files
        {
            linked_files.extend(extract_linked_files(component)?);
        }

        let ComponentOrReference::Reference(component_ref) = component else {
            continue;
        };

        // If we have a method to extract linked files, we need to materialize the component and call it.
        // Otherwise, we just add the reference to the linked_files list.
        if let Some(ref extract_linked_files) = extract_linked_files {
            let component_bytes = get_file_bytes_from_reference(
                store,
                &PathReference::YmlOrYaml {
                    base_path: PathBuf::from(component_ref.r#ref.clone()),
                    base_name: component_name,
                },
                budget,
            )
            .await?;
            let file_path =
                PathBuf::from(component_ref.r#ref.clone()).join(format!("{component_name}.yaml"));
            linked_files.push(PathReference::Retrieved {
                file_path: file_path.clone(),
                file_bytes: component_bytes.clone(),
            });

            let component_rdr = std::io::Cursor::new(component_bytes);
            let component: ComponentType =
                yaml::from_reader(component_rdr).context(UnableToParseSpicepodComponentSnafu)?;
            linked_files.extend(extract_linked_files(&component)?);
        } else {
            linked_files.push(PathReference::YmlOrYaml {
                base_path: PathBuf::from(component_ref.r#ref.clone()),
                base_name: component_name,
            });
        }
    }

    Ok(())
}

fn extract_linked_files_from_view(view: &View) -> Result<Vec<PathReference>> {
    let mut linked_files = Vec::new();
    if let Some(sql_ref) = &view.sql_ref {
        linked_files.push(PathReference::Direct {
            path: Path::parse(sql_ref).context(LinkedFileNotAValidPathSnafu)?,
            origin: Origin::View {
                name: view.name.clone(),
                sql_ref: sql_ref.clone(),
            },
        });
    }
    Ok(linked_files)
}

fn extract_linked_files_from_dataset(dataset: &Dataset) -> Result<Vec<PathReference>> {
    let mut linked_files = Vec::new();
    if let Some(from) = dataset.from.strip_prefix("file:") {
        let mut file_path = from.strip_prefix("//").unwrap_or(from);
        file_path = file_path.strip_prefix("./").unwrap_or(file_path);
        linked_files.push(PathReference::Direct {
            path: Path::parse(file_path).context(LinkedFileNotAValidPathSnafu)?,
            origin: Origin::Dataset {
                name: dataset.name.clone(),
                from: dataset.from.clone(),
            },
        });
    }
    Ok(linked_files)
}

async fn get_file_bytes_from_reference(
    store: &dyn ObjectStore,
    reference: &PathReference,
    budget: &mut PackageBudget,
) -> Result<Bytes> {
    let origin = reference.origin();
    match reference {
        PathReference::Direct { path, .. } => {
            download(store, path, budget, &origin, 1, &mut 0).await
        }
        PathReference::YmlOrYaml {
            base_path,
            base_name,
        } => {
            let yaml_files = vec![format!("{base_name}.yaml"), format!("{base_name}.yml")];

            let mut error: Option<Error> = None;
            for yaml_file in yaml_files {
                let file_path = Path::parse(base_path.join(yaml_file).to_string_lossy())
                    .context(LinkedFileNotAValidPathSnafu)?;
                match download(store, &file_path, budget, &origin, 1, &mut 0).await {
                    Ok(bytes) => return Ok(bytes),
                    Err(e) => error = Some(e),
                }
            }

            let Some(error) = error else {
                unreachable!(
                    "unexpected error while trying to find a yaml file for a component reference"
                )
            };
            Err(error)
        }
        PathReference::Retrieved { file_bytes, .. } => Ok(file_bytes.clone()),
    }
}

/// Downloads `file_path`, counting it as a file of `origin` (which lists `origin_files`) and its
/// bytes as they arrive, so the download stops as soon as the package would pass its budget.
/// `origin_bytes` is what `origin` has added so far, for the error.
async fn download(
    store: &dyn ObjectStore,
    file_path: &Path,
    budget: &mut PackageBudget,
    origin: &Origin,
    origin_files: usize,
    origin_bytes: &mut u64,
) -> Result<Bytes> {
    tracing::trace!("[zip package] Downloading file content: {file_path:?}");
    let mut stream = store
        .get(file_path)
        .await
        .context(FailedToReadObjectSnafu)?
        .into_stream();
    budget.count_file(origin, origin_files)?;
    let mut chunks: Vec<Bytes> = Vec::new();
    let mut total = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context(FailedToReadObjectSnafu)?;
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        *origin_bytes = origin_bytes.saturating_add(len);
        budget.count_bytes(origin, len, *origin_bytes)?;
        total = total.saturating_add(chunk.len());
        chunks.push(chunk);
    }
    // A body that arrived in one chunk is returned as it is, without a copy.
    if chunks.len() <= 1 {
        return Ok(chunks.pop().unwrap_or_default());
    }
    let mut body = BytesMut::with_capacity(total);
    for chunk in chunks {
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn add_file_to_zip(
    zip: &mut zip::ZipWriter<std::io::Cursor<Vec<u8>>>,
    options: zip::write::SimpleFileOptions,
    directories: &mut HashSet<String>,
    file_path: &std::path::Path,
    file_bytes: &Bytes,
) -> Result<()> {
    let zip_path = file_path
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");

    // Create parent directories if they don't exist.
    if let Some(parent) = std::path::Path::new(&zip_path).parent() {
        let mut current = PathBuf::new();
        for component in parent.components() {
            current.push(component);
            let dir_path = current.to_string_lossy().to_string() + "/";
            if directories.insert(dir_path.clone()) {
                // Only try to create directory if we haven't yet
                zip.add_directory(&dir_path, options)
                    .context(FailedToCreateZipSnafu)?;
            }
        }
    }

    zip.start_file(zip_path, options)
        .context(FailedToCreateZipSnafu)?;
    zip.write_all(file_bytes)
        .context(FailedToWriteZipFileSnafu)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The over-budget messages at the product limits, as the API returns them after
    /// "Failed to generate the package from '…': ".
    #[test]
    fn over_budget_messages_name_the_limit_the_dataset_and_the_fix() {
        let files = Error::TooManyFiles {
            origin: Origin::Dataset {
                name: "images".to_string(),
                from: "file:assets/".to_string(),
            },
            files: 12_400,
            max_files: MAX_PACKAGE_FILES,
        };
        assert_eq!(
            files.to_string(),
            "it references more than 10,000 files, the most a package may hold, and dataset 'images' ('file:assets/') alone lists 12,400. Reference fewer files in that dataset. See https://spiceai.org/docs/api/http/packages"
        );
        let bytes = Error::TooManyBytes {
            origin: Origin::Dataset {
                name: "trips".to_string(),
                from: "file:data/trips/".to_string(),
            },
            bytes: 1_503_238_554,
            max_bytes: MAX_PACKAGE_BYTES,
        };
        assert_eq!(
            bytes.to_string(),
            "its files total more than 1 GiB, the most a package may hold, and dataset 'trips' ('file:data/trips/') alone adds 1.4 GiB. Reference fewer or smaller files in that dataset. See https://spiceai.org/docs/api/http/packages"
        );
        assert!(files.is_over_budget() && bytes.is_over_budget());
    }

    #[test]
    fn sizes_and_counts_read_as_people_write_them() {
        assert_eq!(human_bytes(300), "300 bytes");
        assert_eq!(human_bytes(1 << 10), "1 KiB");
        assert_eq!(human_bytes(512 << 20), "512 MiB");
        assert_eq!(human_bytes((1 << 30) + (1 << 29)), "1.5 GiB");
        assert_eq!(thousands(7), "7");
        assert_eq!(thousands(10_000), "10,000");
        assert_eq!(thousands(1_234_567), "1,234,567");
    }
}
