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

use bytes::Bytes;
use object_store::{ObjectStore, path::Path};
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
    FailedToParseSpicepod { source: serde_yaml::Error },

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
    UnableToParseSpicepodComponent { source: serde_yaml::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

enum PathReference {
    Direct(Path),
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
            PathReference::Direct(path) => write!(f, "Direct({path})"),
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
    fn try_get_path(&self) -> Result<Path> {
        match self {
            PathReference::Direct(path) => Ok(path.clone()),
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
        PathReference::Direct(path) => path.extension().is_none(),
        PathReference::YmlOrYaml { .. } | PathReference::Retrieved { .. } => false,
    }
}

/// Creates a zip package from the given object store and path to a spicepod.yaml.
///
/// It will parse the spicepod and find all of the linked files, and add them to the returned zip archive.
pub async fn make_zip(store: &dyn ObjectStore, spicepod_path: &Path) -> Result<Bytes> {
    let mut linked_files = Vec::new();
    let (spicepod_references, spicepods) = load_spicepods(store, spicepod_path).await?;
    let mut linked_paths = find_linked_files(store, &spicepods).await?;
    linked_paths.extend(spicepod_references);

    tracing::debug!(
        "[zip package] Num linked references: {}",
        linked_paths.len()
    );

    for linked_path in linked_paths {
        // Can get in parallel
        tracing::debug!("[zip package] Extracting content for linked path: {linked_path:?}");
        if is_folder_reference(&linked_path) {
            let files = get_files_from_folder_reference(store, &linked_path).await?;
            tracing::debug!(
                "[zip package] Folder reference {:?} resolved in {} files",
                &linked_path,
                files.len()
            );
            linked_files.extend(files);
        } else {
            let file_bytes = get_file_bytes_from_reference(store, &linked_path).await?;
            linked_files.push((linked_path.try_get_path()?, file_bytes));
        }
    }

    tracing::info!(
        "[zip package] Resolved {} files, {} bytes total",
        linked_files.len(),
        linked_files
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>()
    );

    tracing::info!("[zip package] Creating zip archive...");

    // Add the root spicepod to the zip
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);

    // Add all of the linked files to the zip
    let mut directories = HashSet::new();
    for (file_path, file_bytes) in linked_files {
        let std_file_path = std::path::Path::new(file_path.as_ref());
        add_file_to_zip(
            &mut zip,
            options,
            &mut directories,
            std_file_path,
            &file_bytes,
        )?;
    }

    Ok(Bytes::from(
        zip.finish().context(FailedToCreateZipSnafu)?.into_inner(),
    ))
}

async fn load_spicepods(
    store: &dyn ObjectStore,
    spicepod_path: &Path,
) -> Result<(Vec<PathReference>, Vec<SpicepodDefinition>)> {
    let mut spicepod_references = Vec::new();
    let mut spicepods = Vec::new();

    let (root_spicepod_bytes, root_spicepod) = get_spicepod(store, spicepod_path).await?;

    for dependency in &root_spicepod.dependencies {
        let dependency_pathbuf = PathBuf::from("spicepods")
            .join(dependency)
            .join("spicepod.yaml");
        let dependency_path = Path::parse(dependency_pathbuf.to_string_lossy())
            .context(LinkedFileNotAValidPathSnafu)?;
        let (dependency_spicepod_bytes, dependency_spicepod) =
            get_spicepod(store, &dependency_path).await?;
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
) -> Result<(Bytes, SpicepodDefinition)> {
    let spicepod_bytes = get_file_bytes(store, spicepod_path).await?;
    // A clone of `Bytes` is just incrementing a reference count, so it's cheap.
    let cursor = std::io::Cursor::new(spicepod_bytes.clone());
    Ok((
        spicepod_bytes,
        serde_yaml::from_reader(cursor).context(FailedToParseSpicepodSnafu)?,
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
) -> Result<Vec<PathReference>> {
    let mut linked_files = Vec::new();

    for spicepod in spicepods {
        add_linked_components(
            store,
            &mut linked_files,
            &spicepod.catalogs,
            "catalog",
            None::<fn(&Catalog) -> Result<Vec<PathReference>>>,
        )
        .await?;
        add_linked_components(
            store,
            &mut linked_files,
            &spicepod.datasets,
            "dataset",
            Some(extract_linked_files_from_dataset),
        )
        .await?;
        add_linked_components(
            store,
            &mut linked_files,
            &spicepod.views,
            "view",
            Some(extract_linked_files_from_view),
        )
        .await?;
        add_linked_components(
            store,
            &mut linked_files,
            &spicepod.models,
            "model",
            None::<fn(&Model) -> Result<Vec<PathReference>>>,
        )
        .await?;
        add_linked_components(
            store,
            &mut linked_files,
            &spicepod.embeddings,
            "embeddings",
            None::<fn(&Embeddings) -> Result<Vec<PathReference>>>,
        )
        .await?;
        add_linked_components(
            store,
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
            )
            .await?;
            let file_path =
                PathBuf::from(component_ref.r#ref.clone()).join(format!("{component_name}.yaml"));
            linked_files.push(PathReference::Retrieved {
                file_path: file_path.clone(),
                file_bytes: component_bytes.clone(),
            });

            let component_rdr = std::io::Cursor::new(component_bytes);
            let component: ComponentType = serde_yaml::from_reader(component_rdr)
                .context(UnableToParseSpicepodComponentSnafu)?;
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
        linked_files.push(PathReference::Direct(
            Path::parse(sql_ref).context(LinkedFileNotAValidPathSnafu)?,
        ));
    }
    Ok(linked_files)
}

fn extract_linked_files_from_dataset(dataset: &Dataset) -> Result<Vec<PathReference>> {
    let mut linked_files = Vec::new();
    if let Some(from) = dataset.from.strip_prefix("file:") {
        let mut file_path = from.strip_prefix("//").unwrap_or(from);
        file_path = file_path.strip_prefix("./").unwrap_or(file_path);
        linked_files.push(PathReference::Direct(
            Path::parse(file_path).context(LinkedFileNotAValidPathSnafu)?,
        ));
    }
    Ok(linked_files)
}

async fn get_file_bytes_from_reference(
    store: &dyn ObjectStore,
    reference: &PathReference,
) -> Result<Bytes> {
    match reference {
        PathReference::Direct(path) => get_file_bytes(store, path).await,
        PathReference::YmlOrYaml {
            base_path,
            base_name,
        } => {
            let yaml_files = vec![format!("{base_name}.yaml"), format!("{base_name}.yml")];

            let mut error: Option<Error> = None;
            for yaml_file in yaml_files {
                let file_path = Path::parse(base_path.join(yaml_file).to_string_lossy())
                    .context(LinkedFileNotAValidPathSnafu)?;
                match get_file_bytes(store, &file_path).await {
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

async fn get_files_from_folder_reference(
    store: &dyn ObjectStore,
    reference: &PathReference,
) -> Result<Vec<(Path, Bytes)>> {
    let path = reference.try_get_path()?;
    let mut files_stream = store.list(Some(&path));

    let mut linked_files = Vec::new();

    while let Some(item) = files_stream.next().await {
        let file_path = item.context(FailedToReadObjectSnafu)?.location;
        let file_bytes = get_file_bytes(store, &file_path).await?;
        linked_files.push((file_path, file_bytes));
    }
    Ok(linked_files)
}

async fn get_file_bytes(store: &dyn ObjectStore, file_path: &Path) -> Result<Bytes> {
    tracing::trace!("[zip package] Downloading file content: {file_path:?}");
    store
        .get(file_path)
        .await
        .context(FailedToReadObjectSnafu)?
        .bytes()
        .await
        .context(FailedToReadObjectSnafu)
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
