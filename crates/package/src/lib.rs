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
use object_store::{path::Path, ObjectStore};
use snafu::prelude::*;
use spicepod::component::view::View;
use spicepod::component::ComponentOrReference;
use spicepod::spec::SpicepodDefinition;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read object from object store.\n{}", source))]
    FailedToReadObject { source: object_store::Error },

    #[snafu(display("Unable to parse the provided Spicepod.\n{}", source))]
    FailedToParseSpicepod { source: serde_yaml::Error },

    #[snafu(display("Failed to create zip archive.\n{}", source))]
    FailedToCreateZip { source: zip::result::ZipError },

    #[snafu(display("Failed to write to zip archive.\n{}", source))]
    FailedToWriteZipFile { source: std::io::Error },

    #[snafu(display(
        "A file referenced by the Spicepod ({}) could not be retrieved.\n{}",
        linked_file_path.display(),
        source
    ))]
    FailedToGetLinkedFile {
        linked_file_path: PathBuf,
        source: object_store::Error,
    },

    #[snafu(display("A file referenced by the Spicepod is not a valid path.\n{}", source))]
    LinkedFileNotAValidPath { source: object_store::path::Error },

    #[snafu(display("Failed to parse the provided Spicepod component.\n{}", source))]
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

/// Creates a zip package from the given object store and path to a spicepod.yaml.
///
/// It will parse the spicepod and find all of the linked files, and add them to the returned zip archive.
pub async fn make_zip(store: &dyn ObjectStore, spicepod_path: &Path) -> Result<Bytes> {
    let (spicepod_bytes, spicepod) = get_root_spicepod(store, spicepod_path).await?;
    let linked_file_paths = find_linked_files(store, &spicepod).await?;
    let mut linked_files = Vec::new();
    for file_path in linked_file_paths {
        // Can get in parallel
        let file_bytes = get_file_bytes_from_reference(store, &file_path).await?;
        linked_files.push((file_path.try_get_path()?, file_bytes));
    }

    // Add the root spicepod to the zip
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    zip.start_file("spicepod.yaml", options)
        .context(FailedToCreateZipSnafu)?;
    zip.write_all(&spicepod_bytes)
        .context(FailedToWriteZipFileSnafu)?;

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

async fn get_root_spicepod(
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
///
/// This could be improved to also include references to local data files for the file data connector.
async fn find_linked_files(
    store: &dyn ObjectStore,
    spicepod: &SpicepodDefinition,
) -> Result<Vec<PathReference>> {
    let mut linked_files = Vec::new();

    for dependency in &spicepod.dependencies {
        let dependency_path = PathBuf::from("spicepods").join(dependency);
        linked_files.push(PathReference::YmlOrYaml {
            base_path: dependency_path,
            base_name: "spicepod",
        });
    }

    add_linked_components(&mut linked_files, &spicepod.catalogs, "catalog");
    add_linked_components(&mut linked_files, &spicepod.datasets, "dataset");
    add_linked_views(store, &mut linked_files, &spicepod.views).await?;
    add_linked_components(&mut linked_files, &spicepod.models, "model");
    add_linked_components(&mut linked_files, &spicepod.embeddings, "embeddings");
    add_linked_components(&mut linked_files, &spicepod.tools, "tool");

    Ok(linked_files)
}

fn add_linked_components<ComponentType>(
    linked_files: &mut Vec<PathReference>,
    components: &Vec<ComponentOrReference<ComponentType>>,
    component_name: &'static str,
) {
    for component in components {
        let ComponentOrReference::Reference(component_ref) = component else {
            continue;
        };

        linked_files.push(PathReference::YmlOrYaml {
            base_path: PathBuf::from(component_ref.r#ref.clone()),
            base_name: component_name,
        });
    }
}

/// Views are a special case since their referenced components can also reference other SQL files.
async fn add_linked_views(
    store: &dyn ObjectStore,
    linked_files: &mut Vec<PathReference>,
    views: &Vec<ComponentOrReference<View>>,
) -> Result<()> {
    for view in views {
        if let ComponentOrReference::Component(view) = &view {
            if let Some(sql_ref) = &view.sql_ref {
                linked_files.push(PathReference::Direct(
                    Path::parse(sql_ref).context(LinkedFileNotAValidPathSnafu)?,
                ));
            }
        }

        let ComponentOrReference::Reference(component_ref) = view else {
            continue;
        };

        // Need to download the view file to see if there are any `sql_ref`s
        let referenced_view_bytes = get_file_bytes_from_reference(
            store,
            &PathReference::YmlOrYaml {
                base_path: PathBuf::from(component_ref.r#ref.clone()),
                base_name: "view",
            },
        )
        .await?;

        let file_path = PathBuf::from(component_ref.r#ref.clone()).join("view.yaml");
        linked_files.push(PathReference::Retrieved {
            file_path: file_path.clone(),
            file_bytes: referenced_view_bytes.clone(),
        });

        let view_rdr = std::io::Cursor::new(referenced_view_bytes);
        let view: View =
            serde_yaml::from_reader(view_rdr).context(UnableToParseSpicepodComponentSnafu)?;

        if let Some(sql_ref) = &view.sql_ref {
            linked_files.push(PathReference::Direct(
                Path::parse(sql_ref).context(LinkedFileNotAValidPathSnafu)?,
            ));
        }
    }

    Ok(())
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

async fn get_file_bytes(store: &dyn ObjectStore, file_path: &Path) -> Result<Bytes> {
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
