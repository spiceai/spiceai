/*
Copyright 2024 The Spice.ai OSS Authors

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
}

pub type Result<T> = std::result::Result<T, Error>;

/// Creates a zip package from the given object store and path to a spicepod.yaml.
///
/// It will parse the spicepod and find all of the linked files, and add them to the returned zip archive.
pub async fn make_zip(store: &dyn ObjectStore, spicepod_path: &Path) -> Result<Bytes> {
    let (spicepod_bytes, spicepod) = get_root_spicepod(store, spicepod_path).await?;
    let linked_file_paths = find_linked_files(&spicepod);
    let mut linked_files = Vec::new();
    for file_path in linked_file_paths {
        // TODO: Can get in parallel
        let file_bytes = get_file_bytes(store, &file_path).await?;
        linked_files.push((file_path, file_bytes));
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

fn find_linked_files(spicepod: &SpicepodDefinition) -> Vec<Path> {
    vec![]
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
