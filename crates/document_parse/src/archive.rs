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

use bytes::Bytes;
use snafu::ResultExt;
use std::io::Cursor;
use zip::ZipArchive;

use crate::{DocumentType, InternalParsingSnafu, Result};

/// Opens a zip archive, rejecting it when a local file header disagrees with
/// its central directory entry, two entries share a name, or entries overlap.
/// Such an archive can show different content to different zip readers ("zip
/// cloaking", GHSA-m665-v7pm-7grj), so it is refused before any entry is read.
pub(crate) fn open_consistent_archive(
    raw: &Bytes,
    format: DocumentType,
) -> Result<ZipArchive<Cursor<Bytes>>> {
    ZipArchive::new(Cursor::new(raw.clone()))
        .and_then(|mut archive| {
            archive.validate_header_consistency()?;
            Ok(archive)
        })
        .boxed()
        .context(InternalParsingSnafu { format })
}
