// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use datafusion_execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion_execution::cache::cache_manager::FileMetadata;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use object_store::ObjectMeta;
use object_store::path::Path;
use vortex::file::Footer;
use vortex::file::VortexFile;

/// Cached Vortex file metadata for use with `DataFusion`'s [`FileMetadataCache`].
pub struct CachedVortexMetadata {
    footer: Footer,
}

impl CachedVortexMetadata {
    /// Create a new cached metadata entry from a `VortexFile`.
    pub fn new(vortex_file: &VortexFile) -> Self {
        Self::from_footer(vortex_file.footer().clone())
    }

    /// Create a cached metadata entry directly from a just-written file's footer,
    /// so the write path can populate the cache without reading the file back.
    pub fn from_footer(footer: Footer) -> Self {
        Self { footer }
    }

    /// Get the cached footer.
    pub fn footer(&self) -> &Footer {
        &self.footer
    }
}

impl FileMetadata for CachedVortexMetadata {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn memory_size(&self) -> usize {
        self.footer
            .approx_byte_size()
            // 64KB is not an insane estimate...
            // We just want to avoid returning zero and _never_ being evicted from the cache.
            .unwrap_or(1024 * 64)
    }

    fn extra_info(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::default()
    }
}

/// The `ObjectMeta` for files whose consumers list from recorded metadata (a
/// catalog / metastore) rather than the object store: the recorded file size
/// plus the Unix-epoch mtime. [`CachedFileMetadataEntry::is_valid_for`]
/// compares size and mtime *exactly*, so a writer caching an entry at write
/// time and a reader listing from recorded metadata must both build their
/// metas through this constructor for the entry to ever hit.
#[must_use]
pub fn synthetic_object_meta(location: Path, size: u64) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: std::time::SystemTime::UNIX_EPOCH.into(),
        size,
        e_tag: None,
        version: None,
    }
}

/// Insert a footer into the file-metadata cache, emitting the footer-cache
/// right-sizing telemetry (the accounted footer size is what fills the cache
/// budget) shared by every population site.
pub(crate) fn cache_footer(
    cache: &Arc<dyn FileMetadataCache>,
    meta: ObjectMeta,
    cached: Arc<CachedVortexMetadata>,
    src: &'static str,
) {
    tracing::debug!(
        target: "vortex::footer_cache",
        path = %meta.location,
        footer_bytes = cached.memory_size(),
        src,
        "footer cached",
    );
    let location = meta.location.clone();
    cache.put(&location, CachedFileMetadataEntry::new(meta, cached));
}
