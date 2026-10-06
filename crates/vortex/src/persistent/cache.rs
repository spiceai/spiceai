// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use datafusion_execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion_execution::cache::cache_manager::FileMetadata;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use object_store::ObjectMeta;
use object_store::path::Path;
use vortex::dtype::StructFields;
use vortex::file::Footer;
use vortex::file::VortexFile;

/// Cached Vortex file metadata for use with `DataFusion`'s [`FileMetadataCache`].
pub struct CachedVortexMetadata {
    footer: Footer,
    /// Fixed at construction: the cache adds this on insert and subtracts it
    /// on eviction by calling [`FileMetadata::memory_size`] each time, so a
    /// value that changed in between would corrupt its running total.
    memory_size: usize,
}

/// Fixed per-entry cost of a cached footer: the entry itself and the parts of
/// the layout tree that exist once per file.
const FOOTER_BASE_BYTES: usize = 4 * 1024;

/// Heap a cached footer retains per top-level column, once a scan has
/// materialized that column's layout subtree (its zone-map schema and
/// statistics are built per column).
const FOOTER_BYTES_PER_COLUMN: usize = 2_560;

/// Heap a cached footer retains per segment, once a scan has materialized the
/// layout node that owns it.
const FOOTER_BYTES_PER_SEGMENT: usize = 768;

/// The heap a cached footer can retain, for the file-metadata cache to evict on.
///
/// `Footer::approx_byte_size` counts only the serialized flatbuffers. The
/// footer also holds the deserialized segment map and file statistics, and its
/// layout tree materializes child layouts lazily and keeps them, so an entry
/// grows while scans read through it. Counting only the serialized bytes let
/// the cache hold several times its configured limit
/// (spiceai/spiceai#12917).
///
/// None of that is observable without materializing the tree, so this charges
/// the fully-expanded size up front, from quantities the footer already knows:
/// its column and segment counts. The constants are calibrated by
/// `tests/footer_cache_accounting.rs`, which measures the heap an entry
/// actually frees and fails if the estimate falls below it.
fn footer_memory_size(footer: &Footer) -> usize {
    let columns = footer
        .dtype()
        .as_struct_fields_opt()
        .map_or(1, StructFields::nfields);
    let segments = footer.segment_map().len();
    FOOTER_BASE_BYTES
        .saturating_add(footer.approx_byte_size().unwrap_or_default())
        .saturating_add(columns.saturating_mul(FOOTER_BYTES_PER_COLUMN))
        .saturating_add(segments.saturating_mul(FOOTER_BYTES_PER_SEGMENT))
}

impl CachedVortexMetadata {
    /// Create a new cached metadata entry from a `VortexFile`.
    pub fn new(vortex_file: &VortexFile) -> Self {
        Self::from_footer(vortex_file.footer().clone())
    }

    /// Create a cached metadata entry directly from a just-written file's footer,
    /// so the write path can populate the cache without reading the file back.
    pub fn from_footer(footer: Footer) -> Self {
        let memory_size = footer_memory_size(&footer);
        Self {
            footer,
            memory_size,
        }
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
        self.memory_size
    }

    fn extra_info(&self) -> datafusion_common::HashMap<String, String> {
        datafusion_common::HashMap::default()
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
    cache: &Arc<FileMetadataCache>,
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
