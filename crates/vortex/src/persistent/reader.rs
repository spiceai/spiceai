// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result as DFResult;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use vortex::array::buffer::BufferHandle;
use vortex::buffer::Alignment;
use vortex::error::VortexResult;
use vortex::io::CoalesceConfig;
use vortex::io::VortexReadAt;
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use super::scan_metrics::ScanOperation;

/// Counts calls without changing their scheduling, coalescing, or cancellation behavior.
#[derive(Clone)]
pub(super) struct ScanReadAt(pub(super) VortexReader);

impl VortexReadAt for ScanReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        self.0.uri()
    }

    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        self.0.coalesce_config()
    }

    fn concurrency(&self) -> usize {
        self.0.concurrency()
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        self.0.size()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        ScanOperation::ReadRequest.record();
        self.0.read_at(offset, length, alignment)
    }
}

/// Shared reader for a Vortex object.
pub type VortexReader = Arc<dyn VortexReadAt>;

/// Factory to create [`VortexReadAt`] instances to read the target file.
pub trait VortexReaderFactory: Debug + Send + Sync + 'static {
    /// Create a reader for a target object.
    fn create_reader(&self, path: &str, session: &VortexSession) -> DFResult<VortexReader>;
}

/// Default factory, creates [`ObjectStore`] backed readers for files,
/// works with multiple cloud providers.
#[derive(Debug)]
pub struct DefaultVortexReaderFactory {
    object_store: Arc<dyn ObjectStore>,
}

impl DefaultVortexReaderFactory {
    /// Creates new instance
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }
}

impl VortexReaderFactory for DefaultVortexReaderFactory {
    fn create_reader(&self, path: &str, session: &VortexSession) -> DFResult<VortexReader> {
        Ok(Arc::new(ObjectStoreReadAt::new(
            Arc::clone(&self.object_store),
            path.into(),
            session.handle(),
        )) as _)
    }
}
