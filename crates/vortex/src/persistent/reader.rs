// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::sync::Arc;

use bytes::Bytes;
use datafusion_common::Result as DFResult;
use datafusion_common_runtime::IoWaitExt;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use object_store::GetResult;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use vortex::io::VortexReadAt;
use vortex::io::object_store::ObjectStoreIoWait;
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

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
    remote_io_wait: bool,
}

impl DefaultVortexReaderFactory {
    /// Creates new instance
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            remote_io_wait: false,
        }
    }

    /// Mark only remote object-store waits for the runtime's I/O-wait hook.
    pub fn with_remote_io_wait(mut self, remote_io_wait: bool) -> Self {
        self.remote_io_wait = remote_io_wait;
        self
    }
}

struct DataFusionIoWait;

impl ObjectStoreIoWait for DataFusionIoWait {
    fn head(
        &self,
        future: BoxFuture<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxFuture<'static, object_store::Result<ObjectMeta>> {
        future.io_wait().boxed()
    }

    fn get(
        &self,
        future: BoxFuture<'static, object_store::Result<GetResult>>,
    ) -> BoxFuture<'static, object_store::Result<GetResult>> {
        future.io_wait().boxed()
    }

    fn stream(
        &self,
        stream: BoxStream<'static, object_store::Result<Bytes>>,
    ) -> BoxStream<'static, object_store::Result<Bytes>> {
        stream.io_wait().boxed()
    }
}

impl VortexReaderFactory for DefaultVortexReaderFactory {
    fn create_reader(&self, path: &str, session: &VortexSession) -> DFResult<VortexReader> {
        let mut reader = ObjectStoreReadAt::new(
            Arc::clone(&self.object_store),
            path.into(),
            session.handle(),
        );
        if self.remote_io_wait {
            reader = reader.with_io_wait(Arc::new(DataFusionIoWait));
        }
        Ok(Arc::new(reader) as _)
    }
}
