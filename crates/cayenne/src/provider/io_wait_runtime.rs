// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Carries a `Ballista` task's vcore into `Vortex` scan and I/O tasks.

use std::sync::Arc;
use std::sync::OnceLock;

use ballista_core::vcore_permit;
use futures::future::BoxFuture;
use vortex::io::runtime::AbortHandleRef;
use vortex::io::runtime::Executor;
use vortex::io::runtime::Handle;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

struct BallistaCarryingExecutor;

impl Executor for BallistaCarryingExecutor {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        Executor::spawn(
            &tokio::runtime::Handle::current(),
            Box::pin(vcore_permit::carry(future)),
        )
    }

    fn spawn_io(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        Executor::spawn_io(
            &tokio::runtime::Handle::current(),
            Box::pin(vcore_permit::carry(future)),
        )
    }

    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        Executor::spawn_cpu(
            &tokio::runtime::Handle::current(),
            Box::new(vcore_permit::carry_blocking(task)),
        )
    }

    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        Executor::spawn_blocking_io(
            &tokio::runtime::Handle::current(),
            Box::new(vcore_permit::carry_blocking(task)),
        )
    }
}

/// Configure a `Vortex` session to carry a `Ballista` task's vcore into spawned
/// split and object-store tasks. Outside `Ballista` execution, this uses the
/// current `Tokio` runtime without changing scheduling or permit accounting.
pub fn carry_ballista_vcore(session: VortexSession) -> VortexSession {
    static EXECUTOR: OnceLock<Arc<dyn Executor>> = OnceLock::new();
    let executor = EXECUTOR.get_or_init(|| Arc::new(BallistaCarryingExecutor));
    session.with_handle(Handle::new(Arc::downgrade(executor)))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use ballista_core::vcore_permit::VcoreHook;
    use ballista_core::vcore_permit::VcorePermit;
    use datafusion::common::runtime::IoWaitExt;
    use datafusion::common::runtime::IoWaitHookError;
    use datafusion::common::runtime::set_io_wait_hook;
    use tokio::sync::Semaphore;
    use tokio::sync::oneshot;

    use super::*;

    async fn probe(carried: bool) -> (usize, u64) {
        let vcores = Arc::new(Semaphore::new(1));
        let permit = VcorePermit::new(Arc::clone(&vcores), Duration::from_millis(20), None);
        permit.acquire().await.expect("acquire the test vcore");
        let session = if carried {
            carry_ballista_vcore(VortexSession::default())
        } else {
            VortexSession::default()
        };
        let (started, ready) = oneshot::channel();
        let (release, remote_response) = oneshot::channel();
        let query = tokio::spawn(Arc::clone(&permit).run(async move {
            session
                .handle()
                .spawn_io(async move {
                    let _ = started.send(());
                    // The channel controls when a remote response would arrive.
                    remote_response
                        .io_wait()
                        .await
                        .expect("release the I/O wait");
                })
                .await;
        }));
        ready.await.expect("the Vortex I/O task should start");
        if carried {
            let lent = tokio::time::timeout(Duration::from_secs(5), async {
                while vcores.available_permits() == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            assert!(lent.is_ok(), "the Vortex I/O wait should lend the vcore");
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let free_while_pending = vcores.available_permits();
        release.send(()).expect("finish the I/O wait");
        query.await.expect("the carried I/O task should complete");
        (
            free_while_pending,
            permit.stats().suspensions.load(Ordering::SeqCst),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vortex_io_wait_lends_only_with_ballista_context() {
        match set_io_wait_hook(&VcoreHook) {
            Ok(()) | Err(IoWaitHookError::AlreadySet) => {}
        }

        assert_eq!(probe(false).await, (0, 0));
        let (free_while_pending, suspensions) = probe(true).await;
        assert_eq!(free_while_pending, 1);
        assert!(suspensions > 0);
    }
}
