// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Carries a Ballista task's vcore into Vortex scan and I/O tasks.

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

/// Configure a Vortex session to carry a Ballista task's vcore into spawned
/// split and object-store tasks. Outside Ballista execution, this uses the
/// current Tokio runtime without changing scheduling or permit accounting.
pub fn carry_ballista_vcore(session: VortexSession) -> VortexSession {
    static EXECUTOR: OnceLock<Arc<dyn Executor>> = OnceLock::new();
    let executor = EXECUTOR.get_or_init(|| Arc::new(BallistaCarryingExecutor));
    session.with_handle(Handle::new(Arc::downgrade(executor)))
}
