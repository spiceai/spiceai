// If calling directly from Rust, there is already tokio runtime so no
// additional work is needed. If calling from Python FFI, there's no existing
// tokio runtime, so we need to start a new one.
use std::{future::Future, sync::OnceLock};

use tokio::runtime::Handle;

pub(crate) struct TokioRuntime(tokio::runtime::Runtime);

#[inline]
pub(crate) fn get_tokio_runtime() -> &'static TokioRuntime {
    static RUNTIME: OnceLock<TokioRuntime> = OnceLock::new();
    RUNTIME.get_or_init(|| TokioRuntime(tokio::runtime::Runtime::new().unwrap()))
}

pub fn execute_in_tokio<F, Fut, T>(f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    get_tokio_runtime().0.block_on(f())
}

pub async fn run_async_with_tokio<F, Fut, T, E>(f: F) -> Result<T, E>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    match Handle::try_current() {
        Ok(_) => f().await,
        Err(_) => execute_in_tokio(f),
    }
}

pub fn run_sync_with_tokio<T, E>(f: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    match Handle::try_current() {
        Ok(_) => f(),
        Err(_) => execute_in_tokio(|| async { f() }),
    }
}
