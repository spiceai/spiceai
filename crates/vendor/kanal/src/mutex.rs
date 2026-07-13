use core::sync::atomic::{AtomicBool, Ordering};
use lock_api::{GuardSend, RawMutex};

use crate::backoff::*;
pub struct RawMutexLock {
    locked: AtomicBool,
}

impl RawMutexLock {
    #[inline(never)]
    fn lock_no_inline(&self) {
        spin_cond(|| self.try_lock());
    }
}

unsafe impl RawMutex for RawMutexLock {
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: RawMutexLock = RawMutexLock {
        locked: AtomicBool::new(false),
    };
    type GuardMarker = GuardSend;
    #[inline(always)]
    fn lock(&self) {
        if self.try_lock() {
            return;
        }
        self.lock_no_inline();
    }

    #[inline(always)]
    fn try_lock(&self) -> bool {
        self.locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    #[inline(always)]
    unsafe fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }
}
#[allow(dead_code)]
pub type Mutex<T> = lock_api::Mutex<RawMutexLock, T>;
#[cfg(not(feature = "std-mutex"))]
pub type MutexGuard<'a, T> = lock_api::MutexGuard<'a, RawMutexLock, T>;
