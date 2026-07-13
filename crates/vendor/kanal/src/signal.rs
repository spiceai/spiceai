use crate::{
    backoff::{self, get_parallelism},
    pointer::KanalPtr,
};
use core::{
    cell::UnsafeCell,
    sync::atomic::{fence, AtomicU8, Ordering},
};
#[cfg(feature = "async")]
use core::{
    task::{Poll, Waker},
    time::Duration,
};
use std::{thread::Thread, time::Instant};

const UNLOCKED: u8 = 0;
const TERMINATED: u8 = 1;
const LOCKED: u8 = 2;
const LOCKED_STARVATION: u8 = 3;

/// `KanalWaker` is a structure to enable synchronization in both async and
/// sync.
#[repr(u8)]
pub(crate) enum KanalWaker {
    #[cfg(feature = "async")]
    None,
    Sync(UnsafeCell<Option<Thread>>),
    #[cfg(feature = "async")]
    Async(Waker),
}

/// `Signal<T>` struct is responsible for communicating between threads and
/// coroutines for both reads and writes.
pub struct Signal<T> {
    state: AtomicU8,
    ptr: KanalPtr<T>,
    waker: KanalWaker,
}

impl<T> Signal<T> {
    /// Signal to send data to a writer
    #[inline(always)]
    #[cfg(feature = "async")]
    pub(crate) fn new_async() -> Self {
        Self {
            state: AtomicU8::new(LOCKED),
            ptr: Default::default(),
            waker: KanalWaker::None,
        }
    }

    #[inline(always)]
    #[cfg(feature = "async")]
    pub(crate) fn poll(&self) -> Poll<bool> {
        let v = self.state.load(Ordering::Relaxed);
        if v < LOCKED {
            fence(Ordering::Acquire);
            Poll::Ready(v == UNLOCKED)
        } else {
            Poll::Pending
        }
    }

    /// Signal to send data to a writer for specific kanal pointer
    #[inline(always)]
    #[cfg(feature = "async")]
    pub(crate) fn new_async_ptr(ptr: KanalPtr<T>) -> Self {
        Self {
            state: AtomicU8::new(LOCKED),
            ptr,
            waker: KanalWaker::None,
        }
    }

    /// Returns new sync signal for the provided thread
    #[inline(always)]
    pub(crate) fn new_sync(ptr: KanalPtr<T>) -> Self {
        Self {
            state: AtomicU8::new(LOCKED),
            ptr,
            waker: KanalWaker::Sync(None.into()),
        }
    }

    /// Waits for finishing async signal for a short time
    #[cfg(feature = "async")]
    pub(crate) fn async_blocking_wait(&self) -> bool {
        let v = self.state.load(Ordering::Relaxed);
        if v < LOCKED {
            fence(Ordering::Acquire);
            return v == UNLOCKED;
        }

        for _ in 0..32 {
            //backoff::spin_wait(96);
            backoff::yield_now_std();
            let v = self.state.load(Ordering::Relaxed);
            if v < LOCKED {
                fence(Ordering::Acquire);
                return v == UNLOCKED;
            }
        }

        // Usually this part will not happen but you can't be sure
        let mut sleep_time: u64 = 1 << 10;
        loop {
            backoff::sleep(Duration::from_nanos(sleep_time));
            let v = self.state.load(Ordering::Relaxed);
            if v < LOCKED {
                fence(Ordering::Acquire);
                return v == UNLOCKED;
            }
            // increase sleep_time gradually to 262 microseconds
            if sleep_time < (1 << 18) {
                sleep_time <<= 1;
            }
        }
    }

    /// Waits for the signal event in sync mode,
    #[inline(always)]
    pub(crate) fn wait(&self) -> bool {
        let v = self.state.load(Ordering::Relaxed);
        if v < LOCKED {
            fence(Ordering::Acquire);
            return v == UNLOCKED;
        }
        for _ in 0..256 {
            backoff::yield_now_std();
            let v = self.state.load(Ordering::Relaxed);
            if v < LOCKED {
                fence(Ordering::Acquire);
                return v == UNLOCKED;
            }
        }
        match &self.waker {
            KanalWaker::Sync(waker) => {
                // waker is not shared as the state is not `LOCKED_STARVATION`
                unsafe {
                    *waker.get() = Some(std::thread::current());
                }
                match self.state.compare_exchange(
                    LOCKED,
                    LOCKED_STARVATION,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => loop {
                        std::thread::park();
                        let v = self.state.load(Ordering::Acquire);
                        if v < LOCKED {
                            return v == UNLOCKED;
                        }
                    },
                    Err(v) => v == UNLOCKED,
                }
            }
            #[cfg(feature = "async")]
            KanalWaker::None | KanalWaker::Async(_) => unreachable!(),
        }
    }

    /// Waits for the signal event in sync mode with a timeout
    pub(crate) fn wait_timeout(&self, until: Instant) -> bool {
        if get_parallelism() > 1 {
            for _ in 0..32 {
                let v = self.state.load(Ordering::Relaxed);
                if v < LOCKED {
                    fence(Ordering::Acquire);
                    return v == UNLOCKED;
                }
                // randomize next entry with yield_now
                backoff::yield_now();
            }
        }
        //return self.v.load(Ordering::Acquire);
        while Instant::now() < until {
            let v = self.state.load(Ordering::Relaxed);
            if v < LOCKED {
                fence(Ordering::Acquire);
                return v == UNLOCKED;
            }
            backoff::yield_now_std();
        }
        self.state.load(Ordering::Acquire) == UNLOCKED
    }

    /// Set pointer to data for receiving or sending
    #[inline(always)]
    #[cfg(feature = "async")]
    pub(crate) fn set_ptr(&mut self, ptr: KanalPtr<T>) {
        self.ptr = ptr;
    }

    /// Registers the async waker in the Signal
    #[inline(always)]
    #[cfg(feature = "async")]
    pub(crate) fn register_waker(&mut self, waker: &Waker) {
        self.waker = KanalWaker::Async(waker.clone())
    }

    /// Set pointer to data for receiving or sending
    #[inline(always)]
    #[cfg(feature = "async")]
    pub(crate) fn will_wake(&self, waker: &Waker) -> bool {
        match &self.waker {
            KanalWaker::Async(w) => w.will_wake(waker),
            KanalWaker::Sync(_) | KanalWaker::None => unreachable!(),
        }
    }

    /// Returns true if signal is terminated
    pub(crate) fn is_terminated(&self) -> bool {
        self.state.load(Ordering::Relaxed) == TERMINATED
    }

    /// Reads kanal ptr and returns its value
    pub(crate) unsafe fn assume_init(&self) -> T {
        self.ptr.read()
    }

    /// Wakes the sleeping thread or coroutine
    unsafe fn wake(this: *const Self, state: u8) {
        match &(*this).waker {
            KanalWaker::Sync(waker) => {
                if (*this)
                    .state
                    .compare_exchange(LOCKED, state, Ordering::Release, Ordering::Acquire)
                    .is_err()
                {
                    let thread = (*waker.get()).as_ref().unwrap().clone();
                    (*this).state.store(state, Ordering::Release);
                    thread.unpark();
                }
            }
            #[cfg(feature = "async")]
            KanalWaker::Async(w) => {
                let w = w.clone();
                (*this).state.store(state, Ordering::Release);
                w.wake();
            }
            #[cfg(feature = "async")]
            KanalWaker::None => unreachable!(),
        }
    }

    /// Sends object to receive signal
    /// Safety: it's only safe to be called only once on the receive signals
    /// that are not terminated
    pub(crate) unsafe fn send(this: *const Self, d: T) {
        (*this).ptr.write(d);
        Self::wake(this, UNLOCKED);
    }

    /// Sends object to receive signal by coping the pointer
    /// Safety: it's only safe to be called only once on the receive signals
    /// that are not terminated
    #[allow(unused)]
    pub(crate) unsafe fn send_copy(this: *const Self, d: *const T) {
        (*this).ptr.copy(d);
        Self::wake(this, UNLOCKED);
    }

    /// Receives object from send signal
    /// Safety: it's only safe to be called only once on send signals that are
    /// not terminated
    pub(crate) unsafe fn recv(this: *const Self) -> T {
        let r = (*this).ptr.read();
        Self::wake(this, UNLOCKED);
        r
    }

    /// Terminates the signal and notifies its waiter
    /// Safety: it's only safe to be called only once on send/receive signals
    /// that are not finished or terminated
    pub(crate) unsafe fn terminate(this: *const Self) {
        Self::wake(this, TERMINATED);
    }

    /// Loads pointer data and drops it in place
    /// Safety: it should only be used once, and only when data in ptr is valid
    /// and not moved.
    #[cfg(feature = "async")]
    pub(crate) unsafe fn load_and_drop(&self) {
        _ = self.ptr.read();
    }

    /// Returns signal terminator for other side of channel
    pub(crate) fn get_terminator(&self) -> SignalTerminator<T> {
        (self as *const Signal<T>).into()
    }
}

pub(crate) struct SignalTerminator<T>(*const Signal<T>);

impl<T> From<*const Signal<T>> for SignalTerminator<T> {
    fn from(value: *const Signal<T>) -> Self {
        Self(value)
    }
}

impl<T> SignalTerminator<T> {
    pub(crate) unsafe fn send(self, data: T) {
        Signal::send(self.0, data)
    }
    #[allow(unused)]
    pub(crate) unsafe fn send_copy(self, data: *const T) {
        Signal::send_copy(self.0, data)
    }
    pub(crate) unsafe fn recv(self) -> T {
        Signal::recv(self.0)
    }
    pub(crate) unsafe fn terminate(&self) {
        Signal::terminate(self.0)
    }
}

impl<T> PartialEq<Signal<T>> for SignalTerminator<T> {
    fn eq(&self, other: &Signal<T>) -> bool {
        self.0 == other as *const Signal<T>
    }
}

// If internal<T> is safe to send SignalPtr<T> is safe to send.
unsafe impl<T: Send> Send for SignalTerminator<T> {}
// If internal<T> is safe to send Signal<T> is safe to send.
unsafe impl<T: Send> Send for Signal<T> {}
