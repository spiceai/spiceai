// Copyright 2024-2026 The Spice.ai OSS Authors
//
// In-process, ptrace-free, all-thread native backtrace capture for stall diagnosis.
//
// The cold-tier promotion freeze must be discriminated between a lost async wakeup and a
// synchronous lock (`parking_lot`) deadlock. External tools (`gdb`/`eu-stack`) cannot attach on the
// CI runners (containerized, no `CAP_SYS_PTRACE`; `ptrace_scope` unsettable), and tokio task dumps
// only see async awaits — invisible to a thread blocked in `parking_lot::…::lock`. This captures
// every OS thread's *native* user-space stack from inside the process, so a lock-blocked thread
// shows its `parking_lot` frames and an idle runtime worker shows its park frames.
//
// Mechanism (the pprof-rs pattern): a signal handler captures only frame instruction pointers into
// static atomics (async-signal-safe-ish: `trace_unsynchronized` + atomic stores, no allocation);
// symbolization happens outside the handler on the watchdog thread. Threads are dumped one at a
// time (serialized) so a single static slot suffices and handlers never race.

/// Capture and log every OS thread's native user-space backtrace (Linux only; no-op elsewhere).
/// Runs on the caller (the watchdog OS thread), never a runtime worker.
#[cfg(target_os = "linux")]
pub(crate) use linux_impl::dump_all_threads;

#[cfg(not(target_os = "linux"))]
pub(crate) fn dump_all_threads(_reason: &str) {}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicI32;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    /// Max frames captured per thread.
    const MAX_FRAMES: usize = 64;
    /// Cap on threads dumped, so a huge idle worker pool can't produce an unbounded log.
    const MAX_THREADS: usize = 300;
    /// Rarely-used signal not touched by std/tokio/mio. Default disposition is ignore.
    const DUMP_SIGNAL: libc::c_int = libc::SIGURG;

    static SLOT_IPS: [AtomicUsize; MAX_FRAMES] = [const { AtomicUsize::new(0) }; MAX_FRAMES];
    static SLOT_LEN: AtomicUsize = AtomicUsize::new(0);
    static SLOT_DONE: AtomicBool = AtomicBool::new(false);
    static TARGET_TID: AtomicI32 = AtomicI32::new(0);
    static HANDLER_INSTALLED: AtomicBool = AtomicBool::new(false);
    /// Serializes whole dumps: two concurrent stalled ops must not interleave captures.
    static DUMPING: AtomicBool = AtomicBool::new(false);

    fn gettid() -> i32 {
        // SAFETY: `gettid` takes no arguments and only reads the caller's TID.
        unsafe { libc::syscall(libc::SYS_gettid) as i32 }
    }

    extern "C" fn handler(_sig: libc::c_int) {
        // Only the targeted thread records (tgkill targets one tid, but guard anyway).
        if TARGET_TID.load(Ordering::Acquire) != gettid() {
            return;
        }
        let mut n = 0usize;
        // SAFETY: dumps are serialized and one-thread-at-a-time, so no other thread traces
        // concurrently; the closure only performs relaxed atomic stores — no allocation, no locks.
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                if n < MAX_FRAMES {
                    SLOT_IPS[n].store(frame.ip() as usize, Ordering::Relaxed);
                    n += 1;
                    true
                } else {
                    false
                }
            });
        }
        SLOT_LEN.store(n, Ordering::Relaxed);
        SLOT_DONE.store(true, Ordering::Release);
    }

    fn install_handler_once() {
        if HANDLER_INSTALLED.swap(true, Ordering::AcqRel) {
            return;
        }
        // SAFETY: installs a process-wide handler for a signal std/tokio do not use.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = handler as usize;
            libc::sigemptyset(&mut sa.sa_mask);
            sa.sa_flags = libc::SA_RESTART;
            libc::sigaction(DUMP_SIGNAL, &sa, std::ptr::null_mut());
        }
    }

    fn symbolize(ips: &[usize]) -> String {
        let mut out = String::new();
        for &ip in ips {
            let mut named = false;
            // SAFETY: `resolve` is called outside any signal handler, on the watchdog thread.
            backtrace::resolve(ip as *mut std::ffi::c_void, |sym| {
                if !named {
                    match sym.name() {
                        Some(name) => out.push_str(&format!("\n      {name:#}")),
                        None => out.push_str(&format!("\n      <{ip:#x}>")),
                    }
                    named = true;
                }
            });
            if !named {
                out.push_str(&format!("\n      <{ip:#x}>"));
            }
        }
        out
    }

    pub(crate) fn dump_all_threads(reason: &str) {
        if DUMPING.swap(true, Ordering::AcqRel) {
            return; // another dump in progress
        }
        install_handler_once();

        let pid = std::process::id() as i32;
        let self_tid = gettid();
        let Ok(entries) = std::fs::read_dir("/proc/self/task") else {
            DUMPING.store(false, Ordering::Release);
            return;
        };

        tracing::warn!(
            target: "cayenne::stall",
            %reason,
            "=== in-process all-thread native backtrace BEGIN (parking_lot frames => lock block; runtime park frames => idle/lost-wake) ==="
        );

        let mut dumped = 0usize;
        for entry in entries.flatten() {
            if dumped >= MAX_THREADS {
                tracing::warn!(target: "cayenne::stall", "backtrace dump hit MAX_THREADS cap");
                break;
            }
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Ok(tid) = name.parse::<i32>() else {
                continue;
            };
            if tid == self_tid {
                continue;
            }
            let comm = std::fs::read_to_string(format!("/proc/self/task/{tid}/comm"))
                .unwrap_or_default()
                .trim()
                .to_string();

            SLOT_DONE.store(false, Ordering::Release);
            SLOT_LEN.store(0, Ordering::Relaxed);
            TARGET_TID.store(tid, Ordering::Release);

            // SAFETY: tgkill delivers DUMP_SIGNAL to one specific thread of this process. Args are
            // passed as `c_long` because `syscall` is variadic and reads each via `va_arg(long)`.
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_tgkill,
                    pid as libc::c_long,
                    tid as libc::c_long,
                    DUMP_SIGNAL as libc::c_long,
                )
            };
            if rc != 0 {
                continue; // thread exited between readdir and signal
            }

            // Bounded wait for the handler (a thread wedged inside the unwinder never sets DONE).
            let mut waited = 0;
            while !SLOT_DONE.load(Ordering::Acquire) && waited < 250 {
                std::thread::sleep(Duration::from_millis(2));
                waited += 1;
            }
            if !SLOT_DONE.load(Ordering::Acquire) {
                tracing::warn!(target: "cayenne::stall", tid, comm, "thread did not respond to backtrace signal (wedged inside a non-signal-safe critical section?)");
                dumped += 1;
                continue;
            }

            let n = SLOT_LEN.load(Ordering::Relaxed).min(MAX_FRAMES);
            let ips: Vec<usize> = (0..n).map(|i| SLOT_IPS[i].load(Ordering::Relaxed)).collect();
            let frames = symbolize(&ips);
            tracing::warn!(target: "cayenne::stall", tid, comm, "thread backtrace:{frames}");
            dumped += 1;
        }

        tracing::warn!(target: "cayenne::stall", threads = dumped, "=== in-process all-thread native backtrace END ===");
        DUMPING.store(false, Ordering::Release);
    }
}
