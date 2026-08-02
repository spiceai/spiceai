/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Fatal-signal reporting.
//!
//! A `SIGSEGV` is not a panic: the panic hook never runs, `RUST_BACKTRACE` has no
//! effect, and the log simply stops mid-stream. A deployment that exits 139 therefore
//! has no evidence of where it died. This prints one line naming the signal, the
//! faulting address, the instruction pointer and the thread before the process dies.
//!
//! The instruction pointer plus the load base captured at install time make the
//! report symbolizable from logs alone, without access to the node:
//!
//! ```text
//! addr2line -e spiced -fCi $((ip - base))
//! ```
//!
//! The callback runs inside the signal handler on the crashing thread, so it must be
//! async-signal-safe: no allocation, no locks, no `tracing`, no `println!` — any of
//! which can deadlock if the signal arrived while that thread held the allocator or
//! stdout lock. The line is formatted into a fixed stack buffer and emitted with a
//! single `write(2)`.

use std::sync::OnceLock;

/// Kept alive for the process lifetime; dropping a [`CrashHandler`] detaches it.
static HANDLER: OnceLock<crash_handler::CrashHandler> = OnceLock::new();

/// Load base of the main executable, resolved once at install time. Signal handlers
/// cannot parse `/proc/self/maps`, and the value cannot change while we run.
static LOAD_BASE: OnceLock<usize> = OnceLock::new();

/// Process start, for the `uptime` field — crashes that cluster at a fixed point
/// after startup look very different from ones that need hours of load.
static START: OnceLock<std::time::Instant> = OnceLock::new();

/// Build identifier, so a report names the binary it should be symbolized against.
static VERSION: OnceLock<&'static str> = OnceLock::new();

/// Install fatal-signal reporting. Call once, as early in `main` as possible — before
/// the Tokio runtime exists, so faults during startup are covered too.
///
/// Failure to attach is logged and otherwise ignored: crash reporting is a diagnostic
/// aid, and refusing to start without it would be a worse outcome than starting blind.
pub fn install(version: &'static str) {
    let _ = VERSION.set(version);
    let _ = START.set(std::time::Instant::now());
    let _ = LOAD_BASE.set(read_load_base().unwrap_or(0));

    // SAFETY: the closure is async-signal-safe — see the module docs. It formats into
    // a stack buffer and issues one `write(2)`.
    let event = unsafe { crash_handler::make_crash_event(on_crash) };

    match crash_handler::CrashHandler::attach(event) {
        Ok(handler) => {
            let _ = HANDLER.set(handler);
        }
        Err(err) => {
            tracing::warn!(
                "Fatal-signal reporting is unavailable; a native crash will produce no diagnostics: {err}"
            );
        }
    }
}

/// The executable's load base, needed to turn a runtime instruction pointer back into
/// a file offset for `addr2line`. Read from `/proc/self/maps`: the first executable
/// mapping backed by the running binary.
#[cfg(target_os = "linux")]
fn read_load_base() -> Option<usize> {
    let exe = std::fs::read_link("/proc/self/exe").ok()?;
    let exe = exe.to_str()?;
    let maps = std::fs::read_to_string("/proc/self/maps").ok()?;
    for line in maps.lines() {
        if line.ends_with(exe) && line.split_whitespace().nth(1)?.contains('x') {
            let start = line.split('-').next()?;
            return usize::from_str_radix(start, 16).ok();
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn read_load_base() -> Option<usize> {
    None
}

#[cfg(target_os = "linux")]
fn signal_name(signo: u32) -> &'static str {
    match signo as i32 {
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGBUS => "SIGBUS",
        libc::SIGILL => "SIGILL",
        libc::SIGFPE => "SIGFPE",
        libc::SIGABRT => "SIGABRT",
        libc::SIGTRAP => "SIGTRAP",
        _ => "SIGNAL",
    }
}

/// The instruction pointer at the point of the fault.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn instruction_pointer(cc: &crash_handler::CrashContext) -> u64 {
    cc.context.uc_mcontext.rip
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn instruction_pointer(cc: &crash_handler::CrashContext) -> u64 {
    cc.context.uc_mcontext.pc
}

#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
fn instruction_pointer(_cc: &crash_handler::CrashContext) -> u64 {
    0
}

/// Write `bytes` to stderr with a raw `write(2)`. Partial writes are ignored: there is
/// nothing useful to do about them from a signal handler, and the process is dying.
fn raw_write(bytes: &[u8]) {
    // SAFETY: `write` is async-signal-safe; fd 2 and the slice are valid.
    unsafe {
        libc::write(2, bytes.as_ptr().cast(), bytes.len());
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "signature is dictated by crash_handler::make_crash_event"
)]
fn on_crash(cc: &crash_handler::CrashContext) -> crash_handler::CrashEventResult {
    // Several threads can fault at once. Report only the first, so a cascade cannot
    // loop inside the handler while the process is already dying. (The same guard
    // appears in Zed's and Sentry's handlers, for the same reason.)
    static REPORTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if REPORTED
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Relaxed,
        )
        .is_ok()
    {
        report(cc);
    }
    // `Handled(false)` maps to `RestorePrevious`: restore the handlers that were
    // installed before us, rather than jumping straight to SIG_DFL.
    //
    // This matters because `std` installs its own SIGSEGV/SIGBUS handler during
    // runtime init — before `main`, so before us — purely to detect stack overflow.
    // Restoring it means a guard-page fault still prints
    // `thread '...' has overflowed its stack`, the one message that names stack
    // exhaustion unambiguously; our line alone cannot distinguish it from a wild
    // pointer. On any other fault `std` finds the address outside the guard page,
    // installs SIG_DFL and returns, so the process still dies with the right signal.
    //
    // Consequence: a stack overflow now exits 134 (`std` aborts) rather than 139.
    // Every other fault is unchanged. If `std` had installed nothing, the previous
    // handler is SIG_DFL and this is identical to `Handled(true)`.
    //
    // No signal unblocking is needed here. For a hard fault (`si_code > 0`)
    // crash-handler restores the handler and simply returns; the faulting instruction
    // re-executes and is delivered again. Nothing calls `raise()`, so the signal being
    // masked for the duration of its own handler never matters. (A hand-rolled handler
    // that re-raises explicitly must `pthread_sigmask(SIG_UNBLOCK, ...)` first, or the
    // re-raise sits pending forever.)
    crash_handler::CrashEventResult::Handled(false)
}

#[cfg(target_os = "linux")]
/// The crashing thread's name, into a caller-provided buffer. `pthread_getname_np`
/// does not allocate.
fn thread_name(buf: &mut [u8; 32]) -> usize {
    // SAFETY: writing into a caller-owned buffer of the declared length.
    unsafe {
        libc::pthread_getname_np(libc::pthread_self(), buf.as_mut_ptr().cast(), buf.len());
    }
    buf.iter().position(|&b| b == 0).unwrap_or(buf.len())
}

/// Linux: the full report. `ip` and `base` make it symbolizable from logs alone, and
/// the callback runs in the signal handler on the crashing thread, so the thread name
/// identifies the pool that faulted.
#[cfg(target_os = "linux")]
fn report(cc: &crash_handler::CrashContext) {
    use std::io::Write as _;

    let mut name = [0u8; 32];
    let name_len = thread_name(&mut name);
    let thread = core::str::from_utf8(&name[..name_len]).unwrap_or("?");

    let ip = instruction_pointer(cc);
    let base = LOAD_BASE.get().copied().unwrap_or(0) as u64;
    // The ASLR-removed offset: what `addr2line` actually wants. Emitted alongside the
    // raw values so the operator never has to subtract two hex numbers by hand, and
    // the printed command is directly runnable. (Bun's crash reporter encodes
    // "addresses with ASLR removed" for the same reason.)
    let offset = ip.saturating_sub(base);
    let uptime = START.get().map_or(0, |s| s.elapsed().as_secs());

    // Formatting into a fixed slice cannot allocate.
    let mut buf = [0u8; 512];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    let _ = write!(
        cur,
        "\n=== spiced native crash ===\n\
         signal={} code={} addr=0x{:x} ip=0x{:x} base=0x{:x} offset=0x{:x}\n\
         thread=\"{}\" pid={} tid={} uptime={}s version={}\n\
         symbolize: addr2line -e spiced -fCi 0x{:x}\n\
         === end spiced native crash ===\n",
        signal_name(cc.siginfo.ssi_signo),
        cc.siginfo.ssi_code,
        cc.siginfo.ssi_addr,
        ip,
        base,
        offset,
        thread,
        cc.pid,
        cc.tid,
        uptime,
        VERSION.get().copied().unwrap_or("unknown"),
        offset,
    );
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the cursor position is bounded by the buffer length"
    )]
    let written = cur.position() as usize;
    raw_write(&buf[..written]);
}

/// Non-Linux: a reduced report. `CrashContext` is platform-specific — on macOS it
/// carries a Mach exception rather than a signal, and the callback runs on a dedicated
/// handler thread, so neither the faulting instruction pointer nor the crashing
/// thread's name is available the way it is on Linux. Deployments run Linux; this path
/// exists so the crate builds and is minimally useful on a developer machine.
#[cfg(not(target_os = "linux"))]
fn report(_cc: &crash_handler::CrashContext) {
    use std::io::Write as _;

    let uptime = START.get().map_or(0, |s| s.elapsed().as_secs());
    let mut buf = [0u8; 256];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    let _ = write!(
        cur,
        "\n=== spiced native crash ===\n\
         (reduced report: full detail is Linux-only)\n\
         uptime={}s version={}\n\
         === end spiced native crash ===\n",
        uptime,
        VERSION.get().copied().unwrap_or("unknown"),
    );
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the cursor position is bounded by the buffer length"
    )]
    let written = cur.position() as usize;
    raw_write(&buf[..written]);
}

#[cfg(all(test, unix))]
mod tests {
    /// Set in the child process to select the crashing role.
    const CHILD: &str = "SPICED_CRASH_HANDLER_TEST_CHILD";

    /// Install the handler, fault for real, and assert the report reached stderr.
    ///
    /// The process under test necessarily dies, so the test re-executes its own binary
    /// with `CHILD` set: that run installs the handler and segfaults, and the parent
    /// makes the assertions on its output. `sadness-generator` is from the authors of
    /// `crash-handler` and exists for exactly this.
    #[test]
    fn reports_a_fatal_signal() {
        use std::os::unix::process::ExitStatusExt as _;

        if std::env::var_os(CHILD).is_some() {
            super::install("test-version");
            // SAFETY: faulting deliberately — this is the behaviour under test.
            unsafe { sadness_generator::raise_segfault() }
        }

        // `module_path!` is crate-qualified; libtest filters are not.
        let module = module_path!();
        let filter = module
            .split_once("::")
            .map_or(module, |(_, rest)| rest)
            .to_owned()
            + "::reports_a_fatal_signal";

        let exe = std::env::current_exe().expect("locate the test binary");
        let output = std::process::Command::new(exe)
            .args(["--exact", &filter, "--nocapture"])
            .env(CHILD, "1")
            .output()
            .expect("run the crashing child");

        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            stderr.contains("=== spiced native crash ==="),
            "no crash report on stderr.\nstderr: {stderr}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );

        // The child must die from the signal, not exit normally: the handler reports
        // and then lets the fault kill the process.
        assert_eq!(
            output.status.signal(),
            Some(libc::SIGSEGV),
            "child should die from SIGSEGV, got {:?}",
            output.status
        );

        // Fields that only the Linux report carries.
        #[cfg(target_os = "linux")]
        for field in ["signal=SIGSEGV", "ip=0x", "base=0x", "offset=0x", "thread=", "version=test-version"] {
            assert!(stderr.contains(field), "report is missing `{field}`: {stderr}");
        }
    }
}
