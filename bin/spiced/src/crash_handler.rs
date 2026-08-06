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
//! A `SIGSEGV` is not a panic, so the panic hook and `RUST_BACKTRACE` never run and
//! the log stops mid-stream: a process that exits 139 leaves no evidence of where it
//! died. This prints one line first, carrying the instruction pointer and load base
//! so the report is symbolizable from logs alone:
//!
//! ```text
//! addr2line -e spiced -fCi <offset>
//! ```
//!
//! Everything here runs in the signal handler on the crashing thread and must be
//! async-signal-safe — no allocation, no locks, no `tracing` — since the fault may
//! have interrupted that thread mid-allocation. The line is formatted into a fixed
//! stack buffer and emitted with one `write(2)`.

use std::sync::OnceLock;

use snafu::{ResultExt, Snafu};

/// Kept alive for the process lifetime; dropping a [`CrashHandler`] detaches it.
static HANDLER: OnceLock<crash_handler::CrashHandler> = OnceLock::new();

/// Resolved at install time: parsing `/proc/self/maps` is not signal-safe, and the
/// value cannot change while the process runs.
static LOAD_BASE: OnceLock<usize> = OnceLock::new();

/// For the `uptime` field: crashes clustered at a fixed point after startup look
/// very different from ones that need hours of load.
static START: OnceLock<std::time::Instant> = OnceLock::new();

#[derive(Debug, Snafu)]
#[snafu(display(
    "Fatal-signal reporting is unavailable; a native crash will produce no diagnostics: {source}"
))]
pub struct InstallError {
    source: crash_handler::Error,
}

/// Install fatal-signal reporting. Call once, as early in `main` as possible, so
/// faults during startup are covered.
///
/// A failure to attach is returned rather than logged, and the caller does nothing
/// with it beyond reporting it: refusing to start without crash reporting would be
/// worse than starting without it. It is returned because this runs before any
/// tracing subscriber exists — a message emitted here would reach no one — so the
/// caller logs it once there is somewhere for it to go.
///
/// # Errors
///
/// Fails when the signal handler cannot be attached, most commonly because another
/// handler already owns the process's fatal signals.
pub fn install() -> Result<(), InstallError> {
    let _ = START.set(std::time::Instant::now());
    let _ = LOAD_BASE.set(read_load_base().unwrap_or(0));

    // SAFETY: the closure is async-signal-safe — see the module docs.
    let event = unsafe { crash_handler::make_crash_event(on_crash) };

    let handler = crash_handler::CrashHandler::attach(event).context(InstallSnafu)?;
    let _ = HANDLER.set(handler);
    Ok(())
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
        // Take the mapping at file offset 0, not the first executable segment: the
        // r-xp segment sits at a non-zero offset, so using it would make `ip - base`
        // an offset into the text segment rather than into the file and every
        // `addr2line` would resolve to the wrong place.
        //
        // Fields: <start>-<end> <perms> <file-offset> <dev> <inode> <path>
        if !line.ends_with(exe) {
            continue;
        }
        let mut fields = line.split_whitespace();
        let range = fields.next()?;
        let _perms = fields.next()?;
        let file_offset = fields.next()?;
        if file_offset.trim_start_matches('0').is_empty() {
            return usize::from_str_radix(range.split('-').next()?, 16).ok();
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
    // `siginfo` carries the number unsigned; libc's constants are signed.
    match signo.cast_signed() {
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGBUS => "SIGBUS",
        libc::SIGILL => "SIGILL",
        libc::SIGFPE => "SIGFPE",
        libc::SIGABRT => "SIGABRT",
        libc::SIGTRAP => "SIGTRAP",
        _ => "SIGNAL",
    }
}

/// RIP's index into `mcontext_t::gregs`. `crash-context` exposes the register file
/// as a bare array with no named accessors, so the index has to be spelled out;
/// the assertion pins it to libc's definition rather than trusting a literal.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const REG_RIP: usize = 16;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const _: () = assert!(libc::REG_RIP == 16);

/// The instruction pointer at the point of the fault.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn instruction_pointer(cc: &crash_handler::CrashContext) -> u64 {
    cc.context.uc_mcontext.gregs[REG_RIP].cast_unsigned()
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

fn on_crash(cc: &crash_handler::CrashContext) -> crash_handler::CrashEventResult {
    // Several threads can fault at once; report only the first, so a cascade cannot
    // loop inside the handler while the process is already dying.
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
    // `Handled(false)` restores the handler installed before ours rather than going
    // straight to SIG_DFL. `std` installs one during runtime init to detect stack
    // overflow, so keeping it means a guard-page fault still prints
    // `thread '...' has overflowed its stack` — which our line cannot distinguish
    // from a wild pointer. On any other fault `std` installs SIG_DFL and returns, so
    // the process still dies with the right signal.
    //
    // A stack overflow therefore exits 134 (`std` aborts) rather than 139; every
    // other fault is unchanged.
    crash_handler::CrashEventResult::Handled(false)
}

#[cfg(target_os = "linux")]
/// The crashing thread's name.
///
/// `prctl(PR_GET_NAME)` is one syscall; `pthread_getname_np` is not signal-safe, as
/// glibc implements it by reading `/proc` through buffered stdio. The buffer is
/// `TASK_COMM_LEN`, which is also the kernel's cap when a thread name is set.
fn thread_name(buf: &mut [u8; 16]) -> usize {
    // SAFETY: PR_GET_NAME writes at most 16 bytes into the caller-owned buffer.
    unsafe {
        libc::prctl(libc::PR_GET_NAME, buf.as_mut_ptr());
    }
    buf.iter().position(|&b| b == 0).unwrap_or(buf.len())
}

/// The full report. The callback runs on the crashing thread, so the thread name
/// identifies the pool that faulted.
#[cfg(target_os = "linux")]
fn report(cc: &crash_handler::CrashContext) {
    use std::io::Write as _;

    let mut name = [0u8; 16];
    let name_len = thread_name(&mut name);
    let thread = core::str::from_utf8(&name[..name_len]).unwrap_or("?");

    let ip = instruction_pointer(cc);
    let base = LOAD_BASE.get().copied().unwrap_or(0) as u64;
    // The ASLR-removed offset is what `addr2line` wants; emitting it alongside the
    // raw values keeps the printed command directly runnable.
    let offset = ip.saturating_sub(base);
    let uptime = START.get().map_or(0, |s| s.elapsed().as_secs());

    // Formatting into a fixed slice cannot allocate.
    let mut buf = [0u8; 512];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    let _ = write!(
        cur,
        "\n=== native crash ===\n\
         signal={} code={} addr=0x{:x} ip=0x{:x} base=0x{:x} offset=0x{:x}\n\
         thread=\"{}\" pid={} tid={} uptime={}s\n\
         symbolize: addr2line -e spiced -fCi 0x{:x}\n\
         === end native crash ===\n",
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
        offset,
    );
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the cursor position is bounded by the buffer length"
    )]
    let written = cur.position() as usize;
    raw_write(&buf[..written]);
}

/// A reduced report. `CrashContext` is platform-specific: on macOS it carries a Mach
/// exception rather than a signal, and the callback runs on a dedicated handler
/// thread, so neither the instruction pointer nor the crashing thread's name is
/// available. Deployments run Linux; this keeps the crate usable elsewhere.
#[cfg(not(target_os = "linux"))]
fn report(_cc: &crash_handler::CrashContext) {
    use std::io::Write as _;

    let uptime = START.get().map_or(0, |s| s.elapsed().as_secs());
    let mut buf = [0u8; 256];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    let _ = write!(
        cur,
        "\n=== native crash ===\n\
         (reduced report: full detail is Linux-only)\n\
         uptime={uptime}s\n\
         === end native crash ===\n",
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

    /// Where the child faults. `inline(never)` and `no_mangle` keep it a distinct,
    /// named symbol so a report can be resolved back to it.
    #[inline(never)]
    #[unsafe(no_mangle)]
    extern "C" fn spiced_crash_handler_test_fault() {
        // SAFETY: a deliberate null write — the behaviour under test.
        unsafe {
            std::ptr::null_mut::<u8>().write(1);
        }
    }

    /// Install the handler, take a real fault, and assert the report reached stderr.
    ///
    /// The process under test necessarily dies, so this re-executes its own binary
    /// with `CHILD` set and asserts on the child's output. A genuine fault exercises
    /// the `si_code > 0` path and populates `addr` and `ip`; a sent signal does not.
    #[test]
    fn reports_a_fatal_signal() {
        use std::os::unix::process::ExitStatusExt as _;

        if std::env::var_os(CHILD).is_some() {
            super::install();
            spiced_crash_handler_test_fault();
            unreachable!("the fault must terminate the process");
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
            .expect("run the faulting child");

        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            stderr.contains("=== native crash ==="),
            "no crash report on stderr.\nstderr: {stderr}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );

        // The child must die from the signal. A clean exit would mean the handler
        // swallowed the fault and let execution resume.
        assert_eq!(
            output.status.signal(),
            Some(libc::SIGSEGV),
            "child should die from SIGSEGV, got {:?}",
            output.status
        );

        // Fields only the Linux report carries. `base` must be non-zero: a zero load
        // base means /proc/self/maps parsing failed and reports are unsymbolizable.
        #[cfg(target_os = "linux")]
        {
            for field in ["signal=SIGSEGV", "ip=0x", "base=0x", "offset=0x", "thread="] {
                assert!(
                    stderr.contains(field),
                    "report is missing `{field}`: {stderr}"
                );
            }
            assert!(
                !stderr.contains("base=0x0 "),
                "load base was not resolved; reports would not be symbolizable: {stderr}"
            );
        }
    }
}
