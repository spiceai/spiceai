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
//! died. This prints a short report first, naming which build crashed and carrying
//! the instruction pointer and load base, so it is symbolizable from logs alone:
//!
//! ```text
//! addr2line -e spiced -fCi <offset>
//! ```
//!
//! Everything here runs in the signal handler on the crashing thread and must be
//! async-signal-safe — no allocation, no locks, no `tracing` — since the fault may
//! have interrupted that thread mid-allocation. Anything that would need to allocate,
//! such as the build identity, is resolved at install time.
//!
//! The report is formatted into fixed stack buffers and emitted in a few bounded
//! `write(2)` calls. The fields are flushed before the stack is inspected, since a
//! corrupt stack pointer would fault a second time and end the process.

use std::sync::OnceLock;

/// Kept alive for the process lifetime; dropping a [`CrashHandler`] detaches it.
static HANDLER: OnceLock<crash_handler::CrashHandler> = OnceLock::new();

/// Where the running binary is mapped.
///
/// Resolved at install time: parsing `/proc/self/maps` is not signal-safe, and none
/// of it changes while the process runs.
#[cfg(target_os = "linux")]
struct Image {
    /// Start of the mapping at file offset 0 — what `ip - base` is an offset into.
    base: usize,
    /// The executable mapping. An instruction pointer outside it is not code from
    /// this binary, so no file offset computed from it means anything.
    text: core::ops::Range<usize>,
}

#[cfg(target_os = "linux")]
static IMAGE: OnceLock<Image> = OnceLock::new();

/// For the `uptime` field: crashes clustered at a fixed point after startup look
/// very different from ones that need hours of load.
static START: OnceLock<std::time::Instant> = OnceLock::new();

/// Which binary this is, formatted once at install so the handler only has to write
/// bytes: several release artifacts share a version, and a report that cannot name
/// its own artifact cannot be symbolized against the right one.
static IDENTITY: OnceLock<String> = OnceLock::new();

/// The buffer each part of the report is formatted into.
const REPORT_BUF: usize = 1024;

/// How much of the identity line is kept. The version has no inherent length limit,
/// so it is clipped at install; every other field is fixed-width and well inside what
/// is left. A part that outgrows the buffer anyway is caught at runtime, not here —
/// see `flush`.
const MAX_IDENTITY: usize = 192;

const _: () = assert!(MAX_IDENTITY < REPORT_BUF / 2);

/// Install fatal-signal reporting. Call once, as early in `main` as possible, so
/// faults during startup are covered.
///
/// `version` is passed in rather than rebuilt here: `main` already composes it, and
/// the feature flags it encodes belong with the rest of the version logic.
///
/// A failure to attach is logged and ignored: refusing to start without crash
/// reporting would be worse than starting without it.
pub fn install(version: &str) {
    let _ = START.set(std::time::Instant::now());
    resolve_image();
    // Formatted now, not in the handler: allocating there is not signal-safe, and a
    // pre-built line cannot crowd out the rest of the report's fixed buffer.
    let mut identity = format!(
        "spiced={version} features={} profile={}",
        env!("SPICED_BUILD_FEATURES"),
        env!("SPICED_BUILD_PROFILE"),
    );
    // Clip here rather than let the handler truncate the fields after it: the version
    // is the one input with no length limit of its own.
    identity.truncate(identity.floor_char_boundary(MAX_IDENTITY - 1));
    identity.push('\n');
    let _ = IDENTITY.set(identity);

    // SAFETY: the closure is async-signal-safe — see the module docs.
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

/// Resolve everything the report needs about the running binary. Only the Linux
/// report carries an instruction pointer, so only Linux has anything to resolve.
#[cfg(target_os = "linux")]
fn resolve_image() {
    if let Some(image) = read_image() {
        let _ = IMAGE.set(image);
    }
    if let Some(fds) = open_probe() {
        let _ = PROBE_FDS.set(fds);
    }
}

#[cfg(not(target_os = "linux"))]
fn resolve_image() {}

/// Where the running binary is mapped, from `/proc/self/maps`.
#[cfg(target_os = "linux")]
fn read_image() -> Option<Image> {
    let exe = std::fs::read_link("/proc/self/exe").ok()?;
    let exe = exe.to_str()?;
    let maps = std::fs::read_to_string("/proc/self/maps").ok()?;

    let mut base = None;
    let mut text_start = usize::MAX;
    let mut text_end = 0;

    for line in maps.lines() {
        // Fields: <start>-<end> <perms> <file-offset> <dev> <inode> <path>
        if !line.ends_with(exe) {
            continue;
        }
        let mut fields = line.split_whitespace();
        let (Some(range), Some(perms), Some(file_offset)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let Some((start, end)) = range.split_once('-') else {
            continue;
        };
        let (Ok(start), Ok(end)) = (
            usize::from_str_radix(start, 16),
            usize::from_str_radix(end, 16),
        ) else {
            continue;
        };

        // Take the mapping at file offset 0, not the first executable segment: the
        // r-xp segment sits at a non-zero offset, so using it would make `ip - base`
        // an offset into the text segment rather than into the file and every
        // `addr2line` would resolve to the wrong place.
        if base.is_none() && file_offset.trim_start_matches('0').is_empty() {
            base = Some(start);
        }
        // `-z separate-code` can split the text segment, so take the whole span.
        if perms.contains('x') {
            text_start = text_start.min(start);
            text_end = text_end.max(end);
        }
    }

    if text_start >= text_end {
        return None;
    }
    Some(Image {
        base: base?,
        text: text_start..text_end,
    })
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

/// The `si_code` name, so a report does not need a lookup table to be read.
#[cfg(target_os = "linux")]
fn signal_code_name(signo: u32, code: i32) -> &'static str {
    // `libc` exports `SI_*`, `BUS_*` and `TRAP_*` for Linux, but not the per-signal
    // codes for `SIGSEGV`, `SIGILL` or `SIGFPE`. From `asm-generic/siginfo.h`.
    const SEGV_MAPERR: i32 = 1;
    const SEGV_ACCERR: i32 = 2;
    const ILL_ILLOPC: i32 = 1;
    const ILL_ILLOPN: i32 = 2;
    const ILL_ILLADR: i32 = 3;
    const ILL_PRVOPC: i32 = 5;
    const FPE_INTDIV: i32 = 1;
    const FPE_INTOVF: i32 = 2;
    const FPE_FLTDIV: i32 = 3;
    const FPE_FLTINV: i32 = 7;

    // The sender codes are signal-independent and are matched first; none of them
    // collide with a fault code.
    match (signo.cast_signed(), code) {
        (_, libc::SI_USER) => "SI_USER",
        (_, libc::SI_KERNEL) => "SI_KERNEL",
        (_, libc::SI_QUEUE) => "SI_QUEUE",
        (_, libc::SI_TKILL) => "SI_TKILL",
        (libc::SIGSEGV, SEGV_MAPERR) => "SEGV_MAPERR",
        (libc::SIGSEGV, SEGV_ACCERR) => "SEGV_ACCERR",
        (libc::SIGBUS, libc::BUS_ADRALN) => "BUS_ADRALN",
        (libc::SIGBUS, libc::BUS_ADRERR) => "BUS_ADRERR",
        (libc::SIGBUS, libc::BUS_OBJERR) => "BUS_OBJERR",
        (libc::SIGTRAP, libc::TRAP_BRKPT) => "TRAP_BRKPT",
        (libc::SIGTRAP, libc::TRAP_TRACE) => "TRAP_TRACE",
        (libc::SIGILL, ILL_ILLOPC) => "ILL_ILLOPC",
        (libc::SIGILL, ILL_ILLOPN) => "ILL_ILLOPN",
        (libc::SIGILL, ILL_ILLADR) => "ILL_ILLADR",
        (libc::SIGILL, ILL_PRVOPC) => "ILL_PRVOPC",
        (libc::SIGFPE, FPE_INTDIV) => "FPE_INTDIV",
        (libc::SIGFPE, FPE_INTOVF) => "FPE_INTOVF",
        (libc::SIGFPE, FPE_FLTDIV) => "FPE_FLTDIV",
        (libc::SIGFPE, FPE_FLTINV) => "FPE_FLTINV",
        _ => "?",
    }
}

// `crash-handler` copies `size_of::<signalfd_siginfo>()` bytes out of the kernel's
// `siginfo_t` (`crash-handler-0.8.0/src/linux/state.rs:431`), so reading them back as
// a `siginfo_t` needs the destination to fit, and to be no less aligned. A libc change
// should break the build rather than the report.
#[cfg(target_os = "linux")]
const _: () = assert!(size_of::<libc::siginfo_t>() <= size_of::<libc::signalfd_siginfo>());
#[cfg(target_os = "linux")]
const _: () = assert!(align_of::<libc::siginfo_t>() <= align_of::<libc::signalfd_siginfo>());

/// The signal information the kernel actually delivered.
///
/// `CrashContext::siginfo` is typed `libc::signalfd_siginfo` but holds `siginfo_t`
/// bytes: `crash-handler` reinterprets the kernel's pointer before copying it. The two
/// layouts agree only up to `si_code` — `ssi_addr` sits at offset 72, which is padding
/// in a `siginfo_t`, so it reads `0x0` for every fault. Upstream:
/// `EmbarkStudios/crash-handling#49`; delete this when that is fixed.
///
/// Reinterpreted once, here, so the fields can be read through `libc`'s own accessors,
/// which derive each offset per architecture instead of hardcoding it.
#[cfg(target_os = "linux")]
fn kernel_siginfo(cc: &crash_handler::CrashContext) -> &libc::siginfo_t {
    // SAFETY: the bytes are a `siginfo_t` (see above), and the assertions establish
    // that one fits within, and is no more aligned than, what holds them.
    unsafe { &*std::ptr::from_ref(&cc.siginfo).cast::<libc::siginfo_t>() }
}

/// The faulting address, or `None` when the signal carries none.
#[cfg(target_os = "linux")]
fn fault_address(cc: &crash_handler::CrashContext) -> Option<u64> {
    // `_sigfault` is the live union member only for a kernel-raised fault. A signal
    // delivered by `kill`/`raise`/`pthread_kill` (`SI_USER`, `SI_TKILL`, …) carries
    // the sender's pid and uid in those same bytes.
    if !matches!(
        cc.siginfo.ssi_signo.cast_signed(),
        libc::SIGSEGV | libc::SIGBUS | libc::SIGILL | libc::SIGFPE | libc::SIGTRAP
    ) || cc.siginfo.ssi_code <= 0
        || cc.siginfo.ssi_code == libc::SI_KERNEL
    {
        return None;
    }

    // SAFETY: `si_addr` is the live union member for the signals filtered above.
    Some(unsafe { kernel_siginfo(cc).si_addr() } as u64)
}

/// Who sent the signal, when it was sent rather than raised by a fault.
///
/// A `SIGABRT` reaches this handler whether the process aborted itself or something
/// outside sent it. A sender pid equal to our own means it was self-inflicted. An OOM
/// kill is not covered: the kernel sends `SIGKILL`, which cannot be caught.
#[cfg(target_os = "linux")]
fn signal_sender(cc: &crash_handler::CrashContext) -> Option<(i32, u32)> {
    // `_kill` (and `_rt`, for `sigqueue`) is the live union member only for these.
    if !matches!(
        cc.siginfo.ssi_code,
        libc::SI_USER | libc::SI_TKILL | libc::SI_QUEUE
    ) {
        return None;
    }

    // SAFETY: `_kill` is the live union member for the codes filtered above.
    let si = kernel_siginfo(cc);
    Some(unsafe { (si.si_pid(), si.si_uid()) })
}

/// A pipe used to test whether an address is readable: `write(2)` reports `EFAULT`
/// instead of raising a signal, and is async-signal-safe.
///
/// Not `/dev/null`, which the kernel completes without reading the buffer, so every
/// probe would pass. Both ends are kept: closing the read end would make each probe
/// raise `SIGPIPE`. One report's probes are far below the pipe's capacity.
#[cfg(target_os = "linux")]
static PROBE_FDS: OnceLock<[i32; 2]> = OnceLock::new();

#[cfg(target_os = "linux")]
fn open_probe() -> Option<[i32; 2]> {
    let mut fds = [0i32; 2];
    // SAFETY: `pipe2` fills the caller-owned array.
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    (rc == 0).then_some(fds)
}

/// Whether `len` bytes at `ptr` can be read without faulting.
///
/// Any error, not only `EFAULT`, counts as unreadable: a wrong answer here faults
/// inside the handler and ends the process.
#[cfg(target_os = "linux")]
fn readable(ptr: *const u8, len: usize) -> bool {
    let Some(fds) = PROBE_FDS.get() else {
        return false;
    };
    // SAFETY: `write` reads at most `len` bytes from `ptr` and reports `EFAULT` rather
    // than faulting if it cannot. The fd is a pipe this process holds both ends of.
    unsafe { libc::write(fds[1], ptr.cast(), len) >= 0 }
}

/// `addr` as an offset into the executable file, if it points into this binary's code.
#[cfg(target_os = "linux")]
fn text_offset(addr: u64) -> Option<u64> {
    let image = IMAGE.get()?;
    let within = usize::try_from(addr).ok()?;
    image
        .text
        .contains(&within)
        .then(|| addr.saturating_sub(image.base as u64))
}

/// RSP's index into `mcontext_t::gregs`, as [`REG_RIP`].
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const REG_RSP: usize = 15;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const _: () = assert!(libc::REG_RSP == 15);

/// The stack pointer at the point of the fault.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn stack_pointer(cc: &crash_handler::CrashContext) -> u64 {
    cc.context.uc_mcontext.gregs[REG_RSP].cast_unsigned()
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn stack_pointer(cc: &crash_handler::CrashContext) -> u64 {
    cc.context.uc_mcontext.sp
}

#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
fn stack_pointer(_cc: &crash_handler::CrashContext) -> u64 {
    0
}

/// The return address, where the architecture keeps one in a register.
///
/// aarch64's `blr` leaves it in `x30`, so no memory read is needed. x86-64 pushes it,
/// and it is recovered from the stack instead.
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn link_register(cc: &crash_handler::CrashContext) -> Option<u64> {
    Some(cc.context.uc_mcontext.regs[30])
}

#[cfg(all(target_os = "linux", not(target_arch = "aarch64")))]
fn link_register(_cc: &crash_handler::CrashContext) -> Option<u64> {
    None
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

/// Emit what was formatted, and say so when it did not all fit.
///
/// A `Cursor` over a fixed slice stops at capacity and reports the failure rather than
/// panicking, so a report that outgrows its buffer ends mid-field. `complete` carries
/// whether that happened; the marker goes out in its own `write(2)`, since by then
/// there is no room in the buffer for it.
fn flush(buf: &[u8], position: u64, complete: bool) {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the cursor position is bounded by the buffer length"
    )]
    let written = position as usize;
    raw_write(&buf[..written]);
    if !complete {
        raw_write(b"=== crash report truncated ===\n");
    }
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
    let image = IMAGE.get();
    let base = image.map_or(0, |image| image.base) as u64;
    // An offset only means anything when `ip` is code from this binary. After a jump
    // through a corrupted pointer it is not — it can be in a shared library, or not
    // mapped at all — and `ip - base` is then a number `addr2line` will accept and
    // resolve to the wrong place.
    let offset = text_offset(ip);
    let uptime = START.get().map_or(0, |s| s.elapsed().as_secs());

    // Formatting into a fixed slice cannot allocate; every write is checked so an
    // overrun is reported rather than silently swallowed. See `flush`.
    let mut buf = [0u8; REPORT_BUF];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    let mut complete = true;
    complete &= write!(cur, "\n=== native crash ===\n").is_ok();
    // Already a formatted line, ending in a newline; reading it is an atomic load.
    if let Some(identity) = IDENTITY.get() {
        complete &= cur.write_all(identity.as_bytes()).is_ok();
    }
    complete &= write!(
        cur,
        "signal={} code={} ({}) ",
        signal_name(cc.siginfo.ssi_signo),
        cc.siginfo.ssi_code,
        signal_code_name(cc.siginfo.ssi_signo, cc.siginfo.ssi_code),
    )
    .is_ok();
    // A signal that was sent rather than faulted carries no address, but it does say
    // who sent it — which is the difference between aborting ourselves and being
    // killed from outside.
    complete &= match (fault_address(cc), signal_sender(cc)) {
        (Some(addr), _) => write!(cur, "addr=0x{addr:x}"),
        (None, Some((sender_pid, sender_uid))) => {
            write!(cur, "sender_pid={sender_pid} sender_uid={sender_uid}")
        }
        (None, None) => write!(cur, "addr=n/a"),
    }
    .is_ok();
    complete &= write!(cur, " ip=0x{ip:x} base=0x{base:x}").is_ok();
    // Say why there is no offset rather than print one that cannot be used. The two
    // reasons differ: an ip outside the mapping is the program's problem, an
    // unresolved base is this handler's.
    complete &= match offset {
        Some(offset) => write!(cur, " offset=0x{offset:x}"),
        None if image.is_none() => write!(
            cur,
            " (load base unresolved - /proc/self/maps was not parsed)"
        ),
        None => write!(cur, " (ip not in the spiced text mapping)"),
    }
    .is_ok();
    complete &= write!(
        cur,
        "\nthread=\"{}\" pid={} tid={} uptime={}s\n",
        thread, cc.pid, cc.tid, uptime,
    )
    .is_ok();
    // Only when the offset is one `addr2line` can actually use.
    if let Some(offset) = offset {
        complete &= writeln!(cur, "symbolize: addr2line -e spiced -fCi 0x{offset:x}").is_ok();
    }
    // Flushed before the stack section: reading the stack can fault again, and a
    // second fault inside the handler ends the process with the signal still blocked.
    // Writing first costs the stack section rather than the whole report.
    // Ends the cursor's borrow of `buf` before the buffer is read back.
    let position = cur.position();
    flush(&buf, position, complete);

    report_stack(cc, ip, fault_address(cc));
}

/// How many words above the stack pointer are examined for a return address.
#[cfg(target_os = "linux")]
const STACK_WORDS: usize = 16;

/// Where the crash came from, when the instruction pointer no longer says.
///
/// A call through a corrupted pointer still pushed its return address, and on aarch64
/// still left it in `x30`. Only words pointing into this binary's code are reported;
/// the rest of a stack resembles addresses without being them.
///
/// `ip` and `fault_addr` are passed in because the caller already has them.
#[cfg(target_os = "linux")]
fn report_stack(cc: &crash_handler::CrashContext, ip: u64, fault_addr: Option<u64>) {
    use std::io::Write as _;

    let sp = stack_pointer(cc);
    // A return address sits at the stack pointer only if nothing has run since the
    // call, which holds when the fault is the instruction fetch itself. A fault inside
    // a function — this binary's or a library's — has a frame of its own.
    let immediate_call = fault_addr == Some(ip);

    let mut buf = [0u8; REPORT_BUF];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    let mut complete = true;
    complete &= write!(cur, "stack: sp=0x{sp:x}").is_ok();

    // aarch64 keeps the return address in a register, so it needs no stack read.
    let mut caller = link_register(cc).and_then(|lr| {
        complete &= write!(cur, " lr=0x{lr:x}").is_ok();
        immediate_call.then_some(lr).and_then(text_offset)
    });

    // One probe for the whole window: a corrupt stack pointer is the case this guards,
    // and it fails the window as readily as it fails the first word.
    let window = usize::try_from(sp).ok().filter(|sp| {
        readable(
            std::ptr::with_exposed_provenance::<u8>(*sp),
            STACK_WORDS * size_of::<u64>(),
        )
    });

    let mut listed_candidates = false;
    for word in 0..window.map_or(0, |_| STACK_WORDS) {
        let Some(at) = window.and_then(|sp| sp.checked_add(word * size_of::<u64>())) else {
            break;
        };
        // SAFETY: the probe above established the whole window is readable, and a stack
        // word has no validity invariants.
        let value = unsafe {
            std::ptr::with_exposed_provenance::<u8>(at)
                .cast::<u64>()
                .read_unaligned()
        };
        let Some(offset) = text_offset(value) else {
            continue;
        };
        // Only the first word of an interrupted call is a return address; everything
        // else is a candidate, which may be live or long stale.
        if word == 0 && immediate_call && caller.is_none() {
            complete &= write!(cur, " ret=0x{value:x} (+0x{offset:x})").is_ok();
            caller = Some(offset);
        } else {
            if !listed_candidates {
                complete &= write!(cur, "\nstack candidates:").is_ok();
                listed_candidates = true;
            }
            complete &= write!(cur, " +0x{offset:x}").is_ok();
        }
    }
    complete &= writeln!(cur).is_ok();

    if let Some(offset) = caller {
        complete &= writeln!(
            cur,
            "symbolize caller: addr2line -e spiced -fCi 0x{offset:x}"
        )
        .is_ok();
    }
    complete &= write!(cur, "=== end native crash ===\n").is_ok();

    // Ends the cursor's borrow of `buf` before the buffer is read back.
    let position = cur.position();
    flush(&buf, position, complete);
}

/// A reduced report. `CrashContext` is platform-specific: on macOS it carries a Mach
/// exception rather than a signal, and the callback runs on a dedicated handler
/// thread, so neither the instruction pointer nor the crashing thread's name is
/// available. Deployments run Linux; this keeps the crate usable elsewhere.
#[cfg(not(target_os = "linux"))]
fn report(_cc: &crash_handler::CrashContext) {
    use std::io::Write as _;

    let uptime = START.get().map_or(0, |s| s.elapsed().as_secs());
    let mut buf = [0u8; REPORT_BUF];
    let mut cur = std::io::Cursor::new(&mut buf[..]);
    let mut complete = true;
    complete &= write!(cur, "\n=== native crash ===\n").is_ok();
    if let Some(identity) = IDENTITY.get() {
        complete &= cur.write_all(identity.as_bytes()).is_ok();
    }
    complete &= write!(
        cur,
        "(reduced report: full detail is Linux-only)\n\
         uptime={uptime}s\n\
         === end native crash ===\n",
    )
    .is_ok();
    // Ends the cursor's borrow of `buf` before the buffer is read back.
    let position = cur.position();
    flush(&buf, position, complete);
}

#[cfg(all(test, unix))]
mod tests {
    /// Set in the child process to select the crashing role.
    const CHILD: &str = "SPICED_CRASH_HANDLER_TEST_CHILD";

    /// Distinctive, so finding it proves the string came from the caller.
    const TEST_VERSION: &str = "v0.0.0-crash-handler-test";

    /// The address the child faults at.
    ///
    /// Not null: a null write reports `addr=0x0`, which a mis-decoded `siginfo` also
    /// produces, so it is the one address that cannot be checked. Wider than 32 bits
    /// to catch a truncating format, and below bit 47 to stay canonical — x86-64
    /// delivers `si_addr == 0` for a non-canonical address whatever was touched.
    #[cfg(target_os = "linux")]
    const FAULT_ADDR: usize = 0x5eed_dead_0000;

    /// macOS reports no address and need not raise `SIGSEGV` for a high one, so the
    /// non-Linux child keeps the null write.
    #[cfg(not(target_os = "linux"))]
    const FAULT_ADDR: usize = 0;

    /// Where the child faults. `inline(never)` and `no_mangle` keep it a distinct,
    /// named symbol so a report can be resolved back to it.
    #[inline(never)]
    #[unsafe(no_mangle)]
    extern "C" fn spiced_crash_handler_test_fault() {
        // SAFETY: a deliberate wild write — the behaviour under test.
        unsafe {
            std::ptr::with_exposed_provenance_mut::<u8>(FAULT_ADDR).write(1);
        }
    }

    /// Below the load base and never mapped, so the fault happens with `ip` already
    /// outside the image.
    #[cfg(target_os = "linux")]
    const WILD_CALL_ADDR: usize = 0x91;

    /// Jump through a corrupted function pointer, leaving `ip` outside this binary.
    #[cfg(target_os = "linux")]
    #[inline(never)]
    #[unsafe(no_mangle)]
    extern "C" fn spiced_crash_handler_test_wild_call() {
        // SAFETY: a deliberate jump to an unmapped address — the behaviour under test.
        let callee: extern "C" fn() =
            unsafe { std::mem::transmute::<usize, extern "C" fn()>(WILD_CALL_ADDR) };
        callee();
        // Unreachable, but stops the call becoming a tail jump, which would leave no
        // return address to recover.
        std::hint::black_box(());
    }

    /// Run the crashing child and return what it printed.
    ///
    /// The process under test dies, so this re-executes its own binary with `CHILD`
    /// set. The child always re-enters through `reports_a_fatal_signal`, which
    /// dispatches on the role.
    fn crash_child(role: &str) -> std::process::Output {
        // `module_path!` is crate-qualified; libtest filters are not.
        let module = module_path!();
        let filter = module
            .split_once("::")
            .map_or(module, |(_, rest)| rest)
            .to_owned()
            + "::reports_a_fatal_signal";

        let exe = std::env::current_exe().expect("locate the test binary");
        std::process::Command::new(exe)
            .args(["--exact", &filter, "--nocapture"])
            .env(CHILD, role)
            .output()
            .expect("run the crashing child")
    }

    /// Read a `key=value` field back out of a report.
    fn field<'a>(report: &'a str, key: &str) -> Option<&'a str> {
        report
            .split_whitespace()
            .find_map(|token| token.strip_prefix(key))
    }

    #[test]
    fn reports_a_fatal_signal() {
        use std::os::unix::process::ExitStatusExt as _;

        if let Some(role) = std::env::var_os(CHILD) {
            super::install(TEST_VERSION);
            match role.to_str() {
                Some("abort") => {
                    // SAFETY: raising a signal at ourselves is the behaviour under test.
                    unsafe {
                        libc::raise(libc::SIGABRT);
                    }
                }
                #[cfg(target_os = "linux")]
                Some("wild_call") => spiced_crash_handler_test_wild_call(),
                _ => spiced_crash_handler_test_fault(),
            }
            unreachable!("the child must terminate");
        }

        let output = crash_child("fault");
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            stderr.contains("=== native crash ==="),
            "no crash report on stderr.\nstderr: {stderr}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );

        // Which build crashed. Present on every platform, unlike the fields below.
        let version_field = format!("spiced={TEST_VERSION}");
        for field in [version_field.as_str(), "features=", "profile="] {
            assert!(
                stderr.contains(field),
                "report is missing `{field}`: {stderr}"
            );
        }

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

            // The reported address must be the one the child touched: reading it at
            // the wrong offset lands here as `addr=0x0`.
            assert!(
                stderr.contains(&format!("addr=0x{FAULT_ADDR:x}")),
                "reported fault address is not the one faulted on \
                 (expected addr=0x{FAULT_ADDR:x}): {stderr}"
            );
            assert!(
                stderr.contains("(SEGV_MAPERR)"),
                "report is missing the decoded si_code name: {stderr}"
            );
        }
    }

    /// A raised `SIGABRT` carries no fault address, so it covers a branch `SIGSEGV`
    /// never reaches. `abort()`, an allocation failure and an external kill all arrive
    /// this way.
    #[test]
    #[cfg(target_os = "linux")]
    fn reports_a_raised_abort() {
        use std::os::unix::process::ExitStatusExt as _;

        let output = crash_child("abort");
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(
            output.status.signal(),
            Some(libc::SIGABRT),
            "child should die from SIGABRT, got {:?}",
            output.status
        );
        assert!(
            stderr.contains("signal=SIGABRT"),
            "abort was not reported: {stderr}"
        );

        // No `_sigfault` union member, so there is no address to report.
        assert!(
            !stderr.contains("addr=0x"),
            "a signal with no fault address reported one anyway: {stderr}"
        );

        // `raise` targets this process, so it is its own sender.
        let sender = field(&stderr, "sender_pid=").expect("report should name the sender");
        let reported = field(&stderr, "pid=").expect("report should carry its own pid");
        assert_eq!(
            sender, reported,
            "a self-raised signal should name this process as the sender: {stderr}"
        );
    }

    /// A jump through a corrupted pointer leaves `ip` outside this binary, where no
    /// file offset from it is meaningful. The report says so instead of printing a
    /// command that resolves to whatever sits at that offset.
    #[test]
    #[cfg(target_os = "linux")]
    fn declines_to_symbolize_a_wild_jump() {
        use std::os::unix::process::ExitStatusExt as _;

        let output = crash_child("wild_call");
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(
            output.status.signal(),
            Some(libc::SIGSEGV),
            "child should die from SIGSEGV, got {:?}",
            output.status
        );
        assert!(
            stderr.contains(&format!("ip=0x{WILD_CALL_ADDR:x}")),
            "report should carry the wild instruction pointer: {stderr}"
        );
        assert!(
            stderr.contains("not in the spiced text mapping"),
            "report should say the ip is not in this binary: {stderr}"
        );

        // The two things that made such a report misleading: an offset clamped to
        // zero, and a command built from it.
        assert!(
            !stderr.contains("offset="),
            "no file offset should be reported for an ip outside the image: {stderr}"
        );
        assert!(
            !stderr.contains("symbolize:"),
            "a command that cannot work should not be printed: {stderr}"
        );

        // What the instruction pointer no longer says, the stack does.
        assert!(
            stderr.contains("stack: sp=0x"),
            "report should carry the stack pointer: {stderr}"
        );
        assert!(
            stderr.contains("symbolize caller: "),
            "report should recover a caller for a wild jump: {stderr}"
        );
    }
}
