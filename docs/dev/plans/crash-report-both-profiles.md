# Crash reporting: reliable on OSS `release` and Enterprise `release-lto`, gated in CI

Goal: a native crash in any shipped `spiced` — OSS or Enterprise, any flavor — produces
a report that identifies its exact artifact and symbolizes back to source, and every
release build proves that about itself before the artifact is published.

Grounded in two real SIGSEGVs (`v2.1.3-enterprise`, `tokio-rt-worker`), where the
report was present but not actionable: the fault address was garbage, the caller was
unrecoverable on a wild jump, the artifact could not be identified, and the
`addr2line` command the report printed resolved nothing.

---

## Verified build matrix

Checked against OSS `trunk` and `spicehq/spiceai` `trunk` (via API, not the stale
local clone).

| | OSS | Enterprise |
|---|---|---|
| Release workflow | `.github/workflows/build_and_release.yml` | `.github/workflows/ent_build_and_release.yml` |
| Default `profile_option` | `release` (`:56`) | **`release-lto`** (`ent:178`) |
| Profile → output dir | **broken** — builds `--profile $RUST_PROFILE`, moves `target/release/` (`:249`, `:282`, `:315`, `:333`, `:345`) | correct — `mv target/${{ env.RUST_PROFILE }}/…` (`ent:283`, `:314`, `:332`, `:391`) |
| `[profile.release]` | no section — cargo defaults | no section — cargo defaults |
| `[profile.release-lto]` | `lto = true` (`Cargo.toml:451`) | `lto = true`, `codegen-units = 1`, **`strip = true`** (`Cargo.toml:471-475`) |
| Effective strip | `-C strip=debuginfo` (cargo default) | `-C strip=symbols` |
| Shipped binary carries | `.symtab` yes, DWARF no | **neither** |
| Flavors | `default`, `odbc`, `metal` (macOS) | `core`, `default`, `models`, `nas`, + `ent_build_and_release_cuda` |

Two facts confirmed empirically rather than assumed: cargo passes `-C strip=debuginfo`
for both `release` and a `release`-inheriting custom profile when `debug = 0` and
`strip` is unset; and a custom profile's output directory is `target/<profile-name>/`.

**Trap:** `spicehq/spiceai` also contains `.github/workflows/build_and_release.yml`,
which is registered in Actions as *"build_and_release (upstream - disabled)"*. It
carries the OSS `target/release/` paths and is not what builds Enterprise. Every
Enterprise change below goes in `ent_build_and_release.yml`.

### What this changes about the earlier proposal

- **Enterprise is not affected by the packaging bug.** It already derives the output
  directory from the profile, which is exactly the fix that was proposed. The bug is
  real but OSS-only, and because OSS defaults to `release` it only breaks the
  `release-lto` *dispatch option* — not any shipped OSS artifact.
- **The root cause of the unsymbolizable report is `strip = true` at Enterprise
  `Cargo.toml:475`**, not the choice of `release-lto` as such. That single line is why
  the gist had to rebuild the inline chain by hand from `.eh_frame_hdr` and
  `core::panic::Location` records. Under OSS `release` the same crash would at least
  have named the enclosing function from `.symtab`.
- So the two profiles need *different* remediation, and the shared handler work has to
  produce a report that is useful at both symbol levels.

---

## Phase 0 — OSS: remove the `release-lto` dispatch option

OSS is not adopting LTO, so the packaging paths do not need to become
profile-derived. What must go is the *option*: `build_and_release.yml` offers
`release-lto` in `profile_option` (`:52-58`) while every packaging step reads
`target/release/` (`:249`, `:282`, `:315`, `:333`, `:345`). Dispatching it on a clean
runner fails at `mv`; on the self-hosted `spiceai-dev-runners`, whose workspace
persists, a stale `target/release/spiced` is packaged instead and the `release-lto`
output is silently discarded. The odbc (`:277`) and metal (`:310`) builds are
hardcoded `--release` anyway, so such a run would mix profiles in one set of
artifacts.

Fix: drop `release-lto` from the `profile_option` choices, leaving `release`. The
hardcoded `target/release/` paths are then correct by construction and the
stale-artifact trap is gone. Leave `[profile.release-lto]` in `Cargo.toml:451` —
nothing in OSS CI uses it, but deleting it would churn the section the Enterprise fork
overrides, for no gain.

Consequence for Phase 3: the OSS gate asserts `profile=release` and the Enterprise
gate asserts `profile=release-lto`. Both stay meaningful — the field is what proves an
artifact was built the way its repo intends.

---

## Phase 1 — handler correctness and identity (profile- and repo-independent)

`crash_handler.rs` is entirely profile-independent: `/proc/self/maps`, `gregs`,
siginfo, `prctl`. The additions below are too. This lands in OSS and reaches
Enterprise through the normal upstream-merge flow — no separate Enterprise change.

### 1a. Fix `addr=` — it is always garbage

#### What is wrong

`crash-handler-0.8.0/src/linux/state.rs:431` reinterprets the kernel's
`*libc::siginfo_t` as `libc::signalfd_siginfo` and memcpys it into
`CrashContext::siginfo`. These are two unrelated ABIs that happen to share a size.
Verified against `libc-0.2.189`, both 128 bytes on glibc x86_64 and aarch64:

| field | `siginfo_t` | `signalfd_siginfo` |
|---|---|---|
| 0 | `si_signo` | `ssi_signo` |
| 4 | `si_errno` | `ssi_errno` |
| 8 | `si_code` | `ssi_code` |
| 12 | padding | `ssi_pid` |
| 16 | **`si_addr`** (union `_sigfault`) | `ssi_uid` |
| 72 | padding | **`ssi_addr`** |

`crash_handler.rs:225` prints `ssi_addr` — offset 72, which in a `siginfo_t` is
trailing padding. That is why both real reports say `addr=0x0`, and it would have said
so for any fault at any address.

Only offsets 0–11 coincide, so `ssi_signo` and `ssi_code` are the *only* trustworthy
fields on that struct. Note `ssi_pid`/`ssi_uid` are not merely stale — they read
`siginfo_t` padding and the low half of `si_addr`. Our `pid`/`tid` are fine because
they come from `cc.pid`/`cc.tid`, which `state.rs:499` sets from `getpid`/`gettid`.

#### Proposed fix

The memcpy preserves the original bytes, so the address is recoverable in place — no
fork of the crate, no version bump. glibc's `libc` binding exposes no `si_addr()`
accessor (only musl and uclibc do), so read the offset directly.

```rust
/// `si_addr`'s offset within the kernel's `siginfo_t` on 64-bit Linux: the
/// `si_signo`/`si_errno`/`si_code` header plus four bytes of alignment padding,
/// then `_sifields._sigfault.si_addr` as the union's first member.
const SI_ADDR_OFFSET: usize = 16;

// `crash-handler` copies `size_of::<signalfd_siginfo>()` bytes *out of* a
// `siginfo_t`, so the destination type must not be the larger of the two, and
// `si_addr` has to fall inside what was copied. Both hold at 128 bytes today;
// assert rather than assume, so a libc change breaks the build instead of the
// report.
const _: () = assert!(size_of::<libc::signalfd_siginfo>() <= size_of::<libc::siginfo_t>());
const _: () = assert!(SI_ADDR_OFFSET + size_of::<u64>() <= size_of::<libc::signalfd_siginfo>());

/// The faulting address, or `None` when `siginfo` does not carry one.
///
/// `CrashContext::siginfo` is typed `libc::signalfd_siginfo` but holds the bytes
/// of a `siginfo_t`; the two layouts agree only on `si_signo`, `si_errno` and
/// `si_code`, so the address has to be read at its real offset.
#[cfg(target_os = "linux")]
fn fault_address(cc: &crash_handler::CrashContext) -> Option<u64> {
    // `_sigfault` is the live union member only for a kernel-raised fault. A
    // signal delivered by `kill`/`raise`/`pthread_kill` (SI_USER, SI_TKILL, …)
    // carries the sender's pid and uid in those same bytes.
    if !matches!(
        cc.siginfo.ssi_signo.cast_signed(),
        libc::SIGSEGV | libc::SIGBUS | libc::SIGILL | libc::SIGFPE | libc::SIGTRAP
    ) || cc.siginfo.ssi_code <= 0
        || cc.siginfo.ssi_code == libc::SI_KERNEL
    {
        return None;
    }

    // SAFETY: `siginfo` holds `siginfo_t` bytes (see above). The offset and the
    // read are bounded by the assertions, and the field is plain data.
    Some(unsafe {
        std::ptr::from_ref(&cc.siginfo)
            .cast::<u8>()
            .add(SI_ADDR_OFFSET)
            .cast::<u64>()
            .read_unaligned()
    })
}
```

`report()` then prints `addr=0x{:x}` or `addr=n/a`, so a `SIGABRT` no longer implies a
meaningless address. Pair it with a `code` name — `SEGV_MAPERR`/`SEGV_ACCERR`/
`BUS_ADRALN`/`SI_USER`/… — since `code=1` currently requires a lookup. `libc` exports
`SI_*`, `BUS_*` and `TRAP_*` but not `SEGV_*`; define those two locally.

#### Next steps, in order

1. **Land the decode and the `code` name.** Self-contained, no dependency change.
2. **Make the test able to fail.** `spiced_crash_handler_test_fault` writes to null, so
   the correct `si_addr` and the buggy padding are both `0x0` — the one address at
   which this fix is unverifiable. Fault at `0x5eed_dead_0000` instead and assert
   `addr=0x5eeddead0000`: unmapped, wider than 32 bits so a truncating format is caught
   too, and below bit 47 so it stays canonical.

   **Canonicality is not optional here.** On x86-64 a fault at a non-canonical address
   (any of bits 63:48 not sign-extending bit 47) is delivered with `si_addr == 0`
   regardless of the real address — this is what sent `crash-handling#49` down the
   wrong path, after its author changed the test fault to an address with the top 16
   bits set. A test address like `0xdead_beef_dead_beef` would read as zero and look
   exactly like the bug being fixed. Comment the constant so nobody "improves" it.

   The kernel reports the exact faulting address from CR2, not a page-aligned one; if a
   runner ever shows otherwise, compare the page rather than deleting the assertion.
3. **Run it on Linux.** Both the offset and `si_code` semantics are Linux-only, and
   nothing runs `spiced`'s unit tests on Linux — which is how this shipped. The
   manual `crash_report_check.yml` covers it now; add the same assertion to the
   Phase 3 release gate so it holds for the artifact people actually run.
4. **Comment on `EmbarkStudios/crash-handling#49`, which is already open.** "`ssi_addr`
   missing on Linux" (filed 2022-07-21 by the crate's own maintainer) reports exactly
   this symptom and closes on the wrong cause: a Mozilla/Breakpad maintainer attributed
   it to x86-64 *non-canonical* addresses being delivered as `si_addr == 0`, and the
   thread ends with "I have yet to see the kernel ever fill out this field, regardless
   of the instruction address". That last observation is the tell — it is always zero
   because offset 72 of a `siginfo_t` is padding, not because of the address used. The
   crate's stated reason for the cast (`state.rs:428`, "contains the si_pid field that
   we require") does not hold either: glibc's `siginfo_t` carries `si_pid` too. The fix
   is to keep `libc::siginfo_t` and translate explicitly. Still present on `main` and
   in 0.8.0, the latest release.

   Blast radius is wider than our log line: `minidump-writer`'s
   `src/linux/minidump_writer/exception_stream.rs:18` writes
   `exception_address: siginfo.ssi_addr` into the minidump exception stream, so every
   Linux minidump produced through this stack carries address 0. Worth saying in the
   issue — it makes the case for a fix rather than a workaround.

   Track whether a fixed release lets the shim be deleted; until then the local decode
   is the only correct reader, so it must not be "simplified" back to `ssi_addr`.
5. **Fence off the rest of the struct.** Comment at the `cc.siginfo` use site that only
   `ssi_signo` and `ssi_code` are valid, so nobody later reaches for `ssi_pid`,
   `ssi_uid` or `ssi_ptr` and gets padding.

#### Still open after 1a landed

- The `addr=n/a` branch has no test. The unit test faults with `SIGSEGV` only, so the
  non-fault path and the `SI_TKILL`/`SI_USER` code names compile and never run — the
  same shape of gap that let the original bug ship. Fixed by the `abort` scenario in
  3a, which needs no new machinery: one more child role that calls `raise(SIGABRT)`.
- `signal_code_name` decodes `SEGV_*`, `BUS_*` and `TRAP_*` but not `ILL_*` or `FPE_*`,
  so a `SIGILL` or `SIGFPE` report shows `(?)` where it advertises a name. Eight lines.

### 1g. Say who sent the signal, when nobody faulted

`SIGABRT` is hooked — `crash-handler`'s `EXCEPTION_SIGNALS` is
`[Abort, Bus, Fpe, Illegal, Segv, Trap]` — so an `abort()`, an allocation failure, a
double panic, and an external `kill -ABRT` all reach this handler. For those, 1a now
correctly prints `addr=n/a`, and then throws away the one thing `siginfo` does carry.

For `SI_USER`/`SI_TKILL` the live union member is `_kill`, whose `si_pid` and `si_uid`
sit at offsets 16 and 20 — reachable with exactly the machinery 1a already built. Print
them instead of `addr=n/a`:

```text
signal=SIGABRT code=-6 (SI_TKILL) sender_pid=1 sender_uid=0
```

`sender_pid` equal to our own pid means the process aborted itself; anything else means
something outside did. That is the difference between a bug in `spiced` and a liveness
probe or an OOM kill, and it is currently unanswerable from the report — which matters
because the runtime is deployed under Kubernetes, where being killed from outside is a
routine failure mode.

Same guard as 1a: read the field only when `si_code` says `_kill` is the live member,
never unconditionally.

### 1b. Recover the caller when `ip` is wild

Crash #2 (`ip=0x91`) is unresolvable because the return address at `*(RSP)` was never
captured. Add:

- x86_64: `gregs[REG_RSP]` (`libc::REG_RSP == 15`), the return address at `*(rsp)`,
  and a scan of the top ~16 stack qwords keeping only values inside the image range.
- aarch64: `sp` and `regs[30]` (LR), which holds the caller directly after a `blr`.

Two prerequisites:

- **Split the write.** The report is currently one buffered `write(2)` (`:239`); a
  second fault while touching the stack loses everything. Emit the core line first.
- **Probe before dereferencing.** Pre-open `/dev/null` at install and use
  `write(fd, ptr, 8)` — returns `EFAULT` instead of faulting, and is async-signal-safe.

This matters most for Enterprise: with `strip = true` a stack-derived return address is
the *only* handle on the caller, since there is no `.symtab` to fall back on.

### 1c. Dump the general-purpose registers

No dereference, no added risk. The gist had to disassemble to learn `%r15` held the
channel and `%rax` the vtable; the register file shows the corrupted pointer directly.

### 1d. Identify the artifact

Nine distinct Linux `spiced` artifacts ship per version across the two repos
(`default`/`odbc` OSS; `core`/`default`/`models`/`nas`/`cuda` Enterprise). Eliminating
flavors by comparing `.text` was a large part of the manual analysis. The report should
name its own artifact.

#### What today's report can already tell you, and what it can't

`get_version_string()` (`main.rs:173`) is better than "no identity at all": OSS
`version.txt` is `2.2.0-unstable` and Enterprise's is `2.2.0-enterprise-beta`, so the
version alone already separates the two repos. `build_metadata()` (`main.rs:192`)
appends semver build metadata for three features:

```rust
match (cfg!(feature = "models"), cfg!(feature = "metal"), cfg!(feature = "cuda"))
```

But none of it reaches the crash report — it only goes to the startup log line, which
in the real incident was long gone from the buffer. And even if it were printed, it
does not separate the flavors that matter:

| flavor | features | `build_metadata()` | distinguishable? |
|---|---|---|---|
| Ent `models` | `release,models,odbc` | `+models` | **no** — same as `nas` |
| Ent `nas` | `release,models,odbc,nfs,smb` | `+models` | **no** |
| Ent `default` | `release,odbc` | `""` | **no** — same as `core` |
| Ent `core` | `alloc-jemalloc` | `""` | **no** |
| OSS `default` | `release,models[,postgres-accel]` | `+models` | only by version |
| OSS `odbc` | `release,odbc,models` | `+models` | **no** — same as OSS `default` |

`odbc`, `nfs`/`smb`, `postgres-accel` and the allocator choice are all invisible, and
the profile is invisible too. That is exactly the ambiguity the gist had to resolve by
disassembly.

#### Tier 1 — three fields that close it (cheap, no new unsafe)

**`version=` — pass it at `install()`.** `main` already computes it, so the handler
should not recompute or duplicate the `cfg!` logic:

```rust
// main.rs, first statement in main()
spiced::crash_handler::install(&get_version_string());
```

**`flavor=` and `profile=` — `env!()` in `crash_handler.rs`**, populated by
`bin/spiced/build.rs`, which is in the same crate:

```rust
// bin/spiced/build.rs
// Cargo's own `PROFILE` collapses every release-inheriting profile to "release", so
// it cannot tell `release` from `release-lto`. The profile directory can:
// OUT_DIR is `<target>/<profile>/build/<pkg>-<hash>/out`.
let profile = std::env::var("OUT_DIR")
    .ok()
    .and_then(|dir| {
        let path = std::path::Path::new(&dir);
        Some(path.parent()?.parent()?.parent()?.file_name()?.to_str()?.to_owned())
    })
    .unwrap_or_else(|| "unknown".to_owned());
println!("cargo:rustc-env=SPICED_BUILD_PROFILE={profile}");

// Set by the release workflows to the artifact suffix, so a report names the tarball
// it came from. Unset for local builds.
println!("cargo:rerun-if-env-changed=SPICED_BUILD_FLAVOR");
let flavor = std::env::var("SPICED_BUILD_FLAVOR").unwrap_or_else(|_| "local".to_owned());
println!("cargo:rustc-env=SPICED_BUILD_FLAVOR={flavor}");
```

Both mechanisms verified empirically rather than assumed: for a build of the `release-lto`
profile with `--features release,models,odbc,nfs,smb`, cargo reports
`PROFILE=release` (useless) while `OUT_DIR` ends in `target/release-lto/build/…/out`
(exact). The workflows set the label next to where they already name the artifact:

```yaml
env:
  SPICED_BUILD_FLAVOR: ${{ matrix.flavor.name }}   # ent_build_and_release.yml
```

**Format the whole identity line once, at install.** Not field-by-field in the handler:

```rust
static IDENTITY: OnceLock<String> = OnceLock::new();

pub fn install(version: &str) {
    let _ = IDENTITY.set(format!(
        "spiced={version} flavor={} profile={}\n",
        env!("SPICED_BUILD_FLAVOR"),
        env!("SPICED_BUILD_PROFILE"),
    ));
    …
}
```

The handler then emits those bytes with one `raw_write` — no formatting, no allocation,
and no pressure on the 512-byte report buffer (1f). `OnceLock::get` is an atomic load,
so this stays signal-safe.

Resulting first line:

```text
spiced=v2.2.0-enterprise-beta+models flavor=nas profile=release-lto
```

which is unambiguous across all nine artifacts, and directly names the tarball to
download.

#### Tier 2 — `build_id=`, to prove it rather than trust it

Tier 1 is self-reported: it says what the binary was *compiled* as. `build_id=` pins
which binary is actually running, which matters when someone is running something other
than what they believe. Resolve once at install via `dl_iterate_phdr` → `PT_NOTE` →
`NT_GNU_BUILD_ID`, hex-encoded into a static — roughly 50 lines of `unsafe` ELF note
walking, all of it at install time, none in the handler. (Reading the notes out of
`/proc/self/exe` with `std::fs` is the safer-but-wordier alternative; `std::fs` is fine
at install.)

**Build-id is not guaranteed to exist.** GNU ld defaults to `--build-id=sha1` on most
distros; `lld` does not, and `.cargo/config.toml:11` forces `-fuse-ld=lld` on aarch64.
Add `-Clink-arg=-Wl,--build-id=sha1` for the release profiles on all Linux targets in
both repos, and have Phase 3 assert it is present. `strip = true` does **not** remove
the build-id note, so this works on Enterprise as-is.

Pair it with a **build-id manifest published as a release asset** — one line per
artifact, `build-id → flavor, arch, profile, version` — produced by the same workflow
step that packages each tarball. That is what makes a build-id in a customer log
resolvable months later without downloading nine binaries to `readelf` them. It is also
the natural place to record the `.debug` sidecar name from Phase 2.

#### Also log it at startup

`main.rs:160` already logs `Starting runtime {version}`; extend it with the same flavor
and profile. Costs nothing, and means a crash-free support case can answer the same
question.

#### What this would have done for the real crash

Tier 1 alone replaces the whole "Getting the right binary" section of the analysis —
the flavor elimination, the byte-comparison of `.text` across `default`/`models`/`nas`/
`cuda`, and the wrong turn where `nas` showed an unrelated instruction and `cuda`
disassembled misaligned. Tier 2 would have removed the residual "the two agree at this
offset, so it probably doesn't matter" caveat.

### 1e. Do not print a command that cannot work

`offset = ip.saturating_sub(base)` (`crash_handler.rs:299`) clamps to `0` whenever
`ip < base`, and the `symbolize:` line (`:333`) is printed unconditionally. Crash #2
therefore ended with `offset=0x0` and `symbolize: addr2line -e spiced -fCi 0x0` — a
command that resolves to *something*, at the very moment the report should have said
the instruction pointer was not in this binary at all.

#### Widen what install resolves

`LOAD_BASE` records only the base. Record the executable mapping too, so the report can
tell "in this binary" from "somewhere else":

```rust
/// Where the running binary is mapped. Resolved at install: parsing `/proc/self/maps`
/// is not signal-safe, and none of it changes while the process runs.
struct Image {
    /// Start of the mapping at file offset 0 — what `ip - base` is an offset into.
    base: usize,
    /// The executable mapping. An `ip` outside it is not code from this binary, so no
    /// file offset computed from it means anything.
    text: core::ops::Range<usize>,
}

static IMAGE: OnceLock<Image> = OnceLock::new();
```

One pass over `/proc/self/maps` fills both: the exe-backed line at file offset `0` gives
`base` (the existing rule, unchanged), and the exe-backed lines with `x` in their perms
give `text` — take the lowest start and highest end across them, since `-z separate-code`
can split the text segment in two.

**Test against `text`, not against `[base, end_of_image)`.** The mapping set also covers
the read-only and data segments, and an `ip` in those is just as much not-code; more
importantly the common real case is an `ip` inside a *shared library* — libc, libcuda —
where `ip - base` is a large bogus number that `addr2line` will happily accept.

#### Branch the tail of the report

```rust
let image = IMAGE.get();
let base = image.map_or(0, |i| i.base);
let in_text = image.is_some_and(|i| i.text.contains(&(ip as usize)));
```

- `in_text` → `offset=0x…` and the `symbolize:` line, exactly as today.
- `IMAGE` resolved but `ip` outside `text` → no `offset=`, no `symbolize:`, and say why:
  `ip=0x91 (outside the spiced text mapping — wild jump)`.
- `IMAGE` unresolved → also no offset, with the different reason:
  `(load base unresolved — /proc/self/maps could not be parsed)`. Distinguishing the two
  matters: one is a bug in the crashing program, the other is a bug in this handler.

#### Pre-format the symbolize prefix at install

`-e spiced` is not runnable as printed. `read_load_base` already resolves
`/proc/self/exe`, so keep it — and, as with `IDENTITY`, build the whole prefix once at
install rather than formatting a path inside the handler:

```rust
static SYMBOLIZE_PREFIX: OnceLock<String> = OnceLock::new();
// "symbolize: addr2line -e /usr/local/bin/spiced -fCi 0x"
```

The handler then writes those bytes and appends `{offset:x}`. This also removes the one
unbounded-length input from the report, which is what 1f needs to be able to bound it.

#### Verify it

The `wild_call` scenario in 3a is exactly this case: assert the report contains
`outside the spiced text mapping`, contains no `symbolize:` line, and — once 1b lands —
that the recovered return address *is* in `text` and does symbolize.

### 1f. Do not let the report truncate silently

Every `write!` in `report()` is `let _ = write!(…)`, and a `Cursor` over a fixed slice
stops at capacity and reports `WriteZero` rather than panicking. So the failure mode is
a report that simply ends mid-field, with nothing saying it did. The buffer is 1 KiB
against roughly 330 bytes of typical content, so there is margin today — but the margin
is the only thing protecting it, and 1b (registers, stack words) is a large addition
sitting behind an unchecked budget.

Three layers, cheapest first.

#### (a) Bound the variable-length inputs at install

Two inputs have no inherent limit: the version string inside `IDENTITY`, and the exe
path inside the `SYMBOLIZE_PREFIX` that 1e introduces (`PATH_MAX` is 4096, four times
the whole buffer). Cap both where truncation is safe and observable:

```rust
const MAX_IDENTITY: usize = 192;
const MAX_SYMBOLIZE_PREFIX: usize = 320;
```

Clip at install and `tracing::warn!` when clipping happens — install time is not a
signal handler, so logging is fine there. Everything else in the report is then
fixed-width.

#### (b) A `const` assertion on the worst case

With (a) in place the maximum is computable, so make the compiler check it:

| part | max bytes |
|---|---|
| banner + trailer | 47 |
| identity | `MAX_IDENTITY` |
| `signal=` + `code=` + decoded name | 7 + 11 + 13 |
| `addr=0x` + 16 hex | 23 |
| `ip=` / `base=` / `offset=` | 3 × 26 |
| `thread="…"` (`TASK_COMM_LEN`) | 24 |
| `pid=` / `tid=` | 2 × 15 |
| `uptime=` (u64) | 27 |
| symbolize prefix + 16 hex | `MAX_SYMBOLIZE_PREFIX` + 17 |

```rust
const MAX_REPORT: usize = /* the sum above */;
const _: () = assert!(MAX_REPORT <= REPORT_BUF);
```

Adding a field without adjusting the arithmetic then breaks the build rather than
silently eating the `symbolize:` line.

#### (c) A runtime backstop, because (b) is arithmetic a human maintains

Thread the write results and, if any failed, emit a fixed marker in its own `write(2)`
— by definition there is no room for it in the buffer:

```rust
let mut complete = true;
complete &= write!(cur, …).is_ok();
…
raw_write(&buf[..written]);
if !complete {
    raw_write(b"=== crash report truncated ===\n");
}
```

This is what survives someone adding a field and updating neither the constant nor the
table.

#### (d) Make the formatting testable off Linux

(b) is only as good as the arithmetic, and nothing currently exercises it. The way to
test it is to stop formatting straight out of a `CrashContext`:

```rust
struct ReportFields<'a> { identity: &'a str, signal: &'a str, thread: &'a str, … }

fn format_report(fields: &ReportFields<'_>, out: &mut [u8]) -> (usize, bool)
```

`report()` becomes the part that extracts fields from the platform context, and a unit
test can feed the worst case — `MAX_IDENTITY`-length identity, `u64::MAX` for every
numeric field, a full 15-byte thread name — and assert it fits with `complete == true`.

The real prize is that this test is **platform-independent**. Every field the handler
prints today lives behind `#[cfg(target_os = "linux")]`, so the macOS dev loop compiles
none of it and the only feedback is a CI run that takes an hour. A pure formatting
function runs everywhere in milliseconds.

#### Sequencing

(a)+(b)+(c) are one small change and should land together. Do them **before** 1b, not
after: 1b is the change that actually strains the budget, and it should land against a
checked one. (d) is worth doing in the same PR if the refactor stays contained, since
without it (b) is unverified arithmetic — but it is the one piece here that touches the
shape of `report()` rather than adding to it.

---

## Phase 2 — ship symbols, per repo

The two profiles sit at different symbol levels, so this is the one phase that is not
shared.

### 2a. Enterprise — `strip = true` is the headline fix

`Cargo.toml:475` strips the symbol table as well as DWARF, which is why the shipped
binary yields nothing. Compounding it, `lto = true` + `codegen-units = 1` maximizes
cross-crate inlining, so even a symbol table would often name the wrong crate — the
inline chain is what is actually needed, and that is DWARF-only.

Change `[profile.release-lto]` to `debug = 1`, `strip = "none"`, then in
`ent_build_and_release.yml` after each binary is built:

```
objcopy --only-keep-debug spiced spiced.debug
objcopy --strip-debug --add-gnu-debuglink=spiced.debug spiced
```

The shipped binary keeps `.symtab`, the build-id note and the debuglink; DWARF moves
to the sidecar. Net size change against today's fully-stripped artifact is small
(`.symtab` only); measure it, since Enterprise ships four Linux flavors.

Publish `spiced{suffix}_{os}_{arch}.debug.tar.gz` per flavor, tagged with the build-id
so a report matches its sidecar without guessing.

`objcopy` is chosen over `split-debuginfo = "packed"` deliberately: `.dwp`
completeness under fat LTO with `codegen-units = 1` is not something to assume, and
this profile is precisely that configuration. `[profile.release-profiling]`
(`Cargo.toml:491`) already encodes the `.dwp` recipe if it turns out to work — Phase 3
is the test that decides.

Also add `codegen-units = 1` to the *comparison* only, not to OSS: it is an Enterprise
performance choice and changing it is out of scope here.

### 2b. OSS — keep `.symtab`, add the sidecar

OSS `release` already retains `.symtab`, so an OSS crash names its enclosing function
today. The gap is line numbers and inline frames. Same treatment: `debug = 1` +
`strip = "none"` on `[profile.release]`, `objcopy` sidecar published next to each
artifact. Lower urgency than 2a — OSS reports are degraded, not empty.

Both changes lengthen an already-long release job; measure before committing.

---

## Phase 3 — e2e gate on the real artifact, in both workflows

`.github/workflows/crash_report_check.yml` (added, OSS, `workflow_dispatch`) is the
fast dev loop: it faults the `spiced` **test** binary and asserts the emitted `offset`
resolves to `spiced_crash_handler_test_fault`. That is not the shipped artifact —
different link shape, no LTO, different strip, different features — so it cannot tell
you whether a *release* report is usable, and it says nothing about Enterprise at all.

### 3a. Fault injection reachable from the shipped binary

An env-gated hook checked once in `main`, immediately after `crash_handler::install()`:

```
SPICED_CRASH_TEST=wild_write | wild_call | abort
```

| value | shape | asserts |
|---|---|---|
| `wild_write` | write to a **non-null** unmapped address, e.g. `0xdead_0000` | the `addr=` decode (1a) — null is the one address where the buggy and correct values are identical, which is why the current test cannot catch it |
| `wild_call` | indirect call through a corrupted pointer — crash #2's shape | `ip` outside the image is labelled (1e) and the return address symbolizes to the caller (1b) |
| `abort` | `SIGABRT` via `raise` | the non-fault path: reported at all, and `addr=n/a` rather than a fabricated address (1a) |

Security note: reachable only by whoever already controls the process environment, who
can do strictly more than crash it. Undocumented, absent from `--help`. It must **not**
be behind a cargo feature — gating it would mean testing a binary that is not the one
shipped, which is the entire point. It lands in OSS `main.rs` and reaches Enterprise
through the upstream merge, so both binaries carry it.

### 3b. Run it before upload, in each workflow

One step on the Linux legs of `build_and_release.yml` and `ent_build_and_release.yml`,
after the binary and sidecar exist and before publication. Each runs at whatever
profile its repo uses, so `release` and `release-lto` are both covered by
construction — no duplicate `release-lto` build to pay for, and no separate profile
matrix.

Assertions per scenario:

1. child exits 139 (134 for `abort`) and stderr contains `=== native crash ===`
2. `base=` is non-zero — `/proc/self/maps` parsing works on this runner
3. `build_id=` present and equal to `readelf -n spiced`
4. `version=` and `profile=` match the job's `REL_VERSION` and `RUST_PROFILE` — the
   OSS gate expects `release`, the Enterprise gate `release-lto`
5. `wild_write`: `addr=` equals the injected fault address, and `code=` names
   `SEGV_MAPERR`
6. `abort`: `addr=n/a` rather than a number — the union carries no address for a
   raised signal
7. `offset` lies within the file size
8. `addr2line -e spiced.debug -fCi <offset>` resolves to the injected fault site
9. `wild_call`: `ip` reported as outside the image, no `symbolize:` line printed, and
   the captured return address resolves to the caller
10. the `.debug` sidecar exists and is non-trivial in size

Failing any of these fails the release build. A report that cannot be symbolized is
not worth shipping — that is the whole lesson of the two crashes.

Run it for one flavor per arch, not all nine: the handler path is flavor-independent,
and `build_id=` correctness is what proves the rest are distinguishable.

### 3c. Coverage

- Both Linux arches — 1b needs a separate aarch64 arm (`sp`/`x30`) and only a real
  aarch64 run proves it. Enterprise excludes `nas` from aarch64; use `default`.
- macOS takes the reduced non-Linux `report()` (`crash_handler.rs:247`); Windows builds
  no `spiced`. Gate is Linux-only; say so in the workflow comment.
- Add `wild_call` to the existing unit test too, so the fast loop covers the shape.

---

## Phase 4 — deferred: a symbolized backtrace via pipe-plus-listener

**Not in scope for this work.** Recorded here so the ceiling of the current design is
explicit, and so the decision to stop at Phases 1–3 is a choice rather than an
oversight.

Everything above keeps the report to a single line written from inside the handler.
That is the right shape while the handler must be async-signal-safe: no allocation, no
locks, no `tracing`. It also caps what the report can ever say — one frame. Phase 1b
recovers the immediate caller from the stack, which covers the two crashes we have,
but it is not a backtrace and will not resolve a fault several frames deep in an
inlined DataFusion or Vortex call chain.

The established way past that ceiling is to do no symbolization in the handler at all.
ClickHouse's `src/Common/SignalHandlers.cpp` is the closest peer implementation — an
in-process fatal handler in a database:

- the handler writes the raw signal number, `siginfo`, `ucontext` and a bare (unsymbolized)
  stack trace into a pipe with `WriteBufferFromFileDescriptorDiscardOnFailure`;
- a dedicated `SignalListener` thread reads the pipe and does the symbolization, where
  allocation and locking are legal;
- the dying thread waits, bounded, for the listener to finish so the log is complete
  before the process goes away.

Its crash line already reports version, build id and git hash — which is Phase 1d, and
independent confirmation that those fields are the ones that matter.

Why this is deferred rather than folded in:

- Phases 1–3 are what the two real crashes actually needed. A backtrace would have been
  a convenience; a correct `addr=`, a recoverable caller and a symbolizable offset were
  the blockers.
- It is a materially larger change — a second thread, a pipe, a bounded shutdown
  handshake, and a stack walker that is safe to run from a signal handler — with its own
  failure modes in the exact situation where the process is already unhealthy.
- Most of its value evaporates without Phase 2. A symbolized backtrace still needs the
  debug info to symbolize *against*; on today's Enterprise `strip = true` binary it
  would print addresses, which is what we already have.

Revisit if, after Phases 1–3 have shipped and caught a real crash, the single frame plus
return address is still not enough to localize a fault. Prerequisites: Phase 2 in both
repos, and Phase 3 green — a second thread in the crash path is only worth adding once
the release gate can prove the simple path still works.

---

## Where each change lands, and in what order

| | repo | file(s) | depends on |
|---|---|---|---|
| Phase 0 | OSS | `build_and_release.yml:52-58` | — |
| 1a, 1e, 1f | OSS → merges to Ent | `crash_handler.rs` | — |
| 1b, 1c | OSS → merges to Ent | `crash_handler.rs` | 1f (shared split-write restructure) |
| 1d | OSS → merges to Ent | `bin/spiced/build.rs`, `crash_handler.rs`, `.cargo/config.toml` | — |
| 2a | **Enterprise only** | `Cargo.toml:471-475`, `ent_build_and_release.yml` | 1d |
| 2b | OSS | `Cargo.toml` `[profile.release]`, `build_and_release.yml` | 1d |
| 3a | OSS → merges to Ent | `bin/spiced/src/main.rs` | 1 |
| 3b | both, separately | `build_and_release.yml`, `ent_build_and_release.yml` | 0, 1, 2 |
| Phase 4 | deferred — not scheduled | — | 2, 3 shipped and proven |

Only Phase 2a and the workflow halves of 0/3b are Enterprise-specific edits; everything
else flows down through the upstream merge, so the handler must not depend on anything
Enterprise-only.

Suggested PR split:

1. OSS: **Phase 1a** — the `addr=` decode, the `code` name, and the test moved off null
   so it can fail. Smallest correct-by-itself unit, and the field the next real crash
   most needs. Verified by the manual `crash_report_check.yml` run on Linux.
2. OSS: Phase 0 — one-line removal of the `release-lto` option.
3. OSS: rest of Phase 1 — 1e/1f, then 1b/1c on the split-write restructure, then 1d.
4. Enterprise: Phase 2a — `strip = true` → `debug = 1` + sidecar. Highest single-step
   value of the whole plan; can land ahead of Phase 3 and immediately makes the next
   Enterprise crash analyzable from `.symtab` alone.
5. OSS: Phase 2b + 3a + 3b, then the mirrored 3b in Enterprise — debug-info shipping
   and the release gate together, since neither is verifiable without the other.
