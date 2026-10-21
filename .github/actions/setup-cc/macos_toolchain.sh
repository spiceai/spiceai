#!/usr/bin/env bash
#
# Puts the cc toolchain on a macOS runner: installs it when a formula is
# genuinely absent, and otherwise only exposes what is already there.
#
# Invoked by action.yml. Kept in its own file so test_macos_toolchain.sh can
# drive it against a fake Homebrew prefix, with `brew` stubbed to succeed or
# fail, which the inline step could not be.
#
# These runners are long-lived and already carry the toolchain, so `brew
# install` is normally a no-op — but it still has to take Homebrew's global
# lock first, and when that lock is not writable the install aborts before
# doing any work. Every macOS job on the pool then fails here over a toolchain
# that is already present (spiceai#13463). So the script probes the Cellar and
# PATH directly, which costs no `brew` invocation, and reaches for `brew` only
# when a formula is missing outright.
#
# Two different conditions used to share one remedy (spiceai#13479):
#
#   - a formula whose opt directory is absent genuinely needs installing;
#   - cmake or pkg-config resolving to something outside the Homebrew prefix
#     is present but unexposed — an unlinked keg, or a foreign copy earlier on
#     PATH shadowing it.
#
# Routing the second through `brew install` reintroduced exactly the failure
# the probe was added to avoid: on a runner where `brew` cannot run, a merely
# unlinked keg blocked every macOS gate. The second condition is now handled
# without `brew` at all, by putting the keg's own bin directory in front.

set -euo pipefail

brew_bin="$(command -v brew || true)"
if [ -z "$brew_bin" ]; then
  echo "::error::'brew' is not on PATH, so the cc toolchain cannot be installed or verified on this runner."
  exit 1
fi
# `command -v brew` is not `$prefix/bin/brew` on the spiceai-macos pool: a
# flock wrapper lives at `/opt/spice/bin/brew` (and a copy under gnu-tar
# gnubin). `dirname` twice of that path is `/opt/spice`, which has no Cellar,
# so every formula would look missing and we would `brew install` kegs that
# are already present. Probe the well-known prefixes first and do not invoke
# `brew` (that takes the global lock this probe exists to avoid). Fall back to
# the dirname heuristic for unusual layouts. The candidate list is overridable
# so the test harness can stand up a prefix of its own; nothing in CI sets it.
brew_prefix=""
for candidate in ${SETUP_CC_WELL_KNOWN_PREFIXES:-/opt/homebrew /usr/local}; do
  if [ -x "$candidate/bin/brew" ]; then
    brew_prefix="$candidate"
    break
  fi
done
if [ -z "$brew_prefix" ]; then
  brew_prefix="$(dirname "$(dirname "$brew_bin")")"
fi

# Both files are appended to line by line, so a value carrying a newline would
# inject entries into every remaining step. `brew_prefix` is derived from
# wherever `brew` resolved, which is not under this script's control.
refuse_multiline() {
  local what="$1" value="$2"
  if [ "$(printf '%s' "$value" | wc -l | tr -d ' ')" != "0" ]; then
    echo "::error::Refusing to export a multi-line ${what} built from '${brew_prefix}'."
    exit 1
  fi
}

# The kegs a formula name may live under. Homebrew ships some of these under
# another name: `pkg-config` is the `pkgconf` keg, `openssl` is `openssl@3`.
# Both names count for the presence check and for the keg's bin directory
# below, so an alias never reads as a missing formula.
keg_candidates() {
  case "$1" in
    pkg-config) echo "pkg-config pkgconf" ;;
    openssl)    echo "openssl openssl@3" ;;
    *)          echo "$1" ;;
  esac
}
formula_present() {
  local keg
  for keg in $(keg_candidates "$1"); do
    [ -d "$brew_prefix/opt/$keg" ] && return 0
  done
  return 1
}
# The keg bin directory holding an executable `$1`, or nothing.
keg_bin_for() {
  local keg
  for keg in $(keg_candidates "$1"); do
    if [ -x "$brew_prefix/opt/$keg/bin/$1" ]; then
      echo "$brew_prefix/opt/$keg/bin"
      return 0
    fi
  done
  return 1
}

formulas=(cmake pkg-config openssl unixodbc llvm libnfs)
absent=()
for formula in "${formulas[@]}"; do
  formula_present "$formula" || absent+=("$formula")
done

if [ ${#absent[@]} -ne 0 ]; then
  echo "cc toolchain incomplete on this runner (${absent[*]}); installing."
  brew install "${formulas[@]}"
  # `brew install` only warns — and still exits 0 — when a formula is already in
  # the Cellar but unlinked, so its binaries never reach PATH. Link the tools
  # that build scripts invoke by bare name; the exposure pass below covers a
  # link that still does not win on PATH. openssl and llvm are keg-only and are
  # deliberately left unlinked; the build locates them via `brew --prefix`.
  for tool in cmake pkg-config; do
    brew link --overwrite "$tool" || true
  done
else
  echo "cc toolchain already present under $brew_prefix; skipping 'brew install'."
fi

# cmake and pkg-config are invoked by bare name from build scripts, so being in
# the Cellar is not sufficient — the name also has to resolve to *this*
# Homebrew. When it does not, the keg is present (its opt directory was checked
# above) and merely unexposed, so the remedy is the keg's bin directory in
# front of PATH: for this script, and via GITHUB_PATH for the steps after it.
# That needs no `brew` invocation, and it also covers what `brew link` does
# not: linking rewrites the prefix's bin entries but does not reorder PATH, so
# a foreign copy earlier on PATH would stay selected. Matching on the prefix
# rather than a fixed path keeps this correct on an Intel runner, where
# Homebrew itself lives under /usr/local.
for tool in cmake pkg-config; do
  resolved="$(command -v "$tool" 2>/dev/null || true)"
  case "$resolved" in
    "$brew_prefix"/*) ;;
    *)
      if keg_bin="$(keg_bin_for "$tool")"; then
        echo "'$tool' resolves to '${resolved:-nothing}' rather than the Homebrew keg; putting $keg_bin first on PATH."
        refuse_multiline "PATH entry" "$keg_bin"
        PATH="$keg_bin:$PATH"
        export PATH
        echo "$keg_bin" >> "$GITHUB_PATH"
      fi
      ;;
  esac
done

# Assert unconditionally, so a runner whose install was skipped still fails
# here with a named cause rather than deep inside a build script. This asserts
# only that the name resolves at all — deliberately not that it resolves to
# $brew_prefix — so a runner that builds today against a tool from elsewhere on
# PATH keeps working instead of newly failing here. Tightening that is a change
# of policy, not a bug fix.
unresolved=()
for tool in cmake pkg-config; do
  command -v "$tool" >/dev/null 2>&1 || unresolved+=("$tool")
done
if [ ${#unresolved[@]} -ne 0 ]; then
  echo "::error::Not on PATH: ${unresolved[*]}. Run 'brew link --overwrite ${unresolved[*]}' on this runner."
  exit 1
fi

# `odbc-sys` takes its whole library search path from `brew --prefix`, so on a
# runner where brew cannot run it falls back to the DYLD defaults and the link
# dies far downstream as "ld: library 'odbc' not found" (#13475). clang expands
# LIBRARY_PATH into -L entries after those already on the command line, so this
# restores the search path without displacing anything a build script emitted,
# and it is not a cargo fingerprint input, so it costs no rebuild.
#
# Only the unixodbc keg goes on the path, never $brew_prefix/lib. The keg holds
# just the libodbc* families; the linked directory holds a few hundred dylibs,
# and measured with `cc -l<name> -Wl,-t`, publishing it newly satisfies -lcrypto,
# -lssl and -lzstd, none of which resolve without it. A crate whose build script
# emits one of those names (openssl-sys, libssh2-sys, zstd-sys) would then link a
# second, differently-built copy on every macOS job in the repo. The keg path is
# also what covers the unlinked-formula case the probe above accepts, so the
# linked directory would contribute nothing but that blast radius.
library_path="${LIBRARY_PATH:+$LIBRARY_PATH:}$brew_prefix/opt/unixodbc/lib"
refuse_multiline "LIBRARY_PATH" "$library_path"
echo "LIBRARY_PATH=$library_path" >> "$GITHUB_ENV"
