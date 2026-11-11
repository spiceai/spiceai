#!/usr/bin/env bash
#
# Unit tests for macos_toolchain.sh. No Homebrew and no network: every case
# builds a fake prefix under a temporary directory, with a `brew` stub that
# records what it is asked to do and can be told to fail the way a runner
# whose global lock is not writable fails (spiceai#13463), then drives the
# real script against it with PATH, GITHUB_PATH and GITHUB_ENV under control.
#
# The cases that matter are the ones that tell the two conditions apart
# (spiceai#13479): a keg that is present but unlinked, a keg shadowed by a
# foreign copy earlier on PATH, and both of those on a runner where `brew`
# cannot run at all. Those must complete without `brew install`; only a
# formula whose opt directory is genuinely absent may reach for it.
#
# Usage: test_macos_toolchain.sh
#   SUBJECT=<path> runs another script through the same cases, which is how
#   the pre-fix step was shown to fail them.

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
subject="${SUBJECT:-$script_dir/macos_toolchain.sh}"
# Made absolute up front: run_subject changes into the fake prefix before it
# invokes the subject, so a relative SUBJECT would be looked up there.
subject="$(cd "$(dirname "$subject")" && pwd)/$(basename "$subject")"
if [ ! -f "$subject" ]; then
  echo "SUBJECT does not exist: ${SUBJECT:-$subject}" >&2
  exit 2
fi

# Tools the subject itself needs (dirname, wc, tr, printf, command). Resolved
# once so the fake PATH can be built from them instead of the host's PATH.
system_bin="$(cd "$(dirname "$(command -v dirname)")" && pwd)"

tests_run=0
failures=0
formulas=(cmake pkg-config openssl unixodbc llvm libnfs)

fail_test() {
  failures=$((failures + 1))
  echo "  FAIL: $1"
}

# A fresh fake prefix. Sets `prefix`, `log`, `github_path`, `github_env` and
# `foreign_bin` for the case that follows.
new_prefix() {
  prefix="$(mktemp -d)"
  log="$prefix/brew.log"
  github_path="$prefix/github_path"
  github_env="$prefix/github_env"
  # Outside the prefix on purpose: the subject tells Homebrew's copy from a
  # foreign one by path, so a foreign copy nested under the prefix would read
  # as Homebrew's.
  foreign_bin="$(mktemp -d)/bin"
  mkdir -p "$prefix/bin" "$prefix/opt" "$foreign_bin"
  : > "$log"
  : > "$github_path"
  : > "$github_env"
  # The stub records every invocation. `install` populates the kegs the way a
  # successful install would, and `link --overwrite` copies a keg's binary into
  # the prefix's bin, which is all the subject can observe of either. With
  # BREW_FAIL set every subcommand aborts before doing anything, the way a
  # runner whose global lock is not writable does.
  cat > "$prefix/bin/brew" <<'STUB'
#!/usr/bin/env bash
echo "$*" >> "$BREW_LOG"
if [ -n "${BREW_FAIL:-}" ]; then
  echo "Error: Could not acquire lock; another Homebrew process is running." >&2
  exit 1
fi
case "$1" in
  install)
    shift
    for formula in "$@"; do
      mkdir -p "$BREW_PREFIX/opt/$formula/bin"
      printf '#!/usr/bin/env bash\necho %s\n' "$formula" > "$BREW_PREFIX/opt/$formula/bin/$formula"
      chmod +x "$BREW_PREFIX/opt/$formula/bin/$formula"
    done
    ;;
  link)
    tool="${*: -1}"
    cp "$BREW_PREFIX/opt/$tool/bin/$tool" "$BREW_PREFIX/bin/$tool"
    ;;
esac
STUB
  chmod +x "$prefix/bin/brew"
}

# A keg in the fake Cellar: opt/<formula>/bin/<formula>, executable.
make_keg() {
  local formula="$1"
  mkdir -p "$prefix/opt/$formula/bin"
  printf '#!/usr/bin/env bash\necho %s\n' "$formula" > "$prefix/opt/$formula/bin/$formula"
  chmod +x "$prefix/opt/$formula/bin/$formula"
}

# Every formula present; neither bare-name tool is linked.
make_all_kegs() {
  local formula
  for formula in "${formulas[@]}"; do
    make_keg "$formula"
  done
}

link_tool() {
  cp "$prefix/opt/$1/bin/$1" "$prefix/bin/$1"
}

# A fresh prefix with every formula present and both bare-name tools linked.
new_linked_toolchain() {
  new_prefix
  make_all_kegs
  link_tool cmake
  link_tool pkg-config
}

# A fresh prefix where pkg-config and openssl exist only under their real keg
# names, pkgconf and openssl@3.
new_aliased_toolchain() {
  new_prefix
  make_keg cmake; make_keg unixodbc; make_keg llvm; make_keg libnfs; make_keg openssl@3
  make_keg pkgconf; cp "$prefix/opt/pkgconf/bin/pkgconf" "$prefix/opt/pkgconf/bin/pkg-config"
}

# Starts a case: names it and counts it.
begin_test() {
  name="$1"
  tests_run=$((tests_run + 1))
}

# A file's lines joined with ';', for failure messages.
flatten() {
  tr '\n' ';' < "$1"
}

# A copy of the tool outside the prefix, the shape of a system or MacPorts
# install that wins on PATH.
make_foreign() {
  printf '#!/usr/bin/env bash\necho foreign-%s\n' "$1" > "$foreign_bin/$1"
  chmod +x "$foreign_bin/$1"
}

# Runs the subject with the given PATH prefix in front of the fake prefix's
# bin. Captures exit status, combined output, and the two GitHub files. The
# subject's well-known-prefix probe is pointed at the fake prefix, the way
# /opt/homebrew is found on a real runner; WELL_KNOWN_PREFIXES overrides that
# for the cases about the probe itself.
run_subject() {
  local path_prefix="$1"
  shift
  output="$(cd "$prefix" && env -i \
    PATH="${path_prefix:+$path_prefix:}$prefix/bin:$system_bin" \
    HOME="$prefix" \
    SETUP_CC_WELL_KNOWN_PREFIXES="${WELL_KNOWN_PREFIXES-$prefix}" \
    BREW_LOG="$log" BREW_PREFIX="$prefix" \
    GITHUB_PATH="$github_path" GITHUB_ENV="$github_env" \
    "$@" bash "$subject" 2>&1)"
  rc=$?
}

expect_exit() {
  local name="$1" want="$2"
  if [ "$rc" -ne "$want" ]; then
    fail_test "$name: expected exit $want, got $rc (output: $output)"
    return 1
  fi
}

expect_no_install() {
  local name="$1"
  if grep -q '^install' "$log"; then
    fail_test "$name: 'brew install' ran for a toolchain that was present (brew log: $(flatten "$log"))"
    return 1
  fi
}

expect_install() {
  local name="$1"
  if ! grep -q "^install ${formulas[*]}\$" "$log"; then
    fail_test "$name: expected 'brew install ${formulas[*]}' (brew log: $(flatten "$log"))"
    return 1
  fi
}

expect_github_path() {
  local name="$1"
  shift
  local want
  for want in "$@"; do
    if ! grep -qx "$want" "$github_path"; then
      fail_test "$name: GITHUB_PATH is missing '$want' (was: $(flatten "$github_path"))"
      return 1
    fi
  done
}

expect_github_path_empty() {
  local name="$1"
  if [ -s "$github_path" ]; then
    fail_test "$name: GITHUB_PATH should be untouched (was: $(flatten "$github_path"))"
    return 1
  fi
}

expect_library_path() {
  local name="$1"
  if ! grep -qx "LIBRARY_PATH=$prefix/opt/unixodbc/lib" "$github_env"; then
    fail_test "$name: GITHUB_ENV should carry LIBRARY_PATH ending in the unixodbc keg (was: $(flatten "$github_env"))"
    return 1
  fi
}

expect_output() {
  local name="$1" want="$2"
  case "$output" in
    *"$want"*) ;;
    *)
      fail_test "$name: output is missing '$want' (was: $output)"
      return 1
      ;;
  esac
}

pass() {
  echo "  ok: $1"
}

# --- every keg present and linked: nothing to do, no brew invocation ----------
begin_test "a complete, linked toolchain is left alone"
new_linked_toolchain
run_subject ""
expect_exit "$name" 0 && expect_no_install "$name" && expect_github_path_empty "$name" \
  && expect_output "$name" "skipping 'brew install'" && expect_library_path "$name" && pass "$name"

begin_test "a complete, linked toolchain needs no working brew"
new_linked_toolchain
run_subject "" BREW_FAIL=1
expect_exit "$name" 0 && expect_no_install "$name" && pass "$name"

# --- present but unlinked: expose the keg, never install ----------------------
begin_test "an unlinked keg is exposed through GITHUB_PATH without brew install"
new_prefix; make_all_kegs   # kegs exist, but neither tool is in $prefix/bin
run_subject ""
expect_exit "$name" 0 && expect_no_install "$name" \
  && expect_github_path "$name" "$prefix/opt/cmake/bin" "$prefix/opt/pkg-config/bin" \
  && expect_output "$name" "putting $prefix/opt/cmake/bin first on PATH" && pass "$name"

begin_test "an unlinked keg is exposed even when brew cannot run"
new_prefix; make_all_kegs
run_subject "" BREW_FAIL=1
expect_exit "$name" 0 && expect_no_install "$name" \
  && expect_github_path "$name" "$prefix/opt/cmake/bin" "$prefix/opt/pkg-config/bin" && pass "$name"

begin_test "only the unexposed tool is put on PATH"
new_prefix; make_all_kegs; link_tool cmake   # pkg-config stays unlinked
run_subject ""
expect_exit "$name" 0 && expect_no_install "$name" \
  && expect_github_path "$name" "$prefix/opt/pkg-config/bin" && pass "$name"
if grep -qx "$prefix/opt/cmake/bin" "$github_path"; then
  fail_test "$name: cmake already resolved under the prefix and should not have been added"
fi

# --- shadowed by a foreign copy earlier on PATH -------------------------------
begin_test "a keg shadowed by a foreign copy earlier on PATH is put in front of it"
new_linked_toolchain; make_foreign cmake
run_subject "$foreign_bin"
expect_exit "$name" 0 && expect_no_install "$name" \
  && expect_github_path "$name" "$prefix/opt/cmake/bin" \
  && expect_output "$name" "'cmake' resolves to '$foreign_bin/cmake'" && pass "$name"

begin_test "a shadowed keg is put in front even when brew cannot run"
new_linked_toolchain; make_foreign cmake; make_foreign pkg-config
run_subject "$foreign_bin" BREW_FAIL=1
expect_exit "$name" 0 && expect_no_install "$name" \
  && expect_github_path "$name" "$prefix/opt/cmake/bin" "$prefix/opt/pkg-config/bin" && pass "$name"

# --- genuinely absent: install, then link -------------------------------------
begin_test "an absent formula is installed and the bare-name tools linked"
new_prefix; make_keg openssl; make_keg unixodbc; make_keg llvm; make_keg libnfs   # no cmake, no pkg-config
run_subject ""
expect_exit "$name" 0 && expect_install "$name" && expect_output "$name" "incomplete on this runner (cmake pkg-config)" \
  && expect_github_path_empty "$name" && pass "$name"
if ! grep -q '^link --overwrite cmake$' "$log" || ! grep -q '^link --overwrite pkg-config$' "$log"; then
  fail_test "$name: expected 'brew link --overwrite' for cmake and pkg-config (brew log: $(flatten "$log"))"
fi

begin_test "an absent formula still fails when brew cannot run"
new_prefix; make_all_kegs; rm -r "$prefix/opt/libnfs"
run_subject "" BREW_FAIL=1
expect_exit "$name" 1 && expect_output "$name" "incomplete on this runner (libnfs)" \
  && expect_output "$name" "Could not acquire lock" && pass "$name"

# --- the unconditional assertion still fires ----------------------------------
begin_test "a tool that is nowhere after install fails with a named cause"
new_linked_toolchain
rm -r "$prefix/opt/cmake" "$prefix/bin/cmake"   # install will be attempted; make it a no-op for cmake
cat > "$prefix/bin/brew" <<'STUB'
#!/usr/bin/env bash
echo "$*" >> "$BREW_LOG"
exit 0
STUB
chmod +x "$prefix/bin/brew"
run_subject ""
expect_exit "$name" 1 && expect_output "$name" "::error::Not on PATH: cmake" && pass "$name"

# --- the prefix is found by probing, not by walking up from brew's path ------
begin_test "a brew wrapper outside the prefix still finds the Cellar through the well-known probe"
new_linked_toolchain
# The spiceai-macos pool resolves brew to a flock wrapper under /opt/spice/bin;
# dirname twice of that is a directory with no Cellar.
wrapper_bin="$(mktemp -d)/bin"; mkdir -p "$wrapper_bin"; cp "$prefix/bin/brew" "$wrapper_bin/brew"
run_subject "$wrapper_bin" BREW_FAIL=1
expect_exit "$name" 0 && expect_no_install "$name" && expect_library_path "$name" \
  && expect_output "$name" "already present under $prefix" && pass "$name"

begin_test "with no well-known prefix, the prefix falls back to the directory above brew's"
new_linked_toolchain
WELL_KNOWN_PREFIXES="/nonexistent/homebrew" run_subject "" BREW_FAIL=1
expect_exit "$name" 0 && expect_no_install "$name" && expect_library_path "$name" && pass "$name"

# --- formula aliases: pkgconf and openssl@3 are the real keg names ------------
begin_test "the pkgconf and openssl@3 kegs count as pkg-config and openssl"
new_aliased_toolchain
link_tool cmake; cp "$prefix/opt/pkgconf/bin/pkg-config" "$prefix/bin/pkg-config"
run_subject "" BREW_FAIL=1
expect_exit "$name" 0 && expect_no_install "$name" && expect_github_path_empty "$name" && pass "$name"

begin_test "an unlinked pkgconf keg is exposed as pkg-config even when brew cannot run"
new_aliased_toolchain
link_tool cmake
run_subject "" BREW_FAIL=1
expect_exit "$name" 0 && expect_no_install "$name" \
  && expect_github_path "$name" "$prefix/opt/pkgconf/bin" && pass "$name"

begin_test "a runner without brew is refused"
new_prefix; make_all_kegs; rm "$prefix/bin/brew"
run_subject ""
expect_exit "$name" 1 && expect_output "$name" "::error::'brew' is not on PATH" && pass "$name"

echo
echo "$tests_run tests, $failures failures"
[ "$failures" -eq 0 ]
