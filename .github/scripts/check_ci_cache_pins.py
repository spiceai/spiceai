#!/usr/bin/env python3
"""Check .github/ci-cache/pins.json: well-formed, and pinning the toolchain rust-toolchain.toml names.

Without a pin for the toolchain in rust-toolchain.toml, setup-rust installs Rust from
static.rust-lang.org on every fresh runner instead of from the CI cache. Nothing fails, so
a toolchain bump that forgets the pin would lose the cache silently; this check fails it.
It also requires a rustup-init pin for every macOS host setup-rust maps, since a
self-hosted Mac job installs its own rustup from it and fails without one.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PINS_PATH = ROOT / ".github" / "ci-cache" / "pins.json"
INSTALL_CARGO_DENY = ROOT / ".github" / "actions" / "install-cargo-deny" / "action.yml"
SETUP_RUST = ROOT / ".github" / "actions" / "setup-rust" / "action.yml"
SHA256 = re.compile(r"[0-9a-f]{64}")
# The arms of install-cargo-deny's `case "$(uname -s)-$(uname -m)"`, such as
# `Darwin-arm64) target=aarch64-apple-darwin ;;`: the targets it can ask pins.json for.
ACTION_TARGET = re.compile(r"^\s*[A-Za-z]+-[A-Za-z0-9_]+\)\s*target=([A-Za-z0-9_.-]+)\s*;;", re.M)
# setup-rust's macOS arms, such as `Darwin-arm64) host_triple=aarch64-apple-darwin ;;`: a
# self-hosted Mac job installs its own rustup from the rustup-init pinned for that host.
SETUP_RUST_MAC_HOST = re.compile(r"^\s*Darwin-[A-Za-z0-9_]+\)\s*host_triple=([A-Za-z0-9_.-]+)\s*;;", re.M)
RUSTUP_ARCHIVE = "https://static.rust-lang.org/rustup/archive/"


def main() -> int:
    errors = []
    pins = json.loads(PINS_PATH.read_text())

    rust = pins["rust"]
    for toolchain, pin in rust["toolchains"].items():
        if not SHA256.fullmatch(pin.get("manifest_sha256", "")):
            errors.append(f"rust.toolchains.{toolchain}.manifest_sha256 is not a lowercase hex SHA-256.")
    if not rust["components"]:
        errors.append("rust.components must name at least one component.")

    channel = re.search(r'^\s*channel\s*=\s*"([^"]+)"', (ROOT / "rust-toolchain.toml").read_text(), re.M)
    if channel is None:
        errors.append("rust-toolchain.toml names no channel.")
    elif channel.group(1) not in rust["toolchains"]:
        toolchain = channel.group(1)
        errors.append(
            f"rust-toolchain.toml names Rust {toolchain}, which {PINS_PATH.relative_to(ROOT)} does not pin, "
            "so CI would install it from static.rust-lang.org instead of the CI cache. Add "
            f'"{toolchain}": {{"manifest_sha256": "<sha256>"}} under rust.toolchains, with the SHA-256 that '
            f"`curl -fsSL https://static.rust-lang.org/dist/channel-rust-{toolchain}.toml.sha256` prints, "
            "and drop toolchains nothing uses any more."
        )

    deny = pins["cargo-deny"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", deny["version"]):
        errors.append(f"cargo-deny.version '{deny['version']}' is not a release version.")
    if "{version}" not in deny["url"] or "{target}" not in deny["url"]:
        errors.append("cargo-deny.url must contain {version} and {target}.")
    for target, sha256 in deny["sha256"].items():
        if not SHA256.fullmatch(sha256):
            errors.append(f"cargo-deny.sha256.{target} is not a lowercase hex SHA-256.")
    action_targets = ACTION_TARGET.findall(INSTALL_CARGO_DENY.read_text())
    if not action_targets:
        errors.append(
            f"Found no `<os>-<arch>) target=<target> ;;` arm in {INSTALL_CARGO_DENY.relative_to(ROOT)}, "
            "so the cargo-deny targets to require cannot be read; update this check with the action."
        )
    for target in action_targets:
        if target not in deny["sha256"]:
            errors.append(
                f"cargo-deny.sha256 has no pin for {target}, which {INSTALL_CARGO_DENY.relative_to(ROOT)} "
                f"installs on that platform, so cargo-deny would fail to install there. Add the SHA-256 of "
                f"cargo-deny-{deny['version']}-{target}.tar.gz from the release."
            )

    rustup = pins.get("rustup-init")
    if rustup is None:
        errors.append(
            f"{PINS_PATH.relative_to(ROOT)} has no rustup-init entry, so setup-rust cannot give a "
            "self-hosted Mac job its own rustup."
        )
    else:
        errors.extend(rustup_init_errors(rustup))

    for error in errors:
        print(f"::error file={PINS_PATH.relative_to(ROOT)}::{error}")
    if not errors:
        print(
            f"OK: {PINS_PATH.relative_to(ROOT)} pins Rust {', '.join(rust['toolchains'])}, "
            f"cargo-deny {deny['version']} and rustup-init {rustup['version']}."
        )
    return 1 if errors else 0


def rustup_init_errors(rustup: dict) -> list:
    """The rustup-init pin: a release on static.rust-lang.org, for every Mac host setup-rust names."""
    errors = []
    version = rustup.get("version", "")
    url = rustup.get("url", "")
    hashes = rustup.get("sha256", {})
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        errors.append(f"rustup-init.version '{version}' is not a release version.")
    if not url.startswith(RUSTUP_ARCHIVE) or "{version}" not in url or "{target}" not in url:
        errors.append(f"rustup-init.url must start with {RUSTUP_ARCHIVE} and contain {{version}} and {{target}}.")
    for target, sha256 in hashes.items():
        if not SHA256.fullmatch(sha256):
            errors.append(f"rustup-init.sha256.{target} is not a lowercase hex SHA-256.")
    hosts = SETUP_RUST_MAC_HOST.findall(SETUP_RUST.read_text())
    if not hosts:
        errors.append(
            f"Found no `Darwin-<arch>) host_triple=<triple> ;;` arm in {SETUP_RUST.relative_to(ROOT)}, so the "
            "rustup-init hosts to require cannot be read; update this check with the action."
        )
    for host in hosts:
        if host not in hashes:
            errors.append(
                f"rustup-init.sha256 has no pin for {host}, which {SETUP_RUST.relative_to(ROOT)} installs "
                "rustup for on a self-hosted Mac, so its jobs would fail there. Add the SHA-256 that "
                f"`curl -fsSL {RUSTUP_ARCHIVE}{version}/{host}/rustup-init.sha256` prints, and check it "
                "against a download."
            )
    return errors


if __name__ == "__main__":
    sys.exit(main())
