#!/usr/bin/env python3
"""Check .github/ci-cache/pins.json: well-formed, and pinning the toolchain rust-toolchain.toml names.

Without a pin for the toolchain in rust-toolchain.toml, setup-rust installs Rust from
static.rust-lang.org on every fresh runner instead of from the CI cache. Nothing fails, so
a toolchain bump that forgets the pin would lose the cache silently; this check fails it.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PINS_PATH = ROOT / ".github" / "ci-cache" / "pins.json"
SHA256 = re.compile(r"[0-9a-f]{64}")


def main() -> int:
    errors = []
    pins = json.loads(PINS_PATH.read_text())

    rust = pins["rust"]
    for toolchain, pin in rust["toolchains"].items():
        if not SHA256.fullmatch(pin.get("manifest_sha256", "")):
            errors.append(f"rust.toolchains.{toolchain}.manifest_sha256 is not a lowercase hex SHA-256.")
    if not rust["hosts"] or not rust["components"]:
        errors.append("rust.hosts and rust.components must each name at least one entry.")

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

    for error in errors:
        print(f"::error file={PINS_PATH.relative_to(ROOT)}::{error}")
    if not errors:
        print(f"OK: {PINS_PATH.relative_to(ROOT)} pins Rust {', '.join(rust['toolchains'])} and cargo-deny {deny['version']}.")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
