#!/usr/bin/env python3
"""Check .github/ci-cache/pins.json: well-formed, and pinning the toolchain rust-toolchain.toml names.

Without a pin for the toolchain in rust-toolchain.toml, setup-rust installs Rust from
static.rust-lang.org on every fresh runner instead of from the CI cache. Nothing fails, so
a toolchain bump that forgets the pin would lose the cache silently; this check fails it.
It also requires a rustup-init pin for every host setup-rust maps: setup-rust installs
rustup only from that pin, so a host without one fails wherever rustup is missing, and on
a self-hosted Mac, whose jobs always install their own, every time.

And for every entry the actions and workflows download through fetch_pinned.sh
(FETCHED_PINS), it requires each file that downloads it to do so through that script, and
a pin for every target the file can ask for, read from its `case` arms. It keeps the
SHA-256s some workflows check inline (INLINE_PINS) and test/tpc-bench/Makefile's DuckDB CLI
archives pinned too, and requires every pip requirements file CI installs (REQUIREMENTS) to
pin each package to one version with its hashes, installed with --require-hashes.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PINS_PATH = ROOT / ".github" / "ci-cache" / "pins.json"
INSTALL_CARGO_DENY = ROOT / ".github" / "actions" / "install-cargo-deny" / "action.yml"
SETUP_RUST = ROOT / ".github" / "actions" / "setup-rust" / "action.yml"
SETUP_NEXTEST = ROOT / ".github" / "actions" / "setup-nextest" / "action.yml"
DOCKERFILE_CUDA = ROOT / "Dockerfile-cuda"
NEXTEST_RELEASES = "https://github.com/nextest-rs/nextest/releases/download/"
# Dockerfile-cuda's rustup-init ARGs, and the host each SHA-256 is for.
DOCKERFILE_RUSTUP = {
    "RUSTUP_INIT_SHA256_X86_64": "x86_64-unknown-linux-gnu",
    "RUSTUP_INIT_SHA256_AARCH64": "aarch64-unknown-linux-gnu",
}
SHA256 = re.compile(r"[0-9a-f]{64}")
# The arms of install-cargo-deny's `case "$(uname -s)-$(uname -m)"`, such as
# `Darwin-arm64) target=aarch64-apple-darwin ;;`: the targets it can ask pins.json for.
ACTION_TARGET = re.compile(r"^\s*[A-Za-z]+-[A-Za-z0-9_]+\)\s*target=([A-Za-z0-9_.-]+)\s*;;", re.M)
# setup-rust's host arms, such as `Darwin-arm64) host_triple=aarch64-apple-darwin ;;`: the
# hosts it installs rustup for, from the rustup-init pinned for each.
SETUP_RUST_HOST = re.compile(r"^\s*[A-Za-z]+-[A-Za-z0-9_]+\)\s*host_triple=([A-Za-z0-9_.-]+)\s*;;", re.M)
RUSTUP_ARCHIVE = "https://static.rust-lang.org/rustup/archive/"

# The pins.json entries that actions and workflows download through
# .github/scripts/fetch_pinned.sh, and the files that download each. Every file listed must
# fetch its entry that way, and every target it can ask for must be pinned: a target without
# a pin fails the job that asks for it, on the platform that asks, and nowhere else.
FETCHED_PINS = {
    "mc": (".github/actions/setup-minio/action.yml", ".github/actions/upload-to-minio/action.yml"),
    "sccache": (".github/actions/setup-sccache/action.yml",),
    "protoc": (".github/actions/install-protoc/action.yml",),
    "oracle-instantclient": (".github/actions/setup-oracle-odpi/action.yml",),
    "simba-spark-odbc": (".github/actions/setup-odbc/action.yml",),
    "athena-odbc": (".github/actions/setup-odbc/action.yml",),
    "awscli": (
        ".github/actions/setup-spicescript/action.yml",
        ".github/workflows/spicescript_dispatch.yml",
        ".github/workflows/testoperator_run_cluster_bench_aws.yml",
    ),
    "duckdb-cli": (".github/actions/setup-spicescript/action.yml", ".github/workflows/spicescript_dispatch.yml"),
    "github-cli-keyring": (
        ".github/actions/push-snap-changes/action.yml",
        ".github/workflows/testoperator_run_bench_aws.yml",
        ".github/workflows/testoperator_run_schema_aws.yml",
        ".github/workflows/testoperator_run_cluster_bench_aws.yml",
    ),
}
# Single-file pins a workflow writes out inline instead, with `sha256sum -c`: the file must
# name the pin's URL and SHA-256 and check every download of that URL. Used where the
# checkout can be an older commit than the workflow, which may lack fetch_pinned.sh.
INLINE_PINS = {
    "docker-apt-key": (
        ".github/workflows/spiced_docker_dev.yml",
        ".github/workflows/build_nightly.yml",
        ".github/workflows/spiced_docker.yml",
    ),
}
# A call in a shell script: `fetch_pinned.sh" NAME DEST [TARGET]`, with TARGET a literal or a
# variable such as "$target".
FETCH_CALL = re.compile(
    r"""fetch_pinned\.sh["']?[ \t]+([A-Za-z0-9_.-]+)[ \t]+(?:"[^"\n]*"|'[^'\n]*'|[^\s;|&)]+)"""
    r"""(?:[ \t]+(?:"([^"\n]*)"|'([^'\n]*)'|([^\s;|&)#\\]+)))?"""
)
# A PowerShell step reads the entry itself, as `(... | ConvertFrom-Json).'NAME'`, and names
# the target it checks as `$target = 'TARGET'`.
PWSH_PIN = re.compile(r"ConvertFrom-Json\s*\)\s*\.\s*'([A-Za-z0-9_.-]+)'")
PWSH_TARGET = re.compile(r"\$target\s*=\s*'([^'\n]+)'")
# test/tpc-bench/Makefile downloads the DuckDB CLI its TPC-H generator runs, and pins each
# release asset in the `case` arm that picks it: `ASSET=<file>; SHA256=<sha256> ;;`.
TPC_BENCH_MAKEFILE = ROOT / "test" / "tpc-bench" / "Makefile"
MAKEFILE_ASSET = re.compile(r"ASSET=([A-Za-z0-9_.-]+)(?:;\s*SHA256=([0-9a-f]{64}))?")
# The pip requirements CI installs with --require-hashes, and the files that install each.
# Every requirement must name one version (`==`) and the SHA-256 of its files, and every
# `pip install` in those files must pass --require-hashes, so pip installs nothing unhashed.
REQUIREMENTS = {
    ".github/requirements/ducklake_bootstrap.txt": (
        ".github/workflows/testoperator_run_bench_aws.yml",
        ".github/workflows/testoperator_run_schema_aws.yml",
    ),
    ".github/requirements/scylladb_bootstrap.txt": (".github/actions/setup-scylladb/action.yml",),
    ".github/requirements/cold_start_fixture_build.txt": (".github/actions/seed-cold-start-fixture/action.yml",),
    ".github/requirements/cold_start_fixture.txt": (".github/actions/seed-cold-start-fixture/action.yml",),
    ".github/requirements/hf_cli.txt": (".github/workflows/hf_cache_differential.yml",),
    ".github/requirements/bigquery_gate.txt": (".github/workflows/e2e_test_ci.yml",),
    "test/object_store_occ/requirements.txt": (".github/workflows/object_store_wal.yml",),
}
# The DuckLake bootstrap writes a catalog spiced reads with the DuckDB release
# test/duckdb_versions.json lists last, so it must pin that duckdb.
DUCKLAKE_REQUIREMENTS = ".github/requirements/ducklake_bootstrap.txt"
DUCKDB_VERSIONS = ROOT / "test" / "duckdb_versions.json"
REQUIREMENT = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9][A-Za-z0-9._+!-]*)\s*(?:;(?:(?!--hash=).)*)?((?:\s*--hash=sha256:[0-9a-f]{64})+)\s*"
)
SHELL_VARIABLE = re.compile(r"\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?")


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

    nextest = pins.get("cargo-nextest")
    if nextest is None:
        errors.append(f"{PINS_PATH.relative_to(ROOT)} has no cargo-nextest entry, so setup-nextest cannot install it.")
    else:
        errors.extend(cargo_nextest_errors(nextest))

    rustup = pins.get("rustup-init")
    if rustup is None:
        errors.append(
            f"{PINS_PATH.relative_to(ROOT)} has no rustup-init entry, so setup-rust cannot install rustup "
            "anywhere, which a self-hosted Mac job always does."
        )
    else:
        errors.extend(rustup_init_errors(rustup))
        errors.extend(dockerfile_rustup_errors(rustup))

    for name, users in FETCHED_PINS.items():
        errors.extend(fetched_pin_errors(pins, name, users))
    for name, users in INLINE_PINS.items():
        errors.extend(inline_pin_errors(pins, name, users))
    errors.extend(tpc_bench_duckdb_errors())
    errors.extend(requirements_errors())

    for error in errors:
        print(f"::error file={PINS_PATH.relative_to(ROOT)}::{error}")
    if not errors:
        print(
            f"OK: {PINS_PATH.relative_to(ROOT)} pins Rust {', '.join(rust['toolchains'])}, "
            f"cargo-deny {deny['version']}, cargo-nextest {nextest['version']} and rustup-init {rustup['version']}."
        )
        fetched = [f"{name} {pins[name]['version']}" if "version" in pins[name] else name for name in FETCHED_PINS]
        print(f"OK: fetched through fetch_pinned.sh, and pinned for every target asked for: {', '.join(fetched)}.")
        print(f"OK: checked inline against pins.json: {', '.join(INLINE_PINS)}.")
        print(f"OK: installed with --require-hashes, every requirement pinned and hashed: {', '.join(REQUIREMENTS)}.")
    return 1 if errors else 0


def fetched_pin_errors(pins: dict, name: str, users: tuple) -> list:
    """An entry fetch_pinned.sh downloads: well formed, and pinned for every target its users ask for."""
    pin = pins.get(name)
    if not isinstance(pin, dict):
        return [
            f"{PINS_PATH.relative_to(ROOT)} has no {name} entry, so {', '.join(users)} cannot download it "
            "through fetch_pinned.sh."
        ]
    errors = []
    url = pin.get("url", "")
    hashes = pin.get("sha256")
    if not isinstance(url, str) or not url.startswith("https://"):
        errors.append(f"{name}.url must be an https URL.")
        url = ""
    if isinstance(hashes, dict):
        version = pin.get("version", "")
        if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", version):
            errors.append(f"{name}.version '{version}' is not a release version.")
        if url and ("{version}" not in url or "{target}" not in url):
            errors.append(f"{name}.url must contain {{version}} and {{target}}, since {name} pins one file per target.")
        if not hashes:
            errors.append(f"{name}.sha256 pins no target.")
        for target, sha256 in hashes.items():
            if not isinstance(sha256, str) or not SHA256.fullmatch(sha256):
                errors.append(f"{name}.sha256.{target} is not a lowercase hex SHA-256.")
    else:
        if "version" in pin:
            errors.append(f"{name} has a version but one SHA-256; pin one file per target, or drop the version.")
        if url and "{" in url:
            errors.append(f"{name}.url names a template, but {name} pins one file.")
        if not isinstance(hashes, str) or not SHA256.fullmatch(hashes):
            errors.append(f"{name}.sha256 is not a lowercase hex SHA-256.")

    for user in users:
        text = (ROOT / user).read_text()
        targets, found, unread = requested_targets(text, name)
        if not found:
            errors.append(
                f"{user} does not download {name} through .github/scripts/fetch_pinned.sh (or read its pin in "
                f"PowerShell as `ConvertFrom-Json).'{name}'`), so nothing checks what it downloads against "
                f"{PINS_PATH.relative_to(ROOT)}."
            )
        for variable in unread:
            errors.append(
                f"{user} fetches {name} for the target in ${variable}, but has no `<case>) {variable}=<target> ;;` "
                "arm to read the targets from; update this check with the file."
            )
        for target in sorted(targets):
            if isinstance(hashes, dict) and not target:
                errors.append(f"{user} fetches {name} with no target, but {name} pins one file per target.")
            elif isinstance(hashes, dict) and target not in hashes:
                errors.append(
                    f"{name}.sha256 has no pin for {target}, which {user} asks for, so it would fail there. "
                    "Take the SHA-256 from the publisher's checksum and check it against a download."
                )
            elif not isinstance(hashes, dict) and target:
                errors.append(f"{user} asks for {name} for {target}, but {name} pins one file for no target.")
    return errors


def inline_pin_errors(pins: dict, name: str, users: tuple) -> list:
    """A single-file pin a workflow checks inline: the same URL and SHA-256 as pins.json, with
    one `sha256sum -c` of that SHA-256 for every download of the URL."""
    pin = pins.get(name)
    if not isinstance(pin, dict) or not isinstance(pin.get("sha256"), str) or not isinstance(pin.get("url"), str):
        return [f"{PINS_PATH.relative_to(ROOT)} has no single-file {name} entry, which {', '.join(users)} check against."]
    errors = []
    url, sha256 = pin["url"], pin["sha256"]
    if not SHA256.fullmatch(sha256):
        errors.append(f"{name}.sha256 is not a lowercase hex SHA-256.")
    if not url.startswith("https://"):
        errors.append(f"{name}.url must be an https URL.")
    for user in users:
        text = (ROOT / user).read_text()
        downloads = text.count(url)
        checks = len(re.findall(rf'echo "{re.escape(sha256)}  [^"\n]*" \| sha256sum -c -', text))
        if downloads == 0:
            errors.append(f"{user} no longer downloads {url}; drop it from INLINE_PINS[{name!r}] or restore the download.")
        elif checks < downloads:
            errors.append(
                f"{user} downloads {url} {downloads} time(s) but checks {checks} against {name}'s SHA-256 in "
                f'{PINS_PATH.relative_to(ROOT)} (`echo "<sha256>  <file>" | sha256sum -c -`).'
            )
    return errors


def _logical_lines(text: str) -> list:
    """Lines with backslash continuations joined and whole-line comments dropped."""
    text = re.sub(r"^[ \t]*#[^\n]*\n", "", text, flags=re.M)
    return text.replace("\\\n", " ").splitlines()


def requirements_errors() -> list:
    """Each requirements file pins and hashes every requirement; each installer passes --require-hashes."""
    errors = []
    pinned = {}
    for name, users in REQUIREMENTS.items():
        path = ROOT / name
        if not path.is_file():
            errors.append(f"{name} is missing, but {', '.join(users)} install it.")
            continue
        for line in _logical_lines(path.read_text()):
            if not line.strip():
                continue
            match = REQUIREMENT.fullmatch(line.strip())
            if match is None:
                errors.append(
                    f"{name} has `{line.strip()[:80]}`, which is not one exact version (`name==version`) with the "
                    "SHA-256 of its files (`--hash=sha256:<sha256>`); regenerate the file as its header says."
                )
            else:
                pinned.setdefault(name, {})[match.group(1).lower().replace("_", "-")] = match.group(2)
        for user in users:
            installs = [line for line in _logical_lines((ROOT / user).read_text()) if re.search(r"\bpip3?\s+install\b", line)]
            if not any(path.name in line and "--require-hashes" in line for line in installs):
                errors.append(f"{user} does not install {name} with `pip install --require-hashes -r`.")
            for line in installs:
                if "--require-hashes" not in line:
                    errors.append(
                        f"{user} runs `{line.strip()[:100]}` without --require-hashes; install from a hashed "
                        "requirements file listed in REQUIREMENTS instead."
                    )
    duckdb = pinned.get(DUCKLAKE_REQUIREMENTS, {}).get("duckdb")
    newest = json.loads(DUCKDB_VERSIONS.read_text())["versions"][-1]["version"]
    if duckdb != newest:
        errors.append(
            f"{DUCKLAKE_REQUIREMENTS} pins duckdb {duckdb}, but {DUCKDB_VERSIONS.relative_to(ROOT)} lists {newest} last, "
            "the release spiced reads the DuckLake catalog with; pin that one."
        )
    return errors


def tpc_bench_duckdb_errors() -> list:
    """Every DuckDB CLI asset test/tpc-bench/Makefile downloads has a SHA-256 it is checked against."""
    where = TPC_BENCH_MAKEFILE.relative_to(ROOT)
    text = TPC_BENCH_MAKEFILE.read_text()
    arms = MAKEFILE_ASSET.findall(text)
    if not arms:
        return [f"Found no `ASSET=<file>; SHA256=<sha256> ;;` arm in {where}; update this check with the Makefile."]
    errors = [
        f"{where} downloads {asset} with no `SHA256=<sha256>` beside it in its `case` arm, so nothing checks it."
        for asset, sha256 in arms
        if not sha256
    ]
    if '"$$GOT" != "$$SHA256"' not in text:
        errors.append(f"{where} no longer compares the DuckDB CLI it downloads (GOT) with its pinned SHA256.")
    return errors


def requested_targets(text: str, name: str) -> tuple:
    """The targets `text` asks fetch_pinned.sh (or a PowerShell step) for NAME's pin, whether
    it asks at all, and the variables naming a target that no `case` arm assigns."""
    targets, unread = set(), []
    found = False
    for match in FETCH_CALL.finditer(text):
        if match.group(1) != name:
            continue
        found = True
        target = next((group for group in match.group(2, 3, 4) if group is not None), "")
        variable = SHELL_VARIABLE.fullmatch(target)
        if variable is None:
            targets.add(target)
            continue
        arms = re.findall(
            rf"^[^\S\n]*[^\n)]+\)[^\S\n]*{re.escape(variable.group(1))}=([A-Za-z0-9_.-]+)[^\S\n]*;;", text, re.M
        )
        if arms:
            targets.update(arms)
        elif variable.group(1) not in unread:
            unread.append(variable.group(1))
    if any(match.group(1) == name for match in PWSH_PIN.finditer(text)):
        found = True
        targets.update(PWSH_TARGET.findall(text))
    return targets, found, unread


def cargo_nextest_errors(nextest: dict) -> list:
    """The cargo-nextest pin: a release on GitHub, for every host setup-nextest maps."""
    errors = []
    version = nextest.get("version", "")
    url = nextest.get("url", "")
    hashes = nextest.get("sha256", {})
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        errors.append(f"cargo-nextest.version '{version}' is not a release version.")
    if not url.startswith(NEXTEST_RELEASES) or "{version}" not in url or "{target}" not in url:
        errors.append(f"cargo-nextest.url must start with {NEXTEST_RELEASES} and contain {{version}} and {{target}}.")
    for target, sha256 in hashes.items():
        if not SHA256.fullmatch(sha256):
            errors.append(f"cargo-nextest.sha256.{target} is not a lowercase hex SHA-256.")
    targets = ACTION_TARGET.findall(SETUP_NEXTEST.read_text())
    if not targets:
        errors.append(
            f"Found no `<os>-<arch>) target=<target> ;;` arm in {SETUP_NEXTEST.relative_to(ROOT)}, so the "
            "cargo-nextest targets to require cannot be read; update this check with the action."
        )
    for target in targets:
        if target not in hashes:
            errors.append(
                f"cargo-nextest.sha256 has no pin for {target}, which {SETUP_NEXTEST.relative_to(ROOT)} installs "
                f"on that platform, so cargo-nextest would fail to install there. Add the SHA-256 that "
                f"`cargo-nextest-{version}-{target}.sha256` in the release lists, and check it against a download."
            )
    return errors


def dockerfile_rustup_errors(rustup: dict) -> list:
    """Dockerfile-cuda installs the same rustup-init, so its ARGs must equal the pins."""
    text = DOCKERFILE_CUDA.read_text()
    args = dict(re.findall(r"^ARG\s+(RUSTUP_\w+)=(\S*)\s*$", text, re.M))
    errors = []
    if args.get("RUSTUP_VERSION") != rustup.get("version"):
        errors.append(
            f"{DOCKERFILE_CUDA.relative_to(ROOT)} has ARG RUSTUP_VERSION={args.get('RUSTUP_VERSION')}, but "
            f"rustup-init.version is {rustup.get('version')}; keep them equal."
        )
    for arg, host in DOCKERFILE_RUSTUP.items():
        if args.get(arg) != rustup.get("sha256", {}).get(host):
            errors.append(
                f"{DOCKERFILE_CUDA.relative_to(ROOT)} has ARG {arg}={args.get(arg)}, but rustup-init.sha256.{host} "
                "is different; keep them equal."
            )
    return errors


def rustup_init_errors(rustup: dict) -> list:
    """The rustup-init pin: a release on static.rust-lang.org, for every host setup-rust maps."""
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
    hosts = SETUP_RUST_HOST.findall(SETUP_RUST.read_text())
    if not hosts:
        errors.append(
            f"Found no `<os>-<arch>) host_triple=<triple> ;;` arm in {SETUP_RUST.relative_to(ROOT)}, so the "
            "rustup-init hosts to require cannot be read; update this check with the action."
        )
    for host in hosts:
        if host not in hashes:
            errors.append(
                f"rustup-init.sha256 has no pin for {host}, which {SETUP_RUST.relative_to(ROOT)} installs "
                "rustup for, so a job there without rustup would fail. Add the SHA-256 that "
                f"`curl -fsSL {RUSTUP_ARCHIVE}{version}/{host}/rustup-init.sha256` prints, and check it "
                "against a download."
            )
    return errors


if __name__ == "__main__":
    sys.exit(main())
