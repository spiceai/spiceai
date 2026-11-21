# CI download cache

CI jobs on the self-hosted pools download the same files many times a day: the Rust
toolchain on every fresh runner pod, and release binaries such as cargo-deny. Each of
those downloads can fail for reasons that have nothing to do with the change under test.
`.github/scripts/cached_fetch.sh` fetches a file whose SHA-256 is pinned here, trying in
order:

1. `CI_CACHE_DIR`, a directory on the runner's host that outlives the job. Used on the
   self-hosted Macs, which keep their disks between jobs.
2. `CI_CACHE_URL`, the `ci-cache` bucket of the CI object store, read anonymously over
   HTTP at `<CI_CACHE_URL>/sha256/<sha256>`. Two attempts (one retry); a 404 is a miss
   and is not retried. Only runners inside the cluster reach it.
3. The file's source, such as a GitHub release or static.rust-lang.org. Four attempts
   (three retries), retrying only network errors and 408, 429 and 5xx answers.

Every copy, from any tier, is checked against its pin before it is used. A cached copy
with the wrong bytes is a miss, so a damaged or tampered entry costs a download from the
source and nothing more. A source copy with the wrong bytes fails the job, because the pin
and the source disagree.

## What is pinned

`pins.json` holds:

- **`rust`**: the SHA-256 of the channel manifest of each toolchain `rust-toolchain.toml`
  uses. The manifest pins every component's hash in turn, so one pin covers the
  toolchain. `.github/scripts/rust_dist_mirror.py` fetches the manifest and the
  `components` for a host and lays them out as a local mirror. `setup-rust` installs from
  that mirror with `RUSTUP_DIST_SERVER=file://…`, and rustup checks each component's hash
  again. If the mirror cannot be built, `setup-rust` installs from static.rust-lang.org as
  before. `hosts` lists the hosts whose components the warm-up uploads.
- **`cargo-deny`**: the release archive for each target, installed by the
  `install-cargo-deny` action.

## How the cache is filled

Jobs only read the bucket, and need no credentials to do so. The `CI cache warm-up`
workflow (`.github/workflows/ci_cache_warm.yml`) reads every pinned file from the cache,
uploads any that are missing or wrong, and reads the upload back anonymously. It runs
when the pins change on trunk and once a day. Until it runs after a new pin merges, jobs
download that file from its source.

## Adding a file

1. Add its pin to `pins.json`. Take the SHA-256 from the publisher's checksum file and
   check it against a download.
2. Fetch it with `bash .github/scripts/cached_fetch.sh <sha256> <dest> <source url>`.
3. List it in `pinned_files()` in `.github/scripts/ci_cache_warm.py`.

A workflow opts in by setting `CI_CACHE_URL` in its top-level `env`, as the workflows that
run `setup-rust` on `spiceai-dev-runners` do:

```yaml
env:
  CI_CACHE_URL: ${{ vars.TEST_MINIO_ENDPOINT && format('{0}/ci-cache', vars.TEST_MINIO_ENDPOINT) || '' }}
```

## Bumping the Rust toolchain

`rust-toolchain.toml` and `pins.json` change together: `check_ci_cache_pins.py` (run by
`workflow_yaml_check.yml`) fails a toolchain without a manifest pin. The pin is the line

```sh
curl -fsSL https://static.rust-lang.org/dist/channel-rust-<version>.toml.sha256
```

prints. Drop toolchains nothing uses any more.
