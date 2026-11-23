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
   and is not retried. Only runners inside the cluster reach it, and reading needs no
   credentials; writing does (see below).
3. The file's source, such as a GitHub release or static.rust-lang.org. Four attempts
   (three retries), retrying only network errors and 408, 429 and 5xx answers.

Every copy, from any tier, is checked against its pin before it is used. A cached copy
with the wrong bytes is a miss, so a damaged or tampered entry costs a download from the
source and nothing more. A source copy with the wrong bytes fails the job, because the pin
and the source disagree.

Every copy is also bounded in size by `CI_FETCH_MAX_BYTES`, 256 MiB unless a workflow sets
it, so a wrong copy cannot fill the runner's disk before its hash is checked. The largest
file pinned today, the rustc archive for x86_64 Linux, is 80 MiB. curl refuses a response
that announces a larger size before writing any of it, and from curl 8.4 also stops one
that grows past the bound without announcing its size. A cached copy over the bound is a
miss; a source copy over it fails the job, and the bound must be raised for that pin.
`setup-rust` then fails too, rather than installing the toolchain with rustup, which would
download the same file with no bound.

## What is pinned

`pins.json` holds:

- **`rust`**: the SHA-256 of the channel manifest of each toolchain `rust-toolchain.toml`
  uses. The manifest pins every component's hash in turn, so one pin covers the
  toolchain. `.github/scripts/rust_dist_mirror.py` fetches the manifest and the
  `components` for a host and lays them out as a local mirror. `setup-rust` installs from
  that mirror with `RUSTUP_DIST_SERVER=file://…`, and rustup checks each component's hash
  again. If the mirror cannot be built, `setup-rust` installs from static.rust-lang.org as
  before.
- **`cargo-deny`**: the release archive for each target, installed by the
  `install-cargo-deny` action.

## How the cache is filled

By the jobs that use it. When the bucket answers that it lacks a file (404) or holds the
wrong bytes, the job downloads the file from its source, checks it against the pin, and
writes it back, so the next job reads it from the bucket. The write is best-effort: one
try, and a failure is a warning. Nothing is written when the bucket could not be reached
or refused the read. Objects expire 30 days after they are written, which drops files no
pin names any more; a file still in use is written again by the next job that misses it.

The CI object store accepts writes only with OIDC credentials. `cached_fetch.sh` exchanges
the job's GitHub OIDC token for temporary credentials of the store with
`.github/scripts/rustfs_credentials.sh`, the exchange the `rustfs-credentials` action
runs, and signs the `PUT` with them (AWS SigV4, with the file's SHA-256 as the signed
payload hash). The credentials stay inside the script and are never printed. Signing needs
curl 7.87 or later; with an older curl the write is skipped with a warning.

A job has an OIDC token only with `permissions: id-token: write`. A job without one, which
includes every pull request from a fork, still reads the cache and logs one line saying it
did not write. The token gets the repository's credentials for the CI object store, which
reach more than this bucket, so grant the permission only to a job in the `ci` environment
that does not run on `pull_request`: never to a job in `secret-free`, which holds no
credential and can be dispatched on any branch, and never to one that runs pull request
code. The store takes a write only from a run on `trunk`; a run elsewhere that holds a
token reads the cache and warns that its write was refused. A reader never trusts the
writer either way: no reader uses bytes that do not match its pin, and one that finds wrong
bytes replaces them with the verified download.

## Adding a file

1. Add its pin to `pins.json`. Take the SHA-256 from the publisher's checksum file and
   check it against a download.
2. Fetch it with `bash .github/scripts/cached_fetch.sh <sha256> <dest> <source url>`.

A workflow opts in by setting `CI_CACHE_URL` in its top-level `env`, as the workflows that
run `setup-rust` on `spiceai-dev-runners` do. The URL is the bucket's path-style URL,
`<endpoint>/ci-cache`, and the write's credentials come from that endpoint:

```yaml
env:
  CI_CACHE_URL: ${{ vars.TEST_MINIO_ENDPOINT && format('{0}/ci-cache', vars.TEST_MINIO_ENDPOINT) || '' }}
```

A job that should also fill the cache needs `id-token: write` among its `permissions`, on
the terms above.

## Bumping the Rust toolchain

`rust-toolchain.toml` and `pins.json` change together: `check_ci_cache_pins.py` (run by
`workflow_yaml_check.yml`) fails a toolchain without a manifest pin. The pin is the line

```sh
curl -fsSL https://static.rust-lang.org/dist/channel-rust-<version>.toml.sha256
```

prints. Drop toolchains nothing uses any more.
