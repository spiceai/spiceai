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
- **`cargo-nextest`**: the release archive for each target `setup-nextest` maps (Linux
  x86_64 and aarch64, and the universal macOS build). `setup-nextest` extracts only
  `cargo-nextest` from the checked archive into `CARGO_HOME`'s bin. Windows has no pin, so
  it fails there rather than downloading one unchecked.
- **`rustup-init`**: one rustup release, with the SHA-256 of `rustup-init` for each Linux
  and macOS host `setup-rust` maps. It is the only way `setup-rust` installs rustup: on a
  runner without one, fetched through `cached_fetch.sh` and run with `--no-modify-path -y
  --default-toolchain none`, with rustup's self-update then turned off so it stays at the
  pinned release. A missing or mismatched `rustup-init` fails the job; nothing falls back
  to an installer piped into a shell (`check_workflow_yaml.py` refuses one anywhere under
  `.github/` or in a Dockerfile). On Windows, which it pins none for, a runner without rustup
  fails. `Dockerfile-cuda` installs the same release, and `check_ci_cache_pins.py` keeps its
  `RUSTUP_*` ARGs equal to these pins.
- **The self-hosted Macs' Rust homes.** There, `setup-rust` gives each job its own
  `CARGO_HOME` and `RUSTUP_HOME` in `$RUNNER_TEMP/rust-home`, which the runner empties when
  a job starts and ends (skipping what it cannot delete), and never reads the host's shared `~/.cargo` or `~/.rustup`, which
  any job there can write. A `rust-home` an earlier job left behind is moved aside, never
  used, and the directory created anew, and a new home always gets rustup from the pin above and its
  toolchain from the mirror. A later `setup-rust` in the same job keeps the job's homes.
- To bump `rustup-init`, take each host's line from
  `https://static.rust-lang.org/rustup/archive/<version>/<host>/rustup-init.sha256` and
  check it against a download. `check_ci_cache_pins.py` fails a host `setup-rust` maps
  that has no pin.

### Tools fetched through `fetch_pinned.sh`

The tools and files below are downloaded by name with
`bash .github/scripts/fetch_pinned.sh <name> <dest> [<target>]`, which reads the entry's URL
and SHA-256 from `pins.json` and hands them to `cached_fetch.sh`. An entry pins one file
(`url`, `sha256`), or one release of a tool for each build of it (`version`, a `url` with
`{version}` and `{target}`, and a `sha256` per target). A name or target without a pin fails
before anything is downloaded. `check_ci_cache_pins.py` keeps each entry well formed, and
requires every file in its `FETCHED_PINS` table to fetch the entry this way and a pin for
every target that file can ask for, read from the `case` arms that set it
(`x86_64) target=linux-amd64 ;;`). A PowerShell step, which cannot run the script, reads the
entry itself as `(Get-Content … | ConvertFrom-Json).'<name>'`, names its target as
`$target = '<target>'`, and checks the download with `Get-FileHash` before using it.

- **`mc`**: the MinIO client, for `setup-minio` and `upload-to-minio` on Linux x86_64.
  MinIO no longer serves mc (dl.min.io answers 410 for every release and checksum), so the
  source is the copy on `public-data.spiceai.org`, and the SHA-256 is the one MinIO's own
  Homebrew tap lists for that release (`mc.rb` in `minio/homebrew-stable`, commit
  `e17bb437a50742ecd571b5ce0c1c72e05baa1487`); the mirror's file matches it. Each job
  installs it into a new directory of its own under `RUNNER_TEMP`, first on PATH, whatever
  mc the runner already has. No source serves a pinnable macOS or Windows build any more,
  so `setup-minio` refuses to run there, and no job runs it there.
- **`sccache`**: the compiler cache `setup-sccache` installs on Linux x86_64 (the musl
  archive) and Windows (the MSVC zip), with the SHA-256s the release publishes beside each
  asset (`<asset>.sha256`). The target names the asset, extension included. On Linux each job
  installs it into a new directory of its own, first on PATH, so `RUSTC_WRAPPER=sccache` is
  always the pinned binary. On another Linux architecture the job compiles uncached and
  nothing runs sccache, whatever the runner has. The self-hosted Macs use the sccache their
  hosts carry.
- **`protoc`**: one protobuf release for every platform `install-protoc` supports (Linux
  x86_64, aarch64, s390x and ppc64le, macOS x86_64 and arm64, Windows 64- and 32-bit). The
  action used to install whatever `releases/latest` named and to take the expected SHA-256
  from that same API answer, so nothing but GitHub's API vouched for the file. protobuf
  publishes no checksum files; the pins are the SHA-256 digests GitHub records for each
  release asset, each checked against a download. To bump it, pick the newest release that
  is not a pre-release and is at least two weeks old, take each zip's `digest` from
  `gh api repos/protocolbuffers/protobuf/releases/tags/v<version>`, and check it against a
  download. Each job unpacks its own checked zip into a new directory under `RUNNER_TEMP`
  and runs that copy, never one kept in the runner's tool cache, which every job on a
  self-hosted host can write.
- **`oracle-instantclient`**: Oracle Instant Client 23.26.4.0.0 Basic for Linux x86_64, for
  `setup-oracle-odpi`. The action used to download Oracle's unversioned
  `instantclient-basic-linuxx64.zip`, which is whatever 23.26 build Oracle points it at; the
  pin is the current release on Oracle's Linux x86-64 download page, with the SHA-256 that
  page lists beside it. A new release lives in its own directory
  (`instantclient/<version without dots>/`), so a bump changes the whole URL.
- **`simba-spark-odbc`** and **`athena-odbc`**: the Databricks (Simba Spark) ODBC driver
  2.8.2.1013 for Debian and the Amazon Athena ODBC driver 2.0.3.0 RPM, for `setup-odbc`.
  Neither vendor publishes a checksum for these releases, so each pin is the SHA-256 of a
  download from the release's own versioned URL (for Athena, the download's MD5 also matches
  the S3 ETag the server reports).
- **`awscli`**: AWS CLI v2 2.40.1 for Linux x86_64, from its versioned URL
  (`awscli-exe-linux-x86_64-<version>.zip`), installed by `install-aws-cli` for
  `setup-spicescript`, `spicescript_dispatch.yml` and `testoperator_run_cluster_bench_aws.yml`,
  which used to install whatever the unversioned URL served. Each job installs it into a new
  directory of its own, first on PATH, whatever aws the runner already has. The action reads
  its own copy of the pin, so a workflow called at `@trunk` installs trunk's. 2.40.1 was the newest release at least two weeks old when it was pinned. The
  zip's `.sig` verifies with the AWS CLI team's key (fingerprint
  `FB5D B77F D5C1 18B8 0511 ADA8 A631 0ACC 4672 475C`, as AWS's install guide lists it); to
  bump it, verify the new zip the same way, in a throwaway `GNUPGHOME`, and pin its SHA-256.
- **`duckdb-cli`**: the DuckDB CLI v1.5.5 zip for Linux x86_64 that `setup-spicescript` and
  `spicescript_dispatch.yml` install. DuckDB publishes no checksum files; the pin is the
  digest GitHub records for the release asset, checked against a download.
  `test/tpc-bench/Makefile` pins the v2.0.0 CLI archives its TPC-H generator downloads in
  its own `case` arms the same way, and `check_ci_cache_pins.py` requires a SHA-256 in each.
- **`gh`**: the GitHub CLI 2.103.3 for Linux x86_64 and arm64 and macOS x86_64 and arm64, the
  SHA-256s `gh_<version>_checksums.txt` publishes beside the release, each checked against a
  download. `install-gh-cli` installs it into a new directory of the job's own, first on PATH,
  whatever gh the runner has (the self-hosted Macs' gh is one any job there can replace), for
  `push-snap-changes`, the bench, schema and cluster bench workflows, and `verify-attestation`,
  which runs the binary it installed by its path. The target names the asset, extension
  included. To bump it, pick the newest release at least two weeks old and take the four
  lines of its checksums file.
- **`docker-apt-key`**: Docker's apt key (`download.docker.com/linux/ubuntu/gpg`), which the
  arm64 jobs of `spiced_docker_dev.yml`, `build_nightly.yml` and `spiced_docker.yml` install.
  It holds one key, `9DC8 5822 9FC7 DD38 854A E2D8 8D81 803C 0EBF CD88`, the fingerprint
  Docker's install guide gave for it. Those workflows check it inline with `sha256sum -c`
  rather than through `fetch_pinned.sh`, because `spiced_docker_dev.yml` builds a commit that
  can predate the script; `check_ci_cache_pins.py` keeps the inline SHA-256 equal to the pin
  (`INLINE_PINS`).

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
token reads the cache and warns that its write was refused. A job that assumes a CI AWS
role off trunk (the merge queue included) does not try: its `Choose the CI role` step
(`.github/actions/ci-role`) sets `CI_CACHE_READ_ONLY=true`, and `cached_fetch.sh` then reads
and logs one line, as setup-sccache and the build actions skip their cache and store writes.
A reader never trusts the writer either way: no reader uses bytes that do not match its pin,
and one that finds wrong bytes replaces them with the verified download.

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
