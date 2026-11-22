# Cloud Connect state-directory tests

`state-dir.sh` checks the Cloud Connect state-directory rule
([#16474](https://github.com/spiceai/spiceai/issues/16474)) against a real Linux
`spiced`, in the layouts that need root to set up: a `cloud-connect/` directory
owned by another account, a config directory shared with other accounts, a
root-owned `0777` Kubernetes `emptyDir`, and `spiced` running as root or as
another uid.

It runs as root, because it creates directories owned by uids 0, 1000, 1001 and
65534 and starts `spiced` as those uids with `setpriv` (util-linux; `cmp` and
`stat` are also used). It needs no
network, no credentials and no Docker: every case works in its own temporary
directory, binds `spiced` to ephemeral ports, points `SPICE_CLOUD_ENDPOINT` at a
closed local port, and removes everything when it ends.

```sh
sudo bash test/cloud-connect/state-dir.sh target/release/spiced
```

On macOS, run it as root in a Linux container, with a `spiced` built for Linux in an
image whose C library matches (the `rust` image the binary was built in works):

```sh
docker run --rm -v "$PWD/test/cloud-connect:/t:ro" -v /path/to/linux/spiced:/spiced:ro \
  rust:1.100-bookworm bash /t/state-dir.sh /spiced
```

Each case prints `ok` or `FAIL` with the reason and the end of `spiced`'s output;
the script exits 1 if any case fails and 2 if it cannot run. The expected
outcomes come from the enhancement's specification, not from a run of the code:

| Case | Expected |
| --- | --- |
| uid 1000, `cloud-connect/` owned by uid 0, mode `0777` | Exits non-zero with the "owned by user id 0, not by this account" ERROR; the deployment is not read |
| uid 65534, `.spice` a root-owned `0777` `emptyDir` holding earlier state | Creates `cloud-connect/` owned by 65534, mode `0700`; moves the files; serves the deployment from it; `.spice` keeps its owner and mode |
| uid 65534, the same layout, `--token` | Creates `cloud-connect/` `0700` and attempts the enrollment |
| root, `cloud-connect/` owned by uid 1000, mode `0777` | Exits non-zero with the `chmod go-w` ERROR naming uid 1000 |
| root, `cloud-connect/` owned by uid 1000, mode `0700` | Used as it is |
| uid 1001, `.spice` owned by uid 1000, mode `0777`, holding earlier state | Exits non-zero with the `chmod go-w` ERROR naming `.spice`; nothing moves |
| root, `.spice` owned by uid 1000 holding earlier state | Moves it into a `cloud-connect/` uid 1000 owns, mode `0700` |
| uid 1000, its own `cloud-connect/` at `0775` | One INFO line (`0775 → 0755`), mode `0755`, used |
| uid 1000, its own `.spice` at `0775` holding earlier state | Moves it; `.spice` stays `0775` |
| An instance that never enrolled | No `cloud-connect/`, nothing logged about Cloud Connect |

CI runs it in the `test_cloud_connect_state_dir` job of `E2E Test CI`
(`.github/workflows/e2e_test_ci.yml`), against the Linux `spiced` that workflow
builds. The cases a non-root process can set up are also end-to-end tests in
`bin/spiced/tests/cloud_managed_spicepod.rs`, which run on macOS and Linux.
