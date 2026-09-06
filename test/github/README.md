# GitHub connector tests

The E2E job runs a local API fixture and an authenticated live smoke test.
The fixture runs the real `spiced` binary against ten federated and accelerated
datasets. It needs Python 3 and no GitHub credentials:

```sh
python3 test/github/test_connector.py --spiced target/debug/spiced \
  --artifacts /tmp/github-connector-results --repeat 3
```

Each iteration uses its own API server, runtime ports, and application directory.
The fixture rejects a full-page registration probe with GitHub's resource-limit
error. It checks exact keys across 125 parents, pagination, SQL LIMIT pushdown,
NULL filtering, and a reviews dataset whose first parent has no reviews. Nested
connection limits must stay intact, and a PR with 26 commits must keep that
total even when its hash list contains only 25 entries. Runtime logs, API requests,
and actual SQL rows are saved under the artifact directory, including on failure.
Pass a fresh artifact directory on each invocation.

The live smoke test uses `github-arrow-smoke.yaml` in this directory. It accelerates
five rows per GraphQL dataset and the README from the `v1.0.0` tag. `check_live.py`
requires exact row counts, populated unique keys, and downloaded file content.
Readiness is polled with a deadline; a fixed startup delay is not used.

All Rust GitHub integration tests run in the dedicated **GitHub Connector
Integration Tests** job, serially and outside the three general integration
partitions. That job and the live E2E job share a queued workflow concurrency
group because GitHub quotas span processes. Failed tests are not rerun by
nextest; HTTP retries remain the connector's responsibility.

For local integration testing, supply `GITHUB_TOKEN`, `GITHUB_ORG_TOKEN`, and
the app credentials `GITHUB_CLIENT_ID`, `GITHUB_INSTALLATION_ID`, and
`GITHUB_PRIVATE_KEY` through the environment or the runtime's dotenv secret
store. Set `SPICE_GITHUB_TEST_REQUIRED=1` to fail on missing credentials, as CI
does. Keep the same package selection and features as your existing build.
An OAuth token or classic personal access token also needs `read:user` or
`user:email` for the requested user email fields, and `read:org` for organization
membership. Repository access alone is insufficient for the full suite.

```sh
SPICE_GITHUB_TEST_REQUIRED=1 cargo nextest run -p runtime --test integration \
  --test-threads 1 -E 'test(/^github::/)'
```

Live tests check schemas and data invariants. Mutable workflow names and rows
are not snapshot fixtures. External service outages can still fail the live
checks; the local fixture provides a reproducible regression check during one.
