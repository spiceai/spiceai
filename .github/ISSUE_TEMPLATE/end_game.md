---
name: Milestone Endgame
about: Ship a milestone release for Spice.ai Open Source!
title: 'v1.x.x Endgame'
labels: 'kind/endgame'
assignees: ''
---

## DRIs

| Role    | DRI |
| ------- | --- |
| Endgame |     |
| QA      |     |
| Docs    |     |
| Comms   |     |

## Milestone Release Timeline

| Date            | Description                                     |
| --------------- | ----------------------------------------------- |
| Planning        | TBD (e.g. Mon, Mar 11)                          |
| Branch Creation | TBD (e.g. Day before release, e.g. Sun, Mar 10) |
| Release         | TBD (e.g. Mon, Mar 11)                          |
| Announcement    | TBD (e.g. Mon, Mar 11)                          |

## Planning Checklist

- [ ] Review the [GitHub Milestone](https://github.com/spiceai/spiceai/milestones).
- [ ] Ensure all Issues and PRs are labeled correctly.
- [ ] Confirm no high-priority security or performance Issues remain open.
- [ ] Communicate the feature freeze date to contributors.
- [ ] **Patch Release (vX.Y.Z)**: Verify no backward-incompatible changes and cherry-pick relevant commits.
- [ ] **Minor Release (vX.Y)**: Plan to merge `trunk` into the release branch.

## Release Branch Creation

- [ ] Create `release/X.Y` from `trunk` one day before release.
  - Refer to [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md).
- [ ] Lock the branch to critical fixes only and notify the team.

## Pre-Release Testing & Validation

1. **Build Validations**

   - [ ] Ensure all builds (including CUDA) pass on **Linux and Windows**.
   - [ ] Verify all CI workflows complete without warnings or errors.

1. **Unit/Integration Tests**

   - [ ] Confirm local and CI tests pass without major failures.

1. **E2E Tests**

   - [ ] Verify [E2E Test CI (core)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_ci.yml) is green on `trunk` and the release branch.
   - [ ] Verify [E2E Test CI (models)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_ci_models.yml) is green on `trunk` and the release branch.
   - [ ] Verify [Test Operator Benchmarks](https://github.com/spiceai/spiceai/actions/workflows/testoperator_run_bench.yml) is green on `trunk` and the release branch.
     - Use the [Test Operator Dispatch](https://github.com/spiceai/spiceai/actions/workflows/testoperator_dispatch.yml) workflow to execute a new benchmark run. Specify `trunk` as the branch source, with the following parameters:
       - Workflow to execute: `bench`
       - All other values left empty.
   - [ ] Verify [E2E Test CLI](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_spice_cli.yml) is green on `trunk` and the release branch.
     - Parameters: Branch: `trunk`
     - Build CLI: `true`
   - [ ] Verify [Throughput Tests](https://github.com/spiceai/spiceai/actions/workflows/testoperator_run_throughput.yml) is green on `trunk` and the release branch.
     - Use the [Test Operator Dispatch](https://github.com/spiceai/spiceai/actions/workflows/testoperator_dispatch.yml) workflow to execute a new throughput run. Specify `trunk` as the branch source, with the following parameters:
       - Workflow to execute: `throughput`
       - All other values left empty.

1. **Documentation Review**

   - [ ] Update and confirm accuracy of `README.md` and [Spice.ai Docs](https://github.com/spiceai/docs).

1. **Cookbook Recipes**

   - **Data Connectors**

     - [ ] [AWS RDS Aurora (MySQL Compatible)](https://github.com/spiceai/cookbook/blob/trunk/mysql/rds-aurora/README.md)
     - [ ] [AWS RDS PostgreSQL](https://github.com/spiceai/cookbook/blob/trunk/postgres/rds/README.md)
     - [ ] [Clickhouse](https://github.com/spiceai/cookbook/blob/trunk/clickhouse/README.md)
     - [ ] [Databricks Delta Lake](https://github.com/spiceai/cookbook/blob/trunk/databricks/delta_lake/README.md)
     - [ ] [Dremio](https://github.com/spiceai/cookbook/blob/trunk/dremio/README.md)
     - [ ] [DuckDB](https://github.com/spiceai/cookbook/blob/trunk/duckdb/connector/README.md)
     - [ ] [FTP](https://github.com/spiceai/cookbook/blob/trunk/ftp/README.md)
     - [ ] [File Connector](https://github.com/spiceai/cookbook/blob/trunk/file/README.md)
     - [ ] [GitHub](https://github.com/spiceai/cookbook/blob/trunk/github/README.md)
     - [ ] [GraphQL](https://github.com/spiceai/cookbook/blob/trunk/graphql/README.md)
     - [ ] [MSSQL](https://github.com/spiceai/cookbook/blob/trunk/mssql/README.md)
     - [ ] [MySQL](https://github.com/spiceai/cookbook/blob/trunk/mysql/connector/README.md)
     - [ ] [ODBC](https://github.com/spiceai/cookbook/blob/trunk/odbc/README.md)
     - [ ] [PostgreSQL](https://github.com/spiceai/cookbook/blob/trunk/postgres/connector/README.md)
     - [ ] [S3](https://github.com/spiceai/cookbook/blob/trunk/s3/README.md)
     - [ ] [SharePoint](https://github.com/spiceai/cookbook/blob/trunk/sharepoint/README.md)
     - [ ] [Snowflake](https://github.com/spiceai/cookbook/blob/trunk/snowflake/README.md)
     - [ ] [Spark](https://github.com/spiceai/cookbook/blob/trunk/spark/README.md)
     - [ ] [Supabase](https://github.com/spiceai/cookbook/blob/trunk/postgres/supabase/README.md)
     - [ ] [Spice.ai Cloud Platform](https://github.com/spiceai/cookbook/blob/trunk/spiceai/README.md)
     - [ ] [Debezium CDC (plain & SASL/SCRAM)](https://github.com/spiceai/cookbook/blob/trunk/cdc-debezium/README.md)
     - [ ] [IMAP](https://github.com/spiceai/cookbook/blob/trunk/imap/README.md)
     - [ ] Update connector status per [Connector RC Criteria](/docs/criteria/connectors/rc.md).

   - **Data Accelerators**

     - [ ] [DuckDB Accelerator](https://github.com/spiceai/cookbook/blob/trunk/duckdb/accelerator/README.md)
     - [ ] [PostgreSQL Accelerator](https://github.com/spiceai/cookbook/blob/trunk/postgres/accelerator/README.md)
     - [ ] [SQLite Accelerator](https://github.com/spiceai/cookbook/blob/trunk/sqlite/accelerator/README.md)
     - [ ] [Arrow Accelerator](https://github.com/spiceai/cookbook/blob/trunk/arrow/README.md)
     - [ ] Update accelerator status per [Accelerator RC Criteria](/docs/criteria/accelerators/rc.md).

   - **Catalog Connectors**

     - [ ] [Databricks Unity Catalog](https://github.com/spiceai/cookbook/blob/trunk/catalogs/databricks/README.md)
     - [ ] [Spice.ai Cloud Platform Catalog](https://github.com/spiceai/cookbook/blob/trunk/catalogs/spiceai/README.md)
     - [ ] [Unity Catalog](https://github.com/spiceai/cookbook/blob/trunk/catalogs/unity_catalog/README.md)
     - [ ] [Iceberg Catalog](https://github.com/spiceai/cookbook/blob/trunk/catalogs/iceberg/README.md)

   - **AI/ML Models**

     - [ ] [Searching GitHub files with Spice](https://github.com/spiceai/cookbook/tree/trunk/search_github_files)
     - [ ] [Text-to-SQL (Tools)](https://github.com/spiceai/cookbook/tree/trunk/text-to-sql)
     - [ ] [Spice with Azure OpenAI](https://github.com/spiceai/cookbook/tree/trunk/azure_openai)
     - [ ] [OpenAI SDK](https://github.com/spiceai/cookbook/tree/trunk/openai_sdk)
     - [ ] [Nvidia NIM](https://github.com/spiceai/cookbook/tree/trunk/nvidia-nim)
     - [ ] [LLM Memory](https://github.com/spiceai/cookbook/tree/trunk/llm-memory)
     - [ ] [Model-Context-Protocol (MCP)](https://github.com/spiceai/cookbook/tree/trunk/mcp)

   - **SDK Samples**

     - [ ] [Spice with gospice SDK sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/gospice-sdk-sample/README.md)
     - [ ] [Spice with Java SDK sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spice-java-sdk-sample/README.md)
     - [ ] [Spice with rust SDK sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spice-rs-sdk-sample/README.md)
     - [ ] [Spice with spice.js SDK sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spice.js-sdk-sample/README.md)
     - [ ] [Spice with spicepy SDK sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spicepy-sdk-sample/README.md)

   - **Other Cookbook Recipes**
     - [ ] [Sales BI (Apache Superset)](https://github.com/spiceai/cookbook/blob/trunk/sales-bi/README.md)
     - [ ] [Accelerated table data quality (constraints)](https://github.com/spiceai/cookbook/blob/trunk/acceleration/constraints/README.md)
     - [ ] [Advanced Data Refresh](https://github.com/spiceai/cookbook/blob/trunk/acceleration/data-refresh/README.md)
     - [ ] [Data Retention Policy](https://github.com/spiceai/cookbook/blob/trunk/retention/README.md)
     - [ ] [Deploying to Kubernetes](https://github.com/spiceai/cookbook/blob/trunk/kubernetes/README.md)
     - [ ] [Federated SQL Query](https://github.com/spiceai/cookbook/blob/trunk/federation/README.md)
     - [ ] [Indexes on Accelerated Data](https://github.com/spiceai/cookbook/blob/trunk/acceleration/indexes/README.md)
     - [ ] [Refresh Data Window](https://github.com/spiceai/cookbook/blob/trunk/refresh-data-window/README.md)
     - [ ] [Results Caching](https://github.com/spiceai/cookbook/blob/trunk/caching/README.md)
     - [ ] [Encryption in transit via TLS](https://github.com/spiceai/cookbook/blob/trunk/tls/README.md)
     - [ ] [TPC-H Benchmarking](https://github.com/spiceai/cookbook/blob/trunk/tpc-h/README.md)
     - [ ] [API Key Authentication](https://github.com/spiceai/cookbook/blob/trunk/api_key/README.md)
     - [ ] [Grafana datasource integration](https://github.com/spiceai/cookbook/blob/trunk/grafana-datasource/README.md)

## Final Updates

- [ ] Merge any pending [Docs PRs](https://github.com/spiceai/docs/pulls).

- [ ] Merge any pending Merge pending [Cookbook PRs](https://github.com/spiceai/cookbook/pulls)

- [ ] Prepare and finalize release notes:

  - [ ] Acknowledge external and new contributors.
  - [ ] List notable dependency updates (e.g. `datafusion`, `datafusion-table-providers`) under `## Dependencies`.
  - [ ] Summarize any cookbook changes under `## Cookbook`.

- [ ] Add references to any SDK releases in the release notes:

  - [ ] [spice.js](https://github.com/spiceai/spice.js/releases)
  - [ ] [spicepy](https://github.com/spiceai/spicepy/releases)
  - [ ] [spice-rs](https://github.com/spiceai/spice-rs/releases)
  - [ ] [spice-java](https://github.com/spiceai/spice-java/releases)
  - [ ] [spice-dotnet](https://github.com/spiceai/spice-dotnet/releases)
  - [ ] [gospice](https://github.com/spiceai/gospice/releases)

- [ ] Run [Generate Acknowledgements](https://github.com/spiceai/spiceai/actions/workflows/generate_acknowledgements.yml) **on the release branch** to update acknowledgements in [docs](https://github.com/spiceai/docs/blob/trunk/website/docs/acknowledgements/index.md).

- [ ] Verify `version.txt` and version in `Cargo.toml` using [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#version-update).

- [ ] **QA DRI sign-off** and **Docs DRI sign-off** confirming readiness and completeness.

## Release Publication Steps

- [ ] Cherry-pick release notes onto the release branch.
- [ ] Create a **pre-release** [GitHub Release](https://github.com/spiceai/spiceai/releases/new) with a tag (e.g. `v1.0.0-rc.1`). Leave the body empty so automation can populate it from the checked-in notes.
- [ ] Tag and release docs (e.g. `v1.0.0`) **after** the [build_and_release workflow](https://github.com/spiceai/spiceai/actions/workflows/build_and_release.yml) completes.
- [ ] Update the [Helm chart](https://github.com/spiceai/spiceai/blob/trunk/deploy/chart) (chart version & image.tag) only after:

  1. Docker build for the release branch completes (~2 hours).
  2. [Release Chart workflow](https://github.com/spiceai/helm-charts/actions/workflows/release.yml) is triggered.

- [ ] Mark the [release](https://github.com/spiceai/spiceai/releases) as official once all binaries and Docker images finish building.
- [ ] Perform a final test pass on the released binaries and Docker images.
- [ ] Run the following workflows to confirm installation health:
  - [ ] [Generate Spicepod JSON schema](https://github.com/spiceai/spiceai/actions/workflows/generate_json_schema.yml)
  - [ ] [E2E Test Release Installation](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_release_install.yml)
  - [ ] [E2E Test Release Installation (AI)](https://github.com/spiceai/spiceai/blob/trunk/.github/workflows/e2e_test_release_install_ai.yml)
  - [ ] [E2E Test CLI](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_spice_cli.yml)
    - Use parameters:
      - Branch: `trunk`
      - Build the CLI: `false`
      - Release Version: the version tag released.

## Post-Release Housekeeping

- [ ] Bump `version.txt` and `Cargo.toml` in `trunk` to the next planned release.
- [ ] Update [brew taps](https://github.com/spiceai/homebrew-spiceai) after the final build completes.
- [ ] Remove or mark the released version in the [ROADMAP](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md).
- [ ] Update the supported version in `SECURITY.md` if necessary.
- [ ] QA DRI: Run SpiceQA via SCP over the following recipes using the template prompt from the [SpiceQA workflow](https://github.com/spiceai/cookbook/blob/trunk/.github/workflows/spice-qa.yml#L45):
  - [ ] File Data Connector
  - [ ] Dremio Data Connector
- [ ] QA DRI: Add metrics to [QA analytics](https://github.com/spiceai/spiceai/blob/trunk/docs/release_notes/qa_analytics.csv).
  - Use number of recipes total from [spiceai.org/docs/cookbook](https://spiceai.org/docs/cookbook).

## Announcement Checklist

- [ ] X (Twitter): [@spice_ai](https://x.com/spice_ai)
- [ ] Reddit: [reddit.com/r/spiceai](https://reddit.com/r/spiceai)
- [ ] Discord: [#announcements](https://discord.gg/zv8ahzZVpf)
- [ ] Telegram: [spiceai](https://t.me/spiceai)
- [ ] Blog: [spiceai.org/blog](https://spiceai.org/blog)
  - [ ] Update docs banner version in [docusaurus.config.ts](https://github.com/spiceai/docs/blob/trunk/website/docusaurus.config.ts#L95).
  - [ ] Ensure version numbers and references match the release.
