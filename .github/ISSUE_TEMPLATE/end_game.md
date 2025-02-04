---
name: Milestone Endgame
about: Ship a milestone release for Spice.ai Open Source!
title: 'v0.x.x-beta endgame'
labels: 'kind/endgame'
assignees: ''
---

## DRIs

|         | DRI |
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

- [ ] Review the specific [GitHub Milestone](https://github.com/spiceai/spiceai/milestones).
- [ ] Ensure all related Issues and PRs are correctly labeled.
- [ ] Confirm no high-priority security or performance Issues remain open.
- [ ] Communicate the feature freeze date to all contributors.
- [ ] **If this is a patch release (vX.Y.Z)**: Verify no backward-incompatible changes.
- [ ] **If this is a patch release**: Plan to cherry-pick each relevant commit into the release branch.
- [ ] **If this is a minor release (vX.Y)**: Plan to merge commit from `trunk` into the release branch.

## Release Branch Creation

- [ ] Create a release branch (`release/X.Y`) from `trunk` one day before the scheduled release (if not already created).
  - Refer to [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md) for more details.
- [ ] If **patch release**: cherry-pick each commit to include.
- [ ] If **minor release**: merge commit from `trunk`.
- [ ] Lock the release branch to critical fixes only. Notify the team of the branch status.

## Pre-Release Testing & Validation

1. **Build Validations**

   - [ ] Ensure all builds (including the CUDA build) pass in `trunk` for at least one supported compute capability on **both Linux and Windows**.
   - [ ] Verify all CI workflows run successfully (no warnings or errors).

2. **Unit/Integration Tests**

   - [ ] Confirm local and CI tests pass without major deprecations or failures.

3. **E2E Tests**

   - [ ] Verify [E2E Test CI (core)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_ci.yml) is green on `trunk` and on the release branch.
   - [ ] Verify [E2E Test CI (models)](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_ci_models.yml) is green on `trunk` and on the release branch.

4. **Documentation Review**

   - [ ] Update and confirm accuracy of `README.md` and docs in [Spice.ai Docs](https://github.com/spiceai/docs).
   - [ ] Ensure version numbers, usage details, and references match the intended release.

5. **Cookbook Recipes**

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

   - **SDK Samples**

     - [ ] [Spice with gospice-sdk sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/gospice-sdk-sample/README.md)
     - [ ] [Spice with Java sdk sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spice-java-sdk-sample/README.md)
     - [ ] [Spice with rust sdk sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spice-rs-sdk-sample/README.md)
     - [ ] [Spice with spice.js sdk sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spice.js-sdk-sample/README.md)
     - [ ] [Spice with spicepy sdk sample](https://github.com/spiceai/cookbook/blob/trunk/client-sdk/spicepy-sdk-sample/README.md)

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

  - [ ] Update Spice version in [docs banner](https://github.com/spiceai/docs/blob/trunk/website/docusaurus.config.ts#L95).

- [ ] Merge any pending [Cookbook PRs](https://github.com/spiceai/cookbook/pulls).

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
- [ ] Trigger [algolia search crawler](https://github.com/spiceai/docs/actions/workflows/trigger_search_reindex.yml) to reindex updated docs.
- [ ] Update the [Helm chart](https://github.com/spiceai/spiceai/blob/trunk/deploy/chart) (chart version & image.tag) only after:

  1. Docker build for the release branch completes (~2 hours).
  2. [Release Chart workflow](https://github.com/spiceai/helm-charts/actions/workflows/release.yml) is triggered.

- [ ] Mark the [release](https://github.com/spiceai/spiceai/releases) as official once all binaries and Docker images finish building.
- [ ] Perform a final test pass on the released binaries and Docker images.
- [ ] Run the following workflows to confirm installation health:
  - [ ] [Generate Spicepod JSON schema](https://github.com/spiceai/spiceai/actions/workflows/generate_json_schema.yml)
  - [ ] [E2E Test Release Installation](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_release_install.yml)

## Post-Release Housekeeping

- [ ] Bump `version.txt` and `Cargo.toml` in `trunk` to the next planned release.
- [ ] Update [brew taps](https://github.com/spiceai/homebrew-spiceai) after the final build completes.
- [ ] Remove or mark the released version in the [ROADMAP](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md).
- [ ] Update the supported version in `SECURITY.md` if necessary.

## Announcement Checklist

- [ ] X (Twitter): [@spice_ai](https://x.com/spice_ai)
- [ ] Reddit: [reddit.com/r/spiceai](https://reddit.com/r/spiceai)
- [ ] Discord: [#announcements](https://discord.gg/zv8ahzZVpf)
- [ ] Telegram: [spiceai](https://t.me/spiceai)
- [ ] Blog: [spiceai.org/blog](https://spiceai.org/blog)
