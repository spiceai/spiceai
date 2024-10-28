---
name: Milestone Endgame
about: Ship a milestone!
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

| Date         | Description            |
| ------------ | ---------------------- |
| Planning     | TBD (E.g. Mon, Mar 11) |
| Release      | TBD (E.g. Mon, Mar 11) |
| Announcement | TBD (E.g. Mon, Mar 11) |

## Planning Checklist

- [ ] Review the specific [GitHub Milestone](https://github.com/spiceai/spiceai/milestones)

## Release Checklist

- [ ] All features/bugfixes to be included in the release have been merged to trunk
- [ ] Full test pass and update if necessary over README.md
- [ ] Full test pass and update if necessary over Docs
- [ ] Full test pass and update if necessary over existing and new Samples
  - [ ] [Sales BI Dashboard](https://github.com/spiceai/samples/blob/trunk/sales-bi/README.md)
  - [ ] [Local Materialization and Acceleration CQRS](https://github.com/spiceai/samples/blob/trunk/acceleration/README.md)
  - [ ] [Accelerated table data quality with constraint enforcement](https://github.com/spiceai/samples/blob/trunk/constraints/README.md)
  - [ ] [Streaming changes in real-time with Debezium CDC](https://github.com/spiceai/samples/blob/trunk/cdc-debezium/README.md)
  - [ ] [Encryption in transit using TLS](https://github.com/spiceai/samples/blob/trunk/tls/README.md)
  - [ ] [Adding Spice as a Grafana datasource](https://github.com/spiceai/samples/blob/trunk/grafana-datasource/README.md)
  - [ ] [FTP/SFTP Data Connector](https://github.com/spiceai/samples/blob/trunk/ftp/README.md)
  - [ ] [Spice with go sdk sample](https://github.com/spiceai/samples/blob/trunk/client-sdk/gospice-sdk-sample/README.md)
  - [ ] [Spice with Java sdk sample](https://github.com/spiceai/samples/blob/trunk/client-sdk/spice-java-sdk-sample/README.md)
  - [ ] [Spice with rust sdk sample](https://github.com/spiceai/samples/blob/trunk/client-sdk/spice-rs-sdk-sample/README.md)
  - [ ] [Spice with spice.js sdk sample](https://github.com/spiceai/samples/blob/trunk/client-sdk/spice.js-sdk-sample/README.md)
  - [ ] [Spice with spicepy sdk sample](https://github.com/spiceai/samples/blob/trunk/client-sdk/spicepy-sdk-sample/README.md)
- [ ] Full test pass and update if necessary over existing and new Quickstarts
  - **Data Connectors:**
  - [ ] [Apache Superset](https://github.com/spiceai/quickstarts/blob/trunk/superset/README.md)
  - [ ] [AWS RDS Aurora (MySQL Compatible) Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/rds-aurora-mysql/README.md)
  - [ ] [AWS RDS PostgreSQL Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/rds-postgresql/README.md)
  - [ ] [Clickhouse Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/clickhouse/README.md)
  - [ ] [Databricks Delta Lake Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/databricks/README.md)
  - [ ] [Dremio Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/dremio/README.md)
  - [ ] [DuckDB Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/duckdb/connector/README.md)
  - [ ] [FTP Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/ftp/README.md)
  - [ ] [File Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/file/README.md)
  - [ ] [GitHub Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/github/README.md)
  - [ ] [GraphQL Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/graphql/README.md)
  - [ ] [MSSQL Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/mssql/README.md)
  - [ ] [MySQL Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/mysql/README.md)
  - [ ] [ODBC Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/odbc/README.md)
  - [ ] [Planetscale Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/planetscale/README.md)
  - [ ] [PostgreSQL Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/postgres/connector/README.md)
  - [ ] [S3 Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/s3/README.md)
  - [ ] [SharePoint Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/sharepoint/README.md)
  - [ ] [Snowflake Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/snowflake/README.md)
  - [ ] [Spark Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/spark/README.md)
  - [ ] [Supabase Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/supabase/README.md)
  - [ ] [Spice.ai Cloud Platform Data Connector](https://github.com/spiceai/quickstarts/blob/trunk/spiceai/README.md)
  - [ ] Update the RC status of any connectors based on the [criteria](/docs/criteria/connectors/rc.md)
  - **Data Accelerators:**
  - [ ] [DuckDB Data Accelerator](https://github.com/spiceai/quickstarts/blob/trunk/duckdb/accelerator/README.md)
  - [ ] [PostgreSQL Data Accelerator](https://github.com/spiceai/quickstarts/blob/trunk/postgres/accelerator/README.md)
  - [ ] [SQLite Data Accelerator](https://github.com/spiceai/quickstarts/blob/trunk/sqlite/accelerator/README.md)
  - [ ] Update the Beta status of any accelerators based on the [criteria](/docs/criteria/accelerators/beta.md)
  - **Catalog Connectors:**
  - [ ] [Databricks Unity Catalog Connector](https://github.com/spiceai/quickstarts/blob/trunk/catalogs/databricks/README.md)
  - [ ] [Spice.ai Cloud Platform Catalog Connector](https://github.com/spiceai/quickstarts/blob/trunk/catalogs/spiceai/README.md)
  - [ ] [Unity Catalog Connector](https://github.com/spiceai/quickstarts/blob/trunk/catalogs/unity_catalog/README.md)
  - **Other Quickstarts:**
  - [ ] [Advanced Data Refresh](https://github.com/spiceai/quickstarts/blob/trunk/acceleration/data-refresh/README.md)
  - [ ] [Data Retention Policy](https://github.com/spiceai/quickstarts/blob/trunk/retention/README.md)
  - [ ] [Deploying to Kubernetes](https://github.com/spiceai/quickstarts/blob/trunk/kubernetes/README.md)
  - [ ] [Federated SQL Query](https://github.com/spiceai/quickstarts/blob/trunk/federation/README.md)
  - [ ] [Indexes on Accelerated Data](https://github.com/spiceai/quickstarts/blob/trunk/acceleration/indexes/README.md)
  - [ ] [Refresh Data Window](https://github.com/spiceai/quickstarts/blob/trunk/refresh-data-window/README.md)
  - [ ] [Results Caching](https://github.com/spiceai/quickstarts/blob/trunk/caching/README.md)
  - [ ] [Securing data in transit via TLS](https://github.com/spiceai/quickstarts/blob/trunk/tls/README.md)
  - [ ] [TPC-H Benchmarking](https://github.com/spiceai/quickstarts/blob/trunk/tpc-h/README.md)
- [ ] Merge [Docs PRs](https://github.com/spiceai/docs/pulls)
  - [ ] Update spice version in [docs banner](https://github.com/spiceai/docs/blob/trunk/spiceaidocs/docusaurus.config.ts#L60)
- [ ] Merge [Samples PRs](https://github.com/spiceai/samples/pulls)
- [ ] Merge [Quickstarts PRs](https://github.com/spiceai/quickstarts/pulls)
- [ ] Update release notes
  - [ ] Ensure any external contributors have been acknowledged.
- [ ] Add any SDK releases to the release notes
  - [ ] [spice.js](https://github.com/spiceai/spice.js/releases)
  - [ ] [spicepy](https://github.com/spiceai/spicepy/releases)
  - [ ] [spice-rs](https://github.com/spiceai/spice-rs/releases)
  - [ ] [spice-java](https://github.com/spiceai/spice-java/releases)
  - [ ] [spice-dotnet](https://github.com/spiceai/spice-dotnet/releases)
  - [ ] [gospice](https://github.com/spiceai/gospice/releases)
- [ ] Update acknowledgements by triggering [Generate Acknowledgements](https://github.com/spiceai/spiceai/actions/workflows/generate_acknowledgements.yml) workflow
  - [ ] Update acknowledgements in [docs](https://github.com/spiceai/docs/blob/trunk/spiceaidocs/docs/acknowledgements/index.md)
- [ ] Verify `version.txt` and version in `Cargo.toml` are correct using [docs/RELEASE.md](https://github.com/spiceai/spiceai/blob/trunk/docs/RELEASE.md#version-update)
- [ ] Ensure [E2E Test CI](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_ci.yml) is green on the trunk branch.
- [ ] QA DRI sign-off
- [ ] Docs DRI sign-off
- [ ] Create a new branch `release-v[semver]` for the release from trunk. E.g. `release-v0.17.0-beta`
- [ ] Release the new version by creating a `pre-release` [GitHub Release](https://github.com/spiceai/spiceai/releases/new) with the tag from the release branch. E.g. `v0.17.0-beta`. Leave the release note empty; the automation will fill it in from the checked in release note.
- [ ] Release any docs updates by creating a `v[semver]` tag.
      **Note**: Docs should be released only after the [binaries have finished building](https://github.com/spiceai/spiceai/actions/workflows/build_and_release.yml).
- [ ] Trigger algolia search crawler [workflow](https://github.com/spiceai/docs/actions/workflows/trigger_search_reindex.yml), to reindex updated docs.
- [ ] Update the [Helm chart](https://github.com/spiceai/spiceai/blob/trunk/deploy/chart) version (image.tag version & chart version). Ensure [docker build](https://github.com/spiceai/spiceai/actions/workflows/spiced_docker.yml) for the tag from the release branch completed (~2 hours) and trigger the [Release Chart](https://github.com/spiceai/helm-charts/actions/workflows/release.yml) workflow.
      **Note**: Release chart workflow should be triggered only after the [binaries have finished building](https://github.com/spiceai/spiceai/actions/workflows/build_and_release.yml) and [docker image have finished building](https://github.com/spiceai/spiceai/actions/workflows/spiced_docker.yml).
- [ ] Final test pass on released binaries
- [ ] Run [Generate Spicepod JSON schema](https://github.com/spiceai/spiceai/actions/workflows/generate_json_schema.yml)
- [ ] Run [E2E Test Release Installation](https://github.com/spiceai/spiceai/actions/workflows/e2e_test_release_install.yml)
- [ ] Update `version.txt` and version in `Cargo.toml` to the next release version.
- [ ] Update versions in [brew taps](https://github.com/spiceai/homebrew-spiceai).
      **Note**: Ensure that the Homebrew taps are updated only after the [binaries have finished building](https://github.com/spiceai/spiceai/actions/workflows/build_and_release.yml).
- [ ] Remove the released version from the [ROADMAP](https://github.com/spiceai/spiceai/blob/trunk/docs/ROADMAP.md)
- [ ] Set the [release](https://github.com/spiceai/spiceai/releases) as latest release.

## Announcement Checklist

- [ ] X: [@spice_ai](https://twitter.com/spice_ai)
- [ ] Reddit: [reddit.com/r/spiceai](https://reddit.com/r/spiceai)
- [ ] Discord: [#announcements](https://discord.gg/zv8ahzZVpf)
- [ ] Telegram: [spiceai](https://t.me/spiceai)
- [ ] Blog: [blog.spiceai.org](https://blog.spiceai.org)
