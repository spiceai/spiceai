# Spice.ai OSS Catalog Connectors - Beta Release Criteria

This document defines the set of criteria that is required before a Catalog Connector is considered to be of Beta quality.

All criteria must be met for the Catalog to be considered Beta, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular Catalog).

## Beta Quality Catalogs

| Catalog       | Beta Quality | DRI Sign-off    |
| ------------- | ------------ | --------------- |
| Databricks    | ✅           | @Sevenannn      |
| Iceberg       | ✅           | @phillipleblanc |
| Spice.ai      | ✅           | @peasee         |
| Unity Catalog | ✅           | @Sevenannn      |

## Beta Release Criteria

The Beta release criteria expand on and require that all [Alpha release criteria](./alpha.md) continue to pass for the Catalog.

- [ ] All [Alpha release criteria](./alpha.md) pass for this Catalog.

Catalogs that use existing Data Connectors requires that the Data Connector must first be at Beta.

Catalogs without an existing Data Connector must perform the Beta Data Connector criteria in addition to the Catalog criteria.

### Existing Data Connector

This section is required if the Catalog uses an existing Data Connector.

- [ ] The associated Data Connector for this Catalog is currently of Beta quality or higher.

### No Data Connector

This section is required if the Catalog does not use an existing Data Connector.

- [ ] The Catalog passes the [Beta Data Connector release criteria](../connectors/beta.md)

### All Catalogs

- [ ] An end-to-end test is created that loads the TPC-H dataset at scale factor 1 using a Catalog, instead of specifying individual datasets.
  - [ ] TPC-H queries pass with a success rate equal to the native connector. For Catalogs without an existing Data Connector, queries pass with a success rate equal or greater than TPC-H execution on Datafusion.
- [ ] The Catalog can load the schema of a dataset without performing a data query at dataset registration time.
- [ ] Known [Major](../definitions.md) bugs are resolved for the Catalog.
- [ ] Known [Minor](../definitions.md) bugs are logged, but not required to be fixed unless needed to achieve TPC-H success.

#### UX

- [ ] All of the connector's error messages follow the [error handling guidelines](../../dev/error_handling.md)

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the Catalog.
- [ ] Documentation includes all known issues/limitations for the Catalog.
- [ ] The Catalog has an easy to follow cookbook recipe.
- [ ] The Catalog status is updated in the table of Catalogs in [spiceai/docs](https://github.com/spiceai/docs).
