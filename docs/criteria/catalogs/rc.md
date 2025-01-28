# Spice.ai OSS Catalog Connectors - RC Criteria

This document defines the set of criteria that is required before a Catalog Connector is considered to be of [RC](../definitions.md) quality.

All criteria must be met for the Catalog to be considered [RC](../definitions.md), with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular Catalog).

## RC Quality Catalogs

| Catalog       | RC Quality | DRI Sign-off |
| ------------- | ------------ | ------------ |
| Databricks    | ➖           |              |
| Iceberg       | ➖           |              |
| Spice.ai      | ➖           |              |
| Unity Catalog | ✅           | @Sevenannn   |

## RC Criteria

The RC criteria expand on and require that all [Beta release criteria](./beta.md) continue to pass for the Catalog.

- [ ] All [Beta release criteria](./beta.md) pass for this Catalog.

Catalogs that use existing Data Connectors requires that the Data Connector must first be at RC.

Catalogs without an existing Data Connector must perform the RC Data Connector criteria in addition to the Catalog criteria.

### Existing Data Connector

This section is required if the Catalog uses an existing Data Connector.

- [ ] The associated Data Connector for this Catalog is currently of RC quality or higher.

### No Data Connector

This section is required if the Catalog does not use an existing Data Connector.

- [ ] The Catalog passes the [RC Data Connector release criteria](../connectors/rc.md)

### All Catalogs

- [ ] The Catalog can refresh the list of datasets, and add/remove datasets that are available.
  - [ ] An end-to-end test for catalog refresh is created, testing the addition/removal of datasets.
- [ ] Known [Major and Minor](../definitions.md) bugs are resolved for the Catalog.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the Catalog.
- [ ] Documentation includes all known issues/limitations for the Catalog.
- [ ] The Catalog has an easy to follow cookbook recipe.
- [ ] The Catalog status is updated in the table of Catalogs in [spiceai/docs](https://github.com/spiceai/docs).
