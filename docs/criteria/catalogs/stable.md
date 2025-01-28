# Spice.ai OSS Catalog Connectors - Stable Release Criteria

This document defines the set of criteria that is required before a Catalog Connector is considered to be of Stable quality.

All criteria must be met for the Catalog to be considered Stable, with exceptions only permitted in some circumstances (e.g. it would be technically infeasible to add a feature/fix a bug for a particular Catalog).

## Stable Quality Catalogs

| Catalog       | Stable Quality | DRI Sign-off |
| ------------- | -------------- | ------------ |
| Databricks    | ➖             |              |
| Iceberg       | ➖             |              |
| Spice.ai      | ➖             |              |
| Unity Catalog | ✅             | @Sevenannn   |

## Stable Release Criteria

The Stable release criteria expand on and require that all [RC criteria](./rc.md) continue to pass for the Catalog.

- [ ] All [RC criteria](./rc.md) pass for this Catalog.

Catalogs that use existing Data Connectors requires that the Data Connector must first be at Stable.

Catalogs without an existing Data Connector must perform the Stable Data Connector criteria in addition to the Catalog criteria.

### Existing Data Connector

This section is required if the Catalog uses an existing Data Connector.

- [ ] The associated Data Connector for this Catalog is currently of Stable quality or higher.

### No Data Connector

This section is required if the Catalog does not use an existing Data Connector.

- [ ] The Catalog passes the [Stable Data Connector release criteria](../connectors/stable.md)

### All Catalogs

- [ ] Known [Major and Minor](../definitions.md) bugs are resolved for the Catalog.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the Catalog.
- [ ] Documentation includes all known issues/limitations for the Catalog.
- [ ] The Catalog has an easy to follow cookbook recipe.
- [ ] The Catalog status is updated in the table of Catalogs in [spiceai/docs](https://github.com/spiceai/docs).
