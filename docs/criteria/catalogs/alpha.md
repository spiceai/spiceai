# Spice.ai OSS Catalog Connectors - Alpha Release Criteria

This document defines the set of criteria that is required before a Catalog Connector is considered to be of Alpha quality.

All criteria must be met for the Catalog to be considered Alpha. As Alpha signifies the lowest release quality, criteria exceptions are not permitted.

## Alpha Quality Catalogs

| Catalog       | Alpha Quality | DRI Sign-off    |
| ------------- | ------------- | --------------- |
| Databricks    | ✅            | @Sevenannn      |
| Iceberg       | ✅            | @phillipleblanc |
| Spice.ai      | ✅            | @peasee         |
| Unity Catalog | ✅            | @Sevenannn      |

## Alpha Release Criteria

The Alpha Release Criteria for Catalogs is set at a level that ensures the Catalog operates in common conditions with a low error rate.

Catalogs that use existing Data Connectors requires that the Data Connector must first be at Alpha.

Catalogs without an existing Data Connector must perform the Alpha Data Connector criteria in addition to the Catalog criteria.

The Alpha Release Criteria is not intended to cover edge cases or advanced functionality.

### Existing Data Connector

This section is required if the Catalog uses an existing Data Connector.

- [ ] The associated Data Connector for this Catalog is currently of Alpha quality or higher.

### No Data Connector

This section is required if the Catalog does not use an existing Data Connector.

- [ ] The datasets created by the Catalog passes the [Alpha Data Connector release criteria](../connectors/alpha.md)

### All Catalogs

- [ ] The Catalog implements the functionality of a catalog - loading multiple datasets from a catalog provider based on the value of an `include` pattern.
- [ ] An end-to-end test is created that loads the TPC-H dataset at scale factor 1 using a Catalog, instead of specifying individual datasets.
  - [ ] TPC-H queries pass with a success rate equal to the native connector. For Catalogs without an existing Data Connector, queries pass with a success rate equal or greater than TPC-H execution on Datafusion.
- [ ] Known [Minor and Major](../definitions.md) bugs are logged, but not required to be fixed unless needed to achieve TPC-H success.

### Documentation

- [ ] Documentation includes all information and steps for a user to set up the Catalog.
- [ ] Documentation includes all known issues/limitations for the Catalog.
- [ ] The Catalog has an easy to follow cookbook recipe.
- [ ] The Catalog is added to the table of Catalogs in [spiceai/docs](https://github.com/spiceai/docs).
