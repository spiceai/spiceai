# DuckLake Connectors

Spice provides two ways to connect to [DuckLake](https://ducklake.select/) catalogs:

1. **Catalog Connector**: Automatically discovers all schemas and tables in a DuckLake catalog
2. **Data Connector**: Connects to specific tables in a DuckLake catalog

Both connectors use DuckDB with the `ducklake` extension to access the DuckLake open lakehouse format.

## Overview

DuckLake is an open lakehouse table format that stores metadata in a simple SQLite-compatible format and data in Parquet files. The Spice DuckLake connectors use DuckDB with the `ducklake` extension to:

- **Automatic Schema Discovery**: Discovers all schemas and tables in the DuckLake catalog
- **Federated Queries**: Query DuckLake tables directly through Spice's SQL interface
- **Cloud Storage Support**: Supports DuckLake catalogs stored in S3, GCS, Azure Blob Storage, or local filesystems
- **Zero Configuration**: Simply point to your DuckLake metadata file and start querying

## Catalog Connector

The catalog connector automatically discovers all schemas and tables in a DuckLake catalog.

### Basic Usage

```yaml
catalogs:
  - from: ducklake
    name: my_catalog
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake
```

### With Custom Catalog Name

```yaml
catalogs:
  - from: ducklake
    name: sales_data
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake
      name: sales  # Name used when attaching the catalog in DuckDB
```

### Using a Persistent DuckDB File

By default, an in-memory DuckDB instance is used. For larger catalogs or persistent caching, specify a DuckDB file:

```yaml
catalogs:
  - from: ducklake
    name: my_catalog
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake
      open: /path/to/local.duckdb
```

### Access Modes

The DuckLake catalog connector supports three access modes:

- `read` (default): Read-only access to the catalog
- `read_write`: Read and write operations (INSERT, UPDATE, DELETE) are allowed
- `read_write_create`: Full access including DDL operations (CREATE TABLE, DROP TABLE, CREATE SCHEMA, DROP SCHEMA)

```yaml
# Read-only access (default)
catalogs:
  - from: ducklake
    name: my_catalog
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake

# Read-write access (DML operations)
catalogs:
  - from: ducklake
    name: my_catalog
    access: read_write
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake

# Full access including DDL
catalogs:
  - from: ducklake
    name: my_catalog
    access: read_write_create
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake
```

With `read_write_create` access, you can execute DDL statements:

```sql
-- Create a new schema
CREATE SCHEMA my_catalog.new_schema;

-- Create a new table
CREATE TABLE my_catalog.new_schema.orders (
    id INTEGER,
    customer_id INTEGER,
    amount DECIMAL(10, 2)
);

-- Drop a table
DROP TABLE my_catalog.new_schema.orders;

-- Drop a schema
DROP SCHEMA my_catalog.new_schema CASCADE;
```

### Catalog Connector Parameters

| Parameter           | Type   | Required | Default     | Description                                      |
| ------------------- | ------ | -------- | ----------- | ------------------------------------------------ |
| `connection_string` | string | No*      | -           | DuckLake metadata file location                  |
| `name`              | string | No       | `ducklake`  | Name to attach the DuckLake catalog as in DuckDB |
| `open`              | string | No       | (in-memory) | Path to a DuckDB file for persistent storage     |

\*`connection_string` can be provided either in `params.connection_string` or in the `from` catalog id (`from: ducklake:<connection_string>`), for example `s3://bucket/path/metadata.ducklake`.

## Data Connector

The data connector allows connecting to specific tables in a DuckLake catalog. Use this when you want to:

- Connect to a single table without discovering the entire catalog
- Configure acceleration or other dataset-specific settings
- Apply transformations or filters to specific tables

### Dataset Configuration

The `from` URI specifies the table path (format: `ducklake:[schema.]table`), while the `connection_string` parameter specifies the DuckLake metadata file location.

```yaml
datasets:
  - from: ducklake:orders
    name: orders
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake
```

By default, tables are looked up in the `main` schema.

### Specifying Schema and Table

```yaml
datasets:
  - from: ducklake:sales.orders
    name: sales_orders
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake
```

### With Acceleration

```yaml
datasets:
  - from: ducklake:sales.orders
    name: orders
    params:
      connection_string: s3://my-bucket/warehouse/metadata.ducklake
      enabled: true
      engine: duckdb
      mode: file
      refresh_check_interval: 10m
```

### Data Connector Parameters

| Parameter           | Type   | Required | Default     | Description                                      |
| ------------------- | ------ | -------- | ----------- | ------------------------------------------------ |
| `connection_string` | string | Yes      | -           | The DuckLake metadata file location              |
| `name`              | string | No       | `ducklake`  | Name to attach the DuckLake catalog as in DuckDB |
| `open`              | string | No       | (in-memory) | Path to a DuckDB file for persistent storage     |

## Connection String Formats

The connection string supports various storage backends. For catalogs, provide it in `params.connection_string` (or in `from` as `ducklake:<connection_string>`). For datasets, provide it in `params.connection_string`.

### Amazon S3

```yaml
catalogs:
  - from: ducklake
    name: s3_catalog
    params:
      connection_string: s3://my-bucket/path/to/metadata.ducklake
```

### Google Cloud Storage

```yaml
catalogs:
  - from: ducklake
    name: gcs_catalog
    params:
      connection_string: gs://my-bucket/path/to/metadata.ducklake
```

### Azure Blob Storage

```yaml
catalogs:
  - from: ducklake
    name: azure_catalog
    params:
      connection_string: az://container/path/to/metadata.ducklake
```

### Local Filesystem

```yaml
catalogs:
  - from: ducklake
    name: local_catalog
    params:
      connection_string: /path/to/local/metadata.ducklake
```

## Example Queries

Once configured, tables from the DuckLake catalog are accessible using standard SQL:

```sql
-- List all tables in a schema
SHOW TABLES FROM my_catalog.my_schema;

-- Query a table
SELECT * FROM my_catalog.my_schema.my_table LIMIT 10;

-- Join tables across schemas
SELECT a.id, b.name
FROM my_catalog.sales.orders a
JOIN my_catalog.customers.profiles b ON a.customer_id = b.id;
```

## How It Works

1. **Extension Loading**: Spice creates a DuckDB instance and loads the `ducklake` extension
2. **Catalog Attachment**: The DuckLake catalog is attached using `ATTACH 'ducklake:<connection_string>' AS <name>`
3. **Schema Discovery**: Spice queries `information_schema.schemata` to discover available schemas
4. **Table Discovery**: For each schema, Spice queries `information_schema.tables` to discover tables
5. **Table Providers**: Each discovered table is exposed as a DataFusion `TableProvider` for query execution

## Cloud Storage Authentication

DuckLake catalogs stored in cloud object storage require appropriate credentials. DuckDB will use standard credential resolution:

- **AWS S3**: Uses AWS credential chain (environment variables, `~/.aws/credentials`, instance profiles)
- **GCS**: Uses Google Cloud credential chain (environment variables, application default credentials)
- **Azure**: Uses Azure credential chain (environment variables, managed identity)

## Limitations

- **Access Mode Required**: Write and DDL operations require explicit `access` configuration (`read_write` or `read_write_create`)
- **Refresh Frequency**: Schema and table discovery occurs on startup and during manual refresh operations
- **Extension Availability**: Requires the `ducklake` extension to be available for installation in DuckDB

## Troubleshooting

### Extension Installation Fails

If you see errors about the `ducklake` extension not being available:

1. Ensure you have network connectivity for DuckDB to download the extension
2. Check that your DuckDB version supports the `ducklake` extension

### Authentication Errors

For cloud storage authentication issues:

1. Verify your cloud credentials are properly configured
2. Check that the credentials have read access to the DuckLake metadata file and data files
3. For S3, ensure the bucket region is correctly configured

### Empty Catalog

If the catalog appears empty after configuration:

1. Verify the connection string points to a valid DuckLake metadata file (`.ducklake` extension)
2. Check that the catalog contains schemas and tables
3. Review the Spice logs for any errors during catalog discovery

## Related Documentation

- [DuckLake Documentation](https://ducklake.select/)
- [DuckDB DuckLake Extension](https://duckdb.org/docs/extensions/ducklake)
- [Spice Catalogs Overview](https://spiceai.org/docs/components/catalogs)
