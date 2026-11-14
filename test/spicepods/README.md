# Test Spicepods

## Naming

Test spicepod names should be formatted according to the following template:

```console
{connector[variant]}-{accelerator[variant]}-{test variant}
```

`[variant]` refers to the connector or accelerator specific information about the connector setup. For example:

* `s3[parquet]` - a S3 connector using Parquet data
* `file[csv]` - a File connector using CSV data
* `odbc[athena]` - an ODBC connector using AWS Athena
* `spark[databricks]` - a Spark connector using databricks
* `cayenne[file]` - a Cayenne accelerator using file-mode acceleration.

Variants can be nested, up to 2 levels. For example, `odbc[s3[parquet]]` is an ODBC connector using an S3 source with Parquet data.

`{test variant}` refers to specific test information about the case under test. For example, `on_zero_results` is a test validating the behavior of the accelerator on-zero-results action.

Do not include scale factor formation in the `{test variant}`. This information is supplied as a query metric dimension/attribute.

When a connector does not use acceleration, the `{accelerator[variant]}` value **must** be `federated`.

Examples of full spicepod names:

* `s3[parquet]-federated` - a non-accelerator S3 connector using Parquet.
* `duckdb-federated` - a DuckDB connector using no acceleration.
* `spicecloud-arrow` - a Spicecloud connector using Arrow acceleration.
* `mysql-cayenne[file]-on_zero_results` - a MySQL connector using file-mode Cayenne acceleration, testing the behavior of on-zero-results action.
* `file[parquet]-cayenne[file]-on_zero_results` - a File connector using Parquet data with Cayenne file-mode acceleration, testing the behavior of on-zero-results action.
* `file[parquet]-cayenne[memory]-on_zero_results` - a File connector using Parquet data with Cayenne memory-mode acceleration, testing the behavior of on-zero-results action.
* `s3[parquet]-cayenne[memory]-on_zero_results` - an S3 connector using Parquet data with Cayenne memory-mode acceleration, testing on-zero-results action.
* `s3[parquet]-cayenne[file]-on_zero_results` - an S3 connector using Parquet data with Cayenne file-mode acceleration, testing on-zero-results action.
