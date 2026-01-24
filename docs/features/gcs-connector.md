# Google Cloud Storage (GCS) Data Connector

The GCS data connector enables querying files stored in Google Cloud Storage buckets as tables in Spice.

## Features

- **Multiple File Formats**: Supports Parquet, CSV, JSON, and other formats
- **Multiple Authentication Methods**: Service account keys, Application Default Credentials (ADC), or anonymous access for public buckets
- **URL Schemes**: Supports both `gcs://` and `gs://` URL schemes
- **Retry Configuration**: Configurable retry behavior with exponential backoff
- **Listing Table Support**: Automatic discovery of files matching patterns

## Configuration

### Basic Usage

```yaml
datasets:
  - from: gcs://my-bucket/path/to/data/
    name: my_data
    params:
      file_format: parquet
```

### Using gs:// URL Scheme

```yaml
datasets:
  - from: gs://my-bucket/path/to/data/
    name: my_data
    params:
      file_format: parquet
```

## Authentication

GCS supports multiple authentication methods. Only one authentication method should be specified at a time.

### Service Account Key File

Provide a path to a service account JSON key file:

```yaml
datasets:
  - from: gcs://my-bucket/data/
    name: my_data
    params:
      gcs_service_account_path: /path/to/service-account.json
      file_format: parquet
```

### Service Account Key (Inline)

Provide the service account JSON key directly as a string:

```yaml
datasets:
  - from: gcs://my-bucket/data/
    name: my_data
    params:
      gcs_service_account_key: '{"type": "service_account", "project_id": "...", ...}'
      file_format: parquet
```

### Application Default Credentials (ADC)

Use Google Application Default Credentials. If the `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set, it will use the key file at that path:

```yaml
datasets:
  - from: gcs://my-bucket/data/
    name: my_data
    params:
      gcs_application_default_credentials: 'true'
      file_format: parquet
```

### Public Buckets (Skip Signature)

For public buckets that don't require authentication:

```yaml
datasets:
  - from: gcs://public-bucket/data/
    name: public_data
    params:
      gcs_skip_signature: 'true'
      file_format: parquet
```

## Parameters

| Parameter                             | Type    | Default | Description                                                          |
| ------------------------------------- | ------- | ------- | -------------------------------------------------------------------- |
| `gcs_service_account_path`            | string  | none    | Path to a GCS service account JSON key file                          |
| `gcs_service_account_key`             | string  | none    | GCS service account JSON key as a string                             |
| `gcs_application_default_credentials` | boolean | `false` | Use Google ADC. Uses `GOOGLE_APPLICATION_CREDENTIALS` env var if set |
| `gcs_skip_signature`                  | boolean | `false` | Skip signing requests. Used for public buckets                       |
| `allow_http`                          | boolean | `false` | Allow insecure HTTP connections                                      |
| `gcs_max_retries`                     | integer | `3`     | Maximum number of retries for failed requests                        |
| `gcs_retry_timeout`                   | string  | none    | Retry timeout duration                                               |
| `gcs_backoff_initial_duration`        | string  | none    | Initial backoff duration                                             |
| `gcs_backoff_max_duration`            | string  | none    | Maximum backoff duration                                             |
| `gcs_backoff_base`                    | float   | none    | Base of the exponential backoff                                      |
| `client_timeout`                      | string  | none    | Timeout for GCS client operations                                    |
| `file_format`                         | string  | none    | File format: `parquet`, `csv`, `json`, `ndjson`                      |
| `hive_partitioning_enabled`           | boolean | `false` | Enable Hive-style partitioning                                       |

## File Format Parameters

### Parquet

```yaml
datasets:
  - from: gcs://my-bucket/parquet-data/
    name: parquet_data
    params:
      file_format: parquet
```

### CSV

```yaml
datasets:
  - from: gcs://my-bucket/csv-data/
    name: csv_data
    params:
      file_format: csv
      csv_has_header: 'true'
      csv_delimiter: ','
```

### JSON / NDJSON

```yaml
datasets:
  - from: gcs://my-bucket/json-data/
    name: json_data
    params:
      file_format: ndjson
```

## Hive Partitioning

GCS connector supports Hive-style partitioning for efficient querying of partitioned datasets:

```yaml
datasets:
  - from: gcs://my-bucket/partitioned-data/
    name: partitioned_data
    params:
      file_format: parquet
      hive_partitioning_enabled: 'true'
```

For a bucket with structure like:

```text
gs://my-bucket/partitioned-data/year=2024/month=01/data.parquet
gs://my-bucket/partitioned-data/year=2024/month=02/data.parquet
```

The `year` and `month` columns will be automatically extracted from the path.

## With Data Acceleration

Accelerate GCS data for faster queries:

```yaml
datasets:
  - from: gcs://my-bucket/data/
    name: accelerated_data
    params:
      file_format: parquet
      gcs_service_account_path: /path/to/service-account.json
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_interval: 1h
```

## Example Queries

```sql
-- Query all data
SELECT * FROM my_data LIMIT 10;

-- Query with filters (pushed down to GCS when possible)
SELECT * FROM my_data WHERE date >= '2024-01-01';

-- Aggregate queries
SELECT date, COUNT(*) as count, SUM(amount) as total
FROM my_data
GROUP BY date;
```

## Troubleshooting

### Authentication Errors

If you see authentication errors:

1. Verify only one authentication method is specified
2. Check that the service account has the required GCS permissions (Storage Object Viewer at minimum)
3. For ADC, ensure `GOOGLE_APPLICATION_CREDENTIALS` is set correctly or you're running in a GCP environment

### File Not Found

1. Verify the bucket and path exist
2. Check the service account has access to the bucket
3. Ensure the `file_format` parameter matches the actual file format

### Timeout Errors

For large datasets or slow connections, increase the client timeout:

```yaml
params:
  client_timeout: 60s
```
