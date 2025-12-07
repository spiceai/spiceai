# Git Data Connector

The Git data connector enables you to query files from Git repositories (local or remote) as tables in Spice.

## Features

- **Multiple Protocols**: Supports HTTPS and SSH Git URLs
- **Version Tracking**: Includes commit SHA, tree SHA, and version information
- **Branch/Tag/Commit Support**: Query any Git reference (branch, tag, or specific commit)
- **File Filtering**: Use glob patterns to filter which files are included
- **Content Fetching**: Optionally fetch file content
- **Timestamp Tracking**: Provides created_at and updated_at timestamps from Git history
- **Automatic Caching**: Clones repositories locally for fast subsequent queries
- **Refresh Support**: Updates repository on each refresh

## Configuration

### Basic Usage

```yaml
datasets:
  - from: git:https://github.com/spiceai/spiceai.git
    name: spiceai_files
    description: Files from the Spice.ai repository
```

### SSH URLs

```yaml
datasets:
  - from: git:git@github.com:spiceai/spiceai.git
    name: spiceai_files
```

### Specific Branch/Tag/Commit

Append `@<reference>` to specify a branch, tag, or commit:

```yaml
datasets:
  # Specific branch
  - from: git:https://github.com/spiceai/spiceai.git@trunk
    name: trunk_files

  # Specific tag
  - from: git:https://github.com/spiceai/spiceai.git@v1.0.0
    name: v1_files

  # Specific commit (short or full SHA)
  - from: git:https://github.com/spiceai/spiceai.git@abc123
    name: commit_files
```

### Parameters

| Parameter       | Type    | Default     | Description                                                                                                                                                                 |
| --------------- | ------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `include`       | string  | none        | Glob pattern(s) to filter files. Separate multiple patterns with `;` or `,`                                                                                                 |
| `fetch_content` | boolean | `false`     | Whether to fetch file content into the `content` column. **Note**: Content is automatically fetched if embeddings or full-text search is configured on the `content` column |
| `cache_path`    | string  | System temp | Custom path for the local repository cache                                                                                                                                  |

### Example with Parameters

```yaml
datasets:
  - from: git:https://github.com/spiceai/spiceai.git@trunk
    name: rust_files
    description: Only Rust source files from the repository
    params:
      include: '**/*.rs'
      fetch_content: 'true'
```

### Multiple File Patterns

```yaml
datasets:
  - from: git:https://github.com/spiceai/spiceai.git
    name: config_files
    params:
      include: '**/*.yaml;**/*.toml;**/*.json'
```

### Embeddings and Full-Text Search

The Git connector automatically detects when embeddings or full-text search is configured on the `content` column and will automatically fetch file content, even if `fetch_content` is not explicitly set:

```yaml
datasets:
  - from: git:https://github.com/spiceai/spiceai.git@trunk
    name: docs
    description: Documentation files with embeddings for semantic search
    params:
      include: 'docs/**/*.md;README.md'
    columns:
      - name: content
        embeddings:
          - from: openai
            model: text-embedding-3-small
```

In this example, the `content` column will be automatically fetched because embeddings are configured, without needing to set `fetch_content: 'true'`.

## Schema

The Git connector provides the following columns:

| Column       | Type      | Description                                                                                                   |
| ------------ | --------- | ------------------------------------------------------------------------------------------------------------- |
| `path`       | String    | Full path to the file in the repository                                                                       |
| `name`       | String    | File name                                                                                                     |
| `size`       | Int64     | File size in bytes                                                                                            |
| `sha`        | String    | Git object SHA of the file (blob SHA)                                                                         |
| `mode`       | String    | File mode (e.g., "100644" for regular file)                                                                   |
| `tree_sha`   | String    | SHA of the tree containing this file                                                                          |
| `commit_sha` | String    | SHA of the commit being queried                                                                               |
| `version`    | String    | Short version of the commit SHA (first 7 characters)                                                          |
| `created_at` | Timestamp | First commit time for this file (milliseconds since epoch)                                                    |
| `updated_at` | Timestamp | Most recent commit time for this file (milliseconds since epoch)                                              |
| `content`    | String    | File content (only if `fetch_content: "true"` or if embeddings/full-text search is configured on this column) |

## Example Queries

### List all files

```sql
SELECT name, path, size, sha FROM spiceai_files;
```

### Find recently modified files

```sql
SELECT path, updated_at, version
FROM spiceai_files
WHERE updated_at > NOW() - INTERVAL '7 days'
ORDER BY updated_at DESC;
```

### Search file content (if enabled)

```sql
SELECT path, content
FROM rust_files
WHERE content LIKE '%async%'
  AND content LIKE '%tokio%';
```

### Files by size

```sql
SELECT path, size, sha
FROM spiceai_files
WHERE size > 100000
ORDER BY size DESC
LIMIT 10;
```

### Track file changes across commits

Query different commits to see how files have changed:

```yaml
datasets:
  - from: git:https://github.com/spiceai/spiceai.git@trunk
    name: current_files

  - from: git:https://github.com/spiceai/spiceai.git@v1.0.0
    name: v1_files
```

```sql
SELECT
  c.path,
  c.sha AS current_sha,
  c.size AS current_size,
  v.sha AS v1_sha,
  v.size AS v1_size
FROM current_files c
LEFT JOIN v1_files v ON c.path = v.path
WHERE c.sha != v.sha;
```

## How It Works

1. **Initial Clone**: On first access, the connector clones the repository to a local cache directory
2. **Updates**: On refresh, it fetches the latest changes from the remote
3. **File Listing**: Walks the Git tree at the specified reference (branch/tag/commit)
4. **History**: Walks the commit history to determine when files were first created and last modified
5. **Filtering**: Applies glob patterns if specified to include only matching files

## Performance Considerations

- **First Query**: May take time to clone large repositories
- **Subsequent Queries**: Fast, reading from local cache
- **Refresh**: Only fetches updates, not a full re-clone
- **Content Fetching**: Enabling `fetch_content` increases memory usage and query time
- **Large Repositories**: Consider using `include` patterns to limit the files processed

## Limitations

- File content is only available for UTF-8 encoded files when `fetch_content` is enabled
- Very large repositories may take time to clone initially
- Commit history walking for timestamps can be slow on repositories with deep history

## Use Cases

- **Code Analysis**: Query source code structure and metrics
- **Documentation**: Extract and analyze documentation files
- **Configuration Management**: Track configuration file changes
- **License Auditing**: Find and analyze license files across repositories
- **Dependency Tracking**: Query dependency manifests (package.json, Cargo.toml, etc.)
- **Repository Metrics**: Analyze repository structure and file distribution
