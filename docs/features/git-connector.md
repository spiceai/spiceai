# Git Data Connector

The Git data connector enables you to query files from Git repositories (local or remote) as tables in Spice.

## Features

- **Multiple Protocols**: Supports HTTPS, SSH (`ssh://`, `git+ssh://`), local file (`file://`), and `git@host:repo` URLs
- **Authentication**: HTTP(S) basic auth (username + password or personal access token) and SSH auth (explicit private key + passphrase or the running `ssh-agent`)
- **git-lfs**: Optional Large File Storage support by invoking the local `git-lfs` CLI after clone/fetch
- **Version Tracking**: Includes commit SHA, tree SHA, and version information
- **Branch/Tag/Commit Support**: Query any Git reference (branch, tag, or specific commit)
- **File Filtering**: Use glob patterns to filter which files are included
- **Content Fetching**: Optionally fetch file content (controlled by `fetch_content` parameter)
- **File Limits**: Configurable maximum file count and file size for content fetching
- **Timestamp Tracking**: Provides created_at and updated_at timestamps from Git history
- **Automatic Caching**: Clones repositories locally for fast subsequent queries
- **Automatic Updates**: Fetches latest changes from remote on each scan
- **Connection Resilience**: Per-remote concurrency limiting, bounded retries with exponential or fibonacci backoff, and automatic circuit-breaking on permanent errors (e.g. 401/403)
- **Observability**: Exposes an `inflight_operations` gauge via the runtime metrics endpoint

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

#### File selection and materialization

| Parameter        | Type    | Default     | Description                                                                                                                                             |
| ---------------- | ------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `include`        | string  | none        | Glob pattern(s) to filter files. Separate multiple patterns with `;` or `,`                                                                             |
| `fetch_content`  | boolean | `false`     | Whether to fetch file content into the `content` column                                                                                                 |
| `cache_path`     | string  | System temp | Custom path for the local repository cache                                                                                                              |
| `max_files`      | integer | `5000`      | Maximum number of files to include (hard cap: 50,000)                                                                                                   |
| `max_file_bytes` | integer | `524288`    | Maximum file size in bytes (512 KiB default, hard cap: 5 MiB). Files larger than this are excluded from results entirely, regardless of `fetch_content` |
| `enable_lfs`     | boolean | `false`     | Fetch git-lfs objects after clone/fetch. Requires the `git-lfs` CLI to be on `PATH`                                                                     |

#### Authentication

HTTP(S) credentials are stored as secrets and can be supplied directly or referenced via the secrets store (e.g. `${ env:GIT_TOKEN }`).

| Parameter            | Type    | Default | Description                                                                                                       |
| -------------------- | ------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| `git_username`       | string  | none    | Username for HTTP(S) basic authentication                                                                         |
| `git_password`       | secret  | none    | Password or personal access token for HTTP(S) basic authentication                                                |
| `git_token`          | secret  | none    | Personal access token used for HTTP(S) authentication. Sent as the password with username defaulting to `x-access-token`; override the username by also setting `git_username` |
| `git_ssh_key`        | string  | none    | Absolute path to an SSH private key used when authenticating to the remote                                        |
| `git_ssh_passphrase` | secret  | none    | Passphrase for the SSH private key identified by `git_ssh_key`                                                    |
| `git_ssh_use_agent`  | boolean | `true`  | Authenticate via the running `ssh-agent` when no explicit `git_ssh_key` is provided                               |

#### Resilience

| Parameter                    | Type    | Default       | Description                                                                                                      |
| ---------------------------- | ------- | ------------- | ---------------------------------------------------------------------------------------------------------------- |
| `max_concurrent_requests`    | integer | `4`           | Maximum number of concurrent Git network operations (clone/fetch) across datasets sharing the same remote URL    |
| `git_max_retries`            | integer | `3`           | Maximum retry count for transient errors (network hiccups, temporary 5xx)                                        |
| `backoff_method`             | string  | `exponential` | Backoff strategy for retries. One of `exponential` or `fibonacci`                                                |
| `disable_on_permanent_error` | boolean | `true`        | When true, a permanent error (e.g. 401 Unauthorized, authentication failure) disables the connector             |

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

When using embeddings or full-text search on the `content` column, you must set `fetch_content: 'true'` to ensure file content is available:

```yaml
datasets:
  - from: git:https://github.com/spiceai/spiceai.git@trunk
    name: docs
    description: Documentation files with embeddings for semantic search
    params:
      include: 'docs/**/*.md;README.md'
      fetch_content: 'true'
    columns:
      - name: content
        embeddings:
          - from: openai
            model: text-embedding-3-small
```

## Schema

The Git connector provides the following columns:

| Column       | Type      | Description                                                      |
| ------------ | --------- | ---------------------------------------------------------------- |
| `name`       | String    | File name                                                        |
| `path`       | String    | Full path to the file in the repository                          |
| `size`       | Int64     | File size in bytes                                               |
| `sha`        | String    | Git object SHA of the file (blob SHA)                            |
| `mode`       | String    | File mode (e.g., "100644" for regular file)                      |
| `tree_sha`   | String    | SHA of the tree containing this file                             |
| `commit_sha` | String    | SHA of the commit being queried                                  |
| `version`    | String    | Short version of the commit SHA (first 7 characters)             |
| `created_at` | Timestamp | First commit time for this file (milliseconds since epoch)       |
| `updated_at` | Timestamp | Most recent commit time for this file (milliseconds since epoch) |
| `content`    | String    | File content (only present when `fetch_content: "true"` is set)  |

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
2. **Updates**: On each scan (including query execution), it fetches the latest changes from the remote
3. **File Listing**: Walks the Git tree at the specified reference (branch/tag/commit)
4. **History**: Walks the commit history to determine when files were first created and last modified
5. **Filtering**: Applies glob patterns if specified to include only matching files
6. **File Limits**: Enforces `max_files` and `max_file_bytes` limits to prevent excessive resource usage

## Performance Considerations

- **First Query**: May take time to clone large repositories
- **Subsequent Queries**: Fast, reading from local cache
- **Refresh**: Only fetches updates, not a full re-clone
- **Content Fetching**: Enabling `fetch_content` increases memory usage and query time. Files exceeding `max_file_bytes` are skipped.
- **File Limits**: By default, only the first 5,000 files are included. Adjust `max_files` for larger repositories.
- **Large Repositories**: Consider using `include` patterns to limit the files processed

## Authentication

### HTTPS with a personal access token

```yaml
datasets:
  - from: git:https://github.com/spiceai/internal.git@trunk
    name: internal_files
    params:
      git_token: ${ env:GIT_TOKEN }
```

### SSH with an explicit private key

```yaml
datasets:
  - from: git:git@github.com:spiceai/internal.git@trunk
    name: internal_files
    params:
      git_ssh_key: /home/app/.ssh/id_ed25519
      git_ssh_passphrase: ${ env:GIT_SSH_PASSPHRASE }
```

### SSH via the running agent (default)

If no `git_ssh_key` is provided, the connector authenticates through the host's `ssh-agent`. Disable this fallback with `git_ssh_use_agent: "false"` if you require an explicit key.

## git-lfs

Set `enable_lfs: "true"` to have the connector fetch git-lfs objects after cloning or refreshing. The connector shells out to the `git-lfs` CLI, so it must be installed and on `PATH`. The connector validates availability on first use and returns a descriptive error if `git-lfs` is missing.

```yaml
datasets:
  - from: git:https://github.com/owner/repo.git@trunk
    name: repo_with_lfs
    params:
      enable_lfs: 'true'
      fetch_content: 'true'
```

## Observability

The Git connector exposes `inflight_operations` via the runtime metrics endpoint. The gauge reflects the current number of Git network operations that are holding a concurrency permit (i.e. cloning or fetching from the remote).

## Limitations

- File content is only available for UTF-8 encoded files when `fetch_content` is enabled
- Very large repositories may take time to clone initially
- Commit history walking for timestamps can be slow on repositories with deep history
- `enable_lfs` requires the `git-lfs` CLI to be installed on `PATH`; when absent the connector returns an error instead of silently skipping LFS pointers

## Use Cases

- **Code Analysis**: Query source code structure and metrics
- **Documentation**: Extract and analyze documentation files
- **Configuration Management**: Track configuration file changes
- **License Auditing**: Find and analyze license files across repositories
- **Dependency Tracking**: Query dependency manifests (package.json, Cargo.toml, etc.)
- **Repository Metrics**: Analyze repository structure and file distribution
