# Git Connector Example

This example demonstrates the Git data connector, which allows you to query files from Git repositories as tables in Spice.

## Prerequisites

- Spice CLI installed (`spice` command available)
- Git repositories accessible (HTTPS or SSH)
- For SSH URLs: SSH keys configured and added to your SSH agent

## Running the Example

1. Navigate to this directory:

   ```bash
   cd examples/git-connector
   ```

2. Start Spice:

   ```bash
   spice run
   ```

3. In another terminal, connect to the Spice SQL REPL:

   ```bash
   spice sql
   ```

4. Try some queries from `queries.sql`:

   ```sql
   -- List files
   SELECT name, path, size FROM spiceai_all_files LIMIT 10;

   -- Count files by extension
   SELECT
     SUBSTRING(name FROM '\.[^.]+$') AS extension,
     COUNT(*) AS count
   FROM spiceai_all_files
   GROUP BY extension
   ORDER BY count DESC;
   ```

## Datasets

The example includes several datasets demonstrating different use cases:

### `spiceai_all_files`

All files from the Spice.ai repository (trunk branch).

### `spiceai_rust_files`

Only Rust source files (`.rs` extension), demonstrating file filtering with glob patterns.

### `spiceai_config_files`

Configuration files (YAML, TOML, JSON), showing multiple glob patterns.

### `spiceai_rust_with_content`

Rust files with content fetching enabled, allowing you to search within file contents.

### `spiceai_v1_8_0`

Files from a specific release tag, useful for version comparisons.

### `spiceai_docs`

Documentation files only (Markdown files in docs/ directory).

## Embeddings and Search Example

The `spicepod-embeddings.yaml` file demonstrates automatic content fetching for embeddings and full-text search. When you configure embeddings or full-text search on the `content` column, the Git connector automatically fetches file content without requiring the `fetch_content` parameter.

To try this example:

```bash
spice run --spicepod spicepod-embeddings.yaml
```

This demonstrates:

- **Automatic Content Fetching**: Content is fetched when embeddings are configured
- **Semantic Search**: Use embeddings for semantic search over documentation
- **Full-Text Search**: Use full-text search for keyword-based search

## Key Features Demonstrated

1. **File Filtering**: Using `include` parameter with glob patterns
2. **Content Fetching**: Enabling `fetch_content` to query file contents
3. **Version Tracking**: Querying specific branches, tags, or commits
4. **Version Comparison**: Comparing files between different versions
5. **Git Metadata**: Accessing SHA, tree SHA, commit info, and timestamps

## Example Queries

See `queries.sql` for a comprehensive set of example queries including:

- Basic file listing and statistics
- Version comparison between releases
- Large file detection
- Recent modifications tracking
- Code pattern searching
- Repository structure analysis
- File mode analysis

## Configuration

### URL Formats

- **HTTPS**: `git:https://github.com/owner/repo.git`
- **SSH**: `git:git@github.com:owner/repo.git`

### Specifying References

Append `@<reference>` to query specific branches, tags, or commits:

- Branch: `git:https://github.com/owner/repo.git@main`
- Tag: `git:https://github.com/owner/repo.git@v1.0.0`
- Commit: `git:https://github.com/owner/repo.git@abc123def`

### Parameters

File selection:

- `include`: Glob patterns to filter files (semicolon or comma separated)
- `fetch_content`: Set to `"true"` to fetch file content (also automatically enabled when embeddings or full-text search is configured on the `content` column)
- `cache_path`: Custom location for repository cache
- `enable_lfs`: Set to `"true"` to fetch git-lfs objects after clone/fetch (requires `git-lfs` CLI on `PATH`)

Authentication:

- `git_username`, `git_password`: HTTP(S) basic authentication credentials
- `git_token`: Personal access token for HTTP(S) authentication
- `git_ssh_key`: Absolute path to an SSH private key
- `git_ssh_passphrase`: Passphrase for the SSH private key
- `git_ssh_use_agent`: When `"true"` (default), fall back to the host's `ssh-agent`

Resilience:

- `max_concurrent_requests`: Concurrency budget per repository URL (default `4`)
- `git_max_retries`: Maximum retries on transient errors (default `3`)
- `backoff_method`: `exponential` or `fibonacci` (default `exponential`)
- `disable_on_permanent_error`: When `"true"` (default), a permanent error (e.g. 401) disables the connector

## Performance Tips

1. **First Query**: The initial clone may take time for large repositories
2. **Subsequent Queries**: Fast, as they read from the local cache
3. **Content Fetching**: Only enable when needed, as it increases memory usage
4. **File Filtering**: Use `include` patterns to limit files processed
5. **Refreshes**: Refreshing fetches updates incrementally (not a full re-clone)

## Use Cases

- **Code Analysis**: Analyze source code structure and patterns
- **Version Tracking**: Compare files across different versions
- **Documentation Extraction**: Query and analyze documentation
- **Dependency Auditing**: Track dependencies across repositories
- **License Compliance**: Find and analyze license files
- **Repository Metrics**: Analyze repository structure and statistics

## Troubleshooting

### Repository Not Found

- Verify the URL is correct
- For private repositories, ensure the appropriate credentials (`git_token`, `git_ssh_key`, or an active `ssh-agent`) are configured
- Check network connectivity

### Authentication Failed

- HTTP(S): provide `git_token` or `git_username`/`git_password`
- SSH: provide `git_ssh_key` + optional `git_ssh_passphrase`, or ensure `ssh-agent` has the relevant identity loaded
- The connector disables itself after a permanent error (401/403) to prevent retry storms. Update the configuration and restart Spice after resolving the issue; set `disable_on_permanent_error: "false"` if you need the connector to keep retrying.

### git-lfs Objects Not Materialized

- Install `git-lfs` and ensure it is on `PATH`
- Set `enable_lfs: "true"` in the dataset `params`

### Slow First Query

- Large repositories take time to clone initially
- Consider using `include` to filter files
- Subsequent queries will be fast

### Content Not Available

- Ensure `fetch_content: "true"` is set
- Only UTF-8 encoded files can have content fetched
- Binary files will have `NULL` content

## Learn More

- [Git Connector Documentation](../../docs/features/git-connector.md)
- [Spice Documentation](https://docs.spice.ai)
