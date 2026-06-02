# Git Connector Example Queries

## Basic File Listing

-- List all files with basic info
SELECT name, path, size, sha 
FROM spiceai_all_files 
LIMIT 10;

-- Count files by extension
SELECT 
  SUBSTRING(name FROM '\.[^.]+$') AS extension,
  COUNT(*) AS count,
  SUM(size) AS total_size
FROM spiceai_all_files
GROUP BY extension
ORDER BY count DESC;

## Version and Git Information

-- Show file version information
SELECT path, version, commit_sha, tree_sha
FROM spiceai_trunk
LIMIT 10;

-- Compare files between versions
SELECT 
  t.path,
  t.sha AS trunk_sha,
  t.size AS trunk_size,
  v.sha AS v1_8_sha,
  v.size AS v1_8_size,
  t.updated_at AS trunk_updated,
  v.updated_at AS v1_8_updated
FROM spiceai_trunk t
LEFT JOIN spiceai_v1_8_0 v ON t.path = v.path
WHERE t.sha != v.sha OR v.sha IS NULL
LIMIT 20;

## File Analysis

-- Find large files
SELECT path, size, sha
FROM spiceai_all_files
WHERE size > 100000
ORDER BY size DESC
LIMIT 20;

-- Recently modified files
SELECT 
  path, 
  updated_at,
  version,
  size
FROM spiceai_trunk
WHERE updated_at > NOW() - INTERVAL '30 days'
ORDER BY updated_at DESC;

-- Files created in a time range
SELECT 
  path,
  created_at,
  updated_at,
  EXTRACT(EPOCH FROM (updated_at - created_at)) / 86400 AS days_between
FROM spiceai_trunk
WHERE created_at > NOW() - INTERVAL '90 days'
ORDER BY created_at DESC;

## Rust-Specific Queries

-- Find all Rust modules
SELECT path, size, sha
FROM spiceai_rust_files
WHERE name = 'mod.rs'
ORDER BY path;

-- Count Rust files by directory
SELECT 
  SUBSTRING(path FROM '^([^/]+)/') AS top_level_dir,
  COUNT(*) AS rust_file_count,
  SUM(size) AS total_rust_size
FROM spiceai_rust_files
GROUP BY top_level_dir
ORDER BY rust_file_count DESC;

-- Search for patterns in Rust code (requires fetch_content: true)
SELECT 
  path,
  LENGTH(content) AS content_length
FROM spiceai_rust_with_content
WHERE content LIKE '%async fn%' 
  AND content LIKE '%tokio%'
LIMIT 10;

-- Find files with specific imports
SELECT path
FROM spiceai_rust_with_content
WHERE content LIKE '%use datafusion::%'
ORDER BY path;

## Configuration Files

-- List all config files
SELECT path, name, size
FROM spiceai_config_files
ORDER BY path;

-- Find Cargo.toml files
SELECT path, size, sha
FROM spiceai_config_files
WHERE name = 'Cargo.toml'
ORDER BY path;

-- Find all YAML files
SELECT path, size
FROM spiceai_config_files
WHERE path LIKE '%.yaml' OR path LIKE '%.yml'
ORDER BY size DESC;

## Documentation

-- List all documentation files
SELECT path, size, updated_at
FROM spiceai_docs
ORDER BY updated_at DESC;

-- Find documentation by size
SELECT 
  path,
  size,
  ROUND(size / 1024.0, 2) AS size_kb
FROM spiceai_docs
WHERE size > 5000
ORDER BY size DESC;

## Repository Structure Analysis

-- Top-level directory structure
SELECT 
  SUBSTRING(path FROM '^([^/]+)') AS directory,
  COUNT(*) AS file_count,
  SUM(size) AS total_size,
  AVG(size) AS avg_size
FROM spiceai_all_files
GROUP BY directory
ORDER BY file_count DESC;

-- File count by depth
SELECT 
  LENGTH(path) - LENGTH(REPLACE(path, '/', '')) AS depth,
  COUNT(*) AS file_count
FROM spiceai_all_files
GROUP BY depth
ORDER BY depth;

-- Largest directories
SELECT 
  SUBSTRING(path FROM '^([^/]+/[^/]+)') AS directory,
  COUNT(*) AS file_count,
  SUM(size) AS total_size
FROM spiceai_all_files
GROUP BY directory
ORDER BY total_size DESC
LIMIT 10;

## File Mode Analysis

-- Count files by mode
SELECT 
  mode,
  COUNT(*) AS count,
  CASE 
    WHEN mode = '100644' THEN 'Regular file'
    WHEN mode = '100755' THEN 'Executable'
    WHEN mode = '120000' THEN 'Symbolic link'
    ELSE 'Other'
  END AS mode_description
FROM spiceai_all_files
GROUP BY mode
ORDER BY count DESC;

## Version Comparison

-- Files that exist in trunk but not in v1.8.0
SELECT t.path, t.size, t.created_at
FROM spiceai_trunk t
LEFT JOIN spiceai_v1_8_0 v ON t.path = v.path
WHERE v.path IS NULL
ORDER BY t.created_at DESC
LIMIT 20;

-- Files that were removed between versions
SELECT v.path, v.size, v.updated_at
FROM spiceai_v1_8_0 v
LEFT JOIN spiceai_trunk t ON v.path = t.path
WHERE t.path IS NULL
ORDER BY v.path
LIMIT 20;

-- Files with size changes
SELECT 
  t.path,
  v.size AS old_size,
  t.size AS new_size,
  t.size - v.size AS size_diff,
  ROUND(((t.size - v.size)::FLOAT / v.size * 100), 2) AS percent_change
FROM spiceai_trunk t
JOIN spiceai_v1_8_0 v ON t.path = v.path
WHERE t.size != v.size
ORDER BY ABS(t.size - v.size) DESC
LIMIT 20;

## Advanced Analysis

-- Files with no recent updates (stable files)
SELECT path, size, updated_at
FROM spiceai_trunk
WHERE updated_at < NOW() - INTERVAL '180 days'
ORDER BY updated_at
LIMIT 20;

-- Most frequently updated files
-- Note: This requires multiple commits to be meaningful
SELECT path, updated_at, created_at
FROM spiceai_trunk
WHERE updated_at != created_at
ORDER BY updated_at DESC
LIMIT 20;

-- File statistics
SELECT 
  COUNT(*) AS total_files,
  SUM(size) AS total_size,
  AVG(size) AS avg_size,
  MIN(size) AS min_size,
  MAX(size) AS max_size,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY size) AS median_size
FROM spiceai_all_files;
