# Spice v1.8.2 (Oct 21, 2025)

Spice v1.8.2 is a patch release that...

## What's New in v1.8.2

###

### Additional Improvements & Bugfixes
- **Reliability**:
- **Validation**:
- **Performance**:
- **Bugfix**:

## Contributors
- [@krinart](https://github.com/krinart)
- [@lukekim](https://github.com/lukekim)
- [@kczimm](https://github.com/kczimm)
- [@Jeadie](https://github.com/Jeadie)
- [@phillipleblanc](https://github.com/phillipleblanc)
- [@sgrebnov](https://github.com/sgrebnov)
- [@peasee](https://github.com/peasee)

## Breaking Changes

No breaking changes.

## Cookbook Updates

---

## Upgrading

To upgrade to v1.8.2, use one of the following methods:

**CLI**:

```console
spice upgrade
```

**Homebrew**:

```console
brew upgrade spiceai/spiceai/spice
```

**Docker**:

Pull the `spiceai/spiceai:1.8.2` image:

```console
docker pull spiceai/spiceai:1.8.2
```

For available tags, see [DockerHub](https://hub.docker.com/r/spiceai/spiceai/tags).

**Helm**:

```console
helm repo update
helm upgrade spiceai spiceai/spiceai
```

**AWS Marketplace**:

🎉 Spice is now available in the [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-jmf6jskjvnq7i)!

## What's Changed

### Changelog

- Update mongo config for benchmarks by [@krinart](https://github.com/krinart) in [#7546](https://github.com/spiceai/spiceai/pull/7546)
- Configurable DuckDB duckdb_index_scan_percentage  & duckdb_index_scan_max_count by [@lukekim](https://github.com/lukekim) in [#7551](https://github.com/spiceai/spiceai/pull/7551)
- Fix race condition in S3 Vectors index and bucket creation by [@kczimm](https://github.com/kczimm) in [#7577](https://github.com/spiceai/spiceai/pull/7577)
- Check if index/bucket exists after ConflictException by [@kczimm](https://github.com/kczimm) in [#7577](https://github.com/spiceai/spiceai/pull/7577)
- Use 'location' as primary key for document tables by [@Jeadie](https://github.com/Jeadie) in [#7567](https://github.com/spiceai/spiceai/pull/7567)
- Update official Docker builds to use release binaries by [@phillipleblanc](https://github.com/phillipleblanc) in [#7597](https://github.com/spiceai/spiceai/pull/7597)
- Hive-style partitioning for DuckDB file mode by [@kczimm](https://github.com/kczimm) in [#7563](https://github.com/spiceai/spiceai/pull/7563)
- New Generate Changelog workflow by [@krinart](https://github.com/krinart) in [#7562](https://github.com/spiceai/spiceai/pull/7562)
- Add support for DuckDB table-based partitioning by [@sgrebnov](https://github.com/sgrebnov) in [#7581](https://github.com/spiceai/spiceai/pull/7581)
- DuckDB table partitioning: delete partitions that no longer exist after full refresh by [@sgrebnov](https://github.com/sgrebnov) in [#7614](https://github.com/spiceai/spiceai/pull/7614)
- Rename `duckdb_partition_mode` to `partition_mode` param by [@sgrebnov](https://github.com/sgrebnov) in [#7622](https://github.com/spiceai/spiceai/pull/7622)
- Fix license issue in table-providers  by [@phillipleblanc](https://github.com/phillipleblanc) in [#7620](https://github.com/spiceai/spiceai/pull/7620)
- Make DuckDB table partition data write threshold configurable by [@sgrebnov](https://github.com/sgrebnov) in [#7626](https://github.com/spiceai/spiceai/pull/7626)
- fix: Don't nullify DuckDB release callbacks for schemas by [@peasee](https://github.com/peasee) in [#7628](https://github.com/spiceai/spiceai/pull/7628)
- Fix integration tests by reverting the use of batch inserts w/ prepared statements by [@phillipleblanc](https://github.com/phillipleblanc) in [#7630](https://github.com/spiceai/spiceai/pull/7630)
- Return TableProvider from CandidateGeneration::search by [@Jeadie](https://github.com/Jeadie) in [#7559](https://github.com/spiceai/spiceai/pull/7559)
- Handle table relations in HTTP v1/search by [@Jeadie](https://github.com/Jeadie) in [#7615](https://github.com/spiceai/spiceai/pull/7615)
