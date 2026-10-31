# spicescript

`spicescript` runs scenario scripts against a prebuilt `spiced`. A script is a
YAML file that:
1. starts the sources the scenario needs (PostgreSQL, MySQL, ClickHouse, an
   object store, any Docker image, a mock HTTP server);
2. starts `spiced` on a spicepod;
3. changes things while it runs: source writes, restarts, kills, upgrades,
   refreshes, outages;
4. checks every answer against an oracle that shares no code with Spice.

The scripts live in `test/spicescript/`; see its README for their layout.

## Approach

- **End to end.** Every script drives the shipped `spiced` binary the way a user
  does: SQL over Flight and HTTP, the `spice` CLI, signals, spicepod edits. So
  it exercises what only exists in the binary — the linked crates and
  features, wrappers in their real order, spicepod parsing and defaults,
  startup, readiness and shutdown.
- **Differential.** Answers are checked against an independent oracle:
  - the source itself (the same SQL sent straight to PostgreSQL, MySQL,
    ClickHouse or DuckDB);
  - another engine reading the same rows;
  - a literal answer;
  - Spice's own earlier answer.

  Every variant and every binary in a run must also agree with the others.
- **No rebuild to change a test.** Scripts are data. Changing a scenario
  means editing YAML; the runner and `spiced` are built once.
- **Exact and non-vacuous.** Rows are compared as a multiset (in order under
  `ORDER BY … LIMIT`). Two empty answers fail unless the script says empty is
  the point. Scripts check that the path under test ran (a plan fragment, a
  metric, a log line), not just that nothing failed.
- **Deterministic.** Generated data is seeded, waits poll a condition with a
  bound (`eventually`, `wait-metric`) rather than sleeping, and every instance
  gets its own database so instances never see each other's writes.

## Running

```bash
cargo build -p spicescript
# Every script in a directory, against the installed spiced (~/.spice/bin/spiced):
target/debug/spicescript test/spicescript
# The whole suite, eight spiced processes at a time:
target/debug/spicescript --jobs 8 --spiced trunk=target/release/spiced test/spicescript
# One script, against two builds; they must agree:
target/debug/spicescript --jobs 4 \
  --spiced release=$HOME/.spice/bin/spiced --spiced dev=target/release/spiced \
  test/spicescript/postgres-cdc-parity.yaml
# Start on a release and upgrade in place to the build under test at each `upgrade` step:
target/debug/spicescript --from $HOME/.spice/bin/spiced \
  --spiced dev=target/release/spiced test/spicescript/postgres-upgrade.yaml
# Every instance as a scheduler with one executor, over node mTLS:
target/debug/spicescript --cluster test/spicescript/query-coverage.yaml
# Known-failing scripts of one group, without waiting out every limit:
target/debug/spicescript --max-wait 20s test/spicescript/known-failing/clickhouse
# One matrix combination and one variant of a script:
target/debug/spicescript --matrix src=clickhouse --variant cayenne test/spicescript/full-refresh-chaos.yaml
# Parse only:
target/debug/spicescript --check test/spicescript
```

Docker must be running. The `aws` CLI is needed for object-store sources, and
`duckdb` for Iceberg sources and `dbgen`. The DuckLake source fetches the
DuckDB CLI matching Spice's embedded DuckDB into `~/.cache/spicescript/`
(override with `SPICESCRIPT_DUCKLAKE_DUCKDB`).

| Option | Meaning |
| --- | --- |
| `<SCRIPTS>…` | Script files, or directories of `*.yaml` scripts (not their subdirectories, except that `known-failing/` includes its group directories). |
| `--spiced [NAME=]PATH` | A `spiced` to test; repeat it to run every script on several binaries, which must give identical answers. Default: `~/.spice/bin/spiced`. |
| `--from PATH` | Start every instance on this binary; each `upgrade` step moves it to the binary under test, over the same data. |
| `--cluster` | Run every instance as a scheduler plus one executor over node mTLS. Scripts that can't run distributed (memory-mode Cayenne, DuckDB or SQLite accelerations) are skipped. |
| `--jobs N` | Run up to N `spiced` processes at once (default 1). Every instance of every matrix combination is a job of its own, taking a slot per process, executors included, and freeing it when it finishes. A combination that stops and starts a source its instances share, or whose variants work together (`run-variants: side-by-side`), runs its instances a step at a time as one job; a script with `defaults.exclusive` runs alone. Scripts ask for slots longest first, from recorded durations. Each script's output is printed when it finishes, with progress lines and the slowest scripts at the end. |
| `--partition I/N` | Run only share I of N (1-based), for splitting a suite across CI jobs. Scripts go longest first, by recorded duration, to the share with the least work so far, so every job computes the same split and the shares take about as long. |
| `--record-durations` | Write each script's run time to `recordings/durations.json` beside it. A script without a recorded duration is refused unless this is set, so a new script gets one when it's added. Record from a `--jobs 1` run. |
| `--max-wait DURATION` | Cap every wait (`eventually`, readiness, refreshes) at this length. For known-failing scripts, which otherwise wait out every limit. |
| `--matrix KEY=VALUE` | Choose which **runs** happen: only the matrix combinations with this value of a key under `matrix:` (as `${matrix.KEY}` prints it), such as `src=clickhouse`; repeat a key for several values. Scripts without the key or any of the values are skipped. Expected failures of the combinations left out are neither required to fail nor dropped by `--record-failures`. |
| `--variant NAME` | Choose which **instances** start in each run: only this name under `variants:`, such as `cayenne`; repeat it for several. Scripts with none of them are skipped. Expected failures of the variants left out, and of comparisons between variants (the run compares a different set), are neither required to fail nor dropped by `--record-failures`. Neither option combines with `--record-durations`. |
| `--work-dir DIR` | Where instances keep their spicepods, data and `spiced.log` (default `target/spicescript`). |
| `--keep` | Leave source containers running after the run, for debugging. |
| `--spice-cli PATH` | The `spice` CLI that `spice` steps run. |
| `--record-failures` | Rewrite each run script's entries in its directory's `recordings/expected-failures.json` with the failures this run saw (see [Expected failures](#expected-failures)). |
| `--check` | Parse every script, including its corpora, and stop. Also fails on an `avoid:` whose defect no longer fails anywhere. |

**Logs and cleanup.** Each failure prints the step, what was expected and what
was seen, and the instance's `spiced.log` path. A run removes the containers,
networks and `spiced` processes it started, including after SIGINT or SIGTERM
and after a failed or interrupted start. A runner killed with SIGKILL can't
clean up, so the next run reaps what dead runners left: `spiced` processes
recorded in `spiced.pid`, and containers and networks named after the dead
runner's pid.

## Scripts

```yaml
name: orders-cdc
description: >
  What the script shows, in a sentence or two.
sources:
  src: { kind: postgres }            # started in Docker; version from test/source_versions.json
matrix:                              # every combination is a separate run: ${matrix.mode}
  mode: [file, memory]
variants:                            # instances that must give identical answers
  federated: { datasets: { '*': { acceleration: null } } }
  cayenne: {}
  duckdb: duckdb-file                # a shared variant from variants.yaml: DuckDB, file mode
spicepod:                            # the spicepod, as YAML
  version: v1
  kind: Spicepod
  name: orders-cdc
  datasets:
    - from: postgres:public.orders
      name: orders
      params: ${sources.src.params}  # a whole-value placeholder takes the mapping
      acceleration: { enabled: true, engine: cayenne, mode: '${matrix.mode}', refresh_mode: changes }
defaults:
  expect: { oracle: src }            # every query is checked against the source
steps:
  - source-exec: { src: "CREATE TABLE orders (...); INSERT INTO orders ..." }
  - start: {}
  - query: SELECT * FROM orders
    eventually: 60s
```

### Sources

| Kind | What it is |
| --- | --- |
| `postgres`, `mysql`, `clickhouse` | A database in Docker. Each instance gets its own database inside it. `${sources.<name>.params}` holds the connector parameters. |
| `kafka` | Redpanda (`v26.1.6`), with a topic prefix per instance. Tables are `<namespace>_<table>`, created with three partitions on first write. Connector params include bootstrap servers and PLAINTEXT security. |
| `mongodb` | MongoDB (`8.0`) single-node replica set, with a database per instance. Connector params include its connection string. |
| `dynamodb` | LocalStack (`4.9`), with a table-name prefix per instance. Tables are `<namespace>_<table>`, created on first write with a numeric or string HASH key and `NEW_AND_OLD_IMAGES` streams. Connector params include endpoint, region and test credentials. |
| `rustfs` | An S3-compatible store with conditional writes, with a bucket per instance, managed with the `aws` CLI. `${sources.<name>.params}` holds the `s3_*` parameters. `.bucket`, `.endpoint`, `.key` and `.secret` serve other uses, such as a Cayenne data tier or a state location. `.shared_bucket` is one bucket every instance shares, for two variants that act as replicas over one state location. |
| `files` | A directory per instance (`${sources.<name>.path}`) holding generated Parquet or CSV part files. |
| `iceberg` | An Iceberg REST catalog over rustfs. `source-exec` and oracle queries run through the `duckdb` CLI attached to the catalog, so a script writes Iceberg with plain SQL and checks Spice against DuckDB reading the same tables. |
| `ducklake` | A DuckLake catalog written with the DuckDB CLI matching Spice's embedded DuckDB. |
| `docker` | Any image: `image`, `ports`, `env` (`${port.N}` is the host port of container port N), `command`, and `ready` (a shell command that succeeds once the container is ready). `source-exec` runs a shell command inside it and `source-query` returns the lines it prints. `host-network: true` lets the container reach `spiced` and the other sources on the host: on Linux it shares the host's network, elsewhere Docker routes to the host, and `${sources.<name>.host_from_container}` is the host's address from inside the container either way. Useful for services such as Debezium Server. |
| `mock-http` | An HTTP server whose routes the script sets with `source-exec`: `path`, and optionally `method`, `query`, `headers-match`, `status`, `headers`, `body`, `delay-ms` and `times`; `reset` removes them all. `source-query` reads what it received: `count <path>`, `log <path>`, `header <path> <name>`. |

Files and object stores answer no queries themselves, so a script mirrors their
rows into a database to have an oracle (`generate: … mirror: <source>`).

### Instances, variants and the matrix

A script has two axes, and they mean different things:

| | `matrix:` | `variants:` |
| --- | --- | --- |
| What it varies | the setup: a source, a format, a mode | the spicepod: an engine, a refresh mode, a setting |
| What one value gets | a separate **run**, with its own sources and containers | an **instance** (`spiced`) within each run, sharing that run's sources |
| Compared? | no: each run is checked on its own | yes: every variant of a run must give the same answers |
| Placeholder / step filter | `${matrix.<key>}`, `when:` | `${variant.name}`, `only:` / `except:` |
| Ledger `run` / `who` | `name[key=value,…]` | the variant's name |
| Select from the command line | `--matrix KEY=VALUE` | `--variant NAME` |

Use the matrix when the sources themselves differ (PostgreSQL vs MySQL); use
variants when the same sources are read in different ways (Cayenne vs Arrow),
so their answers can be compared with each other.

A run is every `--spiced` binary crossed with every variant, separately for
each matrix combination. Each instance has its own ports, working directory,
`SPICE_INSTANCE_ID`, and its own database inside each source. Writes must
therefore be deterministic (no `now()`, no `random()`): variants are compared
with each other, and each ran the writes separately.

**Variants** are [JSON merge patches](https://www.rfc-editor.org/rfc/rfc7386).
`spicepod:` patches the whole spicepod, and `datasets:` patches datasets by name
(`*` for all), since a merge patch replaces a list wholesale. A variant may also
set `env:`, and `when: { <matrix key>: <value> }` to exist only in some
combinations. Variants run in alphabetical order, and the first is the one the
others are compared with.

A variant may name a shared variant instead of writing the patch:
`cayenne: cayenne-file`. Shared variants are defined, each as its full patch, in
the `variants.yaml` in the script's directory or the nearest directory above it
([`test/spicescript/variants.yaml`](../../test/spicescript/variants.yaml) for the
suite): `federated` (the spicepod as written), and an engine and mode that set
`acceleration: { enabled: true, engine, mode }` on every dataset, such as
`cayenne-file`, `duckdb-memory` or `arrow`. A name the file does not define
fails `--check`. A variant used by one script only is written in that script.

`$merge: '${…}'` spreads a mapping placeholder into a mapping, with the
mapping's own keys winning: `params: { $merge: '${sources.lake.params}',
file_format: parquet }`.

**Placeholders**: `${sources.<name>.<field>}`, `${matrix.<key>}`,
`${variant.name}`, `${instance.name}`, `${instance.dir}`, `${instance.port}`
(a free port for the script's own use), `${instance.http_port}` (the port
`spiced` serves HTTP on, for a container that posts to it), `${script.dir}`, `${pki.dir}` (this run's node mTLS CA and certificates: `ca.crt`, `scheduler.crt`/`.key`, `executor.crt`/`.key`, generated when the run starts).
Spice's own `${secrets:…}` and `${env:…}` pass through to `spiced`.

### Steps

| Step | Does |
| --- | --- |
| `start: {}` | Start `spiced` and wait for `/v1/ready` and the Flight port (`ready-timeout:` to change the wait). Fails early if a dataset stays in `Error` while not ready. `{ ready: false }` waits only for HTTP, for datasets expected to fail. `patch:` (shaped like a variant) changes the spicepod for this and every later start. |
| `stop: {}` | SIGTERM, then SIGKILL after 45 s; fails if the shutdown wasn't clean. |
| `kill-spiced: {}` | SIGKILL; `{ signal: STOP-then-KILL }` freezes it first. |
| `restart: {}` | Stop (or `{ kill: KILL }`) and start again; takes `start`'s options. |
| `upgrade: {}` | Like `restart`, but starts the binary under test (see `--from`). |
| `wait-ready: { timeout }` | Wait for `/v1/ready`. |
| `edit-spicepod: <patch>` | Rewrite the running instance's spicepod (for `--pods-watcher-enabled`). |
| `start-source`, `stop-source`, `pause-source`, `unpause-source: <name>` | Control a source container; it keeps its host port across a stop. |
| `source-exec: { <name>: <sql, list, or { file }> }` | Run statements on a source, in this instance's database. |
| `write: { <source>: <sql or list> }` | Execute PostgreSQL DML on the required `mirror: <postgres source>` and apply its row images to the target. `key: <column>` defaults to `id`. |
| `source-query: { <name>: <sql> }` | Ask a source directly and check `expect: { rows }`. |
| `generate: { <source>: <table spec> }` | Create a table of seeded generated rows (see below); `mirror: <source>` loads the same rows into a database. |
| `dbgen: { scale, into: [<source>…] }` | Load TPC-H, generated with the `duckdb` CLI and cached. |
| `query: <sql>` | Run SQL over Flight and check the answer (see Checking answers). |
| `spice-exec: <sql>` | Run a statement through `/v1/sql` (accepts `BEGIN; …; COMMIT;`); `expect-error`. |
| `flight-put: { dataset, rows \| from-source }` | Write rows with Flight `DoPut`; `times: n` repeats it. |
| `flight-exec: <sql>` | Flight SQL `ExecuteUpdate`; `count: n` affected rows. |
| `http: { path, method, headers, body, api-key }` | One request; `expect: { status, headers, lacks-headers, body-contains, body-matches, body-lacks }`. |
| `spice: [args…]` | Run the `spice` CLI against the instance; `output-contains`, `expect-error`. |
| `refresh: <dataset>` | Trigger a refresh and wait for it to finish; `expect-error`; `wait: false` only triggers it. |
| `explain: <sql>` | `EXPLAIN` (`analyze: true`), requiring `contains:` fragments or the absence of `omits:` ones. |
| `assert-status: { dataset, status, message-contains, ready, has, lacks }` | Check a dataset's status from `/v1/datasets`. |
| `assert-log: { contains, lacks, count, clean, allow }` | Check the instance's `spiced.log` with regular expressions; `count: { <pattern>: N \| '>=N' }`. |
| `assert-files: { contains, lacks }` | Check files under the instance's directory. |
| `assert-objects: { source, prefix, contains, min }` | Require objects under a prefix of an object store or files source; `contains` counts only keys containing that text. |
| `mark-metrics: <id>` | Keep a `/metrics` scrape for a later `wait-metric … since:`. |
| `wait-metric: { name, labels, above \| below \| increases \| increased-by, since, timeout, drive }` | Wait for a series to meet a condition. `drive` writes new data each round while waiting, for conditions (such as a compaction) that need new data rather than time. |
| `background: { id, query \| source-exec, concurrency, every, expect }` | Loop a query or a source write until `stop-background: <id>`. `expect: { column: value }` is an invariant every answer must keep. `${round}` counts iterations. |
| `stop-background: { id, allow }` | Stop it; fails on any error not matching `allow`. Prints the iteration count, the P99 latency and the commonest allowed errors. |
| `kill-executor: n`, `start-executors: {}`, `add-executor: {}` | Cluster: kill executor n, restart the stopped ones, or start a spare (`defaults.cluster.spare`). |
| `corpus: <file.sql>` | One `query` per statement in the file (see Corpora). |
| `slt-setup` / `slt: { file, source }` | Run a sqllogictest file: statements on the source, then its queries through Spice against the recorded answers. |
| `tlp: { table, seed, queries, depth, aggregate, oracle, avoid }` | Ternary logic partitioning over random predicates. |
| `random-queries: { tables, joins, seed, queries, depth, oracle, protocols, avoid }` | Random nested-subquery queries checked against an oracle or across protocols. |
| `sleep: 5s` | Wait; prefer `eventually`, `consistently` and `drive`. |

Any step takes `only: <variant>` or `except: <variant>` (or a list), and
`when: { <matrix key>: <value> }`. A failed lifecycle step (`start`, `stop`,
`restart`, `upgrade`, `kill-spiced`, `wait-ready`) skips the rest of that
instance; any other failure is a failed check and the script goes on.

`protocols` on `query`, `corpus`, and inside `random-queries` selects the
transports to compare: `flight`, `flightsql`, and `http` (`/v1/sql`, JSON).
Existing queries retain their current path; random queries default to `[flight]`.
Without a random-query `oracle`, select at least two distinct protocols,
including `flight` or `flightsql` to supply the Arrow result schema. HTTP JSON
requires unique result column names. Queries must be deterministic and read
unchanging data. Every selected transport must succeed and return the same
rows, including duplicates and NULLs; empty results are also compared.
An optional random-query `oracle` additionally checks those results against a source.
Explicit protocol comparisons check values exactly after normalizing unordered
rows and signed zero. The protocol fixture uses one execution core to keep
floating aggregate evaluation stable between independent requests. Flight SQL checks the advertised schema against every
stream, including decimal precision/scale, nullability, and metadata. HTTP is
decoded from its raw JSON text using the Arrow schema, retaining decimal digits.
Prepared queries (`parameters` or `flight-session`) support an explicit
`protocols: [flightsql]`; the existing prepared path is retained when omitted.

[`query-protocols.yaml`](../../test/spicescript/query-protocols.yaml) compares
Flight SQL with HTTP using the generated SQL reference corpus and seeded
random queries over local Parquet files. One seed also exercises plain Flight:

```bash
target/debug/spicescript --spiced target/release/spiced test/spicescript/query-protocols.yaml
```

The [SQL surface generator](../../test/spicescript/corpus/sql_surface.py) maintains
an inventory pinned to a revision of `spiceai/docs`, explicit function arguments,
and the generated [coverage index](../../test/spicescript/corpus/sql-surface-coverage.json).
It includes documented aliases, operators, query clauses, optional argument
forms, and literal-sensitive decimal schema cases. Core scalar cases use typed
literals, columns with NULLs and duplicate rows, and empty inputs. This is
representative surface coverage, not an exhaustive enumeration of SQL programs
or a proof of each function's semantics. Existing oracle suites keep checking
results independently of protocol parity.

```bash
python3 test/spicescript/corpus/sql_surface.py
python3 test/spicescript/corpus/sql_surface.py --check
# Audit against a checkout of the documentation repository:
python3 test/spicescript/corpus/sql_surface.py --check --docs-dir /path/to/docs/website/docs/reference/sql
```

Reproduced failures remain generated and executable in
[`known-failing/sql-protocols`](../../test/spicescript/known-failing/sql-protocols).
Their case identifiers, reasons, documentation revision, source commit and
runtime binary hash are recorded in `corpus/sql-surface-known-failures.json`
and `corpus/sql-surface-known-values.json`. They are not counted as passing
surface coverage. Remove resolved entries and regenerate to return those
cases to the passing suite; the failure ledger also detects when a case stops
failing or changes its symptom.

When the reference changes, refresh its inventory with `--docs-dir ...
--refresh-inventory`, add cases for new entries, and regenerate. CI checks the
generated files and alias parsing without requiring network access. AI,
embeddings, indexed search, prepared statements and writes have separate
fixtures named in the coverage index. The prepared decimal and NULL cases also
run in `sql-schema-protocols.yaml`, and `sql-operator-values.yaml` checks
operator semantics against literal expected answers. They are not counted as deterministic
core protocol cases. `sql-explain-protocols.yaml` exercises documented EXPLAIN
options independently on HTTP and Flight SQL because live plan metrics vary.

ADBC is a client API; its Flight SQL driver uses the Flight SQL protocol.
This scenario exercises Flight SQL directly, rather than an ADBC driver.

### Source-neutral writes

```yaml
sources:
  src: { kind: '${matrix.src}' }
  ref: { kind: postgres }
matrix:
  src: [kafka, mongodb, dynamodb, postgres, mysql]
steps:
  - generate:
      src:
        table: t
        rows: 100
        primary-key: id
        columns:
          - { name: id, sequence: 1 }
          - { name: s, choice: [start] }
    mirror: ref
  - write: { src: "UPDATE t SET s = 'x' WHERE id <= 10" }
    mirror: ref
    key: id
```

`write` accepts one or more `INSERT`, `UPDATE` or `DELETE` statements, either
in a SQL string or a list. The mirror must be a separate PostgreSQL source.
Each statement runs there with `RETURNING *`; full INSERT/UPDATE after-images
become target upserts, and DELETE images become deletes by key. Statements
apply in order. The key must be present and non-NULL in every row image and
uniquely identify a row in both sources. The mirror requires a single-column
PRIMARY KEY or UNIQUE constraint on that key; a composite or partial index
does not establish this. Key-changing updates and explicit
RETURNING clauses are rejected; use DELETE then INSERT
to change a key. Use unqualified table names for Kafka, MongoDB and DynamoDB.
Tables must already exist on PostgreSQL and MySQL (for example via `generate`).

PostgreSQL and MySQL use their own upsert dialects. Kafka emits keyed JSON and
tombstones; MongoDB replaces documents with `_id` equal to the key; DynamoDB
puts or deletes items. Signed integer values in MongoDB use BSON Long; larger positive integers
use Decimal128 so they cannot wrap at the signed boundary. Numeric and boolean mirror columns become JSON numbers
and booleans, NULL stays null, and other columns become strings. Non-finite
numbers fail rather than silently changing their value. MongoDB adds `_id`
when the key has another name: use `key: _id` and a matching generated primary
key to keep `SELECT *` identical to the mirror.

`generate` also seeds these three kinds with typed upserts. It requires at
least one row, since the document schema is inferred from data; an empty
generation is rejected before clearing the existing table. `primary-key`
selects their key (default `id`); replace mode clears the existing collection,
topic or table first. `${sources.src.from_prefix}t${sources.src.from_suffix}`
selects a table for any of these built-in kinds, including across a matrix.
Their `source-exec` and `source-query` steps run container shell commands.
`version` overrides the image tag.

The mirror statement commits before target application. A target failure fails
the step and reports that the mirror has committed; there is no cross-source
transaction or automatic retry. Connector refresh semantics still apply:
Kafka append acceleration does not promise UPDATE/DELETE parity with a SQL
mirror. Choose the connector's supported refresh mode in the spicepod.

`feed-chaos.yaml` shares mutation and restart phases across these feeds and
PostgreSQL through Debezium. Its MongoDB source-restart check retains the
known, timing-dependent failure tracked in #15544. Kafka append receives only
inserts and runs without a primary key so deduplication cannot hide replayed
messages. `debezium-offset-recovery.yaml` separately keeps the stress case
that kills the supervisor while background mutations are in flight.

### Checking answers

`expect:` takes one of:

- `oracle: <source>` — the same SQL run directly on the source in this
  instance's database (`oracle-sql:` gives different text for the source's
  dialect). The source's text answer is typed with the column types of Spice's
  answer, so this checks values, not type mapping.
- `rows: [[...], ...]` — a literal answer, cells as strings, `null` for NULL.
- `same-as: <id>` — this instance's own earlier answer to the query with that
  `id:`, for "the old data is still served" while the source is down.
- `{}` — record the answer without checking it.

Rows are compared with `crates/result-compare`, the comparator testoperator
and the Cayenne correctness lanes use: as a multiset, or in order under a
top-level `ORDER BY … LIMIT`, with ties at the cutoff allowed and floats within
0.1%.

- **Timing.** None checks once; `eventually: 30s` retries until it matches;
  `consistently: 10s` must match on every attempt for the whole time. On a
  `corpus`, `eventually` is one limit for all its queries, counted from the
  first: each retries until then, and one reached after it gets one attempt.
- **Empty answers.** `expect-empty: true` is required for an empty answer.
- **Errors.** `expect-error: true` or `"<text>"` expects a rejection.
- **Prepared statements.** `parameters: [...]` runs the query as a Flight SQL
  prepared statement; `flight-session: <name>` keeps a Flight SQL session.

Defaults (`defaults:`):
- **Expectations and timing:** `expect`, `eventually`, `query-timeout` (default 120 s), `ready-timeout`.
- **Comparing variants:** `compare-variants: false` turns off agreement between variants; `compare-to: <variant>` compares every variant with one.
- **Protocols and auth:** `also-http: true` also asks every query over HTTP JSON and requires the same answer; `also-async: true` also runs every query as a `/v1/queries` job (a distributed job on a cluster) and requires the same answer; `api-key`; `tls: { ca }`.
- **Running spiced:** `spiced-args`, `executor-args`.
- **Clusters:** `cluster: { executors, spare, containers }`, and `cluster-partition-by: { <dataset or *>: <column> }` for `--cluster` runs. `containers: true` runs each executor in a container of its own on the host's network (Linux only; elsewhere the script is skipped), with the work and script directories mounted. Each executor writes shuffle output to its own `/tmp`, so a stage reads another executor's output over the network, as on separate hosts; a spicepod that sets `runtime.query.temp_directory` to a shared path undoes this, since an executor reads a peer's shuffle file from disk wherever its path exists.
- **Scheduling:**
  - `exclusive: true` runs the script alone, its instances one after another;
  - `run-variants: side-by-side` runs variants that work together (a snapshot writer and its reader) a step at a time, as one job.

After the run, every instance's final answer to each query is compared with the
first instance's, so variants and binaries must agree even where a query names
no oracle.

### Corpora

A `corpus: <file.sql>` step runs each statement in the file (relative to the
script) as a `query`, with the step's other keys. Comment lines before a
statement apply to it:

| Comment | Meaning |
| --- | --- |
| `-- expect-empty`, `-- expect-error` | As on a query. |
| `-- oracle-sql: <sql>` | Different text for the oracle's dialect. |
| `-- sends <place>: <fragment>` / `-- keeps <place>: <fragment>` | At that place, the SQL sent to the source contains, or lacks, the fragment. |
| `-- plan-has`, `-- plan-lacks`, `-- plan-count <place>: <n> <fragment>` | The same over the whole `EXPLAIN` output. |

Corpora for literal matrices are generated by Python scripts beside them
(`corpus/postgres_literals.py` and others); regenerate after editing the script.

### Generated tables

```yaml
- generate:
    src:
      table: orders
      rows: 2000000
      seed: 7
      primary-key: o_id
      columns:                                     # a list: the table keeps this order
        - { name: o_id,   sequence: 1 }
        - { name: cust,   int: [1, 50000] }
        - { name: status, choice: [O, F, P] }
        - { name: total,  decimal: { range: [0, 10000], scale: 2 }, nulls: 0.01 }
        - { name: placed, timestamp: ['2024-01-01', '2025-01-01'] }
        - { name: note,   text: [5, 40], nulls: 0.1 }
```

Kinds: `sequence`, `int`, `bigint`, `float`, `decimal`, `choice`, `text`,
`bool` (probability of true), `date`, `timestamp`; any column takes `nulls`.
For a `files` source a spec also takes `format: parquet|csv`, `files: N` and
`mode: replace|append`. Rows come from a SplitMix64 stream per column, seeded
from the table's seed and the column's name. So every instance and machine
gets the same rows, and adding a column leaves the others unchanged. The
runner loads them in bulk (`COPY` on PostgreSQL, multi-row `INSERT` elsewhere).

## Writing a script

- Name what the script shows in `description`, as a sentence a reader can
  check against the steps.
- Prefer an oracle to literal rows, and a corpus to many `query` steps.
- Show that the path under test ran: an `explain` fragment, a `wait-metric`, an
  `assert-log`, or `expect-empty: false` where the feature might fall back.
- Wait on conditions (`eventually`, `wait-metric`, `drive`), never on `sleep`,
  unless time itself is under test.
- Record the new script's duration (`--record-durations`) and commit
  `recordings/durations.json` with it.
- A script that reproduces an open bug goes in
  `test/spicescript/known-failing/<group>/`. It moves to `test/spicescript/`
  once the bug is fixed.

## Expected failures

Every check runs everywhere; `recordings/expected-failures.json` beside the
scripts says which failures are known. Each entry names a check (the run,
the instance, the step as `action: argument`, and for generated queries the
query number), the failure's kind and **signature** (its first line with
ports, paths, times, ids and durations normalized), and its issue **group**.
A run fails on:

- a failure the ledger doesn't list;
- a listed check that fails with another signature (an error became a wrong
  answer, a row count moved);
- a listed check that passes: if it is fixed, re-record and move the script
  out of `known-failing/`;
- an entry with no group.

When a run fails before its steps (a source or instance that did not start),
its listed checks may never have run: they are reported in one line as not
run, not as fixed. Fix the setup failure; don't re-record from that run.

`--record-failures` writes the ledger from a run, keeping the group and
`may-pass` of entries that are still there. A new entry's group is the
generated query's defect labels, else its `known-failing/<group>/`
directory, else `unassigned`, which must be set by hand. Three fields
handle failures that depend on timing:

- `may-pass: true`: the check may also pass, but must not fail any other
  way. When every expected failure of such a script passes, the run prints a
  note instead of failing.
- `varies: true`: the bug shows several symptoms; recording adds each new
  signature to `also`, and any listed one is expected.
- `pattern: <regex>`: the signature must match it, for a failure whose
  details vary too much to list around a stable core (which reader saw a
  torn read, which operator ran out of memory).

A generated query is labeled with the known defect shapes it uses (`not`,
`decimal-int`, …): the names `avoid:` takes. Passing scripts avoid them, the
same generators without `avoid:` in `known-failing/` record which queries
each defect breaks, and `--check` reports an `avoid:` whose defect no longer
fails anywhere.

## Code map

| File | Holds |
| --- | --- |
| `main.rs` | Command line, `--jobs` scheduling, progress and summaries. |
| `script.rs` | The script format: parsing, validation, durations. |
| `runner/mod.rs` | Running a script: instances, variants, the comparison of their answers. |
| `runner/steps.rs` | The dispatcher for every step, and how steps appear in reports. |
| `runner/query.rs` | `query` and `explain`: answers, the SQL sent to sources, HTTP agreement. |
| `runner/checks.rs` | Status, log, file and metric assertions. |
| `runner/background.rs` | Background loops and their invariants. |
| `runner/api.rs` | Refreshes, the `spice` CLI, HTTP steps, source queries. |
| `runner/generators.rs` | TLP, random queries, sqllogictest. |
| `runner/cluster.rs` | Scheduler and executors: arguments, readiness, spicepod changes. |
| `spiced.rs` | One `spiced` process, or container: start, readiness, stop, Flight and HTTP clients. |
| `source/write.rs` | PostgreSQL DML parsing, row-image typing, and SQL upsert generation. |
| `source/document.rs` | Kafka, MongoDB and DynamoDB containers and batched row application. |
| `source/` | One module per source kind; `mod.rs` holds the `Source` trait. |
| `docker.rs` | Containers on fixed host ports, so a restarted container keeps its address. |
| `ledger.rs` | Expected failures: recording, signatures, judging a run against them. |
| `compare.rs`, `template.rs` | Answer comparison glue, placeholders and merge patches. |
| `generate.rs`, `tpch.rs` | Seeded generated tables, TPC-H loading. |
| `tlp.rs`, `rqg.rs`, `slt.rs` | Ternary logic partitioning, random queries, sqllogictest. |
| `cleanup.rs`, `reap.rs` | Teardown on signals and drops; reaping after a killed runner. |
