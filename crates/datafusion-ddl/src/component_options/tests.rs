/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::path::{Path, PathBuf};

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use spicepod::component::ComponentOrReference;
use spicepod::spec::SpicepodDefinition;

use super::*;

/// The `WITH` options of a `CREATE TABLE` statement, parsed as the runtime's
/// default dialect parses them.
fn create_table(sql: &str) -> CreateTable {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("statement parses");
    match statements.pop().expect("one statement") {
        Statement::CreateTable(create) => create,
        other => panic!("expected CREATE TABLE, got {other}"),
    }
}

fn dataset_from_sql(sql: &str) -> DFResult<Dataset> {
    let create = create_table(sql);
    let options = options_from_sql(create_table_with_options(&create))?;
    let clauses = dataset_clause_fields(&create)?;
    component_from_options(
        &component_name_from_sql(&create.name),
        &clauses,
        &options,
        None,
    )
}

fn dataset_from_yaml(yaml_text: &str) -> Dataset {
    yaml::from_str(yaml_text).expect("dataset YAML parses")
}

fn catalog_from_yaml(yaml_text: &str) -> Catalog {
    yaml::from_str(yaml_text).expect("catalog YAML parses")
}

/// `CREATE CATALOG` is Spice syntax the SQL parser does not read; its options
/// are parsed as a `CREATE TABLE`'s, which share the grammar.
fn catalog_from_sql(name: &str, with: &str) -> DFResult<Catalog> {
    let create = create_table(&format!("CREATE TABLE {name} WITH ({with})"));
    let options = options_from_sql(create_table_with_options(&create))?;
    component_from_options(name, &ClauseFields::default(), &options, None)
}

#[test]
fn field_names_come_from_the_spicepod_types() {
    let dataset = ComponentKind::Dataset.fields();
    for field in [
        "from",
        "name",
        "description",
        "params",
        "acceleration",
        "columns",
        "time_column",
        "access",
        "dependsOn",
    ] {
        assert!(
            dataset.contains(&field),
            "dataset field {field}: {dataset:?}"
        );
    }
    // A serde alias is a field name too: `mode` is `access`.
    assert!(dataset.contains(&"mode"), "{dataset:?}");

    let catalog = ComponentKind::Catalog.fields();
    for field in [
        "from",
        "name",
        "include",
        "exclude",
        "params",
        "dataset_params",
    ] {
        assert!(
            catalog.contains(&field),
            "catalog field {field}: {catalog:?}"
        );
    }
}

/// SQL options and the Spicepod YAML they mirror deserialize to the same
/// dataset: the YAML is the oracle.
#[test]
fn options_define_the_dataset_the_equivalent_yaml_defines() {
    let from_sql = dataset_from_sql(
        r#"CREATE TABLE sales.orders WITH (
            from = 'postgres:public.orders',
            pg_host = 'db.internal',
            pg_port = 5432,
            pg_pass = '${secrets:pg_pass}',
            description = 'Orders',
            "metadata.owner" = 'sales',
            time_column = 'created_at',
            time_format = 'timestamptz',
            access = 'read_write',
            ready_state = 'on_registration',
            "acceleration.engine" = 'cayenne',
            "acceleration.mode" = 'file',
            "acceleration.refresh_mode" = 'changes',
            "acceleration.refresh_check_interval" = '30s',
            "acceleration.refresh_retry_max_attempts" = 5,
            "acceleration.params.cayenne_file_path" = '/data/orders',
            "acceleration.indexes.customer_id" = 'enabled',
            "acceleration.partition_by" = ARRAY['bucket(8, customer_id)'],
            "columns.notes.description" = 'Free text',
            "columns.notes.full_text_search.enabled" = true
        )"#,
    )
    .expect("dataset from SQL");

    let from_yaml = dataset_from_yaml(
        "
from: postgres:public.orders
name: sales.orders
description: Orders
metadata:
  owner: sales
params:
  pg_host: db.internal
  pg_port: 5432
  pg_pass: ${secrets:pg_pass}
time_column: created_at
time_format: timestamptz
access: read_write
ready_state: on_registration
acceleration:
  engine: cayenne
  mode: file
  refresh_mode: changes
  refresh_check_interval: 30s
  refresh_retry_max_attempts: 5
  params:
    cayenne_file_path: /data/orders
  indexes:
    customer_id: enabled
  partition_by:
    - bucket(8, customer_id)
columns:
  - name: notes
    description: Free text
    full_text_search:
      enabled: true
",
    );

    assert_eq!(from_sql, from_yaml);
}

#[test]
fn clauses_set_columns_key_partitioning_clustering_and_description() {
    let from_sql = dataset_from_sql(
        "CREATE TABLE events (
            id BIGINT NOT NULL COMMENT 'Event id',
            region TEXT,
            payload VARCHAR(255) NULL,
            PRIMARY KEY (id, region)
        ) WITH (from = 'sink:events', \"acceleration.engine\" = 'cayenne')
        PARTITION BY region",
    )
    .expect("dataset from SQL");

    let from_yaml = dataset_from_yaml(
        "
from: sink:events
name: events
columns:
  - name: id
    type: BIGINT
    nullable: false
    description: Event id
  - name: region
    type: TEXT
  - name: payload
    type: VARCHAR(255)
    nullable: true
acceleration:
  engine: cayenne
  primary_key: (id, region)
  partition_by:
    - region
",
    );
    assert_eq!(from_sql, from_yaml);
}

#[test]
fn a_structured_value_is_written_as_json() {
    let from_sql = dataset_from_sql(
        r#"CREATE TABLE docs WITH (
            from = 'file:docs.parquet',
            "columns.body.embeddings" = JSON '[{"from": "openai", "chunking": {"enabled": true, "target_chunk_size": 512}}]'
        )"#,
    )
    .expect("dataset from SQL");

    let from_yaml = dataset_from_yaml(
        "
from: file:docs.parquet
name: docs
columns:
  - name: body
    embeddings:
      - from: openai
        chunking:
          enabled: true
          target_chunk_size: 512
",
    );
    assert_eq!(from_sql, from_yaml);
}

#[test]
fn catalog_options_define_the_catalog_the_equivalent_yaml_defines() {
    let from_sql = catalog_from_sql(
        "lake",
        "from = 'iceberg:https://catalog.example.com/v1/namespaces',
         include = ARRAY['sales.*', 'marketing.orders'],
         access = 'read_write',
         iceberg_s3_region = 'us-west-2',
         \"dataset_params.file_format\" = 'parquet'",
    )
    .expect("catalog from SQL");
    let from_yaml = catalog_from_yaml(
        "
from: iceberg:https://catalog.example.com/v1/namespaces
name: lake
include:
  - sales.*
  - marketing.orders
access: read_write
params:
  iceberg_s3_region: us-west-2
dataset_params:
  file_format: parquet
",
    );
    assert_eq!(from_sql, from_yaml);
}

#[test]
fn a_misspelled_field_names_the_option_and_the_fix() {
    let err = dataset_from_sql(
        "CREATE TABLE orders WITH (from = 'postgres:public.orders', \"acceleration.engin\" = 'cayenne')",
    )
    .expect_err("unknown acceleration field");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'acceleration.engin' is not a dataset field. Did you mean 'acceleration.engine'? See https://spiceai.org/docs/reference/spicepod/datasets"
    );
}

#[test]
fn an_invalid_value_says_which_dataset() {
    let err = dataset_from_sql(
        "CREATE TABLE orders WITH (from = 'postgres:public.orders', \"acceleration.refresh_mode\" = 'sometimes')",
    )
    .expect_err("invalid refresh mode");
    let message = err.strip_backtrace();
    assert!(
        message.starts_with(
            "Error during planning: The definition of dataset 'orders' is not valid: "
        ),
        "{message}"
    );
    assert!(message.contains("sometimes"), "{message}");
}

#[test]
fn the_name_comes_from_the_statement() {
    let err = dataset_from_sql("CREATE TABLE orders WITH (from = 'file:o.csv', name = 'other')")
        .expect_err("name is the statement's");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'name' cannot be set: a dataset's name is the one the statement gives it, as in CREATE TABLE <name>."
    );
}

#[test]
fn a_field_set_by_a_clause_and_an_option_is_refused() {
    let err = dataset_from_sql(
        "CREATE TABLE t (id BIGINT, PRIMARY KEY (id)) WITH (from = 'sink:t', \"acceleration.primary_key\" = 'id')",
    )
    .expect_err("primary key set twice");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: PRIMARY KEY and Option 'acceleration.primary_key' set the same field. Set it once."
    );

    let err = dataset_from_sql(
        "CREATE TABLE t WITH (from = 'file:t.csv', acceleration = JSON '{\"engine\": \"cayenne\"}', \"acceleration.mode\" = 'file')",
    )
    .expect_err("a field inside a value set whole");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'acceleration' and Option 'acceleration.mode' set the same field. Set it once."
    );
}

#[test]
fn a_key_given_twice_is_refused() {
    let err =
        dataset_from_sql("CREATE TABLE t WITH (from = 'file:t.csv', pg_host = 'a', pg_host = 'b')")
            .expect_err("duplicate key");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'pg_host' is set more than once. Set it once."
    );
}

#[test]
fn the_dataset_prefix_stays_an_alias_and_params_can_be_explicit() {
    let aliased = dataset_from_sql(
        "CREATE TABLE t WITH (from = 'file:t.csv', \"dataset.time_column\" = 'ts', \"params.file_format\" = 'csv')",
    )
    .expect("aliased keys");
    let plain = dataset_from_sql(
        "CREATE TABLE t WITH (from = 'file:t.csv', time_column = 'ts', file_format = 'csv')",
    )
    .expect("plain keys");
    assert_eq!(aliased, plain);
    assert_eq!(plain.time_column.as_deref(), Some("ts"));
    assert_eq!(
        plain
            .params
            .as_ref()
            .and_then(|p| p.data.get("file_format"))
            .map(spicepod::param::ParamValue::as_string)
            .as_deref(),
        Some("csv")
    );
}

#[test]
fn unsupported_values_and_clauses_are_refused() {
    let err = dataset_from_sql("CREATE TABLE t WITH (from = 'file:t.csv', file_format = 1 + 1)")
        .expect_err("expression value");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'file_format' has the value 1 + 1, which is not supported. Use a string, number, or boolean literal, ARRAY['a', 'b'] for a list, or JSON '<json>' for a structured value."
    );

    let err = dataset_from_sql("CREATE TABLE t (id BIGINT DEFAULT 1) WITH (from = 'sink:t')")
        .expect_err("column default");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Column option 'DEFAULT 1' on column 'id' is not supported for a dataset. A column declares its type, NOT NULL, and COMMENT."
    );

    let err = dataset_from_sql("CREATE TEMPORARY TABLE t WITH (from = 'sink:t')")
        .expect_err("temporary table");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: CREATE TABLE ... TEMPORARY is not supported for a dataset. Define the dataset with WITH (from = '...', ...)."
    );
}

#[test]
fn alter_set_changes_fields_and_null_unsets_them() {
    let dataset = dataset_from_sql(
        "CREATE TABLE orders WITH (from = 'postgres:public.orders', pg_host = 'a', \"acceleration.engine\" = 'cayenne', \"acceleration.refresh_check_interval\" = '10s')",
    )
    .expect("dataset");
    let changed = component_with_options(
        &dataset,
        &[
            ComponentOption::new(
                "acceleration.refresh_check_interval",
                OptionValue::String("30s".to_string()),
            ),
            ComponentOption::new("pg_host", OptionValue::String("b".to_string())),
            ComponentOption::new("acceleration.engine", OptionValue::Null),
            ComponentOption::new("description", OptionValue::String("Orders".to_string())),
        ],
        None,
    )
    .expect("changed dataset");
    let expected = dataset_from_yaml(
        "
from: postgres:public.orders
name: orders
description: Orders
params:
  pg_host: b
acceleration:
  refresh_check_interval: 30s
",
    );
    assert_eq!(changed, expected);
}

#[test]
fn alter_reset_unsets_fields_and_refuses_one_that_is_not_set() {
    let dataset = dataset_from_sql(
        "CREATE TABLE orders WITH (from = 'postgres:public.orders', pg_host = 'a', \"acceleration.refresh_check_interval\" = '10s', \"columns.id.description\" = 'Id')",
    )
    .expect("dataset");
    let reset = component_without_options(
        &dataset,
        &[
            "acceleration.refresh_check_interval".to_string(),
            "pg_host".to_string(),
            "columns.id.description".to_string(),
        ],
        None,
    )
    .expect("reset dataset");
    // As in a Spicepod, an `acceleration` group left with no field still
    // accelerates the dataset, with every setting at its default.
    assert_eq!(
        reset,
        dataset_from_yaml(
            "from: postgres:public.orders\nname: orders\nacceleration:\n  enabled: true\n"
        )
    );
    let unaccelerated = component_without_options(&reset, &["acceleration".to_string()], None)
        .expect("reset the acceleration group");
    assert_eq!(
        unaccelerated,
        dataset_from_yaml("from: postgres:public.orders\nname: orders\n")
    );

    let err = component_without_options(
        &dataset,
        &["acceleration.refresh_check_intervl".to_string()],
        None,
    )
    .expect_err("not set");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'acceleration.refresh_check_intervl' is not set on dataset 'orders', so it cannot be reset. Did you mean 'acceleration.refresh_check_interval'?"
    );

    let err = component_without_options(&dataset, &["from".to_string()], None).expect_err("from");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'from' cannot be reset: every dataset reads from a source. Set a new one with SET (from = '...')."
    );
}

#[test]
fn the_create_statement_reads_back_as_the_same_dataset() {
    let dataset = dataset_from_yaml(
        "
from: postgres:public.orders
name: Sales.Orders
description: It's the orders
params:
  pg_host: db.internal
  pg_port: 5432
  description: a parameter named like a field
acceleration:
  engine: cayenne
  refresh_check_interval: 30s
  indexes:
    '(customer_id, region)': unique
metadata:
  ratio: 1.0
columns:
  - name: body
    embeddings:
      - from: openai
",
    );
    let sql = create_statement(&dataset, &|_| true).expect("statement");
    assert_eq!(
        sql,
        r#"CREATE TABLE sales.orders WITH (
  from = 'postgres:public.orders',
  "params.description" = 'a parameter named like a field',
  pg_host = 'db.internal',
  pg_port = 5432,
  description = 'It''s the orders',
  "metadata.ratio" = 1.0,
  "columns.body.embeddings" = JSON '[{"from":"openai"}]',
  "acceleration.engine" = 'cayenne',
  "acceleration.refresh_check_interval" = '30s',
  "acceleration.indexes.(customer_id, region)" = 'unique'
)"#
    );
    // The statement names the table as SQL resolves the Spicepod name, so it
    // reads back under the name SQL gives it.
    let read_back = dataset_from_sql(&sql).expect("statement reads back");
    assert_eq!(
        read_back,
        Dataset {
            name: "sales.orders".to_string(),
            ..dataset
        }
    );
}

#[test]
fn secret_parameters_can_be_redacted() {
    let dataset = dataset_from_yaml(
        "
from: postgres:public.orders
name: orders
params:
  pg_host: db.internal
  pg_pass: hunter2
  pg_user: ${secrets:pg_user}
",
    );
    let sql = create_statement(&dataset, &|path| {
        !path
            .last()
            .is_some_and(|name| name.ends_with("_pass") || name.ends_with("_user"))
    })
    .expect("statement");
    assert_eq!(
        sql,
        "CREATE TABLE orders WITH (
  from = 'postgres:public.orders',
  pg_host = 'db.internal',
  pg_pass = '<redacted>',
  pg_user = '${secrets:pg_user}'
)"
    );

    // Replaying the statement does not store the placeholder as the password.
    let err = dataset_from_sql(&sql).expect_err("the placeholder is not a value");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'pg_pass' holds '<redacted>', which SHOW CREATE prints in place of a secret it does not show. Set the value, or a reference to it such as '${secrets:pg_pass}'."
    );
}

/// A value that holds a reference beside other text may hold a secret, so
/// `SHOW CREATE` prints it only when the parameter is shown; a value that is
/// a reference and nothing else is always printed.
#[test]
fn only_a_value_that_is_a_reference_is_printed_unshown() {
    let dataset = dataset_from_yaml(
        r#"
from: postgres:public.orders
name: orders
params:
  pg_connection_string: "${secrets:pg_host}/db?password=hunter2"
  pg_pass: "${secrets:pg_pass}"
"#,
    );
    assert_eq!(
        create_statement(&dataset, &|_| false).expect("statement"),
        "CREATE TABLE orders WITH (
  from = 'postgres:public.orders',
  pg_connection_string = '<redacted>',
  pg_pass = '${secrets:pg_pass}'
)"
    );
}

/// Every parameters map is redacted the same way, at any depth: the
/// accelerator's, a snapshot location's, and one inside a list printed as
/// `JSON`. `shown` is given each entry's path in the component.
#[test]
fn parameters_maps_at_any_depth_are_redacted() {
    let dataset = dataset_from_yaml(
        "
from: file:orders.csv
name: orders
params:
  file_format: csv
acceleration:
  enabled: true
  engine: cayenne
  params:
    cayenne_s3_key: AKIAEXAMPLE
    cayenne_s3_secret: hunter2
    cayenne_s3_region: us-west-2
  snapshots_params:
    s3_secret: hunter3
columns:
  - name: body
    embeddings:
      - from: openai
        engine: s3_vectors
        params:
          s3_vectors_aws_secret_access_key: hunter4
          s3_vectors_bucket: vectors
",
    );
    let paths = std::sync::Mutex::new(Vec::new());
    let shown = |path: &[String]| {
        paths.lock().expect("paths").push(path.join("."));
        matches!(
            path.last().map(String::as_str),
            Some("file_format" | "cayenne_s3_region" | "s3_vectors_bucket")
        )
    };
    let sql = create_statement(&dataset, &shown).expect("statement");
    assert_eq!(
        sql,
        r#"CREATE TABLE orders WITH (
  from = 'file:orders.csv',
  file_format = 'csv',
  "columns.body.embeddings" = JSON '[{"from":"openai","engine":"s3_vectors","params":{"s3_vectors_aws_secret_access_key":"<redacted>","s3_vectors_bucket":"vectors"}}]',
  "acceleration.engine" = 'cayenne',
  "acceleration.params.cayenne_s3_key" = '<redacted>',
  "acceleration.params.cayenne_s3_region" = 'us-west-2',
  "acceleration.params.cayenne_s3_secret" = '<redacted>',
  "acceleration.snapshots_params.s3_secret" = '<redacted>'
)"#
    );
    let mut paths = paths.into_inner().expect("paths");
    paths.sort();
    assert_eq!(
        paths,
        [
            "acceleration.params.cayenne_s3_key",
            "acceleration.params.cayenne_s3_region",
            "acceleration.params.cayenne_s3_secret",
            "acceleration.snapshots_params.s3_secret",
            "columns.0.embeddings.0.params.s3_vectors_aws_secret_access_key",
            "columns.0.embeddings.0.params.s3_vectors_bucket",
            "params.file_format",
        ]
    );

    // Replaying it stores no placeholder, at any depth.
    let err = dataset_from_sql(&sql).expect_err("the placeholder is not a value");
    assert!(
        err.to_string()
            .contains("holds '<redacted>', which SHOW CREATE prints in place of a secret"),
        "{err}"
    );
}

/// A connector that reads the parameters named in `reads`.
struct Reads(&'static [&'static str]);

impl ConnectorParameters for Reads {
    fn connector(&self) -> &'static str {
        "databricks"
    }

    fn reads(&self, key: &str) -> bool {
        self.0.contains(&key)
    }
}

/// A key that is both a field of the dataset and a parameter of its connector
/// is refused rather than guessed: the Databricks connector's `mode` is also
/// the alias of the dataset field `access`.
#[test]
fn a_key_that_is_both_a_field_and_a_parameter_is_refused() {
    let create = create_table(
        "CREATE TABLE sales WITH (from = 'databricks:main.default.sales', mode = 'delta_lake')",
    );
    let options = options_from_sql(create_table_with_options(&create)).expect("options");
    let err = component_from_options::<Dataset>(
        "sales",
        &ClauseFields::default(),
        &options,
        Some(&Reads(&["mode"])),
    )
    .expect_err("ambiguous");
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: Option 'mode' is both a dataset field and a parameter of the 'databricks' connector. Write \"params.mode\" to set the parameter."
    );

    let create = create_table(
        "CREATE TABLE sales WITH (from = 'databricks:main.default.sales', \"params.mode\" = 'delta_lake')",
    );
    let options = options_from_sql(create_table_with_options(&create)).expect("options");
    let dataset: Dataset = component_from_options(
        "sales",
        &ClauseFields::default(),
        &options,
        Some(&Reads(&["mode"])),
    )
    .expect("an explicit parameter");
    assert_eq!(
        dataset,
        dataset_from_yaml(
            "from: databricks:main.default.sales\nname: sales\nparams:\n  mode: delta_lake\n"
        )
    );
}

#[test]
fn unquoted_dotted_option_keys_are_quoted_and_the_rest_is_kept() {
    let dialect = PostgreSqlDialect {};
    let sql = "CREATE TABLE t (id INT) WITH (\n  from = 'file:it''s.csv', acceleration.engine = 'cayenne',\n  \"acceleration.mode\" = 'file', columns.id.description = 'Id'\n)";
    assert_eq!(
        quote_dotted_option_keys(sql, &dialect),
        "CREATE TABLE t (id INT) WITH (\n  from = 'file:it''s.csv', \"acceleration.engine\" = 'cayenne',\n  \"acceleration.mode\" = 'file', \"columns.id.description\" = 'Id'\n)"
    );
    assert_eq!(
        quote_dotted_option_keys(
            "ALTER TABLE s.t RESET (acceleration.refresh_check_interval, pg_host)",
            &dialect
        ),
        "ALTER TABLE s.t RESET (\"acceleration.refresh_check_interval\", pg_host)"
    );
    assert_eq!(
        quote_dotted_option_keys(
            "ALTER CATALOG lake SET (dataset_params.file_format = 'csv')",
            &dialect
        ),
        "ALTER CATALOG lake SET (\"dataset_params.file_format\" = 'csv')"
    );
    for untouched in [
        "SELECT a.b FROM s.t WHERE x = 'a.b'",
        "CREATE TABLE s.t AS WITH x AS (SELECT a.b FROM s.u) SELECT * FROM x",
        "UPDATE t SET a.b = 1",
    ] {
        assert!(
            matches!(
                quote_dotted_option_keys(untouched, &dialect),
                Cow::Borrowed(_)
            ),
            "{untouched}"
        );
    }

    // The quoted statement defines what the quoted-by-hand one does.
    assert_eq!(
        dataset_from_sql(&quote_dotted_option_keys(
            "CREATE TABLE t WITH (from = 'file:t.csv', acceleration.engine = 'cayenne')",
            &dialect
        ))
        .expect("dotted keys"),
        dataset_from_sql(
            "CREATE TABLE t WITH (from = 'file:t.csv', \"acceleration.engine\" = 'cayenne')"
        )
        .expect("quoted keys")
    );
}

#[test]
fn secret_option_values_are_redacted_from_statement_text() {
    let dialect = PostgreSqlDialect {};
    let shown = |name: &str| !matches!(name, "pg_pass" | "pg_user");
    let sql = "CREATE TABLE orders WITH (\n  from = 'postgres:public.orders',\n  pg_pass = 'hunter''2', \"params.pg_user\" = 'app',\n  pg_host = 'db', pg_db = '${secrets:pg_pass}'\n)";
    assert_eq!(
        redact_secret_option_values(sql, &dialect, &shown),
        "CREATE TABLE orders WITH (\n  from = 'postgres:public.orders',\n  pg_pass = '<redacted>', \"params.pg_user\" = '<redacted>',\n  pg_host = 'db', pg_db = '${secrets:pg_pass}'\n)"
    );
    let reference = "ALTER TABLE orders SET (pg_pass = '${secrets:pg_pass}')";
    assert!(matches!(
        redact_secret_option_values(reference, &dialect, &shown),
        Cow::Borrowed(_)
    ));
    // Every spelling the option reader takes as a value is redacted whole.
    for (value, kept) in [
        ("E'hunter\\'2'", false),
        ("$$hunter2$$", false),
        ("U&'hunter2'", false),
        ("hunter2", false),
        ("\"hunter2\"", false),
        ("12345", false),
        ("-12345", false),
        ("ARRAY['hunter2']", false),
        ("JSON '\"hunter2\"'", false),
        ("$$${secrets:pg_pass}$$", true),
    ] {
        let sql = format!(
            "CREATE TABLE o WITH (from = 'postgres:public.o', pg_pass = {value}, pg_host = 'db')"
        );
        let expected = if kept {
            sql.clone()
        } else {
            "CREATE TABLE o WITH (from = 'postgres:public.o', pg_pass = '<redacted>', pg_host = 'db')"
                .to_string()
        };
        assert_eq!(
            redact_secret_option_values(&sql, &dialect, &shown),
            expected,
            "{value}"
        );
    }
    // After a comment, which is taken out, as one that held an old value
    // would be, and under EXPLAIN, which records the statement's text without
    // running it.
    for (prefix, kept) in [
        ("-- add the orders\n", "\n"),
        ("/* add the orders */ ", "  "),
        ("EXPLAIN ", "EXPLAIN "),
    ] {
        let sql = format!(
            "{prefix}CREATE TABLE o WITH (from = 'postgres:public.o', pg_pass = 'hunter2')"
        );
        assert_eq!(
            redact_secret_option_values(&sql, &dialect, &shown),
            format!(
                "{kept}CREATE TABLE o WITH (from = 'postgres:public.o', pg_pass = '<redacted>')"
            ),
            "{prefix}"
        );
    }
    let query = "SELECT * FROM t WHERE pg_pass = 'hunter2'";
    assert!(matches!(
        redact_secret_option_values(query, &dialect, &shown),
        Cow::Borrowed(_)
    ));
}

/// Every parameters map is redacted in the recorded text, not only the
/// connector's: the accelerator's, a snapshot location's, and one inside a
/// `JSON` value. A parameter not known to be non-secret is redacted; a field
/// is not.
#[test]
fn parameters_of_every_map_are_redacted_from_statement_text() {
    let dialect = PostgreSqlDialect {};
    let shown = |name: &str| {
        matches!(
            name,
            "file_format" | "cayenne_s3_region" | "s3_vectors_bucket"
        )
    };
    let sql = "CREATE TABLE o WITH (from = 'file:o.csv', file_format = 'csv', file_fromat = 'csv', description = 'Orders', acceleration.params.cayenne_s3_secret = 'hunter2', \"acceleration.params.cayenne_s3_region\" = 'us-west-2', \"acceleration.snapshots_params.s3_key\" = 'AKIA', dataset.params.pg_pass = 'hunter3')";
    assert_eq!(
        redact_secret_option_values(sql, &dialect, &shown),
        "CREATE TABLE o WITH (from = 'file:o.csv', file_format = 'csv', file_fromat = '<redacted>', description = 'Orders', acceleration.params.cayenne_s3_secret = '<redacted>', \"acceleration.params.cayenne_s3_region\" = 'us-west-2', \"acceleration.snapshots_params.s3_key\" = '<redacted>', dataset.params.pg_pass = '<redacted>')"
    );

    let json = r#"CREATE TABLE o WITH (from = 'file:o.csv', columns.body.embeddings = JSON '[{"from": "openai", "params": {"s3_vectors_aws_secret_access_key": "hunter4", "s3_vectors_bucket": "v"}}]')"#;
    assert_eq!(
        redact_secret_option_values(json, &dialect, &shown),
        r#"CREATE TABLE o WITH (from = 'file:o.csv', columns.body.embeddings = JSON '[{"from":"openai","params":{"s3_vectors_aws_secret_access_key":"<redacted>","s3_vectors_bucket":"v"}}]')"#
    );
    let whole_map = r#"ALTER TABLE o SET (acceleration.params = JSON '{"cayenne_s3_key": "AKIA2", "cayenne_s3_region": "us-west-2"}')"#;
    assert_eq!(
        redact_secret_option_values(whole_map, &dialect, &shown),
        r#"ALTER TABLE o SET (acceleration.params = JSON '{"cayenne_s3_key":"<redacted>","cayenne_s3_region":"us-west-2"}')"#
    );
    // A list's every element, a key that does not start with a field (a
    // connector parameter named by the whole key), and a value that holds a
    // reference beside other text.
    for (option, recorded) in [
        ("pg_pass = ARRAY['x', 'hunter2']", "pg_pass = '<redacted>'"),
        (
            "PARAMS.pg_pass = 'hunter2'",
            "PARAMS.pg_pass = '<redacted>'",
        ),
        (
            "pg_connection_string = '${secrets:pg_host}/db?password=hunter2'",
            "pg_connection_string = '<redacted>'",
        ),
        (
            "pg_pass = '${secrets:pg_pass}'",
            "pg_pass = '${secrets:pg_pass}'",
        ),
    ] {
        let sql = format!(
            "CREATE TABLE o WITH (from = 'postgres:public.o', {option}, file_format = 'csv')"
        );
        assert_eq!(
            redact_secret_option_values(&sql, &dialect, &shown),
            format!(
                "CREATE TABLE o WITH (from = 'postgres:public.o', {recorded}, file_format = 'csv')"
            ),
            "{option}"
        );
    }
    // A structured value with no parameters map is kept as written.
    let plain = r#"CREATE TABLE o WITH (from = 'file:o.csv', columns.body.embeddings = JSON '[{"from": "openai"}]')"#;
    assert!(matches!(
        redact_secret_option_values(plain, &dialect, &shown),
        Cow::Borrowed(_)
    ));
}

/// However a statement that defines a component is written -- behind
/// comments the parser reads, mistyped, cut short, or after another statement
/// -- its recorded text holds no value that is not known to be safe to show.
#[test]
fn a_statement_records_no_secret_however_it_is_written() {
    let dialect = PostgreSqlDialect {};
    let shown = |name: &str| {
        matches!(
            name,
            "pg_host" | "pg_port" | "pg_db" | "file_format" | "s3_vectors_bucket"
        )
    };
    let pg = "from = 'postgres:public.t', pg_host = '127.0.0.1', pg_port = 1, pg_db = 'd'";
    for (sql, recorded) in [
        // Comments end where the parser ends them.
        (
            format!("/*a/*b*/c*/ CREATE TABLE p1 WITH ({pg}, pg_pass = 'hunter2')"),
            format!("  CREATE TABLE p1 WITH ({pg}, pg_pass = '<redacted>')"),
        ),
        (
            format!("--comment\rCREATE TABLE p2 WITH ({pg}, pg_pass = 'hunter2')"),
            format!("\rCREATE TABLE p2 WITH ({pg}, pg_pass = '<redacted>')"),
        ),
        // Text that does not tokenize is not recorded.
        (
            format!("CREATE TABLE p3 WITH ({pg}, pg_pass = 'hunter2') '"),
            UNREADABLE_STATEMENT.to_string(),
        ),
        // `${key}` is not a reference the runtime resolves, so it is a value.
        (
            "CREATE TABLE p4 WITH (from = 'file:o.csv', acceleration.params.cayenne_s3_secret = '${hunter2}')".to_string(),
            "CREATE TABLE p4 WITH (from = 'file:o.csv', acceleration.params.cayenne_s3_secret = '<redacted>')".to_string(),
        ),
        // Each element of a list, as an element's parameters maps.
        (
            r#"CREATE TABLE p5 WITH (from = 'file:o.csv', "columns.name.embeddings" = ARRAY[JSON '{"from": "m", "params": {"s3_vectors_aws_secret_access_key": "hunter2", "s3_vectors_bucket": "v"}}'])"#.to_string(),
            r#"CREATE TABLE p5 WITH (from = 'file:o.csv', "columns.name.embeddings" = ARRAY[JSON '{"from":"m","params":{"s3_vectors_aws_secret_access_key":"<redacted>","s3_vectors_bucket":"v"}}'])"#.to_string(),
        ),
        // A parameter is named by all of the key after its map.
        (
            r#"CREATE TABLE p6 WITH (from = 'file:o.csv', "params.custom.file_format" = 'hunter2')"#.to_string(),
            r#"CREATE TABLE p6 WITH (from = 'file:o.csv', "params.custom.file_format" = '<redacted>')"#.to_string(),
        ),
        // Mistyped: no `=`, no parentheses, a value of two literals.
        (
            format!("CREATE TABLE p7 WITH ({pg}, pg_pass 'hunter2')"),
            format!("CREATE TABLE p7 WITH ({pg}, pg_pass '<redacted>')"),
        ),
        (
            "CREATE TABLE p8 WITH pg_pass = 'hunter2'".to_string(),
            "CREATE TABLE p8 WITH '<redacted>'".to_string(),
        ),
        (
            "CREATE TABLE p9 WITH (from = 'file:o.csv', description = 'a' 'hunter2')".to_string(),
            "CREATE TABLE p9 WITH (from = 'file:o.csv', description = '<redacted>')".to_string(),
        ),
        // After another statement.
        (
            format!("SELECT 1; CREATE TABLE p10 WITH ({pg}, pg_pass = 'hunter2')"),
            format!("SELECT 1; CREATE TABLE p10 WITH ({pg}, pg_pass = '<redacted>')"),
        ),
        // Outside an option list: a word that is a value, and every literal.
        (
            "CREATE TABLE p11 (pg_pass = hunter2) WITH (from = 'file:o.csv')".to_string(),
            "CREATE TABLE p11 (pg_pass = '<redacted>') WITH (from = 'file:o.csv')".to_string(),
        ),
        (
            "CREATE TABLE p12 (id INT COMMENT 'hunter2') WITH (from = 'file:o.csv')".to_string(),
            "CREATE TABLE p12 (id INT COMMENT '<redacted>') WITH (from = 'file:o.csv')".to_string(),
        ),
        // A commented-out option.
        (
            "CREATE TABLE p13 WITH (\n  from = 'file:o.csv',\n  -- pg_pass = 'hunter2',\n  file_format = 'csv'\n)".to_string(),
            "CREATE TABLE p13 WITH (\n  from = 'file:o.csv',\n  \n  file_format = 'csv'\n)".to_string(),
        ),
    ] {
        let recorded_text = redact_secret_option_values(&sql, &dialect, &shown);
        assert_eq!(recorded_text, recorded, "{sql}");
        assert!(!recorded_text.contains("hunter2"), "{sql}");
        // A caller keeps the text it sent when the recorded text is borrowed.
        assert!(matches!(recorded_text, Cow::Owned(_)), "{sql}");
    }

    // Text that cannot define a component is kept as written, including text
    // that only looks like a definition.
    for sql in [
        "SELECT 'hunter2'",
        "SELECT created_at FROM t WHERE altered = 'hunter2'",
        "SELECT 'CREATE TABLE t WITH (pg_pass = ''hunter2'')'",
        "DROP TABLE t",
    ] {
        assert!(
            matches!(
                redact_secret_option_values(sql, &dialect, &shown),
                Cow::Borrowed(_)
            ),
            "{sql}"
        );
    }
}

/// An error a statement fails with is recorded without the values its text
/// had taken out: the parser quotes the token it stopped at.
#[test]
fn values_taken_out_of_the_text_are_taken_out_of_its_error() {
    let dialect = PostgreSqlDialect {};
    let shown = |name: &str| name == "pg_host";
    let recorded = recorded_statement(
        "CREATE TABLE t WITH (from = 'postgres:public.t', pg_host = 'db', pg_user = 'ab', pg_pass 'hunter2')",
        &dialect,
        &shown,
    );
    assert_eq!(
        recorded.text,
        "CREATE TABLE t WITH (from = 'postgres:public.t', pg_host = 'db', pg_user = '<redacted>', pg_pass '<redacted>')"
    );
    assert_eq!(
        redact_values(
            r#"SQL error: ParserError("Expected: =, found: 'hunter2' at Line: 1, Column: 85")"#,
            &recorded.removed
        ),
        r#"SQL error: ParserError("Expected: =, found: <redacted> at Line: 1, Column: 85")"#
    );
    // A short value goes only as it was written, quotes included, so it does
    // not take other text with it.
    assert_eq!(
        redact_values("role 'ab' denied: ab hunter2 db", &recorded.removed),
        "role <redacted> denied: ab <redacted> db"
    );
    assert!(matches!(
        redact_values("Dataset 't' cannot be loaded", &recorded.removed),
        Cow::Borrowed(_)
    ));
}

/// A value is a reference exactly when the runtime resolves it as one:
/// `${ store:key }` and nothing else.
#[test]
fn only_a_value_the_runtime_resolves_is_a_reference() {
    for value in [
        "${secrets:pg_pass}",
        "${ secrets:pg_pass }",
        "${env:PG-PASS}",
        " ${ secrets : pg_pass } ",
        "${\tsecrets:pg_pass\n}",
    ] {
        assert!(is_secret_reference(value), "{value:?}");
    }
    for value in [
        "${hunter2}",
        "${secrets:pg_pass} and more",
        "x${secrets:pg_pass}",
        "${secrets:pg:pass}",
        "${secrets:}",
        "${:pg_pass}",
        "${1secrets:pg_pass}",
        "${secrets:pg_pass}}",
        "$${secrets:pg_pass}",
        "{secrets:pg_pass}",
        "${secrets.pg_pass}",
        "${secrets:pg pass}",
        "",
    ] {
        assert!(!is_secret_reference(value), "{value:?}");
    }

    // SHOW CREATE prints a value that only looks like a reference as the
    // secret it may be.
    let dataset = dataset_from_yaml(
        "
from: postgres:public.orders
name: orders
params:
  pg_pass: ${hunter2}
",
    );
    assert_eq!(
        create_statement(&dataset, &|_| false).expect("statement"),
        "CREATE TABLE orders WITH (
  from = 'postgres:public.orders',
  pg_pass = '<redacted>'
)"
    );
}

#[test]
fn the_first_word_skips_whitespace_and_comments() {
    assert_eq!(first_word("  CREATE TABLE t"), "CREATE");
    assert_eq!(first_word("-- one\n-- two\n\tdrop catalog c"), "drop");
    assert_eq!(first_word("/* a */ /* b */ALTER TABLE t"), "ALTER");
    assert_eq!(first_word("-- only a comment"), "");
    assert_eq!(first_word("/* unterminated"), "");
    assert_eq!(first_word("(SELECT 1)"), "");
    // As PostgreSQL reads them: a carriage return ends a line comment, and
    // block comments nest.
    assert_eq!(first_word("--one\rCREATE TABLE t"), "CREATE");
    assert_eq!(first_word("/*a/*b*/c*/ CREATE TABLE t"), "CREATE");
    assert_eq!(first_word("/*/ a */ALTER TABLE t"), "ALTER");
    assert_eq!(first_word("/*a/*b*/ CREATE TABLE t"), "");
}

fn spicepod_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).expect("read test/spicepods");
    for entry in entries {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            spicepod_files(&path, files);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "yaml" || ext == "yml")
        {
            files.push(path);
        }
    }
}

/// Every dataset and catalog the repository's test Spicepods declare reads
/// back from its own `CREATE` statement as the same component: the Spicepod
/// definition is the oracle.
#[test]
fn every_test_spicepod_component_round_trips_through_its_create_statement() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test/spicepods");
    let mut files = Vec::new();
    spicepod_files(&root, &mut files);
    files.sort();

    let mut datasets = 0_usize;
    let mut catalogs = 0_usize;
    for file in &files {
        let text = std::fs::read_to_string(file).expect("read spicepod");
        let Ok(definition) = yaml::from_str::<SpicepodDefinition>(&text) else {
            // Not a Spicepod (a testoperator scenario or a fragment).
            continue;
        };
        for dataset in definition.datasets {
            let ComponentOrReference::Component(dataset) = dataset else {
                continue;
            };
            let sql = create_statement(&dataset, &|_| true).expect("statement");
            let read_back = dataset_from_sql(&sql).unwrap_or_else(|e| {
                panic!(
                    "{}: dataset '{}' does not read back from\n{sql}\n{e}",
                    file.display(),
                    dataset.name
                )
            });
            let expected_name = component_name_from_sql(&create_table(&sql).name);
            assert_eq!(
                read_back,
                Dataset {
                    name: expected_name,
                    ..dataset.clone()
                },
                "{}: dataset '{}' changed through\n{sql}",
                file.display(),
                dataset.name
            );
            // The runtime keeps SQL definitions as JSON.
            let stored: Dataset =
                serde_json::from_slice(&serde_json::to_vec(&dataset).expect("to JSON"))
                    .expect("from JSON");
            assert_eq!(
                stored,
                dataset,
                "{}: dataset '{}' changed through JSON",
                file.display(),
                dataset.name
            );
            datasets += 1;
        }
        for catalog in definition.catalogs {
            let ComponentOrReference::Component(catalog) = catalog else {
                continue;
            };
            let options = component_options(&catalog).expect("options");
            let read_back: Catalog =
                component_from_options(&catalog.name, &ClauseFields::default(), &options, None)
                    .unwrap_or_else(|e| {
                        panic!(
                            "{}: catalog '{}' does not read back: {e}",
                            file.display(),
                            catalog.name
                        )
                    });
            assert_eq!(
                read_back,
                catalog,
                "{}: catalog '{}'",
                file.display(),
                catalog.name
            );
            let stored: Catalog =
                serde_json::from_slice(&serde_json::to_vec(&catalog).expect("to JSON"))
                    .expect("from JSON");
            assert_eq!(
                stored,
                catalog,
                "{}: catalog '{}' changed through JSON",
                file.display(),
                catalog.name
            );
            catalogs += 1;
        }
    }
    // Non-vacuous: the corpus is the repository's test Spicepods, hundreds of
    // datasets across connectors, accelerators, and search features.
    assert!(datasets > 500, "only {datasets} datasets round-tripped");
    assert!(catalogs > 5, "only {catalogs} catalogs round-tripped");
}
