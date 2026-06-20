/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use datafusion::{
    common::{
        plan_err,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    error::DataFusionError,
    logical_expr::{DdlStatement, Expr, LogicalPlan, Statement},
};

use crate::datafusion::DataFusion;

// Re-export the single-source-of-truth list of write-capable extension node
// names. The `cache` crate owns this list so that both the read-only validator
// below and the SQL results-cache eligibility check in `cache` stay in lockstep
// — any write-capable extension must be non-cacheable AND blocked by read-only.
pub(super) use cache::WRITE_CAPABLE_EXTENSION_NAMES;

/// Config namespaces that `SET`/`RESET` may target.
///
/// These cover display (`explain`), plan shape (`optimizer`), and execution
/// tuning (`execution`) — perf/diagnostic knobs that change neither query
/// results, security posture, nor write behavior. Everything outside this set
/// (e.g. `datafusion.catalog.*`, `datafusion.sql_parser.*`) stays rejected so a
/// session cannot, for example, repoint the default catalog or alter SQL
/// parsing semantics.
const SAFE_SET_PREFIXES: &[&str] = &[
    "datafusion.explain.", // show_statistics, logical_plan_only, etc. — display only
    "datafusion.optimizer.", // plan shape — perf only
    "datafusion.execution.", // target_partitions, batch_size — perf only
];

/// Config keys (or sub-namespaces) that are otherwise prefix-allowed above but
/// are pinned by the runtime at startup (see `datafusion::builder`) and must
/// not be overridden per-session.
const UNSAFE_SET_KEYS: &[&str] = &[
    // Parquet reader behavior is fixed by the runtime (e.g. `pushdown_filters`);
    // letting a session flip it can change scan correctness expectations.
    "datafusion.execution.parquet.",
];

/// Whether a `SET`/`RESET <variable>` targets a config key that is safe to
/// expose to SQL clients. Case-insensitive: `DataFusion` lower-cases config keys,
/// but SQL identifiers may arrive in any case.
fn is_safe_set_variable(variable: &str) -> bool {
    let v = variable.to_ascii_lowercase();
    SAFE_SET_PREFIXES.iter().any(|p| v.starts_with(p))
        && !UNSAFE_SET_KEYS.iter().any(|p| v.starts_with(p))
}

/// Validates that a logical plan only performs allowed operations on datasets.
///
/// Reads (SELECT queries) are allowed on all tables.
/// INSERT and DELETE operations are only allowed on datasets that are configured as writable,
/// and are not allowed on internal Spice tables.
/// UPDATE operations are only allowed on writable Cayenne catalog tables.
/// DDL operations are only allowed on catalogs configured with `access: read_write_create`.
/// DML (other than allowed INSERT/DELETE/UPDATE) and COPY are not permitted.
/// Statement operations are rejected except PREPARE/EXECUTE/DEALLOCATE and
/// SET/RESET of an allowlisted, safe config namespace (see [`is_safe_set_variable`]).
///
/// # Returns
/// * `Ok(())` if the plan is valid
/// * `Err(DataFusionError)` if the plan contains invalid operations, such as DDL, DML on read-only datasets,
///   or INSERT on internal datasets
pub fn validate_sql_query_operations(
    plan: &LogicalPlan,
    df: &Arc<DataFusion>,
) -> Result<(), DataFusionError> {
    plan.apply_with_subqueries(|node| match node {
        // Data Definition Language (DDL): CREATE / DROP TABLES / VIEWS / SCHEMAS
        LogicalPlan::Ddl(ddl) => {
            validate_ddl_operation(ddl, df)
        }
        // Data Manipulation Language (DML): Insert / Update / Delete
        LogicalPlan::Dml(dml) => {
            if let datafusion::logical_expr::WriteOp::Insert(op) = &dml.op {
                if op != &datafusion_expr::dml::InsertOp::Append {
                    return plan_err!("Only Append (`INSERT INTO`) operations are permitted: '{op}' is not allowed");
                }

                if super::is_spice_internal_dataset(&dml.table_name) {
                    return plan_err!(
                        "INSERT operations are not allowed on Spice system dataset '{}'.",
                        dml.table_name
                    );
                }

                // Check if attempting to write into a catalog.
                if let Some(catalog) = dml.table_name.catalog()
                    && df.is_catalog_writable(catalog)
                {
                    return Ok(TreeNodeRecursion::Continue);
                }

                // Fall back to per-table writable check
                if df.is_writable(&dml.table_name) || df.is_catalog_writable(super::SPICE_DEFAULT_CATALOG) {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    plan_err!(
                        "INSERT operations are not allowed on read-only dataset '{}'. Verify the dataset is configured with 'access: read_write' and try again.",
                        dml.table_name
                    )
                }
            } else if matches!(&dml.op, datafusion::logical_expr::WriteOp::Delete) {
                if super::is_spice_internal_dataset(&dml.table_name) {
                    return plan_err!(
                        "DELETE operations are not allowed on Spice system dataset '{}'.",
                        dml.table_name
                    );
                }

                // Check if attempting to delete from a catalog table.
                if let Some(catalog) = dml.table_name.catalog() && catalog != super::SPICE_DEFAULT_CATALOG {
                    if !df.is_catalog_writable(catalog) {
                        return plan_err!(
                            "DELETE operations are not allowed on read-only catalog table '{}'. Verify the catalog is configured with 'access: read_write' and try again.",
                            dml.table_name
                        );
                    }
                    return Ok(TreeNodeRecursion::Continue);
                }

                if df.is_writable(&dml.table_name) || df.is_catalog_writable(super::SPICE_DEFAULT_CATALOG) {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    plan_err!(
                        "DELETE operations are not allowed on read-only dataset '{}'. Verify the dataset is configured with 'access: read_write' and try again.",
                        dml.table_name
                    )
                }
            } else if matches!(&dml.op, datafusion::logical_expr::WriteOp::Update) {
                if super::is_spice_internal_dataset(&dml.table_name) {
                    return plan_err!(
                        "UPDATE operations are not allowed on Spice system dataset '{}'.",
                        dml.table_name
                    );
                }

                // Check if attempting to update a catalog table.
                if let Some(catalog) = dml.table_name.catalog() && catalog != super::SPICE_DEFAULT_CATALOG {
                    if !df.is_catalog_writable(catalog) {
                        return plan_err!(
                            "UPDATE operations are not allowed on read-only catalog table '{}'. Verify the catalog is configured with 'access: read_write' and try again.",
                            dml.table_name
                        );
                    }
                    return Ok(TreeNodeRecursion::Continue);
                }

                if df.is_writable(&dml.table_name) || df.is_catalog_writable(super::SPICE_DEFAULT_CATALOG) {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    plan_err!(
                        "UPDATE operations are not allowed on read-only dataset '{}'. Verify the dataset is configured with 'access: read_write' and try again.",
                        dml.table_name
                    )
                }
            } else { plan_err!("Operation is not allowed: {}", dml.name()) }
        }
        LogicalPlan::Copy(_) => {
            plan_err!("COPY operations are not allowed")
        }
        LogicalPlan::Statement(stmt) => {
            // Allow PREPARE, EXECUTE, and DEALLOCATE statements, plus SET/RESET of
            // a safe subset of DataFusion config (display/perf knobs only).
            match stmt {
                Statement::Prepare(_) | Statement::Execute(_) | Statement::Deallocate(_) => {
                    Ok(TreeNodeRecursion::Continue)
                }
                Statement::SetVariable(set) if is_safe_set_variable(&set.variable) => {
                    Ok(TreeNodeRecursion::Continue)
                }
                Statement::ResetVariable(reset) if is_safe_set_variable(&reset.variable) => {
                    Ok(TreeNodeRecursion::Continue)
                }
                _ => plan_err!("Statements are not allowed: {}", stmt.name()),
            }
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(())
}

/// Validates DDL operations. DDL is only allowed on catalogs with `access: read_write_create`.
fn validate_ddl_operation(
    ddl: &DdlStatement,
    df: &Arc<DataFusion>,
) -> Result<TreeNodeRecursion, DataFusionError> {
    // Extract the catalog name from the DDL statement's table reference
    let catalog_name = match ddl {
        DdlStatement::CreateExternalTable(create) => create.name.catalog().map(String::from),
        DdlStatement::CreateMemoryTable(create) => create.name.catalog().map(String::from),
        DdlStatement::DropTable(drop) => drop.name.catalog().map(String::from),
        DdlStatement::CreateView(create) => create.name.catalog().map(String::from),
        DdlStatement::DropView(drop) => drop.name.catalog().map(String::from),
        // Schema-level operations - schema_name is a string that may contain catalog.schema format
        DdlStatement::CreateCatalogSchema(create) => {
            // schema_name may be in format "catalog.schema" or just "schema"
            create
                .schema_name
                .split('.')
                .next()
                .filter(|_| create.schema_name.contains('.'))
                .map(String::from)
        }
        DdlStatement::DropCatalogSchema(drop) => {
            // Extract catalog from SchemaReference if it's a full reference
            match &drop.name {
                datafusion::common::SchemaReference::Full { catalog, .. } => {
                    Some(catalog.to_string())
                }
                datafusion::common::SchemaReference::Bare { .. } => None,
            }
        }
        // Catalog-level operations - use the catalog name directly
        DdlStatement::CreateCatalog(_)
        | DdlStatement::CreateIndex(_)
        | DdlStatement::CreateFunction(_)
        | DdlStatement::DropFunction(_) => None,
    };

    // Check if the operation is targeting a DDL-enabled catalog
    if let Some(catalog) = catalog_name {
        if df.is_catalog_ddl_enabled(&catalog) {
            return Ok(TreeNodeRecursion::Continue);
        }
        return plan_err!(
            "DDL operation '{}' is not allowed on catalog '{}'. Verify the catalog is configured with 'access: read_write_create' and try again.",
            ddl.name(),
            catalog
        );
    }

    // No catalog specified — check if the default catalog is DDL-enabled
    if df.is_catalog_ddl_enabled(super::SPICE_DEFAULT_CATALOG) {
        return Ok(TreeNodeRecursion::Continue);
    }

    // DDL on the default catalog or without a catalog is not allowed
    plan_err!(
        "DDL operation '{}' is not allowed. DDL operations are only supported on catalogs configured with 'access: read_write_create'.",
        ddl.name()
    )
}

/// Strict read-only validator.
///
/// Rejects any plan containing DDL, DML, COPY, or any `LogicalPlan::Statement` node
/// (including `PREPARE` / `EXECUTE` / `DEALLOCATE`). `EXECUTE` can indirectly invoke a
/// prepared DDL/DML statement and `PREPARE` / `DEALLOCATE` mutate session state, so all
/// three are disallowed on surfaces that must guarantee read-only execution — notably
/// the built-in `sql` tool and the LLM-generated SQL path in `/v1/nsql`.
///
/// The one exception is `SET` / `RESET` of an allowlisted, safe config namespace
/// (display/perf only — see [`is_safe_set_variable`]). These mutate session config,
/// not data, and cannot change query results, security, or write behavior.
///
/// Spice's planner can also represent DDL/DML as [`LogicalPlan::Extension`] nodes
/// (for example, `DdlExtensionNode` from `datafusion-ddl`, `DmlExtensionNode` from
/// `datafusion-dml`, and the `DistributedCayenne{Insert,Update,Delete,Merge}` /
/// `CayenneMerge` distributed DML nodes). Those nodes are matched here by their
/// stable [`UserDefinedLogicalNodeCore::name`] so that write-capable plans produced
/// by Spice's custom planner cannot bypass the read-only guarantee. Any new
/// write-capable extension node type MUST be added to [`WRITE_CAPABLE_EXTENSION_NAMES`].
///
/// # Returns
/// * `Ok(())` if the plan contains only read operations.
/// * `Err(DataFusionError)` if the plan contains any write, schema-mutating, or
///   session-mutating operation.
pub fn validate_sql_query_read_only(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    plan.apply_with_subqueries(|node| match node {
        LogicalPlan::Ddl(ddl) => plan_err!(
            "DDL operation '{}' is not allowed in read-only SQL context.",
            ddl.name()
        ),
        LogicalPlan::Dml(dml) => plan_err!(
            "{} operations are not allowed in read-only SQL context.",
            dml.name()
        ),
        LogicalPlan::Copy(_) => {
            plan_err!("COPY operations are not allowed in read-only SQL context.")
        }
        LogicalPlan::Statement(stmt) => match stmt {
            // SET/RESET of display/perf config is harmless under read-only auth:
            // it mutates session config, not data. The allowlist excludes anything
            // that could change results, security, or write behavior.
            Statement::SetVariable(set) if is_safe_set_variable(&set.variable) => {
                Ok(TreeNodeRecursion::Continue)
            }
            Statement::ResetVariable(reset) if is_safe_set_variable(&reset.variable) => {
                Ok(TreeNodeRecursion::Continue)
            }
            _ => plan_err!(
                "Statement '{}' is not allowed in read-only SQL context.",
                stmt.name()
            ),
        },
        LogicalPlan::Extension(ext) => {
            let name = ext.node.name();
            if WRITE_CAPABLE_EXTENSION_NAMES.contains(&name) {
                plan_err!(
                    "Write-capable extension plan '{name}' is not allowed in read-only SQL context."
                )
            } else {
                validate_no_code_executing_functions(node)?;
                Ok(TreeNodeRecursion::Continue)
            }
        }
        _ => {
            validate_no_code_executing_functions(node)?;
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(())
}

fn validate_no_code_executing_functions(plan: &LogicalPlan) -> Result<(), DataFusionError> {
    for expr in plan.expressions() {
        expr.apply(|expr| {
            if let Expr::ScalarFunction(function) = expr {
                let function_name = function.func.name();
                if crate::datafusion::udf::is_code_executing_function(function_name) {
                    return plan_err!(
                        "Function '{function_name}' requires a read-write API key and is not allowed in read-only SQL context."
                    );
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        dataaccelerator::AcceleratorEngineRegistry,
        datafusion::{SPICE_RUNTIME_SCHEMA, builder::DataFusionBuilder},
        status::RuntimeStatus,
    };

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use data_components::arrow::write::MemTable;
    use datafusion::{catalog::MemoryCatalogProvider, sql::TableReference};
    use runtime_datafusion::schema_provider::SpiceSchemaProvider;
    use std::sync::Arc;
    use tokio::runtime::Handle;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn create_test_datafusion() -> Arc<DataFusion> {
        let df = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                Handle::current(),
            )
            .build(),
        );

        let mem_table = Arc::new(
            MemTable::try_new(create_test_schema(), vec![]).expect("mem table should be created"),
        );

        df.ctx
            .register_table(
                "tbl_read_only",
                Arc::<data_components::arrow::write::MemTable>::clone(&mem_table),
            )
            .expect("table should be registered");

        df.ctx
            .register_table(
                "tbl_writable",
                Arc::<data_components::arrow::write::MemTable>::clone(&mem_table),
            )
            .expect("table should be registered");

        df.data_writers
            .write()
            .expect("data writers should be acquired")
            .insert("tbl_writable".into());

        let internal_table = TableReference::partial(SPICE_RUNTIME_SCHEMA, "spice_table");
        df.ctx
            .register_table(
                internal_table.clone(),
                Arc::<data_components::arrow::write::MemTable>::clone(&mem_table),
            )
            .expect("table should be registered");

        df.data_writers
            .write()
            .expect("data writers should be acquired")
            .insert(internal_table);

        df.ctx.register_catalog(
            "readonly_catalog".to_string(),
            Arc::new(MemoryCatalogProvider::new()),
        );

        df.ctx
            .catalog("readonly_catalog")
            .expect("catalog should be found")
            .register_schema("public", Arc::new(SpiceSchemaProvider::new()))
            .expect("schema should be registered");

        df.ctx
            .register_table(
                TableReference::full("readonly_catalog", "public", "test_table"),
                Arc::<data_components::arrow::write::MemTable>::clone(&mem_table),
            )
            .expect("table should be registered");

        df.ctx.register_catalog(
            "writable_catalog".to_string(),
            Arc::new(MemoryCatalogProvider::new()),
        );

        df.ctx
            .catalog("writable_catalog")
            .expect("catalog should be found")
            .register_schema("public", Arc::new(SpiceSchemaProvider::new()))
            .expect("schema should be registered");

        df.ctx
            .register_table(
                TableReference::full("writable_catalog", "public", "test_table"),
                mem_table,
            )
            .expect("table should be registered");

        // Mark writable_catalog as writable
        df.mark_catalog_writable("writable_catalog")
            .expect("catalog should be marked as writable");

        // Add a DDL-enabled catalog for testing DDL operations
        df.ctx.register_catalog(
            "ddl_enabled_catalog".to_string(),
            Arc::new(MemoryCatalogProvider::new()),
        );

        df.ctx
            .catalog("ddl_enabled_catalog")
            .expect("catalog should be found")
            .register_schema("public", Arc::new(SpiceSchemaProvider::new()))
            .expect("schema should be registered");

        // Mark ddl_enabled_catalog as DDL-enabled (which also makes it writable)
        df.mark_catalog_ddl_enabled("ddl_enabled_catalog")
            .expect("catalog should be marked as DDL-enabled");

        df
    }

    #[tokio::test]
    async fn test_validate_read_only_query_allowed() {
        let df = create_test_datafusion();

        let sql = "SELECT * FROM tbl_read_only";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_ok(), "Read-only queries should be allowed");
    }

    #[tokio::test]
    async fn test_validate_insert_on_writable_dataset() {
        let df = create_test_datafusion();

        let sql = "INSERT INTO tbl_writable VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // INSERT should be allowed on writable dataset
        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "INSERT should be allowed on writable dataset"
        );
    }

    #[tokio::test]
    async fn test_validate_insert_on_writable_dataset_full_reference() {
        let df = create_test_datafusion();

        let sql = "INSERT INTO spice.public.tbl_writable VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // INSERT should be allowed on writable dataset
        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "INSERT should be allowed on writable dataset with full reference"
        );
    }

    #[tokio::test]
    async fn test_validate_insert_on_readonly_dataset_blocked() {
        let df = create_test_datafusion();

        let sql = "INSERT INTO tbl_read_only VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "INSERT should fail on read-only dataset");
    }

    #[tokio::test]
    async fn test_validate_insert_on_internal_table_blocked() {
        let df = create_test_datafusion();

        let sql = "INSERT INTO runtime.spice_table VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_err(),
            "INSERT should fail on Spice internal dataset"
        );
    }

    #[tokio::test]
    async fn test_validate_update_operation_blocked() {
        let df = create_test_datafusion();

        let sql = "UPDATE tbl_read_only SET name = 'updated' WHERE id = 1";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // UPDATE on a read-only dataset should be blocked
        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_err(),
            "UPDATE operations should be blocked on read-only datasets"
        );
    }

    #[tokio::test]
    async fn test_validate_delete_on_writable_dataset_allowed() {
        let df = create_test_datafusion();

        let sql = "DELETE FROM tbl_writable WHERE id = 1";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // DELETE operations should be allowed on writable datasets
        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "DELETE should be allowed on writable dataset"
        );
    }

    #[tokio::test]
    async fn test_validate_delete_on_readonly_dataset_blocked() {
        let df = create_test_datafusion();

        let sql = "DELETE FROM tbl_read_only WHERE id = 1";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // DELETE operations should be blocked on read-only datasets
        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "DELETE should fail on read-only dataset");
    }

    #[tokio::test]
    async fn test_validate_delete_on_internal_table_blocked() {
        let df = create_test_datafusion();

        let sql = "DELETE FROM runtime.spice_table WHERE id = 1";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // DELETE operations should be blocked on internal tables
        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_err(),
            "DELETE should fail on Spice internal dataset"
        );
    }

    #[tokio::test]
    async fn test_validate_ddl_operations_blocked() {
        let df = create_test_datafusion();

        // Test CREATE TABLE
        let sql = "CREATE TABLE test_table (id INT, name VARCHAR(50))";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "CREATE TABLE should be blocked");
    }

    #[tokio::test]
    async fn test_validate_ddl_operations_allowed_on_ddl_enabled_catalog() {
        let df = create_test_datafusion();

        // Test CREATE TABLE on DDL-enabled catalog
        let sql = "CREATE TABLE ddl_enabled_catalog.public.new_table (id INT, name VARCHAR(50))";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "CREATE TABLE should be allowed on DDL-enabled catalog"
        );

        // Test DROP TABLE on DDL-enabled catalog
        let sql = "DROP TABLE IF EXISTS ddl_enabled_catalog.public.some_table";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "DROP TABLE should be allowed on DDL-enabled catalog"
        );

        // Test CREATE SCHEMA on DDL-enabled catalog
        let sql = "CREATE SCHEMA ddl_enabled_catalog.new_schema";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "CREATE SCHEMA should be allowed on DDL-enabled catalog"
        );
    }

    #[tokio::test]
    async fn test_validate_ddl_blocked_on_writable_but_not_ddl_enabled_catalog() {
        let df = create_test_datafusion();

        // Test CREATE TABLE on writable_catalog (which is only read_write, not read_write_create)
        let sql = "CREATE TABLE writable_catalog.public.new_table (id INT, name VARCHAR(50))";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_err(),
            "CREATE TABLE should be blocked on writable (but not DDL-enabled) catalog"
        );
    }

    #[tokio::test]
    async fn test_validate_copy_operations_blocked() {
        let df = create_test_datafusion();

        // Test COPY TO using the registered table
        let sql = "COPY tbl_read_only TO 'output.csv'";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "COPY operations should be blocked");
    }

    #[tokio::test]
    #[ignore = "temporarily disabled"]
    async fn test_validate_statement_operations_blocked() {
        let df = create_test_datafusion();

        // Test SET statement
        let sql = "SET datafusion.execution.batch_size = 1000";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "SET statements should be blocked");
    }

    #[tokio::test]
    async fn test_validate_complex_read_query_allowed() {
        let df = create_test_datafusion();

        let sql = "SELECT name, SUM(value) as total FROM tbl_read_only WHERE value > 0.0 GROUP BY name ORDER BY total DESC LIMIT 10";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_ok(), "Complex read queries should be allowed");
    }

    #[tokio::test]
    async fn test_validate_subquery_operations() {
        let df = create_test_datafusion();

        let sql =
            "SELECT * FROM tbl_writable WHERE id IN (SELECT id FROM tbl_read_only WHERE id > 5)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_ok(), "Read-only subqueries should be allowed");
    }

    #[tokio::test]
    async fn test_validate_insert_on_writable_catalog() {
        let df = create_test_datafusion();

        let sql = "INSERT INTO writable_catalog.public.test_table VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "INSERT should be allowed on writable catalog table"
        );
    }

    #[tokio::test]
    async fn test_validate_insert_on_readonly_catalog_blocked() {
        let df = create_test_datafusion();

        let sql = "INSERT INTO readonly_catalog.public.test_table VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "INSERT should fail on read-only catalog");
    }

    #[tokio::test]
    async fn test_validate_insert_overwrite_blocked() {
        let df = create_test_datafusion();

        let sql = "INSERT OVERWRITE tbl_writable VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "INSERT OVERWRITE should be blocked");
    }

    #[tokio::test]
    async fn test_validate_default_catalog_table_uses_dataset_rules() {
        let df = create_test_datafusion();

        // Default catalog should follow dataset writable rules, not catalog rules
        let sql = "INSERT INTO spice.public.tbl_writable VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "INSERT should be allowed on writable dataset in default catalog"
        );

        // Test read-only dataset in default catalog
        let sql = "INSERT INTO spice.public.tbl_read_only VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_err(),
            "INSERT should fail on read-only dataset in default catalog"
        );
    }

    #[tokio::test]
    async fn test_validate_ddl_on_default_catalog_when_ddl_enabled() {
        let df = create_test_datafusion();

        // Mark the default catalog as DDL-enabled (simulates Iceberg catalog override)
        df.mark_catalog_ddl_enabled("spice")
            .expect("should mark default catalog as DDL-enabled");

        // CREATE TABLE on default catalog (bare name) should be allowed
        let sql = "CREATE TABLE new_table (id INT, name VARCHAR(50))";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "CREATE TABLE should be allowed on DDL-enabled default catalog (bare name)"
        );

        // CREATE TABLE with explicit spice catalog should also work
        let sql = "CREATE TABLE spice.public.another_table (id INT, name VARCHAR(50))";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "CREATE TABLE should be allowed on DDL-enabled default catalog (explicit name)"
        );

        // DROP TABLE should also work
        let sql = "DROP TABLE IF EXISTS new_table";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "DROP TABLE should be allowed on DDL-enabled default catalog"
        );
    }

    #[tokio::test]
    async fn test_validate_dml_on_default_catalog_when_writable() {
        let df = create_test_datafusion();

        // Mark the default catalog as DDL-enabled (which also marks it writable)
        df.mark_catalog_ddl_enabled("spice")
            .expect("should mark default catalog as DDL-enabled");

        // Register a test table to INSERT into
        let mem_table = Arc::new(
            MemTable::try_new(create_test_schema(), vec![]).expect("mem table should be created"),
        );
        df.ctx
            .register_table("catalog_table", mem_table)
            .expect("table should be registered");

        // INSERT on a catalog-managed table (not individually marked writable) should work
        // because the default catalog is writable
        let sql = "INSERT INTO catalog_table VALUES (1, 'foo', 42.0)";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        let result = validate_sql_query_operations(&plan, &df);
        assert!(
            result.is_ok(),
            "INSERT should be allowed on table in writable default catalog"
        );
    }

    /// [`validate_sql_query_read_only`] must allow SELECT but reject every class of
    /// write/schema-mutating plan, independent of per-catalog writability. This is the
    /// contract that the built-in `sql` tool and `/v1/nsql` rely on to contain
    /// LLM-generated SQL.
    #[tokio::test]
    async fn test_read_only_validator_allows_select() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("SELECT * FROM tbl_writable")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan).expect("SELECT must be allowed in read-only context");
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_insert_on_writable_dataset() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("INSERT INTO tbl_writable VALUES (1, 'foo', 42.0)")
            .await
            .expect("plan should be created");

        let err = validate_sql_query_read_only(&plan)
            .expect_err("INSERT must be rejected in read-only context");
        assert!(
            err.to_string().contains("read-only"),
            "error should cite read-only context, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_code_executing_function() {
        use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
        use datafusion::scalar::ScalarValue;

        let df = create_test_datafusion();
        let function_name = "code_exec_test_fn";
        let _cleanup = scopeguard::guard(function_name, |name| {
            crate::datafusion::udf::remove_code_executing_function(name);
        });

        let udf = create_udf(
            function_name,
            vec![],
            DataType::Utf8,
            Volatility::Volatile,
            Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "ok".to_string(),
                ))))
            }),
        );
        df.ctx.register_udf(udf);
        crate::datafusion::udf::add_code_executing_function(function_name);

        let plan = df
            .ctx
            .state()
            .create_logical_plan("SELECT code_exec_test_fn()")
            .await
            .expect("plan should be created");

        let err = validate_sql_query_read_only(&plan)
            .expect_err("code-executing functions must be rejected");
        assert!(
            err.to_string().contains("read-write API key"),
            "error should cite read-write API key requirement, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_delete_on_writable_dataset() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("DELETE FROM tbl_writable WHERE id = 1")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan)
            .expect_err("DELETE must be rejected in read-only context");
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_update_on_writable_dataset() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("UPDATE tbl_writable SET name = 'bar' WHERE id = 1")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan)
            .expect_err("UPDATE must be rejected in read-only context");
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_insert_on_writable_catalog() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan(
                "INSERT INTO writable_catalog.public.test_table VALUES (1, 'foo', 42.0)",
            )
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan)
            .expect_err("INSERT into a writable catalog must be rejected in read-only context");
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_ddl() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("DROP TABLE IF EXISTS tbl_writable")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan).expect_err("DDL must be rejected in read-only context");
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_copy() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("COPY tbl_writable TO '/tmp/out.parquet'")
            .await
            .expect("plan should be created");

        let err = validate_sql_query_read_only(&plan)
            .expect_err("COPY must be rejected in read-only context");
        assert!(
            err.to_string().contains("COPY"),
            "error should cite COPY, got: {err}"
        );
    }

    /// `PREPARE` mutates session state and the prepared statement could later be
    /// `EXECUTE`d to run DDL/DML. The strict read-only validator must reject it.
    #[tokio::test]
    async fn test_read_only_validator_rejects_prepare() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("PREPARE my_plan AS SELECT * FROM tbl_writable")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan)
            .expect_err("PREPARE must be rejected in read-only context");
    }

    /// `EXECUTE` can invoke a prepared DDL/DML statement and must therefore be
    /// rejected by the strict read-only validator.
    #[tokio::test]
    async fn test_read_only_validator_rejects_execute() {
        let df = create_test_datafusion();

        // PREPARE first to get a prepared plan on the session, then verify EXECUTE
        // is rejected. PREPARE itself is also rejected, so run it through the
        // non-strict path by bypassing the validator.
        df.ctx
            .sql("PREPARE my_plan AS SELECT 1")
            .await
            .expect("prepare should succeed");

        let plan = df
            .ctx
            .state()
            .create_logical_plan("EXECUTE my_plan")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan)
            .expect_err("EXECUTE must be rejected in read-only context");
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_deallocate() {
        let df = create_test_datafusion();

        df.ctx
            .sql("PREPARE my_plan AS SELECT 1")
            .await
            .expect("prepare should succeed");

        let plan = df
            .ctx
            .state()
            .create_logical_plan("DEALLOCATE my_plan")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan)
            .expect_err("DEALLOCATE must be rejected in read-only context");
    }

    /// Spice's custom planner represents DDL/DML as [`LogicalPlan::Extension`]
    /// nodes (e.g. `DdlExtensionNode`, `DmlExtensionNode`, `DistributedCayenne*Node`).
    /// The strict read-only validator must reject those by
    /// [`UserDefinedLogicalNodeCore::name`] so a write-capable plan cannot
    /// bypass the check by being wrapped in an extension node.
    ///
    /// Constructing a real `DmlExtensionNode` requires a full catalog-handler
    /// wiring, so this test uses a minimal stub extension node whose `.name()`
    /// matches one of the names in [`WRITE_CAPABLE_EXTENSION_NAMES`] to
    /// exercise the name-based deny directly.
    #[tokio::test]
    async fn test_read_only_validator_rejects_write_capable_extension_node() {
        use datafusion::{
            common::{DFSchema, DFSchemaRef},
            logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore},
        };
        use std::cmp::Ordering;
        use std::fmt;

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct StubWriteExtension {
            schema: DFSchemaRef,
            name: &'static str,
        }

        impl PartialOrd for StubWriteExtension {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                self.name.partial_cmp(other.name)
            }
        }

        impl UserDefinedLogicalNodeCore for StubWriteExtension {
            fn name(&self) -> &'static str {
                self.name
            }
            fn inputs(&self) -> Vec<&LogicalPlan> {
                vec![]
            }
            fn schema(&self) -> &DFSchemaRef {
                &self.schema
            }
            fn expressions(&self) -> Vec<Expr> {
                vec![]
            }
            fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "StubWriteExtension({})", self.name)
            }
            fn with_exprs_and_inputs(
                &self,
                _exprs: Vec<Expr>,
                _inputs: Vec<LogicalPlan>,
            ) -> Result<Self, DataFusionError> {
                Ok(self.clone())
            }
        }

        let schema: DFSchemaRef = Arc::new(DFSchema::empty());

        for banned_name in super::WRITE_CAPABLE_EXTENSION_NAMES {
            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(StubWriteExtension {
                    schema: Arc::clone(&schema),
                    name: banned_name,
                }),
            });
            let err = validate_sql_query_read_only(&plan)
                .err()
                .unwrap_or_else(|| {
                    panic!("extension '{banned_name}' must be rejected in read-only context")
                });
            let err_msg = err.to_string();
            assert!(
                err_msg.contains(banned_name) && err_msg.contains("read-only"),
                "error should cite '{banned_name}' and read-only, got: {err}"
            );
        }

        // A benign (non-write) extension name must still be allowed so
        // read-only optimizer extensions such as `IndexTableScanNode` and
        // `DuckDBAggregatePushdownNode` are not blocked.
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(StubWriteExtension {
                schema,
                name: "IndexTableScanNode",
            }),
        });
        validate_sql_query_read_only(&plan)
            .expect("non-write extension must be allowed in read-only context");
    }

    /// Integration check: DDL/DML produced through Spice's planner wrapper
    /// ([`DataFusion::create_logical_plan`]) — rather than `DataFusion`'s raw
    /// planner — must still be rejected. This covers the statement-level
    /// rewrites (`plan_distributed_dml`, `plan_create_table`, etc.) that can
    /// emit `LogicalPlan::Extension` instead of `LogicalPlan::{Ddl,Dml}`.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_read_only_validator_rejects_dml_via_spice_planner() {
        let df = create_test_datafusion();
        let session = df.ctx.state();

        // INSERT is dispatched through Spice's `plan_distributed_dml`. Without a
        // distributed cluster it returns a standard `LogicalPlan::Dml`, still
        // covered by the read-only arm — but this exercises the Spice wrapper
        // rather than `SessionState::create_logical_plan` directly.
        let plan = df
            .create_logical_plan(&session, "INSERT INTO tbl_writable VALUES (1, 'foo', 42.0)")
            .await
            .expect("plan should be created via Spice planner");

        validate_sql_query_read_only(&plan)
            .expect_err("INSERT via Spice planner must be rejected in read-only context");
    }

    #[test]
    fn test_is_safe_set_variable_allowlist() {
        // Allowed: display / optimizer / execution namespaces, any case.
        assert!(is_safe_set_variable("datafusion.explain.show_statistics"));
        assert!(is_safe_set_variable(
            "datafusion.optimizer.enable_round_robin_repartition"
        ));
        assert!(is_safe_set_variable(
            "datafusion.execution.target_partitions"
        ));
        assert!(is_safe_set_variable("DATAFUSION.EXECUTION.BATCH_SIZE"));

        // Rejected: pinned parquet sub-namespace, and anything outside the allowlist.
        assert!(!is_safe_set_variable(
            "datafusion.execution.parquet.pushdown_filters"
        ));
        assert!(!is_safe_set_variable("datafusion.catalog.default_catalog"));
        assert!(!is_safe_set_variable("datafusion.sql_parser.dialect"));
        assert!(!is_safe_set_variable("datafusion.runtime.memory_limit"));
        assert!(!is_safe_set_variable("some_other_var"));
    }

    #[tokio::test]
    async fn test_validate_set_safe_variable_allowed() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("SET datafusion.explain.show_statistics = true")
            .await
            .expect("plan should be created");

        validate_sql_query_operations(&plan, &df)
            .expect("SET of a safe display config must be allowed");
    }

    #[tokio::test]
    async fn test_validate_set_unsafe_variable_blocked() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("SET datafusion.catalog.default_catalog = 'evil'")
            .await
            .expect("plan should be created");

        validate_sql_query_operations(&plan, &df)
            .expect_err("SET of a non-allowlisted config must be rejected");
    }

    #[tokio::test]
    async fn test_read_only_validator_allows_safe_set() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("SET datafusion.execution.target_partitions = 8")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan)
            .expect("SET of a safe perf config must be allowed under read-only auth");
    }

    #[tokio::test]
    async fn test_read_only_validator_rejects_unsafe_set() {
        let df = create_test_datafusion();

        let plan = df
            .ctx
            .state()
            .create_logical_plan("SET datafusion.execution.parquet.pushdown_filters = false")
            .await
            .expect("plan should be created");

        validate_sql_query_read_only(&plan).expect_err(
            "SET of a pinned parquet config must be rejected even under read-only auth",
        );
    }
}
