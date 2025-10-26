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
    common::{plan_err, tree_node::TreeNodeRecursion},
    error::DataFusionError,
    logical_expr::LogicalPlan,
};

use crate::datafusion::DataFusion;

/// Validates that a logical plan only performs allowed operations on datasets.
///
/// Reads (SELECT queries) are allowed on all tables.
/// INSERT operations are only allowed on datasets that are configured as writable,
/// and are not allowed on internal Spice tables.
/// DDL, DML (other than allowed INSERT), COPY, and Statement operations are not permitted.
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
            plan_err!("Operation is not allowed: {}", ddl.name())
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

                // Check if attempting to write into a catalog. The default catalog name indicates writing to a table registered as a dataset, not via a catalog.
                if let Some(catalog) = dml.table_name.catalog() && catalog != super::SPICE_DEFAULT_CATALOG {
                    if !df.is_catalog_writable(catalog) {
                        return plan_err!(
                            "INSERT operations are not allowed on read-only catalog table '{}'. Verify the catalog is configured with 'access: read_write' and try again.",
                            dml.table_name
                        );
                    }
                    return Ok(TreeNodeRecursion::Continue);
                }

                if df.is_writable(&dml.table_name) {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    plan_err!(
                        "INSERT operations are not allowed on read-only dataset '{}'. Verify the dataset is configured with 'access: read_write' and try again.",
                        dml.table_name
                    )
                }
            } else { plan_err!("Operation is not allowed: {}", dml.name()) }
        }
        LogicalPlan::Copy(_) => {
            plan_err!("COPY operations are not allowed")
        }
        LogicalPlan::Statement(stmt) => {
            plan_err!("Statements are not allowed: {}", stmt.name())
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
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

        let sql = "UPDATE tbl_writable SET name = 'updated' WHERE id = 1";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // UPDATE operations should be blocked
        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "UPDATE operations should be blocked");
    }

    #[tokio::test]
    async fn test_validate_delete_operation_blocked() {
        let df = create_test_datafusion();

        let sql = "DELETE FROM tbl_writable WHERE id = 1";
        let plan = df
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("plan should be created");

        // DELETE operations should be blocked
        let result = validate_sql_query_operations(&plan, &df);
        assert!(result.is_err(), "DELETE operations should be blocked");
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
}
