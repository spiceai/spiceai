// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Persistent implementation of a Vortex table provider.
mod access_plan;
mod cache;
pub use cache::synthetic_object_meta;
mod format;
pub mod metrics;
mod opener;
mod reader;
mod segment_cache;
mod sink;
mod source;
mod stream;

pub use access_plan::{VortexAccessPlan, VortexAccessPlanProvider};
pub use format::ProjectionPushdown;
pub use format::ScanConcurrency;
pub use format::VortexFormat;
pub use format::VortexFormatFactory;
pub use format::VortexTableOptions;
pub use format::WriteShardConfig;
pub use segment_cache::{
    install_process_segment_cache, process_segment_cache_capacity_bytes,
    register_segment_cache_metrics,
};
pub use source::VortexSource;

#[cfg(test)]
mod tests {

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion_physical_plan::display::DisplayableExecutionPlan;
    use insta::assert_snapshot;
    use rstest::rstest;
    use vortex::VortexSessionDefault;
    use vortex::array::IntoArray;
    use vortex::array::arrays::ChunkedArray;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::VarBinArray;
    use vortex::array::validity::Validity;
    use vortex::buffer::buffer;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::VortexWrite;
    use vortex::io::object_store::ObjectStoreWrite;
    use vortex::session::VortexSession;

    use crate::common_tests::TestSessionContext;

    #[rstest]
    #[tokio::test]
    async fn test_query_file(#[values(Some(1), None)] limit: Option<usize>) -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        let session = VortexSession::default();

        let strings = ChunkedArray::from_iter([
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        ])
        .into_array();

        let numbers = ChunkedArray::from_iter([
            buffer![1u32, 2, 3, 4].into_array(),
            buffer![5u32, 6, 7, 8].into_array(),
        ])
        .into_array();

        let st = StructArray::try_new(
            ["strings", "numbers"].into(),
            vec![strings, numbers],
            8,
            Validity::NonNullable,
        )?;

        let mut writer = ObjectStoreWrite::new(ctx.store.clone(), &"test.vortex".into()).await?;

        let summary = session
            .write_options()
            .write(&mut writer, st.into_array().to_array_stream())
            .await?;

        writer.shutdown().await?;

        assert_eq!(summary.row_count(), 8);

        let read_row_count = ctx
            .session
            .sql("SELECT * from '/test.vortex'")
            .await?
            .limit(0, limit)?
            .count()
            .await?;

        assert_eq!(read_row_count, limit.unwrap_or(8));

        Ok(())
    }

    /// A `Map` column has to survive a full write/read cycle through a Vortex file.
    ///
    /// Vortex has no `Map` dtype: it aliases the type to `List<Struct<keys, values>>` on
    /// write and rebuilds the map on read from the table's declared schema. Both halves of
    /// that alias live in the `spiceai/vortex` fork, and half of it has been lost across a
    /// fork re-cut once already (spiceai/spiceai#13524), which is only observable at
    /// runtime: the dtype conversion still accepts `Map`, so a table is created happily and
    /// then every write fails with "Array encoding not implemented for Arrow data type
    /// Map(...)". This test fails in Spice if either half goes missing again.
    #[tokio::test]
    async fn map_column_roundtrips_through_a_vortex_file() -> anyhow::Result<()> {
        use std::sync::Arc;

        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::array::RecordBatch;
        use datafusion::arrow::array::builder::MapBuilder;
        use datafusion::arrow::array::builder::StringBuilder;
        use datafusion::arrow::datatypes::DataType;
        use datafusion::arrow::datatypes::Field;
        use datafusion::arrow::datatypes::Fields;
        use datafusion::arrow::datatypes::Schema;
        use datafusion::dataframe::DataFrameWriteOptions;
        use datafusion::datasource::listing::ListingOptions;
        use datafusion::datasource::listing::ListingTable;
        use datafusion::datasource::listing::ListingTableConfig;
        use datafusion::datasource::listing::ListingTableUrl;

        use crate::VortexFormat;

        let ctx = TestSessionContext::default();

        // The shape the HTTP connector produces for `response_headers`.
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        );
        let map_type = DataType::Map(Arc::new(entries), false);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("headers", map_type.clone(), true),
        ]));

        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        builder.keys().append_value("content-type");
        builder.values().append_value("application/json");
        builder.keys().append_value("etag");
        builder.values().append_value("\"abc\"");
        builder.append(true)?;
        builder.append(false)?;
        builder.keys().append_value("content-type");
        builder.values().append_value("text/plain");
        builder.append(true)?;
        let maps = builder.finish();

        // `RecordBatch::try_new` rejects a column whose type differs from the schema, which
        // is what checks that `MapBuilder` still names the entries and its fields
        // `entries`/`keys`/`values` the way the schema above declares.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(maps)],
        )?;

        let format = Arc::new(VortexFormat::new(VortexSession::default()));
        let config = ListingTableConfig::new(ListingTableUrl::parse("file:///maps/")?)
            .with_listing_options(ListingOptions::new(format))
            .with_schema(Arc::clone(&schema));
        ctx.session
            .register_table("maps", Arc::new(ListingTable::try_new(config)?))?;

        ctx.session
            .read_batch(batch)?
            .write_table("maps", DataFrameWriteOptions::new())
            .await?;

        let read_back = ctx
            .session
            .sql("SELECT id, headers FROM maps ORDER BY id")
            .await?;
        assert_eq!(
            read_back.schema().field(1).data_type(),
            &map_type,
            "a map column must not read back as its List<Struct> storage"
        );

        // The snapshot pins every row, the null map included.
        let batches = read_back.collect().await?;
        assert_snapshot!(
            "map_column_roundtrip_result",
            pretty_format_batches(&batches)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_addition_pushdown() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE written_data \
                    (a TINYINT NOT NULL) \
                STORED AS vortex \
                LOCATION '/test/'",
            )
            .await?;

        ctx.session
            .sql("INSERT INTO written_data VALUES (0), (1), (2), (3), (4)")
            .await?
            .collect()
            .await?;

        let result = ctx
            .session
            .sql("SELECT a, a + 5 as five, a + 6 as six FROM written_data WHERE a + 5 > 7")
            .await?
            .collect()
            .await?;

        assert_snapshot!("addition_pushdown_result", pretty_format_batches(&result)?);

        Ok(())
    }

    #[tokio::test]
    async fn create_table_ordered_by() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        // Vortex
        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl_vx \
                (c1 VARCHAR NOT NULL, c2 INT NOT NULL) \
                STORED AS vortex  \
                WITH ORDER (c1 ASC)
                LOCATION '/test/'",
            )
            .await?;

        ctx.session
            .sql("INSERT INTO my_tbl_vx VALUES ('air', 5), ('balloon', 42)")
            .await?
            .collect()
            .await?;

        ctx.session
            .sql("INSERT INTO my_tbl_vx VALUES ('zebra', 5)")
            .await?
            .collect()
            .await?;

        ctx.session
            .sql("INSERT INTO my_tbl_vx VALUES ('texas', 2000), ('alabama', 2000)")
            .await?
            .collect()
            .await?;

        let df = ctx
            .session
            .sql("SELECT * FROM my_tbl_vx ORDER BY c1 ASC limit 3")
            .await?;

        let physical_plan = ctx
            .session
            .state()
            .create_physical_plan(df.logical_plan())
            .await?;

        assert_snapshot!(
            "create_table_ordered_by_plan",
            DisplayableExecutionPlan::new(physical_plan.as_ref())
                .tree_render()
                .to_string()
        );

        let r = df.collect().await?;

        assert_snapshot!("create_table_ordered_by_result", pretty_format_batches(&r)?);

        Ok(())
    }

    /// Returns the indented physical plan for `sql`, used to assert whether a
    /// predicate pushed into the Vortex scan (shown as `predicate:` on the
    /// `DataSourceExec`) or was left in a `FilterExec` above it.
    async fn physical_plan_display(
        session: &datafusion::prelude::SessionContext,
        sql: &str,
    ) -> anyhow::Result<String> {
        let df = session.sql(sql).await?;
        let plan = session
            .state()
            .create_physical_plan(df.logical_plan())
            .await?;
        Ok(DisplayableExecutionPlan::new(plan.as_ref())
            .indent(true)
            .to_string())
    }

    /// End-to-end coverage for `CAST(CASE ...)` pushdown and the `ELSE` guard.
    ///
    /// A non-elided type-changing cast over a `CASE` (`CAST(CASE ... AS DOUBLE)`)
    /// must push into the Vortex scan when the `CASE` has an `ELSE` branch, and
    /// must stay in a `FilterExec` above the scan when it does not (Vortex cannot
    /// represent a `CASE` without `ELSE`). Both must return identical, correct
    /// results across `NULL`s. Also guards that `coalesce(col, literal)` — which
    /// `DataFusion` lowers to a `CASE` — keeps pushing.
    #[tokio::test]
    async fn test_cast_case_and_coalesce_pushdown() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE nums (id INT NOT NULL, v BIGINT) \
                STORED AS vortex LOCATION '/nums/'",
            )
            .await?;
        ctx.session
            .sql("INSERT INTO nums VALUES (1, 10), (2, NULL), (3, 5), (4, NULL), (5, 20)")
            .await?
            .collect()
            .await?;

        // 1. CAST(CASE ... ELSE ... AS DOUBLE) pushes into the scan.
        //    Per row: v>5 ? v : 0, cast to f64, keep when > 8.0.
        //    v=10→10 keep(1); NULL→0 drop; 5→0 drop; NULL→0 drop; 20→20 keep(5).
        let else_query = "SELECT id FROM nums \
             WHERE CAST(CASE WHEN v > 5 THEN v ELSE 0 END AS DOUBLE) > 8.0 ORDER BY id";
        let else_plan = physical_plan_display(&ctx.session, else_query).await?;
        assert!(
            else_plan.contains("predicate:") && !else_plan.contains("FilterExec"),
            "CAST(CASE ... ELSE) should push into the scan, got plan:\n{else_plan}"
        );
        let else_result = ctx.session.sql(else_query).await?.collect().await?;
        let else_fmt = pretty_format_batches(&else_result)?.to_string();
        assert_snapshot!("cast_case_else_result", else_fmt);

        // 2. CAST(CASE ... END AS DOUBLE) *without* ELSE must NOT push (the guard),
        //    but must return the same rows: v>5 ? v : NULL, cast, keep when > 8.0.
        //    NULL comparisons drop, so the matching rows are identical to case 1.
        let no_else_query = "SELECT id FROM nums \
             WHERE CAST(CASE WHEN v > 5 THEN v END AS DOUBLE) > 8.0 ORDER BY id";
        let no_else_plan = physical_plan_display(&ctx.session, no_else_query).await?;
        assert!(
            no_else_plan.contains("FilterExec") && !no_else_plan.contains("predicate:"),
            "CAST(CASE) without ELSE must stay in a FilterExec above the scan and must \
             NOT be pushed into the scan, got plan:\n{no_else_plan}"
        );
        let no_else_result = ctx.session.sql(no_else_query).await?.collect().await?;
        assert_eq!(
            else_fmt,
            pretty_format_batches(&no_else_result)?.to_string(),
            "ELSE and no-ELSE forms must return identical rows here"
        );

        // 3. coalesce(v, 0) is lowered by DataFusion to a CASE and still pushes.
        //    v ?? 0 > 8: 10 keep(1); NULL→0 drop; 5 drop; NULL→0 drop; 20 keep(5).
        let coalesce_query = "SELECT id FROM nums WHERE coalesce(v, 0) > 8 ORDER BY id";
        let coalesce_plan = physical_plan_display(&ctx.session, coalesce_query).await?;
        assert!(
            coalesce_plan.contains("predicate:") && !coalesce_plan.contains("FilterExec"),
            "coalesce(col, literal) should push into the scan, got plan:\n{coalesce_plan}"
        );
        let coalesce_result = ctx.session.sql(coalesce_query).await?.collect().await?;
        assert_eq!(
            else_fmt,
            pretty_format_batches(&coalesce_result)?.to_string(),
            "coalesce(v, 0) > 8 must match the CASE-based result"
        );

        Ok(())
    }

    /// Boolean `NOT` filters (including `col = false`, which `DataFusion`
    /// normalizes to `NOT col`) must push into the Vortex scan and must honor SQL
    /// three-valued logic — a `NULL` operand yields `NULL`, which `WHERE` treats
    /// as "not kept".
    #[tokio::test]
    async fn test_not_boolean_filter_pushes_down_and_handles_nulls() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE flags (id INT NOT NULL, active BOOLEAN) \
                STORED AS vortex LOCATION '/flags/'",
            )
            .await?;

        ctx.session
            .sql(
                "INSERT INTO flags VALUES \
                    (1, true), (2, false), (3, NULL), (4, false), (5, true)",
            )
            .await?
            .collect()
            .await?;

        // The predicate must be pushed into the Vortex `DataSourceExec` (shown as
        // `predicate:` on the scan) and there must be no `FilterExec` re-applying it
        // above the scan — that is the whole point of the pushdown.
        let df = ctx
            .session
            .sql("SELECT id FROM flags WHERE NOT active")
            .await?;
        let plan = ctx
            .session
            .state()
            .create_physical_plan(df.logical_plan())
            .await?;
        let plan_display = DisplayableExecutionPlan::new(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            plan_display.contains("predicate:") && plan_display.contains("vortex"),
            "NOT filter should push into the Vortex scan, got plan:\n{plan_display}"
        );
        assert!(
            !plan_display.contains("FilterExec"),
            "NOT filter should not leave a FilterExec above the scan, got plan:\n{plan_display}"
        );

        // Correctness across three-valued logic: true → dropped, false → kept,
        // NULL → dropped. Rows 2 and 4 (active = false) are the only matches.
        let result = ctx
            .session
            .sql("SELECT id FROM flags WHERE NOT active ORDER BY id")
            .await?
            .collect()
            .await?;
        let result_fmt = pretty_format_batches(&result)?.to_string();
        assert_snapshot!("not_boolean_filter_result", result_fmt);

        // `active = false` is simplified by DataFusion to `NOT active`; it must
        // return the identical set, proving the pushdown covers the common
        // boolean-equals-false shape too.
        let eq_false = ctx
            .session
            .sql("SELECT id FROM flags WHERE active = false ORDER BY id")
            .await?
            .collect()
            .await?;
        assert_eq!(
            result_fmt,
            pretty_format_batches(&eq_false)?.to_string(),
            "`active = false` must match `NOT active`"
        );

        Ok(())
    }

    /// Doc example: demonstrates creating, writing, reading, and filtering a Vortex table.
    #[tokio::test]
    async fn doc_example() -> anyhow::Result<()> {
        // [setup]
        use std::sync::Arc;

        use datafusion::datasource::provider::DefaultTableFactory;
        use datafusion::execution::SessionStateBuilder;
        use datafusion::prelude::SessionContext;
        use datafusion_common::GetExt;
        use object_store::memory::InMemory;

        use crate::VortexFormatFactory;

        let factory = Arc::new(VortexFormatFactory::new());
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_table_factory(
                factory.get_ext().to_uppercase(),
                Arc::new(DefaultTableFactory::new()),
            )
            .with_file_formats(vec![factory])
            .build();
        let ctx = SessionContext::new_with_state(state).enable_url_table();
        // [setup]

        // Register an in-memory object store for the test.
        let store = Arc::new(InMemory::new());
        ctx.register_object_store(
            &url::Url::try_from("file://").expect("file:// should parse as a URL"),
            store,
        );

        // [create]
        ctx.sql(
            "CREATE EXTERNAL TABLE my_table \
                (name VARCHAR NOT NULL, age INT NOT NULL) \
            STORED AS vortex \
            LOCATION '/demo/'",
        )
        .await?;
        // [create]

        // [write]
        ctx.sql(
            "INSERT INTO my_table VALUES \
                ('Alice', 30), ('Bob', 25), ('Charlie', 35), ('Diana', 28)",
        )
        .await?
        .collect()
        .await?;
        // [write]

        // [query]
        let result = ctx
            .sql("SELECT name, age FROM my_table WHERE age > 28 ORDER BY age")
            .await?
            .collect()
            .await?;
        // [query]

        assert_snapshot!("doc_example_result", pretty_format_batches(&result)?);

        Ok(())
    }

    /// Regression test for spiceai/vortex#51: casts of `DECIMAL` columns to
    /// primitive floating-point arrays must apply the decimal scale and
    /// preserve validity through our vendored DataFusion adapter.
    #[tokio::test]
    async fn test_decimal_to_float_cast_applies_scale() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE amounts \
                    (amount DECIMAL(15, 2)) \
                STORED AS vortex \
                LOCATION '/decimal_cast/'",
            )
            .await?;

        ctx.session
            .sql(
                "INSERT INTO amounts VALUES \
                    (CAST(1.23 AS DECIMAL(15, 2))), \
                    (CAST(-4.56 AS DECIMAL(15, 2))), \
                    (NULL), \
                    (CAST(100.00 AS DECIMAL(15, 2)))",
            )
            .await?
            .collect()
            .await?;

        let result = ctx
            .session
            .sql(
                "SELECT CAST(amount AS DOUBLE) AS amount_f64 \
                 FROM amounts ORDER BY amount NULLS LAST",
            )
            .await?
            .collect()
            .await?;

        assert_snapshot!(
            "decimal_to_float_cast_result",
            pretty_format_batches(&result)?
        );

        Ok(())
    }

    /// Run `sql` and read the single `Int64` it returns.
    ///
    /// The fork guards below assert counts rather than rendered batches on purpose: a
    /// snapshot can be regenerated, and a guard that a lost patch can be made to pass
    /// by re-recording it guards nothing.
    async fn scalar_count(ctx: &TestSessionContext, sql: &str) -> anyhow::Result<i64> {
        use datafusion::arrow::array::AsArray as _;
        use datafusion::arrow::datatypes::Int64Type;

        let batches = ctx.session.sql(sql).await?.collect().await?;
        let total = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .sum();
        Ok(total)
    }

    /// Vortex has to be able to cast a `vortex.date` column to `vortex.timestamp`.
    ///
    /// Vortex stores a `Date32` column as the `vortex.date` extension type, and a
    /// pushed-down `CAST(date_col AS TIMESTAMP)` is evaluated by Vortex rather than
    /// by `DataFusion`. Upstream Vortex refuses that cast; the kernel that performs
    /// it lives in the `spiceai/vortex` fork (fork PR #28), and a re-cut that drops
    /// it takes no build with it — the plan still pushes the filter down and the
    /// scan then fails on a cast Vortex no longer knows how to do.
    ///
    /// This asserts the kernel directly rather than through a SQL filter, because
    /// the two are not the same question: the kernel is registered on
    /// `ExtensionArray`, and the scan also evaluates the pushed-down predicate
    /// against *constant* arrays built from chunk statistics, which reach a
    /// different cast path the fork does not patch. That second path fails today
    /// (`No CastReduce to cast constant array from vortex.date[days] to
    /// vortex.timestamp[ns]`), so a SQL-level assertion would be pinning a bug
    /// rather than the patch.
    #[test]
    fn test_date_to_timestamp_extension_cast() -> anyhow::Result<()> {
        use datafusion::arrow::array::{Array as _, AsArray as _, Date32Array};
        use datafusion::arrow::datatypes::{DataType, Field, TimeUnit, TimestampMillisecondType};
        use vortex::array::ArrayRef as VortexArrayRef;
        use vortex::array::VortexSessionExecute;
        use vortex::array::builtins::ArrayBuiltins;
        use vortex::arrow::{ArrowSessionExt, FromArrowArray, FromArrowType};
        use vortex::dtype::{DType, Nullability};

        // 1970-01-01, 2024-01-15, and a NULL, so the cast has to carry validity as
        // well as values.
        let dates = Date32Array::from(vec![Some(0), Some(19_737), None]);
        let source = VortexArrayRef::from_arrow(&dates, true)?;

        let millis = DataType::Timestamp(TimeUnit::Millisecond, None);
        let target = DType::from_arrow((&millis, Nullability::Nullable));
        let cast = source.cast(target.clone())?;
        assert_eq!(
            cast.dtype(),
            &target,
            "the cast has to land on the target type"
        );

        // Read the values back rather than stopping at the type: a cast that lands
        // on `vortex.timestamp` and puts the wrong instants in it is the failure
        // this guard is for, and it would pass a type-and-length assertion.
        let session = VortexSession::default();
        let arrow = session.arrow().execute_arrow(
            cast,
            Some(&Field::new("event_ts", millis, true)),
            &mut session.create_execution_ctx(),
        )?;
        let timestamps = arrow.as_primitive::<TimestampMillisecondType>();

        assert_eq!(timestamps.len(), 3, "the cast has to preserve every row");
        assert_eq!(timestamps.value(0), 0, "1970-01-01 is the epoch");
        assert_eq!(
            timestamps.value(1),
            1_705_276_800_000,
            "2024-01-15 is 19_737 days after the epoch, at midnight"
        );
        assert!(
            timestamps.is_null(2),
            "a NULL date has to stay NULL through the cast, not become the epoch"
        );

        Ok(())
    }

    /// A large `IN` list has to stay evaluable.
    ///
    /// An `IN (…)` filter is pushed into the Vortex scan as one `list_contains`
    /// call, and Vortex evaluates it by OR-ing one equality array per list element.
    /// Upstream accumulates those into a left-deep chain, so a list of N elements
    /// builds a tree N deep and evaluating it recurses N frames — a large enough
    /// `IN` list overflows the stack and takes the process down. The fork balances
    /// the OR tree to depth `log2(N)` instead (fork PR #37).
    ///
    /// Losing the balance is invisible to the compiler: the same call, the same
    /// results for the small lists every other test uses. This one is sized past the
    /// point where a left-deep chain is a problem, so if the patch goes missing it
    /// stops passing — by failing, or by crashing the test binary, which `nextest`
    /// reports either way.
    #[tokio::test]
    async fn test_large_in_list_filter_pushdown_stays_evaluable() -> anyhow::Result<()> {
        // Deep enough that a left-deep OR chain is thousands of levels, small enough
        // that the balanced form is a handful of milliseconds.
        const IN_LIST_LEN: i32 = 8_192;
        // Rows are 0..ROWS. The IN list starts at ROWS / 2, so half of it matches a
        // row and half of it matches nothing — a list that matched everything would
        // pass on a filter that was dropped rather than evaluated.
        const ROWS: i32 = 2_048;

        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE ids \
                    (id INT NOT NULL) \
                STORED AS vortex \
                LOCATION '/large_in_list/'",
            )
            .await?;

        let values = (0..ROWS)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(", ");
        ctx.session
            .sql(&format!("INSERT INTO ids VALUES {values}"))
            .await?
            .collect()
            .await?;

        let in_list = (0..IN_LIST_LEN)
            .map(|offset| (ROWS / 2 + offset).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let matched = scalar_count(
            &ctx,
            &format!("SELECT count(*) FROM ids WHERE id IN ({in_list})"),
        )
        .await?;

        assert_eq!(
            matched,
            i64::from(ROWS / 2),
            "the IN list covers the upper half of the rows and nothing else"
        );

        Ok(())
    }
}
