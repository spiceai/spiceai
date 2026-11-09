// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Rewrite the suites' SQL into the dialect of a standalone oracle, keeping its
//! meaning.
//!
//! The TPC-H, TPC-DS, ClickBench and CH-benCHmark queries are written for
//! DataFusion. SQLite and ClickHouse (chDB) parse most of them, but several
//! constructs either fail to parse or — worse — parse and mean something else:
//! SQLite compares dates as text and has no `INTERVAL`, ClickHouse's `/` is always
//! floating-point and its `NULL`s sort last under `DESC`. Each rule below maps one
//! such construct onto the oracle's spelling of the same operation.
//!
//! The rewrite works on the parsed statement only. It never plans the query in
//! DataFusion: an oracle fed DataFusion's reading of a query would agree with
//! Cayenne on exactly the DataFusion bugs it exists to catch. The column types it
//! needs come from the fixture's Arrow schemas ([`ColumnKinds`]).
//!
//! Every rule is meaning-preserving or fails. Where the oracle has no faithful
//! spelling, [`untranslatable`] names the construct so the inventory records a
//! falsifiable exclusion, and [`translate`] returns an error rather than a
//! rewrite that would compare something else.

use std::collections::HashMap;
use std::ops::ControlFlow;

use arrow::datatypes::{DataType as ArrowType, Schema};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, CastKind, DataType, DateTimeField, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, Interval, ObjectName,
    ObjectNamePart, OrderByExpr, OrderByKind, Query, SelectItem, SetExpr, Statement, TableFactor,
    Value, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// A standalone engine the suites compare Cayenne against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Oracle {
    Sqlite,
    ClickHouse,
}

/// What an expression evaluates to, as far as the rewrites need to tell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Integer,
    Decimal,
    Float,
    Date,
    Timestamp,
    Text,
    Boolean,
}

impl Kind {
    #[must_use]
    pub fn of_arrow(data_type: &ArrowType) -> Option<Self> {
        match data_type {
            t if t.is_integer() => Some(Self::Integer),
            ArrowType::Decimal32(..)
            | ArrowType::Decimal64(..)
            | ArrowType::Decimal128(..)
            | ArrowType::Decimal256(..) => Some(Self::Decimal),
            ArrowType::Float16 | ArrowType::Float32 | ArrowType::Float64 => Some(Self::Float),
            ArrowType::Date32 | ArrowType::Date64 => Some(Self::Date),
            ArrowType::Timestamp(..) => Some(Self::Timestamp),
            ArrowType::Utf8 | ArrowType::LargeUtf8 | ArrowType::Utf8View => Some(Self::Text),
            ArrowType::Boolean => Some(Self::Boolean),
            _ => None,
        }
    }

    fn of_sql(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::Integer(_)
            | DataType::BigInt(_) => Some(Self::Integer),
            DataType::Decimal(_) | DataType::Numeric(_) | DataType::Dec(_) => Some(Self::Decimal),
            DataType::Float(_)
            | DataType::Real
            | DataType::Double(_)
            | DataType::DoublePrecision => Some(Self::Float),
            DataType::Date | DataType::Date32 => Some(Self::Date),
            DataType::Timestamp(..) | DataType::Datetime(_) => Some(Self::Timestamp),
            DataType::Char(_)
            | DataType::Varchar(_)
            | DataType::CharVarying(_)
            | DataType::CharacterVarying(_)
            | DataType::Text
            | DataType::String(_) => Some(Self::Text),
            DataType::Boolean | DataType::Bool => Some(Self::Boolean),
            _ => None,
        }
    }
}

/// Column name → [`Kind`], for the tables a suite loads.
///
/// Names are matched case-insensitively and without their table qualifier,
/// which is sound for these suites: each prefixes its column names by table
/// (`l_shipdate`, `ss_sold_date_sk`), and a name two tables share with
/// different kinds is dropped rather than guessed.
#[derive(Clone, Debug, Default)]
pub struct ColumnKinds(HashMap<String, Option<Kind>>);

impl ColumnKinds {
    #[must_use]
    pub fn from_schemas<'a>(schemas: impl IntoIterator<Item = &'a Schema>) -> Self {
        let mut kinds: HashMap<String, Option<Kind>> = HashMap::new();
        for schema in schemas {
            for field in schema.fields() {
                let kind = Kind::of_arrow(field.data_type());
                kinds
                    .entry(field.name().to_ascii_lowercase())
                    .and_modify(|seen| {
                        if *seen != kind {
                            *seen = None;
                        }
                    })
                    .or_insert(kind);
            }
        }
        Self(kinds)
    }

    fn get(&self, name: &str) -> Option<Kind> {
        self.0.get(&name.to_ascii_lowercase()).copied().flatten()
    }

    fn contains(&self, name: &str) -> bool {
        self.0.contains_key(&name.to_ascii_lowercase())
    }
}

/// Why `oracle` cannot run `sql` with its meaning intact, or `None` when a
/// translation exists. A property of the SQL, checkable by running it.
#[must_use]
pub fn untranslatable(sql: &str, oracle: Oracle) -> Option<&'static str> {
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return Some("the statement does not parse as SQL");
    };
    let mut finder = UnsupportedConstruct {
        oracle,
        found: None,
    };
    for statement in &statements {
        let _ = statement.visit(&mut finder);
    }
    finder.found
}

struct UnsupportedConstruct {
    oracle: Oracle,
    found: Option<&'static str>,
}

impl Visitor for UnsupportedConstruct {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.oracle != Oracle::Sqlite {
            return ControlFlow::Continue(());
        }
        let reason = match expr {
            Expr::Rollup(_) | Expr::Cube(_) | Expr::GroupingSets(_) => {
                Some("SQLite has no GROUP BY ROLLUP, CUBE or GROUPING SETS")
            }
            Expr::Function(function) => match function_name(function).as_str() {
                "grouping" => Some("SQLite has no GROUP BY ROLLUP, CUBE or GROUPING SETS"),
                "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop" => {
                    Some(
                        "SQLite as bundled here has no stddev_samp or variance aggregate, \
                         and no sqrt to build one",
                    )
                }
                name if name.starts_with("regexp_") => {
                    Some("SQLite has no regular-expression functions")
                }
                _ => None,
            },
            _ => None,
        };
        match reason {
            Some(reason) => {
                self.found = Some(reason);
                ControlFlow::Break(())
            }
            None => ControlFlow::Continue(()),
        }
    }
}

/// Rewrite `sql` for `oracle`. The result is meant to be printed next to any
/// mismatch it produces: a difference on a query nobody can read is untriageable.
///
/// # Errors
/// When `sql` does not parse, holds more than one statement, uses a construct
/// [`untranslatable`] names, or needs a rewrite whose meaning cannot be kept.
pub fn translate(sql: &str, oracle: Oracle, columns: &ColumnKinds) -> Result<String, String> {
    if let Some(reason) = untranslatable(sql, oracle) {
        return Err(reason.to_string());
    }
    let mut statements =
        Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| format!("parse: {e}"))?;
    if statements.len() != 1 {
        return Err(format!(
            "expected one statement, found {}",
            statements.len()
        ));
    }
    let mut statement = statements.remove(0);
    let kinds = KindContext::new(columns, &statement);
    let mut rewriter = Rewriter {
        oracle,
        kinds,
        error: None,
    };
    let _ = VisitMut::visit(&mut statement, &mut rewriter);
    match rewriter.error {
        Some(error) => Err(error),
        None => Ok(statement.to_string()),
    }
}

/// Resolves the [`Kind`] of an expression from the columns and the select-list
/// aliases the statement defines.
struct KindContext<'a> {
    columns: &'a ColumnKinds,
    aliases: HashMap<String, Option<Kind>>,
}

impl<'a> KindContext<'a> {
    fn new(columns: &'a ColumnKinds, statement: &Statement) -> Self {
        let mut context = Self {
            columns,
            aliases: HashMap::new(),
        };
        // An alias may be defined in terms of another (a CTE column used by the
        // outer query), so resolve until nothing changes. Bounded: each pass can
        // only fill a name in, and a statement has finitely many.
        for _ in 0..8 {
            let mut collector = AliasCollector {
                context: &context,
                found: HashMap::new(),
            };
            let _ = statement.visit(&mut collector);
            let found = collector.found;
            if found == context.aliases {
                break;
            }
            context.aliases = found;
        }
        context
    }

    fn name(&self, name: &str) -> Option<Kind> {
        if self.columns.contains(name) {
            return self.columns.get(name);
        }
        self.aliases
            .get(&name.to_ascii_lowercase())
            .copied()
            .flatten()
    }

    fn of(&self, expr: &Expr) -> Option<Kind> {
        match expr {
            Expr::Identifier(ident) => self.name(&ident.value),
            Expr::CompoundIdentifier(parts) => self.name(&parts.last()?.value),
            Expr::Value(value) => match &value.value {
                Value::Number(number, _) => Some(if number.contains(['.', 'e', 'E']) {
                    Kind::Decimal
                } else {
                    Kind::Integer
                }),
                Value::SingleQuotedString(_) => Some(Kind::Text),
                Value::Boolean(_) => Some(Kind::Boolean),
                _ => None,
            },
            Expr::TypedString(typed) => Kind::of_sql(&typed.data_type),
            Expr::Cast { data_type, .. } => Kind::of_sql(data_type),
            Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => self.of(inner),
            Expr::Extract { .. } => Some(Kind::Integer),
            Expr::Substring { .. } => Some(Kind::Text),
            Expr::Case {
                conditions,
                else_result,
                ..
            } => conditions
                .iter()
                .map(|when| &when.result)
                .chain(else_result.as_deref())
                .find_map(|result| self.of(result)),
            Expr::BinaryOp { left, op, right } => self.of_binary(left, op, right),
            Expr::Function(function) => self.of_function(function),
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Exists { .. }
            | Expr::Like { .. } => Some(Kind::Boolean),
            _ => None,
        }
    }

    fn of_binary(&self, left: &Expr, op: &BinaryOperator, right: &Expr) -> Option<Kind> {
        let (l, r) = (self.of(left), self.of(right));
        match op {
            BinaryOperator::Plus | BinaryOperator::Minus => match (l, r) {
                (Some(Kind::Date), _) if is_interval(right) => Some(Kind::Date),
                (Some(Kind::Timestamp), _) if is_interval(right) => Some(Kind::Timestamp),
                _ => numeric_result(l, r),
            },
            // Integer division truncates in DataFusion, so `int / int` stays an
            // integer — the premise of the ClickHouse `intDiv` rewrite below.
            BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Modulo => {
                numeric_result(l, r)
            }
            BinaryOperator::StringConcat => Some(Kind::Text),
            _ => Some(Kind::Boolean),
        }
    }

    fn of_function(&self, function: &Function) -> Option<Kind> {
        let args = unnamed_args(function);
        let first = args.first().and_then(|arg| self.of(arg));
        match function_name(function).as_str() {
            "count" | "rank" | "dense_rank" | "row_number" | "ntile" | "length" | "char_length"
            | "character_length" | "ascii" | "strpos" | "grouping" => Some(Kind::Integer),
            "sum" | "min" | "max" | "abs" | "coalesce" | "nullif" | "round" | "first_value"
            | "last_value" | "lag" | "lead" => first,
            "avg" => match first {
                Some(Kind::Decimal) => Some(Kind::Decimal),
                _ => Some(Kind::Float),
            },
            "stddev" | "stddev_samp" | "stddev_pop" | "variance" | "var_samp" | "var_pop" => {
                Some(Kind::Float)
            }
            "mod" => numeric_result(first, args.get(1).and_then(|arg| self.of(arg))),
            "to_timestamp" | "date_trunc" => Some(Kind::Timestamp),
            "tofloat64" => Some(Kind::Float),
            "substr" | "substring" | "lower" | "upper" | "trim" | "concat" | "regexp_replace" => {
                Some(Kind::Text)
            }
            _ => None,
        }
    }
}

fn numeric_result(left: Option<Kind>, right: Option<Kind>) -> Option<Kind> {
    match (left?, right?) {
        (Kind::Integer, Kind::Integer) => Some(Kind::Integer),
        (Kind::Float, _) | (_, Kind::Float) => Some(Kind::Float),
        (Kind::Decimal | Kind::Integer, Kind::Decimal | Kind::Integer) => Some(Kind::Decimal),
        _ => None,
    }
}

/// Collects each select-list alias's [`Kind`], plus the column names a derived
/// table or CTE alias list gives its subquery's projection.
struct AliasCollector<'c, 'a> {
    context: &'c KindContext<'a>,
    found: HashMap<String, Option<Kind>>,
}

impl AliasCollector<'_, '_> {
    fn record(&mut self, name: &Ident, kind: Option<Kind>) {
        self.found
            .entry(name.value.to_ascii_lowercase())
            .and_modify(|seen| {
                if *seen != kind {
                    *seen = None;
                }
            })
            .or_insert(kind);
    }

    fn record_alias_list(&mut self, names: &[Ident], query: &Query) {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return;
        };
        for (name, item) in names.iter().zip(&select.projection) {
            let kind = match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    self.context.of(expr)
                }
                _ => None,
            };
            self.record(name, kind);
        }
    }
}

impl Visitor for AliasCollector<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let names: Vec<Ident> = cte.alias.columns.iter().map(|c| c.name.clone()).collect();
                self.record_alias_list(&names, &cte.query);
            }
        }
        if let SetExpr::Select(select) = query.body.as_ref() {
            for item in &select.projection {
                if let SelectItem::ExprWithAlias { expr, alias } = item {
                    let kind = self.context.of(expr);
                    self.record(alias, kind);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = table_factor
        {
            let names: Vec<Ident> = alias.columns.iter().map(|c| c.name.clone()).collect();
            self.record_alias_list(&names, subquery);
        }
        ControlFlow::Continue(())
    }
}

/// More rewrites than any expression in the suites needs; reaching it means a
/// rule keeps firing on its own output.
const MAX_REWRITES_PER_EXPR: usize = 16;

struct Rewriter<'a> {
    oracle: Oracle,
    kinds: KindContext<'a>,
    error: Option<String>,
}

impl Rewriter<'_> {
    fn fail(&mut self, error: String) -> ControlFlow<()> {
        self.error = Some(error);
        ControlFlow::Break(())
    }
}

impl VisitorMut for Rewriter<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if self.oracle == Oracle::Sqlite
            && let Err(error) = unparenthesize_compound_operands(&mut query.body)
        {
            return self.fail(error);
        }
        if let Some(order_by) = &mut query.order_by
            && let OrderByKind::Expressions(terms) = &mut order_by.kind
        {
            state_null_placement(terms);
            if self.oracle == Oracle::ClickHouse
                && let SetExpr::Select(select) = query.body.as_ref()
            {
                order_by_output_position(terms, &select.projection);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(
        &mut self,
        select: &mut datafusion::sql::sqlparser::ast::Select,
    ) -> ControlFlow<()> {
        name_qualified_projections(&mut select.projection);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<()> {
        if self.oracle != Oracle::Sqlite {
            return ControlFlow::Continue(());
        }
        // `FROM (SELECT …) AS t (a, b)` — SQLite accepts no column list on a
        // derived table, so the names move onto the subquery's projection.
        let TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = table_factor
        else {
            return ControlFlow::Continue(());
        };
        if alias.columns.is_empty() {
            return ControlFlow::Continue(());
        }
        let SetExpr::Select(select) = subquery.body.as_mut() else {
            return self.fail(format!(
                "derived table '{}' has a column list over a set operation",
                alias.name
            ));
        };
        if select.projection.len() != alias.columns.len() {
            return self.fail(format!(
                "derived table '{}' names {} columns for a {}-column projection",
                alias.name,
                alias.columns.len(),
                select.projection.len()
            ));
        }
        for (item, column) in select.projection.iter_mut().zip(alias.columns.drain(..)) {
            let placeholder = SelectItem::UnnamedExpr(Expr::Value(Value::Null.into()));
            let expr = match std::mem::replace(item, placeholder) {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                other => {
                    *item = other;
                    return self.fail(format!(
                        "derived table '{}' names a wildcard column",
                        alias.name
                    ));
                }
            };
            *item = SelectItem::ExprWithAlias {
                expr,
                alias: column.name,
            };
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Some(factored) = factor_common_conjuncts(expr) {
            *expr = factored;
        }
        // A rule may produce an expression another rule applies to —
        // `to_timestamp(x)::timestamp` drops its no-op cast and leaves a
        // `to_timestamp` to rewrite — so apply them until none fires. Every rule
        // is idempotent on its own output, so this settles in a few steps; one
        // that does not is a bug in a rule, reported rather than looped on.
        let mut settled = false;
        for _ in 0..MAX_REWRITES_PER_EXPR {
            let rewritten = match self.oracle {
                Oracle::Sqlite => self.sqlite(expr),
                Oracle::ClickHouse => self.clickhouse(expr),
            };
            match rewritten {
                Ok(Some(new_expr)) => *expr = new_expr,
                Ok(None) => {
                    settled = true;
                    break;
                }
                Err(error) => return self.fail(error),
            }
        }
        if !settled {
            return self.fail(format!("rewriting {expr} did not settle"));
        }
        if let Expr::Function(function) = expr
            && let Some(ast_window) = &mut function.over
            && let datafusion::sql::sqlparser::ast::WindowType::WindowSpec(spec) = ast_window
        {
            state_null_placement(&mut spec.order_by);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
        // ClickHouse reads a backslash in a string literal as an escape; the
        // suites mean it literally (the regular expressions in ClickBench).
        if self.oracle == Oracle::ClickHouse
            && let Value::SingleQuotedString(text) = &mut value.value
            && text.contains('\\')
        {
            *text = text.replace('\\', "\\\\");
        }
        ControlFlow::Continue(())
    }
}

impl Rewriter<'_> {
    /// SQLite rewrites. `Ok(None)` leaves the expression as it is.
    fn sqlite(&self, expr: &Expr) -> Result<Option<Expr>, String> {
        Ok(match expr {
            Expr::TypedString(typed) => {
                let text = string_value(&typed.value.value)
                    .ok_or_else(|| format!("typed literal {expr} has no string value"))?;
                match &typed.data_type {
                    DataType::Date => Some(string_literal(canonical_date(text)?)),
                    DataType::Timestamp(..) => Some(string_literal(canonical_timestamp(text)?)),
                    other => return Err(format!("no SQLite spelling for a {other} literal")),
                }
            }
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => self.sqlite_cast(inner, data_type)?,
            Expr::BinaryOp { left, op, right } => self.sqlite_binary(left, op, right)?,
            Expr::Between {
                expr: tested,
                negated,
                low,
                high,
            } => {
                let new_low = self.sqlite_compared_literal(tested, low)?;
                let new_high = self.sqlite_compared_literal(tested, high)?;
                (new_low.is_some() || new_high.is_some()).then(|| Expr::Between {
                    expr: tested.clone(),
                    negated: *negated,
                    low: Box::new(new_low.unwrap_or_else(|| *low.clone())),
                    high: Box::new(new_high.unwrap_or_else(|| *high.clone())),
                })
            }
            Expr::Extract { field, expr, .. } => {
                let format = match field {
                    DateTimeField::Year => "%Y",
                    DateTimeField::Month => "%m",
                    DateTimeField::Day => "%d",
                    DateTimeField::Hour => "%H",
                    DateTimeField::Minute => "%M",
                    DateTimeField::Second => "%S",
                    other => return Err(format!("no SQLite spelling for EXTRACT({other})")),
                };
                Some(cast(
                    call(
                        "strftime",
                        vec![string_literal(format.to_string()), *expr.clone()],
                    ),
                    DataType::Integer(None),
                ))
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut args = vec![*expr.clone()];
                args.push(
                    substring_from
                        .as_deref()
                        .cloned()
                        .unwrap_or_else(|| number_literal("1")),
                );
                if let Some(length) = substring_for {
                    args.push(*length.clone());
                }
                Some(call("substr", args))
            }
            Expr::Function(function) => self.sqlite_function(function)?,
            _ => None,
        })
    }

    fn sqlite_cast(&self, inner: &Expr, data_type: &DataType) -> Result<Option<Expr>, String> {
        Ok(match data_type {
            // SQLite has no DATE type: `CAST(x AS DATE)` takes NUMERIC affinity and
            // turns '2000-02-01' into 2000. `date()` returns the ISO text the
            // loader stores dates as.
            DataType::Date => Some(match string_value_of(inner) {
                Some(text) => string_literal(canonical_date(text)?),
                None => call("date", vec![inner.clone()]),
            }),
            DataType::Timestamp(..) => Some(match string_value_of(inner) {
                Some(text) => string_literal(canonical_timestamp(text)?),
                None if self.kinds.of(inner) == Some(Kind::Timestamp) => inner.clone(),
                None => call("datetime", vec![inner.clone()]),
            }),
            // NUMERIC affinity keeps an integral value an INTEGER, so a DECIMAL
            // cast of two counts would divide as integers. REAL keeps the
            // fractional quotient; the lane compares it within the float tolerance.
            DataType::Decimal(_) | DataType::Numeric(_) | DataType::Dec(_) => {
                Some(cast(inner.clone(), DataType::Real))
            }
            _ => None,
        })
    }

    fn sqlite_binary(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<Option<Expr>, String> {
        if let Some(folded) = fold_decimal_literals(left, op, right) {
            return Ok(Some(folded));
        }
        match op {
            BinaryOperator::Plus | BinaryOperator::Minus if is_interval(right) => {
                self.sqlite_interval_arithmetic(left, op, right).map(Some)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let new_right = self.sqlite_compared_literal(left, right)?;
                let new_left = self.sqlite_compared_literal(right, left)?;
                Ok(
                    (new_left.is_some() || new_right.is_some()).then(|| Expr::BinaryOp {
                        left: Box::new(new_left.unwrap_or_else(|| left.clone())),
                        op: op.clone(),
                        right: Box::new(new_right.unwrap_or_else(|| right.clone())),
                    }),
                )
            }
            _ => Ok(None),
        }
    }

    /// `other` compared with `side`: SQLite compares the stored ISO text, so a
    /// string literal must be written in exactly that form, and a date compared
    /// with a timestamp must be given a time.
    fn sqlite_compared_literal(&self, other: &Expr, side: &Expr) -> Result<Option<Expr>, String> {
        let other_kind = self.kinds.of(other);
        if let Some(text) = string_value_of(side) {
            let canonical = match other_kind {
                Some(Kind::Timestamp) => canonical_timestamp(text)?,
                Some(Kind::Date) => canonical_date(text)?,
                _ => return Ok(None),
            };
            return Ok((canonical != text).then(|| string_literal(canonical)));
        }
        Ok(
            (other_kind == Some(Kind::Timestamp) && self.kinds.of(side) == Some(Kind::Date))
                .then(|| call("datetime", vec![side.clone()])),
        )
    }

    fn sqlite_interval_arithmetic(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<Expr, String> {
        let Expr::Interval(interval) = strip_nested(right) else {
            return Err(format!("{right} is not an interval"));
        };
        let (amount, unit) = interval_amount(interval)?;
        let amount = if *op == BinaryOperator::Minus {
            -amount
        } else {
            amount
        };
        let function = match self.kinds.of(left) {
            Some(Kind::Date) => "date",
            Some(Kind::Timestamp) => "datetime",
            other => return Err(format!("interval arithmetic on {left} of kind {other:?}")),
        };
        // SQLite normalizes 2000-01-31 + 1 month to 2000-03-02; DataFusion clamps
        // to 2000-02-29. They agree only when the day exists in every month.
        if matches!(unit, "months" | "years") {
            let day = literal_date_text(left)
                .and_then(|text| canonical_date(text).ok())
                .and_then(|date| date[8..10].parse::<u32>().ok());
            if day.is_none_or(|day| day > 28) {
                return Err(format!(
                    "month arithmetic on {left}: SQLite does not clamp to the end of the month"
                ));
            }
        }
        let date = match literal_date_text(left) {
            Some(text) => string_literal(if function == "date" {
                canonical_date(text)?
            } else {
                canonical_timestamp(text)?
            }),
            None => left.clone(),
        };
        Ok(call(
            function,
            vec![date, string_literal(format!("{amount:+} {unit}"))],
        ))
    }

    fn sqlite_function(&self, function: &Function) -> Result<Option<Expr>, String> {
        let args = unnamed_args(function);
        Ok(match (function_name(function).as_str(), args.as_slice()) {
            ("mod", [dividend, divisor]) => Some(Expr::Nested(Box::new(Expr::BinaryOp {
                left: Box::new((*dividend).clone()),
                op: BinaryOperator::Modulo,
                right: Box::new((*divisor).clone()),
            }))),
            ("ascii", [text]) => Some(call("unicode", vec![(*text).clone()])),
            ("to_timestamp", [seconds]) if self.kinds.of(seconds) == Some(Kind::Integer) => {
                Some(call(
                    "datetime",
                    vec![(*seconds).clone(), string_literal("unixepoch".to_string())],
                ))
            }
            ("date_trunc", [unit, value]) => {
                let unit = string_value_of(unit)
                    .ok_or_else(|| format!("date_trunc unit {unit} is not a literal"))?;
                let format = match unit.to_ascii_lowercase().as_str() {
                    "second" => "%Y-%m-%d %H:%M:%S",
                    "minute" => "%Y-%m-%d %H:%M:00",
                    "hour" => "%Y-%m-%d %H:00:00",
                    "day" => "%Y-%m-%d 00:00:00",
                    "month" => "%Y-%m-01 00:00:00",
                    "year" => "%Y-01-01 00:00:00",
                    other => return Err(format!("no SQLite spelling for date_trunc('{other}')")),
                };
                Some(call(
                    "strftime",
                    vec![string_literal(format.to_string()), (*value).clone()],
                ))
            }
            ("to_timestamp" | "date_trunc", _) => {
                return Err(format!("no SQLite spelling for {function}"));
            }
            _ => None,
        })
    }

    /// ClickHouse rewrites. `Ok(None)` leaves the expression as it is.
    fn clickhouse(&self, expr: &Expr) -> Result<Option<Expr>, String> {
        // ClickHouse reads `0.06` as a `Float64`, so literal arithmetic lands off
        // the decimal it means exactly as it does in SQLite.
        if let Expr::BinaryOp { left, op, right } = expr
            && let Some(folded) = fold_decimal_literals(left, op, right)
        {
            return Ok(Some(folded));
        }
        Ok(match expr {
            // ClickHouse's `DATE` is a 16-bit day count from 1970 that wraps a
            // `Date32` of 1900-01-02 to 2079-06-08; TPC-DS's `date_dim` starts in
            // 1900. `Date32` holds the range DataFusion's `DATE` does.
            Expr::TypedString(typed) if typed.data_type == DataType::Date => {
                let text = string_value(&typed.value.value)
                    .ok_or_else(|| format!("typed literal {expr} has no string value"))?;
                Some(cast(
                    string_literal(canonical_date(text)?),
                    DataType::Date32,
                ))
            }
            Expr::Cast {
                expr: inner,
                data_type: DataType::Date,
                ..
            } => Some(match string_value_of(inner) {
                Some(text) => cast(string_literal(canonical_date(text)?), DataType::Date32),
                None => cast(*inner.clone(), DataType::Date32),
            }),
            // ClickHouse's `/` is floating-point on integers, where DataFusion
            // truncates (TPC-DS Q34's `hd_dep_count / hd_vehicle_count`), and on two
            // decimals keeps only the dividend's scale — `2.79` where DataFusion
            // keeps `2.799138` (TPC-DS Q12). Integers divide with `intDiv`, which
            // truncates toward zero as DataFusion does; anything else divides as
            // doubles, as DuckDB does, and the compare path reads DataFusion's
            // decimal quotient as that double cut to its places.
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Divide,
                right,
            } => {
                let (l, r) = (self.kinds.of(left), self.kinds.of(right));
                if l == Some(Kind::Integer) && r == Some(Kind::Integer) {
                    Some(call("intDiv", vec![*left.clone(), *right.clone()]))
                } else if l == Some(Kind::Float) && r == Some(Kind::Float) {
                    None
                } else {
                    let as_double = |side: &Expr, kind: Option<Kind>| {
                        if kind == Some(Kind::Float) {
                            side.clone()
                        } else {
                            call("toFloat64", vec![side.clone()])
                        }
                    };
                    Some(Expr::BinaryOp {
                        left: Box::new(as_double(left, l)),
                        op: BinaryOperator::Divide,
                        right: Box::new(as_double(right, r)),
                    })
                }
            }
            // DataFusion parses `0.0` as a `Float64`, so `COALESCE(cr_return_amount,
            // 0.0)` (TPC-DS Q75) is a double there. ClickHouse finds no common type
            // for a nullable decimal and a double; handing it the decimal as a
            // double types the expression as DataFusion does.
            Expr::Case {
                case_token,
                end_token,
                operand,
                conditions,
                else_result,
            } if self.mixes_decimal_and_float_literal(
                conditions
                    .iter()
                    .map(|when| &when.result)
                    .chain(else_result.as_deref()),
            ) =>
            {
                let mut conditions = conditions.clone();
                for when in &mut conditions {
                    when.result = self.decimal_as_double(&when.result);
                }
                Some(Expr::Case {
                    case_token: case_token.clone(),
                    end_token: end_token.clone(),
                    operand: operand.clone(),
                    conditions,
                    else_result: else_result
                        .as_deref()
                        .map(|result| Box::new(self.decimal_as_double(result))),
                })
            }
            Expr::Function(function) => {
                let args = unnamed_args(function);
                match (function_name(function).as_str(), args.as_slice()) {
                    ("to_timestamp", [seconds])
                        if self.kinds.of(seconds) == Some(Kind::Integer) =>
                    {
                        Some(call("toDateTime", vec![(*seconds).clone()]))
                    }
                    ("to_timestamp", _) => {
                        return Err(format!("no ClickHouse spelling for {function}"));
                    }
                    // `length` counts bytes in ClickHouse, characters in DataFusion.
                    ("length", [text]) => Some(call("char_length", vec![(*text).clone()])),
                    // The sample deviation of one value is NULL in the standard and
                    // in DataFusion, NaN in ClickHouse (TPC-DS Q35).
                    ("stddev_samp", [value]) => Some(Expr::Function(Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("if"))]),
                        ..match call(
                            "if",
                            vec![
                                Expr::BinaryOp {
                                    left: Box::new(call("count", vec![(*value).clone()])),
                                    op: BinaryOperator::Gt,
                                    right: Box::new(number_literal("1")),
                                },
                                call("stddevSamp", vec![(*value).clone()]),
                                Expr::Value(Value::Null.into()),
                            ],
                        ) {
                            Expr::Function(function) => function,
                            _ => unreachable!("call builds a function"),
                        }
                    })),
                    ("coalesce", _)
                        if self.mixes_decimal_and_float_literal(args.iter().copied()) =>
                    {
                        Some(call(
                            "coalesce",
                            args.iter().map(|arg| self.decimal_as_double(arg)).collect(),
                        ))
                    }
                    _ => None,
                }
            }
            _ => None,
        })
    }
}

/// `(a AND b) OR (a AND c)` as `a AND (b OR c)`.
///
/// TPC-H Q19 and TPC-DS Q13 and Q48 repeat their join predicate inside every
/// branch of an `OR`. DataFusion factors it out and joins on it; SQLite and
/// ClickHouse do not, and fall back to a cross product of the two tables — hours
/// on TPC-H's `lineitem` × `part`. `AND` distributes over `OR` in SQL's
/// three-valued logic as in Boolean logic, so the factored form keeps the
/// meaning. `None` when no conjunct is common to every branch.
fn factor_common_conjuncts(expr: &Expr) -> Option<Expr> {
    let mut branches = Vec::new();
    flatten(expr, &BinaryOperator::Or, &mut branches);
    if branches.len() < 2 {
        return None;
    }
    let conjuncts: Vec<Vec<&Expr>> = branches
        .iter()
        .map(|branch| {
            let mut parts = Vec::new();
            flatten(branch, &BinaryOperator::And, &mut parts);
            parts
        })
        .collect();
    let common: Vec<&Expr> = conjuncts[0]
        .iter()
        .copied()
        .filter(|candidate| conjuncts[1..].iter().all(|parts| parts.contains(candidate)))
        .collect();
    if common.is_empty() {
        return None;
    }
    let remainders: Vec<Vec<&Expr>> = conjuncts
        .iter()
        .map(|parts| {
            parts
                .iter()
                .copied()
                .filter(|part| !common.contains(part))
                .collect()
        })
        .collect();
    let mut factored: Vec<Expr> = common.into_iter().cloned().collect();
    // A branch left with nothing was only the common conjuncts: it is true
    // whenever they are, and so is the whole `OR`.
    if remainders.iter().all(|remainder| !remainder.is_empty()) {
        let alternatives: Vec<Expr> = remainders
            .into_iter()
            .map(|remainder| {
                join(
                    remainder.into_iter().cloned().collect(),
                    &BinaryOperator::And,
                )
            })
            .collect();
        factored.push(join(alternatives, &BinaryOperator::Or));
    }
    Some(join(factored, &BinaryOperator::And))
}

/// The operands of a chain of `op` (parentheses looked through), in order.
fn flatten<'e>(expr: &'e Expr, op: &BinaryOperator, out: &mut Vec<&'e Expr>) {
    match strip_nested(expr) {
        Expr::BinaryOp {
            left,
            op: found,
            right,
        } if found == op => {
            flatten(left, op, out);
            flatten(right, op, out);
        }
        other => out.push(other),
    }
}

/// `parts` joined by `op`, each operand parenthesized.
fn join(parts: Vec<Expr>, op: &BinaryOperator) -> Expr {
    parts
        .into_iter()
        .map(|part| Expr::Nested(Box::new(part)))
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: op.clone(),
            right: Box::new(right),
        })
        .unwrap_or_else(|| Expr::Value(Value::Boolean(true).into()))
}

/// `0.06 + 0.01` as the exact literal `0.07`.
///
/// DataFusion evaluates arithmetic on decimal literals exactly; SQLite, and
/// ClickHouse — which reads `0.06` as a `Float64` — do it in doubles, where
/// `0.06 + 0.01` is `0.06999999999999999`, below the `0.07` a row holds. TPC-H
/// Q6's `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` then drops every row at
/// the upper bound. Folding the literal arithmetic here gives the oracle the
/// bound DataFusion uses. Only `+`, `-` and `*`
/// on two literals, where the exact result is a finite decimal, and only when
/// at least one side has a fractional part: integer arithmetic SQLite does
/// exactly itself.
fn fold_decimal_literals(left: &Expr, op: &BinaryOperator, right: &Expr) -> Option<Expr> {
    let (Some(left), Some(right)) = (
        decimal_literal(strip_nested(left)),
        decimal_literal(strip_nested(right)),
    ) else {
        return None;
    };
    if left.1 == 0 && right.1 == 0 {
        return None;
    }
    let (value, scale) = match op {
        BinaryOperator::Plus | BinaryOperator::Minus => {
            let scale = left.1.max(right.1);
            let align = |(digits, from): (i128, u32)| {
                digits.checked_mul(10_i128.checked_pow(scale - from)?)
            };
            let (a, b) = (align(left)?, align(right)?);
            let sum = if *op == BinaryOperator::Plus {
                a.checked_add(b)?
            } else {
                a.checked_sub(b)?
            };
            (sum, scale)
        }
        BinaryOperator::Multiply => (left.0.checked_mul(right.0)?, left.1 + right.1),
        _ => return None,
    };
    Some(number_literal(&render_decimal(value, scale)))
}

/// A numeric literal as unscaled digits and a scale: `0.06` is `(6, 2)`.
fn decimal_literal(expr: &Expr) -> Option<(i128, u32)> {
    let Expr::Value(value) = expr else {
        return None;
    };
    let Value::Number(text, _) = &value.value else {
        return None;
    };
    if text.contains(['e', 'E']) {
        return None;
    }
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits: i128 = format!("{whole}{fraction}").parse().ok()?;
    Some((digits, u32::try_from(fraction.len()).ok()?))
}

fn render_decimal(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let sign = if value < 0 { "-" } else { "" };
    let digits = format!(
        "{:0>width$}",
        value.unsigned_abs(),
        width = scale as usize + 1
    );
    let (whole, fraction) = digits.split_at(digits.len() - scale as usize);
    format!("{sign}{whole}.{fraction}")
}

impl Rewriter<'_> {
    /// Whether decimal-typed values sit beside a literal such as `0.0`, which
    /// DataFusion reads as a double.
    fn mixes_decimal_and_float_literal<'e>(&self, values: impl Iterator<Item = &'e Expr>) -> bool {
        let (mut decimal, mut float_literal) = (false, false);
        for value in values {
            if is_float_literal(value) {
                float_literal = true;
            } else if self.kinds.of(value) == Some(Kind::Decimal) {
                decimal = true;
            }
        }
        decimal && float_literal
    }

    /// `value` as a double if it is decimal-typed and not a literal.
    fn decimal_as_double(&self, value: &Expr) -> Expr {
        if self.kinds.of(value) == Some(Kind::Decimal) && !is_float_literal(value) {
            call("toFloat64", vec![value.clone()])
        } else {
            value.clone()
        }
    }
}

/// A numeric literal with a fractional part — a double to DataFusion.
fn is_float_literal(expr: &Expr) -> bool {
    matches!(
        strip_nested(expr),
        Expr::Value(value) if matches!(&value.value, Value::Number(text, _) if text.contains('.'))
    )
}

/// Replace an `ORDER BY` name with the position of the output column it names.
///
/// An `ORDER BY` name refers to an output column before an input one — TPC-DS
/// Q33's `sum(total_sales) AS total_sales … ORDER BY total_sales` orders by the
/// sum. ClickHouse, told to prefer column names so that `WHERE` and `GROUP BY`
/// read them the standard way, applies that preference in `ORDER BY` too; the
/// position means the same column without a name to resolve.
fn order_by_output_position(terms: &mut [OrderByExpr], projection: &[SelectItem]) {
    let names: Vec<Option<String>> = projection.iter().map(output_name).collect();
    for term in terms {
        let Expr::Identifier(ident) = &term.expr else {
            continue;
        };
        let wanted = ident.value.to_ascii_lowercase();
        let mut positions = names
            .iter()
            .enumerate()
            .filter(|(_, name)| name.as_deref() == Some(wanted.as_str()));
        if let (Some((position, _)), None) = (positions.next(), positions.next()) {
            term.expr = number_literal(&(position + 1).to_string());
        }
    }
}

/// The name a select item gives its output column, lowercased.
fn output_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_ascii_lowercase()),
        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.to_ascii_lowercase()),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
            parts.last().map(|part| part.value.to_ascii_lowercase())
        }
        _ => None,
    }
}

/// Name an unaliased `t.col` projection `col`, the name standard SQL gives it.
///
/// ClickHouse keeps the qualifier in the output name when the bare name would be
/// ambiguous, so TPC-DS Q47's outer `WHERE d_year = 2000` finds no `d_year` in a
/// CTE projecting `v1.d_year` from a self-join of `v1`. SQLite reads an
/// `ORDER BY` name as an output column only when the projection aliases it, so
/// TPC-DS Q72's `ORDER BY d_week_seq` over `d1.d_week_seq` is ambiguous among
/// `d1`, `d2` and `d3`; as an alias, it names the output column as the standard
/// does. Skipped where the bare name is already an output column of the same
/// projection.
fn name_qualified_projections(projection: &mut [SelectItem]) {
    let names: Vec<Option<String>> = projection.iter().map(output_name).collect();
    for (index, item) in projection.iter_mut().enumerate() {
        let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) = item else {
            continue;
        };
        let Some(column) = parts.last().cloned() else {
            continue;
        };
        let lowered = column.value.to_ascii_lowercase();
        let taken = names
            .iter()
            .enumerate()
            .any(|(other, name)| other != index && name.as_deref() == Some(lowered.as_str()));
        if taken {
            continue;
        }
        let expr = Expr::CompoundIdentifier(parts.clone());
        *item = SelectItem::ExprWithAlias {
            expr,
            alias: column,
        };
    }
}

/// Drop the parentheses around a compound SELECT's operands.
///
/// SQLite parses no parenthesized operand — TPC-DS Q87 is
/// `(SELECT …) EXCEPT (SELECT …) EXCEPT (SELECT …)` — and applies a compound's
/// operators strictly left to right, where the standard binds `INTERSECT`
/// tighter. A left operand, which that order already groups, loses its
/// parentheses whatever it holds; a right operand keeps its meaning without them
/// only as a single SELECT. A right operand that is itself a compound — written
/// in parentheses or bound by `INTERSECT`'s precedence — has no SQLite spelling
/// that keeps its grouping, and is reported.
fn unparenthesize_compound_operands(body: &mut SetExpr) -> Result<(), String> {
    let SetExpr::SetOperation { left, right, .. } = body else {
        return Ok(());
    };
    unparenthesize(left)?;
    unparenthesize_compound_operands(left)?;
    unparenthesize(right)?;
    if matches!(right.as_ref(), SetExpr::SetOperation { .. }) {
        return Err(format!(
            "SQLite applies compound operators left to right, so the grouped right \
             operand `{right}` cannot keep its meaning"
        ));
    }
    Ok(())
}

/// Replace a parenthesized compound operand with the SELECT inside it.
fn unparenthesize(operand: &mut Box<SetExpr>) -> Result<(), String> {
    while let SetExpr::Query(query) = operand.as_ref() {
        let bare = query.with.is_none()
            && query.order_by.is_none()
            && query.limit_clause.is_none()
            && query.fetch.is_none()
            && query.locks.is_empty()
            && query.for_clause.is_none()
            && query.settings.is_none()
            && query.format_clause.is_none()
            && query.pipe_operators.is_empty();
        if !bare {
            return Err(format!(
                "SQLite has no parenthesized compound operand, and `{query}` holds more \
                 than a SELECT"
            ));
        }
        let body = query.body.clone();
        *operand = body;
    }
    Ok(())
}

/// Give every `ORDER BY` term the `NULL` placement DataFusion uses when none is
/// stated — last ascending, first descending — so an oracle whose default
/// differs (SQLite: first ascending; ClickHouse: last either way) sorts the same.
fn state_null_placement(terms: &mut [OrderByExpr]) {
    for term in terms {
        if term.options.nulls_first.is_none() {
            term.options.nulls_first = Some(term.options.asc == Some(false));
        }
    }
}

fn is_interval(expr: &Expr) -> bool {
    matches!(strip_nested(expr), Expr::Interval(_))
}

fn strip_nested(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => strip_nested(inner),
        other => other,
    }
}

/// The signed amount and SQLite modifier unit of `INTERVAL '30 days'`,
/// `INTERVAL '3' MONTH` or `INTERVAL 90 DAY`.
fn interval_amount(interval: &Interval) -> Result<(i64, &'static str), String> {
    let value = match interval.value.as_ref() {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::Number(text, _) => text.trim().to_string(),
            other => return Err(format!("interval value {other} is not a literal")),
        },
        other => return Err(format!("interval value {other} is not a literal")),
    };
    let (amount, unit_text) = if let Some(field) = &interval.leading_field {
        (value.as_str(), field.to_string())
    } else {
        let mut parts = value.split_whitespace();
        let amount = parts.next().unwrap_or_default();
        let unit = parts.next().unwrap_or_default().to_string();
        if parts.next().is_some() {
            return Err(format!("interval '{value}' has more than one unit"));
        }
        (amount, unit)
    };
    let amount: i64 = amount
        .parse()
        .map_err(|_| format!("interval amount '{amount}' is not an integer"))?;
    let unit = match unit_text.to_ascii_lowercase().trim_end_matches('s') {
        "day" => "days",
        "month" => "months",
        "year" => "years",
        "hour" => "hours",
        "minute" => "minutes",
        "second" => "seconds",
        other => return Err(format!("no SQLite modifier for interval unit '{other}'")),
    };
    Ok((amount, unit))
}

/// `YYYY-MM-DD` for a date literal DataFusion accepts, including the unpadded
/// `2002-5-01` TPC-DS Q2 writes.
fn canonical_date(text: &str) -> Result<String, String> {
    let date = text.trim();
    let date = date.split([' ', 'T']).next().unwrap_or(date);
    let mut parts = date.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(format!("'{text}' is not a date"));
    };
    let parsed = chrono::NaiveDate::from_ymd_opt(
        year.parse()
            .map_err(|_| format!("'{text}' is not a date"))?,
        month
            .parse()
            .map_err(|_| format!("'{text}' is not a date"))?,
        day.parse().map_err(|_| format!("'{text}' is not a date"))?,
    )
    .ok_or_else(|| format!("'{text}' is not a date"))?;
    if text.trim().len() > date.len() {
        // A time part on a date literal is only harmless at midnight.
        let time = canonical_timestamp(text)?;
        if !time.ends_with(" 00:00:00") {
            return Err(format!("'{text}' is a timestamp, not a date"));
        }
    }
    Ok(parsed.format("%Y-%m-%d").to_string())
}

/// The text the SQLite loader stores a timestamp as, for a timestamp literal:
/// `YYYY-MM-DD HH:MM:SS`, with a fraction only when it is not zero.
fn canonical_timestamp(text: &str) -> Result<String, String> {
    let trimmed = text.trim();
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ];
    let timestamp = formats
        .iter()
        .find_map(|format| chrono::NaiveDateTime::parse_from_str(trimmed, format).ok())
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
        })
        .ok_or_else(|| format!("'{text}' is not a timestamp"))?;
    Ok(sqlite_timestamp_text(timestamp))
}

/// The one rendering of a timestamp the SQLite lane uses, for stored values and
/// literals alike, so text comparison orders them as instants.
#[must_use]
pub fn sqlite_timestamp_text(timestamp: chrono::NaiveDateTime) -> String {
    use chrono::Timelike;
    if timestamp.nanosecond() == 0 {
        timestamp.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        timestamp.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}

fn function_name(function: &Function) -> String {
    function
        .name
        .0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.to_ascii_lowercase())
        .unwrap_or_default()
}

fn unnamed_args(function: &Function) -> Vec<&Expr> {
    match &function.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn string_value(value: &Value) -> Option<&str> {
    match value {
        Value::SingleQuotedString(text) => Some(text),
        _ => None,
    }
}

fn string_value_of(expr: &Expr) -> Option<&str> {
    match strip_nested(expr) {
        Expr::Value(value) => string_value(&value.value),
        _ => None,
    }
}

/// The text of a date or timestamp literal in any of the spellings the suites
/// use: `'1993-07-01'`, `DATE '1993-07-01'` or `CAST('1993-07-01' AS DATE)`.
fn literal_date_text(expr: &Expr) -> Option<&str> {
    match strip_nested(expr) {
        Expr::TypedString(typed) => string_value(&typed.value.value),
        Expr::Cast { expr: inner, .. } => string_value_of(inner),
        other => string_value_of(other),
    }
}

fn string_literal(text: String) -> Expr {
    Expr::Value(Value::SingleQuotedString(text).into())
}

fn number_literal(text: &str) -> Expr {
    Expr::Value(Value::Number(text.to_string(), false).into())
}

fn cast(expr: Expr, data_type: DataType) -> Expr {
    Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        array: false,
        format: None,
    }
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}
