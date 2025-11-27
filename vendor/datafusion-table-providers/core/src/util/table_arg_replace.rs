use std::ops::ControlFlow;

use datafusion::sql::{
    sqlparser::ast::{
        FunctionArg, Ident, ObjectName, TableAlias, TableFactor, TableFunctionArgs, VisitorMut,
    },
    TableReference,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableArgReplace {
    pub tables: Vec<(TableReference, TableFunctionArgs)>,
}

impl TableArgReplace {
    /// Constructs a new `TableArgReplace` instance.
    pub fn new(tables: Vec<(TableReference, Vec<FunctionArg>)>) -> Self {
        Self {
            tables: tables
                .into_iter()
                .map(|(table, args)| {
                    (
                        table,
                        TableFunctionArgs {
                            args,
                            settings: None,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Adds a new table argument replacement.
    pub fn with(mut self, table: TableReference, args: Vec<FunctionArg>) -> Self {
        self.tables.push((
            table,
            TableFunctionArgs {
                args,
                settings: None,
            },
        ));
        self
    }

    #[cfg(feature = "federation")]
    /// Converts the `TableArgReplace` instance into an `AstAnalyzerRule`.
    pub fn into_analyzer(self) -> datafusion_federation::sql::ast_analyzer::AstAnalyzerRule {
        let mut visitor = self;
        let x = move |mut statement: datafusion::sql::sqlparser::ast::Statement| {
            let _ = datafusion::sql::sqlparser::ast::VisitMut::visit(&mut statement, &mut visitor);
            Ok(statement)
        };
        Box::new(x)
    }
}

impl VisitorMut for TableArgReplace {
    type Break = ();
    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::Table {
            name, args, alias, ..
        } = table_factor
        {
            let name_as_tableref = name_to_table_reference(name);
            if let Some((table, arg)) = self
                .tables
                .iter()
                .find(|(t, _)| t.resolved_eq(&name_as_tableref))
            {
                *args = Some(arg.clone());
                if alias.is_none() {
                    *alias = Some(TableAlias {
                        name: Ident::new(table.table()),
                        columns: vec![],
                    })
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn name_to_table_reference(name: &ObjectName) -> TableReference {
    let first = name
        .0
        .first()
        .map(|n| n.as_ident().expect("expected Ident").value.to_string());
    let second = name
        .0
        .get(1)
        .map(|n| n.as_ident().expect("expected Ident").value.to_string());
    let third = name
        .0
        .get(2)
        .map(|n| n.as_ident().expect("expected Ident").value.to_string());

    match (first, second, third) {
        (Some(first), Some(second), Some(third)) => TableReference::full(first, second, third),
        (Some(first), Some(second), None) => TableReference::partial(first, second),
        (Some(first), None, None) => TableReference::bare(first),
        _ => panic!("Invalid table name"),
    }
}
