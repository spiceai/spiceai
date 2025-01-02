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

use ::datafusion::sql::{parser, sqlparser::ast, TableReference};
use std::collections::HashSet;

pub(crate) fn get_dependent_table_names(statement: &parser::Statement) -> Vec<TableReference> {
    let mut table_names = Vec::new();
    let mut cte_names = HashSet::new();

    if let parser::Statement::Statement(statement) = statement.clone() {
        if let ast::Statement::Query(statement) = *statement {
            // Collect names of CTEs
            if let Some(with) = statement.with {
                for table in with.cte_tables {
                    cte_names.insert(TableReference::bare(table.alias.name.to_string()));
                    let cte_table_names = get_dependent_table_names(&parser::Statement::Statement(
                        Box::new(ast::Statement::Query(table.query)),
                    ));
                    // Extend table_names with names found in CTEs if they reference actual tables
                    table_names.extend(cte_table_names);
                }
            }
            // Process the main query body
            if let ast::SetExpr::Select(select_statement) = *statement.body {
                for from in select_statement.from {
                    let mut relations = vec![];
                    relations.push(from.relation.clone());
                    for join in from.joins {
                        relations.push(join.relation.clone());
                    }

                    for relation in relations {
                        match relation {
                            ast::TableFactor::Table { name, .. } => {
                                table_names.push(name.to_string().into());
                            }
                            ast::TableFactor::Derived { subquery, .. } => {
                                table_names.extend(get_dependent_table_names(
                                    &parser::Statement::Statement(Box::new(ast::Statement::Query(
                                        subquery,
                                    ))),
                                ));
                            }
                            _ => (),
                        }
                    }
                }
            }
        }
    }

    // Filter out CTEs and temporary views (aliases of subqueries)
    table_names
        .into_iter()
        .filter(|name| !cte_names.contains(name))
        .collect()
}
