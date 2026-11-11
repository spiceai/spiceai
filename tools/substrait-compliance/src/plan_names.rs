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

//! Lowercase the table and column names a Substrait plan reads, so the IBM
//! suite's Isthmus plans (`LINEITEM`, `L_ORDERKEY`) resolve against Spice
//! datasets, which Spice registers under lowercase names.
//!
//! Only names a consumer resolves are rewritten: each `ReadRel`'s
//! `NamedTable` names and its base schema's column names. Field references are
//! positional, so the rest of the plan is untouched, and the root's output
//! names stay as the suite wrote them. Every relation and expression is
//! visited, including subqueries nested inside expressions, and the matches are
//! exhaustive, so a relation type a future Substrait release adds is a compile
//! error here rather than a table this rewrite silently skips.

use datafusion_substrait::substrait::proto::{
    AggregateFunction, Expression, FunctionArgument, Plan, Rel, SortField,
    expand_rel::expand_field::FieldType, expression::RexType, expression::field_reference,
    expression::nested::NestedType, expression::subquery::SubqueryType, fetch_rel,
    function_argument, plan_rel, read_rel, rel::RelType,
};

/// Lowercase every name `plan` reads tables and columns by.
pub fn lowercase_read_names(plan: &mut Plan) {
    for relation in &mut plan.relations {
        match &mut relation.rel_type {
            Some(plan_rel::RelType::Rel(rel)) => visit_rel(rel),
            Some(plan_rel::RelType::Root(root)) => {
                if let Some(input) = &mut root.input {
                    visit_rel(input);
                }
            }
            None => {}
        }
    }
}

fn visit_rel(rel: &mut Rel) {
    let Some(rel_type) = &mut rel.rel_type else {
        return;
    };
    match rel_type {
        RelType::Read(read) => {
            if let Some(base_schema) = &mut read.base_schema {
                for name in &mut base_schema.names {
                    *name = name.to_lowercase();
                }
            }
            if let Some(read_rel::ReadType::NamedTable(table)) = &mut read.read_type {
                for name in &mut table.names {
                    *name = name.to_lowercase();
                }
            }
            visit_boxed_expr(&mut read.filter);
            visit_boxed_expr(&mut read.best_effort_filter);
        }
        RelType::Filter(filter) => {
            visit_boxed_rel(&mut filter.input);
            visit_boxed_expr(&mut filter.condition);
        }
        RelType::Fetch(fetch) => {
            visit_boxed_rel(&mut fetch.input);
            if let Some(fetch_rel::OffsetMode::OffsetExpr(offset)) = &mut fetch.offset_mode {
                visit_expr(offset);
            }
            if let Some(fetch_rel::CountMode::CountExpr(count)) = &mut fetch.count_mode {
                visit_expr(count);
            }
        }
        RelType::Aggregate(aggregate) => {
            visit_boxed_rel(&mut aggregate.input);
            for grouping in &mut aggregate.groupings {
                #[expect(deprecated)]
                visit_exprs(&mut grouping.grouping_expressions);
            }
            visit_exprs(&mut aggregate.grouping_expressions);
            for measure in &mut aggregate.measures {
                if let Some(function) = &mut measure.measure {
                    visit_aggregate_function(function);
                }
                if let Some(filter) = &mut measure.filter {
                    visit_expr(filter);
                }
            }
        }
        RelType::Sort(sort) => {
            visit_boxed_rel(&mut sort.input);
            visit_sorts(&mut sort.sorts);
        }
        RelType::Join(join) => {
            visit_boxed_rel(&mut join.left);
            visit_boxed_rel(&mut join.right);
            visit_boxed_expr(&mut join.expression);
            visit_boxed_expr(&mut join.post_join_filter);
        }
        RelType::Project(project) => {
            visit_boxed_rel(&mut project.input);
            visit_exprs(&mut project.expressions);
        }
        RelType::Set(set) => visit_rels(&mut set.inputs),
        RelType::ExtensionSingle(extension) => visit_boxed_rel(&mut extension.input),
        RelType::ExtensionMulti(extension) => visit_rels(&mut extension.inputs),
        RelType::ExtensionLeaf(_) | RelType::Reference(_) => {}
        RelType::Cross(cross) => {
            visit_boxed_rel(&mut cross.left);
            visit_boxed_rel(&mut cross.right);
        }
        RelType::Write(write) => visit_boxed_rel(&mut write.input),
        RelType::Ddl(ddl) => visit_boxed_rel(&mut ddl.view_definition),
        RelType::Update(update) => {
            visit_boxed_expr(&mut update.condition);
            for transformation in &mut update.transformations {
                if let Some(expression) = &mut transformation.transformation {
                    visit_expr(expression);
                }
            }
        }
        RelType::HashJoin(join) => {
            visit_boxed_rel(&mut join.left);
            visit_boxed_rel(&mut join.right);
            visit_boxed_expr(&mut join.post_join_filter);
        }
        RelType::MergeJoin(join) => {
            visit_boxed_rel(&mut join.left);
            visit_boxed_rel(&mut join.right);
            visit_boxed_expr(&mut join.post_join_filter);
        }
        RelType::NestedLoopJoin(join) => {
            visit_boxed_rel(&mut join.left);
            visit_boxed_rel(&mut join.right);
            visit_boxed_expr(&mut join.expression);
        }
        RelType::Window(window) => {
            visit_boxed_rel(&mut window.input);
            for function in &mut window.window_functions {
                visit_arguments(&mut function.arguments);
            }
            visit_exprs(&mut window.partition_expressions);
            visit_sorts(&mut window.sorts);
        }
        RelType::Exchange(exchange) => visit_boxed_rel(&mut exchange.input),
        RelType::Expand(expand) => {
            visit_boxed_rel(&mut expand.input);
            for field in &mut expand.fields {
                match &mut field.field_type {
                    Some(FieldType::ConsistentField(expression)) => visit_expr(expression),
                    Some(FieldType::SwitchingField(switching)) => {
                        visit_exprs(&mut switching.duplicates);
                    }
                    None => {}
                }
            }
        }
    }
}

fn visit_boxed_rel(rel: &mut Option<Box<Rel>>) {
    if let Some(rel) = rel {
        visit_rel(rel);
    }
}

fn visit_rels(rels: &mut [Rel]) {
    for rel in rels {
        visit_rel(rel);
    }
}

fn visit_boxed_expr(expression: &mut Option<Box<Expression>>) {
    if let Some(expression) = expression {
        visit_expr(expression);
    }
}

fn visit_exprs(expressions: &mut [Expression]) {
    for expression in expressions {
        visit_expr(expression);
    }
}

fn visit_sorts(sorts: &mut [SortField]) {
    for sort in sorts {
        if let Some(expression) = &mut sort.expr {
            visit_expr(expression);
        }
    }
}

fn visit_arguments(arguments: &mut [FunctionArgument]) {
    for argument in arguments {
        if let Some(function_argument::ArgType::Value(expression)) = &mut argument.arg_type {
            visit_expr(expression);
        }
    }
}

fn visit_aggregate_function(function: &mut AggregateFunction) {
    visit_arguments(&mut function.arguments);
    visit_sorts(&mut function.sorts);
    #[expect(deprecated)]
    visit_exprs(&mut function.args);
}

fn visit_expr(expression: &mut Expression) {
    let Some(rex_type) = &mut expression.rex_type else {
        return;
    };
    match rex_type {
        #[expect(deprecated)]
        RexType::Literal(_) | RexType::DynamicParameter(_) | RexType::Enum(_) => {}
        RexType::Selection(reference) => {
            if let Some(field_reference::RootType::Expression(root)) = &mut reference.root_type {
                visit_expr(root);
            }
        }
        RexType::ScalarFunction(function) => {
            visit_arguments(&mut function.arguments);
            #[expect(deprecated)]
            visit_exprs(&mut function.args);
        }
        RexType::WindowFunction(function) => {
            visit_arguments(&mut function.arguments);
            visit_exprs(&mut function.partitions);
            visit_sorts(&mut function.sorts);
            #[expect(deprecated)]
            visit_exprs(&mut function.args);
        }
        RexType::IfThen(if_then) => {
            for clause in &mut if_then.ifs {
                if let Some(condition) = &mut clause.r#if {
                    visit_expr(condition);
                }
                if let Some(then) = &mut clause.then {
                    visit_expr(then);
                }
            }
            visit_boxed_expr(&mut if_then.r#else);
        }
        RexType::SwitchExpression(switch) => {
            visit_boxed_expr(&mut switch.r#match);
            for clause in &mut switch.ifs {
                if let Some(then) = &mut clause.then {
                    visit_expr(then);
                }
            }
            visit_boxed_expr(&mut switch.r#else);
        }
        RexType::SingularOrList(list) => {
            visit_boxed_expr(&mut list.value);
            visit_exprs(&mut list.options);
        }
        RexType::MultiOrList(list) => {
            visit_exprs(&mut list.value);
            for record in &mut list.options {
                visit_exprs(&mut record.fields);
            }
        }
        RexType::Cast(cast) => visit_boxed_expr(&mut cast.input),
        RexType::Subquery(subquery) => match &mut subquery.subquery_type {
            Some(SubqueryType::Scalar(scalar)) => visit_boxed_rel(&mut scalar.input),
            Some(SubqueryType::InPredicate(in_predicate)) => {
                visit_exprs(&mut in_predicate.needles);
                visit_boxed_rel(&mut in_predicate.haystack);
            }
            Some(SubqueryType::SetPredicate(set_predicate)) => {
                visit_boxed_rel(&mut set_predicate.tuples);
            }
            Some(SubqueryType::SetComparison(set_comparison)) => {
                visit_boxed_expr(&mut set_comparison.left);
                visit_boxed_rel(&mut set_comparison.right);
            }
            None => {}
        },
        RexType::Nested(nested) => match &mut nested.nested_type {
            Some(NestedType::Struct(fields)) => visit_exprs(&mut fields.fields),
            Some(NestedType::List(list)) => visit_exprs(&mut list.values),
            Some(NestedType::Map(map)) => {
                for key_value in &mut map.key_values {
                    if let Some(key) = &mut key_value.key {
                        visit_expr(key);
                    }
                    if let Some(value) = &mut key_value.value {
                        visit_expr(value);
                    }
                }
            }
            None => {}
        },
        RexType::Lambda(lambda) => visit_boxed_expr(&mut lambda.body),
        RexType::LambdaInvocation(invocation) => {
            if let Some(lambda) = &mut invocation.lambda {
                visit_boxed_expr(&mut lambda.body);
            }
            if let Some(arguments) = &mut invocation.arguments {
                visit_exprs(&mut arguments.fields);
            }
        }
    }
}
