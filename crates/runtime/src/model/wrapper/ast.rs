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

use tera::ast::{Expr, ExprVal, Node};

pub(super) fn has_variables_in_ast(ast: &[Node]) -> bool {
    ast.iter().any(has_variables_in_node)
}

fn has_variables_in_node(node: &tera::ast::Node) -> bool {
    match node {
        Node::VariableBlock(_, _) => true,
        Node::MacroDefinition(_, macro_def, _) => {
            !macro_def.args.is_empty() || has_variables_in_ast(&macro_def.body)
        }
        Node::Set(_, set) => has_variables_in_expr(&set.value),
        Node::If(
            tera::ast::If {
                conditions,
                otherwise,
            },
            _,
        ) => {
            conditions
                .iter()
                .any(|(_, expr, nodes)| has_variables_in_expr(expr) || has_variables_in_ast(nodes))
                || otherwise
                    .as_ref()
                    .is_some_and(|(_, nodes)| has_variables_in_ast(nodes))
        }
        Node::Forloop(
            _,
            tera::ast::Forloop {
                container,
                body,
                empty_body,
                ..
            },
            _,
        ) => {
            has_variables_in_expr(container)
                || has_variables_in_ast(body)
                || empty_body
                    .as_ref()
                    .is_some_and(|nodes| has_variables_in_ast(nodes))
        }
        Node::Block(_, block, _) => has_variables_in_ast(&block.body),
        Node::FilterSection(_, filter, _) => has_variables_in_ast(&filter.body),
        Node::Raw(_, _, _)
        | Node::Super
        | Node::Extends(_, _)
        | Node::Include(_, _, _)
        | Node::ImportMacro(_, _, _)
        | Node::Break(_)
        | Node::Continue(_)
        | Node::Comment(_, _)
        | Node::Text(_) => false,
    }
}

fn has_variables_in_expr(expr: &Expr) -> bool {
    has_variables_in_expr_val(&expr.val) || expr.filters.iter().any(has_variables_in_function_call)
}

fn has_variables_in_expr_val(expr_val: &ExprVal) -> bool {
    match expr_val {
        ExprVal::String(_) | ExprVal::Int(_) | ExprVal::Float(_) | ExprVal::Bool(_) => false,
        ExprVal::Ident(_) => true,
        ExprVal::Math(math_expr) => {
            has_variables_in_expr(&math_expr.lhs) || has_variables_in_expr(&math_expr.rhs)
        }
        ExprVal::Logic(logic_expr) => {
            has_variables_in_expr(&logic_expr.lhs) || has_variables_in_expr(&logic_expr.rhs)
        }
        ExprVal::Test(test) => has_variables_in_test(test),
        ExprVal::MacroCall(macro_call) => has_variables_in_macro_call(macro_call),
        ExprVal::FunctionCall(function_call) => has_variables_in_function_call(function_call),
        ExprVal::Array(exprs) => exprs.iter().any(has_variables_in_expr),
        ExprVal::StringConcat(string_concat) => has_variables_in_string_concat(string_concat),
        ExprVal::In(in_expr) => {
            has_variables_in_expr(&in_expr.lhs) || has_variables_in_expr(&in_expr.rhs)
        }
    }
}

fn has_variables_in_test(test: &tera::ast::Test) -> bool {
    test.args.iter().any(has_variables_in_expr)
}

fn has_variables_in_macro_call(macro_call: &tera::ast::MacroCall) -> bool {
    macro_call.args.values().any(has_variables_in_expr)
}

fn has_variables_in_function_call(function_call: &tera::ast::FunctionCall) -> bool {
    function_call.args.values().any(has_variables_in_expr)
}

fn has_variables_in_string_concat(string_concat: &tera::ast::StringConcat) -> bool {
    string_concat.values.iter().any(has_variables_in_expr_val)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tera::Tera;

    fn get_template_ast(template: &str) -> Vec<tera::ast::Node> {
        let mut t = Tera::default();
        t.add_raw_template("test", template)
            .expect("Failed to add template");
        t.get_template("test")
            .expect("Failed to get template")
            .ast
            .clone()
    }

    #[test]
    fn test_has_variables_in_ast_basic() {
        let ast = get_template_ast("{{ variable }}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_no_variables() {
        let ast = get_template_ast("Hello world");
        assert!(!has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_if_statement() {
        let ast = get_template_ast("{% if condition %}Hello{% endif %}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_for_loop() {
        let ast = get_template_ast("{% for item in items %}Hello{% endfor %}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_macro() {
        let ast = get_template_ast("{% macro test_macro(arg) %}Hello {{ arg }}{% endmacro %}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_set() {
        let ast = get_template_ast("{% set var = value %}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_filter() {
        let ast = get_template_ast("{{ value | filter }}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_complex() {
        let ast = get_template_ast(
            "
            {% if user.is_admin %}
                {% for item in items %}
                    {{ item.name | capitalize }}
                {% endfor %}
            {% else %}
                Hello {{ user.name }}
            {% endif %}
            ",
        );
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_nested_blocks() {
        let ast = get_template_ast(
            "
            {% block content %}
                {% if condition %}
                    {{ variable }}
                {% endif %}
            {% endblock %}
            ",
        );
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_math_expression() {
        let ast = get_template_ast("{{ value + 1 }}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_logic_expression() {
        let ast = get_template_ast("{{ value and other_value }}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_string_concat() {
        let ast = get_template_ast("{{ 'Hello ' ~ name }}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_array() {
        let ast = get_template_ast("{{ [1, value, 3] }}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_test() {
        let ast = get_template_ast("{{ value is defined }}");
        assert!(has_variables_in_ast(&ast));
    }

    #[test]
    fn test_has_variables_in_ast_in_operator() {
        let ast = get_template_ast("{{ value in array }}");
        assert!(has_variables_in_ast(&ast));
    }
}
