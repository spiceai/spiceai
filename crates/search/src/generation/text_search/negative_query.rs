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

//! Rewrite Tantivy queries so a `BooleanQuery` of only `MustNot` clauses matches
//! every document except those, instead of matching nothing.
//!
//! Tantivy's query parser emits `a AND NOT b` as `+a +(-b)` ([quickwit-oss/tantivy#1980]).
//! A `BooleanQuery` whose clauses are all `MustNot` matches no documents, so the
//! conjunction is empty. `a AND -b` is already `+a -b` and is unaffected.
//!
//! Nested all-`MustNot` queries are not wrapped during recursion: a `Must` child
//! that is still pure-negative is flattened into the parent's `MustNot` clauses
//! so `a AND NOT b` is the same query as `a AND -b`. A leftover all-`MustNot`
//! query at the top level (or under `Should`, e.g. `a OR NOT b`) is wrapped as
//! `Must(AllQuery)` plus those `MustNot` clauses. A leading `NOT x` / `-x` is
//! therefore every document except `x`.

use tantivy::query::{AllQuery, BooleanQuery, Occur, Query};

/// Rewrite parsed full-text queries so `AND NOT` / a leading `NOT` match documents.
#[must_use]
pub(crate) fn rewrite_negative_clauses(query: Box<dyn Query>) -> Box<dyn Query> {
    rewrite_boolean_query(query.as_ref(), true).unwrap_or(query)
}

fn rewrite_boolean_query(query: &dyn Query, wrap_pure_negative: bool) -> Option<Box<dyn Query>> {
    let boolean = query.as_any().downcast_ref::<BooleanQuery>()?;
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(boolean.clauses().len());

    for (occur, child) in boolean.clauses() {
        let rewritten =
            rewrite_boolean_query(child.as_ref(), false).unwrap_or_else(|| child.box_clone());
        if *occur == Occur::Must
            && let Some(inner) = rewritten.as_any().downcast_ref::<BooleanQuery>()
            && is_pure_must_not(inner)
        {
            for (_, negated) in inner.clauses() {
                clauses.push((Occur::MustNot, negated.box_clone()));
            }
            continue;
        }

        let child = match rewritten.as_any().downcast_ref::<BooleanQuery>() {
            Some(inner) if is_pure_must_not(inner) => with_all_query(inner),
            _ => rewritten,
        };
        clauses.push((*occur, child));
    }

    prefer_must_all(&mut clauses);

    if wrap_pure_negative && is_pure_must_not_clauses(&clauses) {
        Some(with_all_query_clauses(clauses))
    } else {
        Some(Box::new(BooleanQuery::new(clauses)))
    }
}

fn is_all_query(query: &dyn Query) -> bool {
    query.as_any().downcast_ref::<AllQuery>().is_some()
}

/// Tantivy's lenient parser turns a leading `NOT x` into `Should(AllQuery)` plus
/// `MustNot` clauses. Prefer `Must(AllQuery)` first so the shape matches a
/// rewritten all-negative query.
fn prefer_must_all(clauses: &mut Vec<(Occur, Box<dyn Query>)>) {
    let should_all = clauses
        .iter()
        .filter(|(occur, query)| *occur == Occur::Should && is_all_query(query.as_ref()))
        .count();
    let has_must = clauses.iter().any(|(occur, _)| *occur == Occur::Must);
    let has_must_not = clauses.iter().any(|(occur, _)| *occur == Occur::MustNot);
    if should_all == 1
        && !has_must
        && has_must_not
        && let Some((occur, _)) = clauses
            .iter_mut()
            .find(|(occur, query)| *occur == Occur::Should && is_all_query(query.as_ref()))
    {
        *occur = Occur::Must;
    }
    if let Some(i) = clauses
        .iter()
        .position(|(occur, query)| *occur == Occur::Must && is_all_query(query.as_ref()))
        && i > 0
    {
        let all = clauses.remove(i);
        clauses.insert(0, all);
    }
}

fn is_pure_must_not(query: &BooleanQuery) -> bool {
    is_pure_must_not_clauses(query.clauses())
}

fn is_pure_must_not_clauses(clauses: &[(Occur, Box<dyn Query>)]) -> bool {
    !clauses.is_empty() && clauses.iter().all(|(occur, _)| *occur == Occur::MustNot)
}

fn with_all_query(query: &BooleanQuery) -> Box<dyn Query> {
    with_all_query_clauses(
        query
            .clauses()
            .iter()
            .map(|(occur, child)| (*occur, child.box_clone()))
            .collect(),
    )
}

fn with_all_query_clauses(must_not_clauses: Vec<(Occur, Box<dyn Query>)>) -> Box<dyn Query> {
    let mut clauses = Vec::with_capacity(must_not_clauses.len() + 1);
    clauses.push((Occur::Must, Box::new(AllQuery) as Box<dyn Query>));
    clauses.extend(must_not_clauses);
    Box::new(BooleanQuery::new(clauses))
}

#[cfg(test)]
mod tests {
    use super::{is_pure_must_not, rewrite_negative_clauses};
    use tantivy::{
        Index,
        collector::Count,
        doc,
        query::{AllQuery, BooleanQuery, Occur, Query, QueryParser},
        schema::{STORED, Schema, TEXT},
    };

    fn parser_and_index() -> (QueryParser, tantivy::Index) {
        let mut builder = Schema::builder();
        let title = builder.add_text_field("title", TEXT | STORED);
        let text = builder.add_text_field("text", TEXT | STORED);
        let schema = builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer(15_000_000).expect("in-memory index writer");
        for (title_value, text_value) in [
            ("spice runtime", "dolphins play"),
            ("spice draft", "dolphins furiously"),
            ("runtime notes", "pearls and dolphins"),
            ("draft only", "pearls furiously"),
            ("other", "only pearls"),
            ("spice", "furiously alone"),
        ] {
            writer
                .add_document(doc!(title => title_value, text => text_value))
                .expect("add document");
        }
        writer.commit().expect("commit");
        let parser = QueryParser::for_index(&index, vec![title, text]);
        (parser, index)
    }

    fn parse_raw(parser: &QueryParser, query: &str) -> Box<dyn Query> {
        parser
            .parse_query(query)
            .unwrap_or_else(|e| panic!("parse {query:?}: {e}"))
    }

    fn parse_rewritten(parser: &QueryParser, query: &str) -> Box<dyn Query> {
        super::super::parse_user_query(parser, query)
            .unwrap_or_else(|e| panic!("parse_user_query {query:?}: {e}"))
    }

    fn describe_query(query: &dyn Query) -> String {
        if let Some(boolean) = query.as_any().downcast_ref::<BooleanQuery>() {
            let parts: Vec<String> = boolean
                .clauses()
                .iter()
                .map(|(occur, child)| {
                    let mark = match *occur {
                        Occur::Must => '+',
                        Occur::MustNot => '-',
                        Occur::Should => '?',
                    };
                    format!("{mark}{}", describe_query(child.as_ref()))
                })
                .collect();
            return format!("({})", parts.join(" "));
        }
        if query.as_any().downcast_ref::<AllQuery>().is_some() {
            return "*".to_string();
        }
        format!("{query:?}")
    }

    fn hit_count(index: &Index, query: &dyn Query) -> usize {
        index
            .reader()
            .expect("reader")
            .searcher()
            .search(query, &Count)
            .expect("count")
    }

    /// `a AND NOT b` is `+a +(-b)`. The nested `(-b)` is a `BooleanQuery` of only
    /// `MustNot` clauses, which matches nothing until it is rewritten.
    #[test]
    fn and_not_parses_as_a_must_of_a_pure_negative() {
        let (parser, _) = parser_and_index();
        let raw = parse_raw(&parser, "dolphins AND NOT furiously");
        let boolean = raw
            .as_any()
            .downcast_ref::<BooleanQuery>()
            .expect("AND NOT is a BooleanQuery");
        let nested_pure_negative = boolean.clauses().iter().any(|(occur, child)| {
            *occur == Occur::Must
                && child
                    .as_any()
                    .downcast_ref::<BooleanQuery>()
                    .is_some_and(is_pure_must_not)
        });
        assert!(
            nested_pure_negative,
            "expected +term +(-term), got {}",
            describe_query(raw.as_ref())
        );
    }

    #[test]
    fn rewrite_flattens_and_not_to_the_and_minus_shape() {
        let (parser, _) = parser_and_index();
        let rewritten = describe_query(parse_rewritten(&parser, "a AND NOT b").as_ref());
        let and_minus = describe_query(parse_raw(&parser, "a AND -b").as_ref());
        assert_eq!(
            rewritten, and_minus,
            "AND NOT must flatten to the AND - form"
        );
        assert_eq!(
            rewritten,
            describe_query(parse_rewritten(&parser, "a AND -b").as_ref()),
            "rewriting AND - must be a no-op"
        );
    }

    #[test]
    fn rewrite_turns_a_leading_not_into_all_except() {
        let (parser, _) = parser_and_index();
        assert!(
            parser.parse_query("NOT furiously").is_err(),
            "Tantivy forbids a query of only excluding terms"
        );
        let described = describe_query(parse_rewritten(&parser, "NOT furiously").as_ref());
        assert!(
            described.starts_with("(+* -"),
            "NOT x must become Must(AllQuery) + MustNot(x), got {described}"
        );
        assert_eq!(
            described,
            describe_query(parse_rewritten(&parser, "-furiously").as_ref()),
            "NOT x and a leading -x must rewrite the same way"
        );
    }

    #[test]
    fn unrewritten_and_not_matches_nothing_rewritten_matches_the_minus_form() {
        let (parser, index) = parser_and_index();
        let cases = [
            ("dolphins AND NOT furiously", "dolphins AND -furiously", 2),
            ("dolphins AND (NOT furiously)", "dolphins AND -furiously", 2),
            (
                "text:(dolphins OR pearls) AND NOT furiously",
                "text:(dolphins OR pearls) AND -furiously",
                3,
            ),
            (
                "title:(spice OR runtime) AND NOT draft",
                "title:(spice OR runtime) AND -draft",
                3,
            ),
        ];
        for (and_not, minus, expected) in cases {
            let raw = parse_raw(&parser, and_not);
            let rewritten = rewrite_negative_clauses(raw.box_clone());
            let minus_query = parse_raw(&parser, minus);
            let raw_count = hit_count(&index, raw.as_ref());
            let rewritten_count = hit_count(&index, rewritten.as_ref());
            let minus_count = hit_count(&index, minus_query.as_ref());
            assert_eq!(
                raw_count, 0,
                "{and_not}: unrewritten AND NOT must match nothing (tantivy#1980)"
            );
            assert_eq!(
                rewritten_count, minus_count,
                "{and_not}: rewritten count must equal {minus}"
            );
            assert_eq!(
                rewritten_count, expected,
                "{and_not}: expected {expected} hits, got {rewritten_count}"
            );
        }
    }

    #[test]
    fn not_alone_matches_every_document_except_the_term() {
        let (parser, index) = parser_and_index();
        assert_eq!(
            hit_count(&index, parse_rewritten(&parser, "NOT furiously").as_ref()),
            3,
            "NOT furiously must keep the three documents that do not contain it"
        );
        assert_eq!(
            hit_count(&index, parse_rewritten(&parser, "-furiously").as_ref()),
            3
        );
    }

    #[test]
    fn or_not_is_more_than_the_positive_term() {
        let (parser, index) = parser_and_index();
        let dolphins = hit_count(&index, parse_raw(&parser, "dolphins").as_ref());
        let raw = parse_raw(&parser, "dolphins OR NOT furiously");
        let rewritten = rewrite_negative_clauses(raw.box_clone());
        assert_eq!(dolphins, 3);
        assert_eq!(
            hit_count(&index, raw.as_ref()),
            dolphins,
            "unrewritten OR NOT ignores the negative clause"
        );
        assert_eq!(
            hit_count(&index, rewritten.as_ref()),
            4,
            "dolphins OR NOT furiously includes documents with neither term"
        );
    }
}
