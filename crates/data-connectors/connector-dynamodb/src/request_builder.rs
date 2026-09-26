/*
Copyright 2025 The Spice.ai OSS Authors

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
use crate::filter::{
    Condition, KeyPredicate, KeyReading, MAX_EXPRESSION_BYTES, Placeholders, SortPredicate,
    Translator, compare, prefix_end, satisfies,
};
use crate::request_plan::{DynamoDBRequestPlan, QueryParams, ScanParams};
use crate::table_schema::DynamoDBTableSchema;
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Debug)]
pub struct DynamoDBRequestPlanBuilder {
    schema: DynamoDBTableSchema,
}

/// Where the filters let a request read from.
#[derive(Debug)]
enum KeyPlan {
    /// Query each of these partitions, with an optional sort-key condition.
    /// `consumed[i]` is whether filter `i` is stated exactly by the key
    /// condition, so needs no filter expression.
    Query {
        partitions: Vec<AttributeValue>,
        sort: Option<SortPredicate>,
        /// Sort-key predicates checked on each item, which `sort` does not state.
        residual: Vec<SortPredicate>,
        consumed: Vec<bool>,
    },
    Scan,
    Empty,
}

/// Builds the `DynamoDB` request (Query or Scan) that reads the rows a scan's
/// filters keep.
///
///  * A Query is issued for every partition-key value an equality or `IN` list
///    names, each read as its own partition, with the sort-key predicates the
///    key condition can state. A key condition carries one sort-key predicate,
///    and a Query's filter expression may not read a key attribute, so a
///    sort-key filter reported exact has to be stated exactly by it; when the
///    exact ones cannot be, the table is scanned instead.
///  * A Scan carries every filter in its filter expression.
///
/// Attribute names and values go through generated placeholders, so any
/// attribute name — a reserved word, a space, a `-` — is expressible.
impl DynamoDBRequestPlanBuilder {
    pub fn new(schema: DynamoDBTableSchema) -> Self {
        Self { schema }
    }

    /// Splits `filters` into those a request can state together within
    /// `DynamoDB`'s expression limits, and those left over, which the rows read
    /// have to be checked against instead. `DataFusion` offers a scan its
    /// filters over several optimizer passes, each accepting what fits
    /// alongside the filters it sees, so the filters a scan is handed can
    /// exceed what one pass accepted. The longest filters that no key
    /// condition states are left over first.
    pub fn split_within_limits(
        &self,
        filters: &[Expr],
        projection_schema: &SchemaRef,
        json_nesting_static_fields: Option<&HashSet<String>>,
    ) -> DataFusionResult<(Vec<Expr>, Vec<Expr>)> {
        let translator = Translator::new(&self.schema);
        let mut remote = filters.to_vec();
        let mut local = Vec::new();
        while !self
            .build_request_plan(&remote, projection_schema, None, json_nesting_static_fields)?
            .fits_expression_limits()
        {
            let cost = |filter: &Expr| {
                let length = translator
                    .condition(filter, &mut Placeholders::default())
                    .map_or(0, |condition| condition.expression.len());
                (translator.key_predicate(filter).is_none(), length)
            };
            let Some(longest) = (0..remote.len()).max_by_key(|&i| cost(&remote[i])) else {
                break;
            };
            local.push(remote.remove(longest));
        }
        Ok((remote, local))
    }

    /// Build a `DynamoDB` request (Query or Scan) based on filters and projections
    pub fn build_request_plan(
        &self,
        filters: &[Expr],
        projection_schema: &SchemaRef,
        limit: Option<usize>,
        json_nesting_static_fields: Option<&HashSet<String>>,
    ) -> DataFusionResult<DynamoDBRequestPlan> {
        let translator = Translator::new(&self.schema);

        // DataFusion hands the scan only the filters `supports_filters_pushdown`
        // accepted, and translating one again gives the same condition, so a
        // failure is a bug rather than a filter to skip: an exact filter is
        // applied nowhere else.
        let conditions = filters
            .iter()
            .map(|filter| {
                translator
                    .condition(filter, &mut Placeholders::default())
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "DynamoDB filter {filter} was accepted for pushdown but could not be translated"
                        ))
                    })
            })
            .collect::<DataFusionResult<Vec<Condition>>>()?;

        let limit = limit
            .map(|l| {
                i32::try_from(l)
                    .map_err(|_| DataFusionError::Execution("Limit too large".to_string()))
            })
            .transpose()?;

        let project = json_nesting_static_fields.is_none();

        match Self::key_plan(&translator, filters, &conditions) {
            KeyPlan::Empty => Ok(DynamoDBRequestPlan::Empty),
            KeyPlan::Scan => {
                let mut out = Placeholders::default();
                let filter_expression = Self::conjunction(&translator, filters, &mut out, |_| true);
                let projection_expression = project
                    .then(|| self.build_projection_expression(projection_schema, &mut out))
                    .flatten();
                let (names, values) = out.into_parts();
                Ok(DynamoDBRequestPlan::Scan(ScanParams {
                    table_name: self.schema.table_name().to_string(),
                    // DynamoDB's `Limit` counts the items evaluated, not those a
                    // filter expression keeps.
                    limit: if filter_expression.is_none() {
                        limit
                    } else {
                        None
                    },
                    filter_expression,
                    expression_attribute_values: values,
                    expression_attribute_names: names,
                    projection_expression,
                }))
            }
            KeyPlan::Query {
                partitions,
                sort,
                residual,
                consumed,
            } => {
                let queries = partitions
                    .into_iter()
                    .map(|partition| {
                        let mut out = Placeholders::default();
                        let pk = out.name(self.schema.partition_key());
                        let pk_value = out.value(partition);
                        let mut key_condition = format!("{pk} = {pk_value}");
                        let mut residual = residual.clone();
                        if let (Some(sort), Some(sort_key)) = (&sort, self.schema.sort_key()) {
                            let sk = out.name(sort_key);
                            match render_sort(&sk, sort, &mut out) {
                                Some(condition) => {
                                    key_condition.push_str(" AND ");
                                    key_condition.push_str(&condition);
                                }
                                None => residual.push(sort.clone()),
                            }
                        }
                        // What the key condition does not state goes to the filter
                        // expression, which may not read a key attribute; `key_plan`
                        // already sent any exact filter that would need to to a Scan.
                        let filter_expression =
                            Self::conjunction(&translator, filters, &mut out, |i| {
                                !consumed[i] && !conditions[i].reads_key
                            });
                        // An item checked against the residual must carry its sort key.
                        let mut projection_expression = project
                            .then(|| self.build_projection_expression(projection_schema, &mut out))
                            .flatten();
                        if let (false, Some(projected), Some(sort_key)) = (
                            residual.is_empty(),
                            &mut projection_expression,
                            self.schema.sort_key(),
                        ) {
                            let sk = out.name(sort_key);
                            if !projected.split(", ").any(|p| p == sk) {
                                projected.push_str(", ");
                                projected.push_str(&sk);
                            }
                        }
                        let (names, values) = out.into_parts();
                        // DynamoDB's `Limit` counts the items read, before the filter
                        // expression or the residual thins them.
                        let thinned = filter_expression.is_some() || !residual.is_empty();
                        QueryParams {
                            table_name: self.schema.table_name().to_string(),
                            key_condition_expression: Some(key_condition),
                            limit: if thinned { None } else { limit },
                            filter_expression,
                            expression_attribute_values: values,
                            expression_attribute_names: names,
                            projection_expression,
                            scan_index_forward: None,
                            residual,
                            sort_key: self.schema.sort_key().map(ToString::to_string),
                            sort_key_reading: self.schema.sort_key_reading(),
                        }
                    })
                    .collect();
                Ok(DynamoDBRequestPlan::Query(queries))
            }
        }
    }

    /// The conjunction of the conditions of the filters `include` selects.
    fn conjunction(
        translator: &Translator<'_>,
        filters: &[Expr],
        out: &mut Placeholders,
        include: impl Fn(usize) -> bool,
    ) -> Option<String> {
        let parts: Vec<String> = filters
            .iter()
            .enumerate()
            .filter(|(i, _)| include(*i))
            .filter_map(|(_, filter)| translator.condition(filter, out))
            .map(|condition| condition.expression)
            .collect();
        (!parts.is_empty()).then(|| parts.join(" AND "))
    }

    fn key_plan(
        translator: &Translator<'_>,
        filters: &[Expr],
        conditions: &[Condition],
    ) -> KeyPlan {
        let predicates: Vec<Option<KeyPredicate>> = filters
            .iter()
            .map(|f| translator.key_predicate(f))
            .collect();
        let mut consumed = vec![false; filters.len()];

        // Every partition-key predicate is an exact equality or membership, so
        // together they name the intersection of their values.
        let mut partitions: Option<Vec<AttributeValue>> = None;
        let mut excluded: Vec<AttributeValue> = Vec::new();
        for (i, predicate) in predicates.iter().enumerate() {
            match predicate {
                Some(KeyPredicate::Partition(values)) => {
                    let mut unique: Vec<AttributeValue> = Vec::with_capacity(values.len());
                    for value in values {
                        if !unique.contains(value) {
                            unique.push(value.clone());
                        }
                    }
                    partitions = Some(match partitions {
                        None => unique,
                        Some(current) => {
                            current.into_iter().filter(|v| unique.contains(v)).collect()
                        }
                    });
                    consumed[i] = true;
                }
                Some(KeyPredicate::PartitionExcept(values)) => {
                    excluded.extend(values.iter().cloned());
                    consumed[i] = true;
                }
                _ => {}
            }
        }
        let Some(partitions) = partitions else {
            // An exclusion alone names no partition to query; the filter
            // expression of a Scan carries it.
            for (i, predicate) in predicates.iter().enumerate() {
                if matches!(predicate, Some(KeyPredicate::PartitionExcept(_))) {
                    consumed[i] = false;
                }
            }
            return KeyPlan::Scan;
        };
        // No item has an empty key.
        let partitions: Vec<AttributeValue> = partitions
            .into_iter()
            .filter(|v| !is_empty(v) && !excluded.contains(v))
            .collect();
        if partitions.is_empty() {
            return KeyPlan::Empty;
        }

        let mut exact = Vec::new();
        let mut inexact = Vec::new();
        for (i, predicate) in predicates.iter().enumerate() {
            if let Some(KeyPredicate::Sort(sort)) = predicate {
                if conditions[i].exact {
                    exact.push((i, sort.clone()));
                } else {
                    inexact.push(sort.clone());
                }
            }
        }

        let (sort, residual) = match choose_sort(&exact, &inexact) {
            SortChoice::Chosen {
                key,
                residual,
                stated,
            } => {
                for i in stated {
                    consumed[i] = true;
                }
                (key, residual)
            }
            SortChoice::Empty => return KeyPlan::Empty,
        };

        // An exact filter must be applied remotely. One the key condition does
        // not state, reading a key attribute, cannot go in a Query's filter
        // expression either.
        if conditions
            .iter()
            .enumerate()
            .any(|(i, condition)| condition.exact && condition.reads_key && !consumed[i])
        {
            return KeyPlan::Scan;
        }

        KeyPlan::Query {
            partitions,
            sort,
            residual,
            consumed,
        }
    }

    /// The projection expression reading `projection`'s columns, or `None` to
    /// read whole items when it would be longer than `DynamoDB` accepts. It is
    /// written last, so its placeholders can be taken back.
    fn build_projection_expression(
        &self,
        projection: &SchemaRef,
        out: &mut Placeholders,
    ) -> Option<String> {
        let checkpoint = out.checkpoint();
        let expression = self.projection_expression(projection, out)?;
        // Room for the sort key a residual check appends.
        if expression.len() + 16 > MAX_EXPRESSION_BYTES {
            out.rollback(checkpoint);
            return None;
        }
        Some(expression)
    }

    fn projection_expression(
        &self,
        projection: &SchemaRef,
        out: &mut Placeholders,
    ) -> Option<String> {
        let mut seen_top_level = HashSet::new();
        let mut projection_expr = Vec::new();

        for field in &projection.fields {
            let field_name = field.name();
            // A flattened column is read by projecting its top-level attribute.
            let top_level = if self.schema.is_flattened_field(field_name) {
                field_name.split('.').next().unwrap_or(field_name)
            } else {
                field_name
            };
            if seen_top_level.insert(top_level) {
                projection_expr.push(out.name(top_level));
            }
        }

        if projection_expr.is_empty() {
            None
        } else {
            Some(projection_expr.join(", "))
        }
    }
}

enum SortChoice {
    /// The key condition's sort-key predicate, if any; the exact predicates it
    /// does not state, which each item is checked against; and the filters
    /// the two state between them.
    Chosen {
        key: Option<SortPredicate>,
        residual: Vec<SortPredicate>,
        stated: Vec<usize>,
    },
    /// The sort-key predicates select no item.
    Empty,
}

/// The key condition that reads the fewest items every sort-key predicate
/// allows, and the exact predicates it does not state, which are checked on
/// each item read. The inexact predicates only narrow the key condition:
/// `DataFusion` applies them again.
fn choose_sort(exact: &[(usize, SortPredicate)], inexact: &[SortPredicate]) -> SortChoice {
    use SortPredicate::Eq;

    let Some((exact, inexact, mut stated)) = without_empty_values(exact, inexact) else {
        return SortChoice::Empty;
    };
    stated.extend(exact.iter().map(|(i, _)| *i));

    // An equality is decided against every other exact predicate here.
    if let Some(v) = exact.iter().find_map(|(_, p)| match p {
        Eq(v) => Some(v.clone()),
        _ => None,
    }) {
        let mut residual = Vec::new();
        for (_, p) in &exact {
            // Both are literals, so neither is read through the column.
            match satisfies(&v, p, KeyReading::Stored) {
                Some(true) => {}
                Some(false) => return SortChoice::Empty,
                None => residual.push(p.clone()),
            }
        }
        return SortChoice::Chosen {
            key: Some(Eq(v)),
            residual,
            stated,
        };
    }

    let predicates: Vec<&SortPredicate> =
        exact.iter().map(|(_, p)| p).chain(inexact.iter()).collect();
    let key = match tightest(&predicates) {
        Bounding::Nothing => return SortChoice::Empty,
        Bounding::Unbounded => None,
        Bounding::Bound(key) => Some(key),
    };
    // An exact predicate the key condition is not itself is checked item by item.
    let residual = exact
        .into_iter()
        .map(|(_, p)| p)
        .filter(|p| key.as_ref() != Some(p))
        .collect();
    SortChoice::Chosen {
        key,
        residual,
        stated,
    }
}

/// A bound on the sort key, and whether the bound value itself is in.
type Bound = (AttributeValue, bool);

/// The higher of two lower bounds; at the same value, the exclusive one.
fn higher(a: Option<Bound>, b: Bound) -> Bound {
    match a {
        None => b,
        Some(a) => match compare(&b.0, &a.0) {
            Some(Ordering::Greater) => b,
            Some(Ordering::Equal) => (a.0, a.1 && b.1),
            _ => a,
        },
    }
}

/// The lower of two upper bounds; at the same value, the exclusive one.
fn lower(a: Option<Bound>, b: Bound) -> Bound {
    match a {
        None => b,
        Some(a) => match compare(&b.0, &a.0) {
            Some(Ordering::Less) => b,
            Some(Ordering::Equal) => (a.0, a.1 && b.1),
            _ => a,
        },
    }
}

/// What a set of sort-key predicates leaves a key condition to state.
enum Bounding {
    /// They select no item.
    Nothing,
    /// They bound nothing a key condition can state.
    Unbounded,
    Bound(SortPredicate),
}

/// The key condition bounding every predicate.
fn tightest(predicates: &[&SortPredicate]) -> Bounding {
    use SortPredicate::{Between, Eq, Except, Lower, OneOf, Prefix, Upper};

    // A lone prefix is stated by `begins_with`, tighter than any range.
    if let [Prefix(p)] = predicates
        .iter()
        .copied()
        .filter(|p| !matches!(p, Except(_)))
        .collect::<Vec<_>>()
        .as_slice()
    {
        return Bounding::Bound(Prefix(p.clone()));
    }

    let (mut low, mut high): (Option<Bound>, Option<Bound>) = (None, None);
    for predicate in predicates {
        match predicate {
            Eq(v) => {
                low = Some(higher(low, (v.clone(), true)));
                high = Some(lower(high, (v.clone(), true)));
            }
            Lower(v, inclusive) => low = Some(higher(low, (v.clone(), *inclusive))),
            Upper(v, inclusive) => high = Some(lower(high, (v.clone(), *inclusive))),
            Between(from, to) => {
                low = Some(higher(low, (from.clone(), true)));
                high = Some(lower(high, (to.clone(), true)));
            }
            // Every string with the prefix lies from it to just below its end.
            Prefix(p) => {
                low = Some(higher(low, (AttributeValue::S(p.clone()), true)));
                if let Some(end) = prefix_end(p) {
                    high = Some(lower(high, (AttributeValue::S(end), false)));
                }
            }
            OneOf(values) => {
                for v in values {
                    // A membership is bounded by its least and greatest values.
                    let least = values
                        .iter()
                        .all(|w| compare(v, w) != Some(Ordering::Greater));
                    let greatest = values.iter().all(|w| compare(v, w) != Some(Ordering::Less));
                    if least {
                        low = Some(higher(low, (v.clone(), true)));
                    }
                    if greatest {
                        high = Some(lower(high, (v.clone(), true)));
                    }
                }
            }
            Except(_) => {}
        }
    }

    match (low, high) {
        (Some((from, from_in)), Some((to, to_in))) => match compare(&from, &to) {
            Some(Ordering::Greater) => Bounding::Nothing,
            Some(Ordering::Equal) if !(from_in && to_in) => Bounding::Nothing,
            Some(Ordering::Equal) => Bounding::Bound(Eq(from)),
            // BETWEEN is inclusive; an exclusive end is checked item by item.
            _ => Bounding::Bound(Between(from, to)),
        },
        (Some((from, inclusive)), None) => Bounding::Bound(Lower(from, inclusive)),
        (None, Some((to, inclusive))) => Bounding::Bound(Upper(to, inclusive)),
        (None, None) => Bounding::Unbounded,
    }
}

/// The predicates with those on an empty value decided: a key is never empty,
/// so a bound below every key is dropped (stating its filter, when exact) and
/// one at or below the empty value selects nothing (`None`).
#[expect(clippy::type_complexity, reason = "the three results of one partition")]
fn without_empty_values(
    exact: &[(usize, SortPredicate)],
    inexact: &[SortPredicate],
) -> Option<(Vec<(usize, SortPredicate)>, Vec<SortPredicate>, Vec<usize>)> {
    use SortPredicate::{Between, Eq, Except, Lower, OneOf, Prefix, Upper};
    // `Some(None)`: always true; `None`: never; `Some(Some(p))`: `p` still applies.
    let decide = |p: &SortPredicate| -> Option<Option<SortPredicate>> {
        match p {
            Eq(v) | Upper(v, _) if is_empty(v) => None,
            Between(_, high) if is_empty(high) => None,
            Between(low, high) if is_empty(low) => Some(Some(Upper(high.clone(), true))),
            Lower(v, _) if is_empty(v) => Some(None),
            Prefix(p) if p.is_empty() => Some(None),
            OneOf(values) if values.iter().any(is_empty) => {
                let values: Vec<AttributeValue> =
                    values.iter().filter(|v| !is_empty(v)).cloned().collect();
                if values.is_empty() {
                    None
                } else {
                    Some(Some(OneOf(values)))
                }
            }
            Except(values) if values.iter().any(is_empty) => {
                let values: Vec<AttributeValue> =
                    values.iter().filter(|v| !is_empty(v)).cloned().collect();
                Some((!values.is_empty()).then_some(Except(values)))
            }
            p => Some(Some(p.clone())),
        }
    };
    let mut remaining_exact = Vec::with_capacity(exact.len());
    let mut stated = Vec::new();
    for (i, p) in exact {
        match decide(p)? {
            Some(p) => remaining_exact.push((*i, p)),
            None => stated.push(*i),
        }
    }
    let mut remaining_inexact = Vec::with_capacity(inexact.len());
    for p in inexact {
        if let Some(p) = decide(p)? {
            remaining_inexact.push(p);
        }
    }
    Some((remaining_exact, remaining_inexact, stated))
}

fn is_empty(v: &AttributeValue) -> bool {
    match v {
        AttributeValue::S(s) => s.is_empty(),
        AttributeValue::B(b) => b.as_ref().is_empty(),
        _ => false,
    }
}

/// The key condition stating `sort`, when one can: a membership or an
/// exclusion is only ever checked item by item.
fn render_sort(sk: &str, sort: &SortPredicate, out: &mut Placeholders) -> Option<String> {
    Some(match sort {
        SortPredicate::Eq(v) => format!("{sk} = {}", out.value(v.clone())),
        SortPredicate::Lower(v, true) => format!("{sk} >= {}", out.value(v.clone())),
        SortPredicate::Lower(v, false) => format!("{sk} > {}", out.value(v.clone())),
        SortPredicate::Upper(v, true) => format!("{sk} <= {}", out.value(v.clone())),
        SortPredicate::Upper(v, false) => format!("{sk} < {}", out.value(v.clone())),
        SortPredicate::Between(low, high) => {
            let (low, high) = (out.value(low.clone()), out.value(high.clone()));
            format!("{sk} BETWEEN {low} AND {high}")
        }
        SortPredicate::Prefix(p) => {
            format!(
                "begins_with({sk}, {})",
                out.value(AttributeValue::S(p.clone()))
            )
        }
        SortPredicate::OneOf(_) | SortPredicate::Except(_) => return None,
    })
}

#[cfg(test)]
mod tests;
