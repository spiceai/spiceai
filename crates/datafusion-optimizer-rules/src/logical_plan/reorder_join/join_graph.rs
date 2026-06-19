// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion_common::{
    Column, DataFusionError, NullEquality, Result, plan_datafusion_err, plan_err,
};
use datafusion_expr::{
    Expr, Filter, JoinType, LogicalPlan, Operator,
    utils::{
        check_all_columns_from_schema, conjunction, disjunction, split_binary,
        split_conjunction, split_conjunction_owned,
    },
};

pub type NodeId = usize;

pub struct Node {
    pub plan: Arc<LogicalPlan>,
    pub(crate) connections: Vec<EdgeId>,
}

impl Node {
    pub fn connections(&self) -> &[EdgeId] {
        &self.connections
    }

    pub(crate) fn connection_with<'graph>(
        &self,
        node_id: NodeId,
        join_graph: &'graph JoinGraph,
    ) -> Option<&'graph Edge> {
        self.connections
            .iter()
            .filter_map(|edge_id| join_graph.get_edge(*edge_id))
            .find(move |x| x.nodes.contains(&node_id))
    }

    pub fn neighbours(&self, node_id: NodeId, join_graph: &JoinGraph) -> Vec<NodeId> {
        self.connections
            .iter()
            .filter_map(|edge_id| join_graph.get_edge(*edge_id))
            .flat_map(|edge| edge.nodes)
            .filter(|&id| id != node_id)
            .collect()
    }
}

pub type EdgeId = usize;

/// An edge connecting two nodes in the join graph.
///
/// For symmetric edges (`join_type == JoinType::Inner`), the order of
/// `nodes` carries no semantic meaning; either side may end up on the
/// physical left or right after reordering.
///
/// For asymmetric edges (`LeftSemi` / `LeftAnti`, established by
/// `flatten_joins_recursive`), the order is load-bearing: `nodes[0]` is
/// always the preserved (LHS) relation that contributes rows to the
/// output, and `nodes[1]` is always the RHS blob that acts as a filter.
/// `RightSemi` / `RightAnti` joins are normalized to the `Left` variants
/// at extraction time so the optimizer interior only ever sees this
/// orientation. The reconstruction code in
/// `left_deep_join_plan::into_logical_plan` relies on this invariant to
/// orient the rebuilt `Join` correctly.
///
/// A "soft" theta-edge has an empty `on` and a `Some(filter)`: a non-equi
/// predicate (e.g. q7's `(n1='JAPAN' AND n2='CHINA') OR …`) that links the
/// two relations but cannot be expressed as equi-keys. It is produced by
/// `derive_theta_edges` so IK84 keeps the two relations adjacent and the
/// reconstruction attaches the predicate as a `Join` filter between them
/// (never a top-level cross join over large subtrees).
pub struct Edge {
    pub nodes: [NodeId; 2],
    pub on: Vec<(Expr, Expr)>,
    /// Non-equi predicate carried by a theta-edge (see the type doc). `None`
    /// for ordinary equi-join edges.
    pub filter: Option<Expr>,
    pub join_type: JoinType,
    pub null_equality: NullEquality,
}

pub struct JoinGraph {
    pub(crate) nodes: VecMap<Node>,
    edges: VecMap<Edge>,
    /// Non-equi predicates hoisted out of decomposed `Join.filter` clauses
    /// and out of `LogicalPlan::Filter` nodes that sit between joins.
    /// The enumerator must reapply these on top of the reordered plan.
    filters: Vec<Expr>,
}

impl JoinGraph {
    pub fn try_from_logical_plan(
        value: LogicalPlan,
    ) -> Result<(JoinGraph, Vec<LogicalPlan>), DataFusionError> {
        // First, extract the join subtree from any wrapper operators
        let (join_subtree, wrappers) = extract_join_subtree(value)?;

        // Now convert only the join subtree to a query graph
        let mut join_graph = JoinGraph::new();
        flatten_joins_recursive(join_subtree, &mut join_graph)?;
        join_graph.derive_implied_single_table_filters();
        // Promote non-equi predicates that link exactly two relations (e.g. q7's
        // `(n1='JAPAN' AND n2='CHINA') OR …`) from the side-channel into soft
        // theta-edges, so IK84 keeps the two relations adjacent and the rebuild
        // attaches the predicate as a `Join` filter between them instead of
        // stranding one into a top-level cross join.
        join_graph.derive_theta_edges();
        // Re-anchor degree-1 inner-join pendants onto the smallest relation in
        // their equi-join equivalence class (e.g. chbench q9's `item`, off the
        // 30M `order_line` and onto `stock`). Sound by construction: the
        // original equality is retained as a redundant side-channel filter, so
        // only the join *order* changes, never the result. Controlled overall
        // by the `join_reorder` rule gate.
        join_graph.rewire_pendants_to_selective_equivalent();
        join_graph.trace_state();
        Ok((join_graph, wrappers))
    }

    /// Emit the final graph state for troubleshooting (replaces the old
    /// `REORDER_DBG` eprintln dump). A one-line `debug` summary, plus per-node /
    /// per-edge / per-residual-filter detail at `trace`. A fragmented 2-node
    /// graph here means the rule ran after projection insertion fragmented the
    /// join tree; a connected N-node graph is what the enumerator reorders.
    ///
    /// Enable with
    /// `RUST_LOG=datafusion_optimizer_rules::logical_plan::reorder_join=trace`.
    fn trace_state(&self) {
        tracing::debug!(
            nodes = self.nodes.iter().count(),
            edges = self.edges.iter().count(),
            filters = self.filters.len(),
            "join graph built"
        );
        // Guard the per-element detail so the `format!`/iteration cost is only
        // paid when `trace` is actually enabled.
        if tracing::enabled!(tracing::Level::TRACE) {
            for (id, node) in self.nodes() {
                let head = format!("{}", node.plan);
                tracing::trace!(
                    node = id,
                    conns = node.connections.len(),
                    plan = head.lines().next().unwrap_or(""),
                    "graph node"
                );
            }
            for (eid, edge) in self.edges.iter() {
                tracing::trace!(
                    edge = eid,
                    nodes = ?edge.nodes,
                    keys = edge.on.len(),
                    theta = edge.filter.is_some(),
                    join_type = ?edge.join_type,
                    "graph edge"
                );
            }
            for (i, f) in self.filters.iter().enumerate() {
                tracing::trace!(side_channel_filter = i, expr = %f, "residual filter");
            }
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            nodes: VecMap::new(),
            edges: VecMap::new(),
            filters: Vec::new(),
        }
    }
    pub fn filters(&self) -> &[Expr] {
        &self.filters
    }

    pub(crate) fn add_filter(&mut self, expr: Expr) {
        self.filters.push(expr);
    }

    pub(crate) fn add_node(&mut self, node_data: Arc<LogicalPlan>) -> NodeId {
        self.nodes.insert(Node {
            plan: node_data,
            connections: Vec::new(),
        })
    }

    pub fn add_node_with_edge(
        &mut self,
        other: NodeId,
        node_data: Arc<LogicalPlan>,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> Option<NodeId> {
        if self.nodes.contains_key(other) {
            let new_id = self.nodes.insert(Node {
                plan: node_data,
                connections: Vec::new(),
            });
            self.add_edge(new_id, other, on, join_type, null_equality);
            Some(new_id)
        } else {
            None
        }
    }

    fn add_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> Option<EdgeId> {
        if self.nodes.contains_key(from) && self.nodes.contains_key(to) {
            let edge_id = self.edges.insert(Edge {
                nodes: [from, to],
                on,
                filter: None,
                join_type,
                null_equality,
            });
            if let Some(from) = self.nodes.get_mut(from) {
                from.connections.push(edge_id);
            }
            if let Some(to) = self.nodes.get_mut(to) {
                to.connections.push(edge_id);
            }
            Some(edge_id)
        } else {
            None
        }
    }

    pub fn remove_node(&mut self, node_id: NodeId) -> Option<Arc<LogicalPlan>> {
        if let Some(node) = self.nodes.remove(node_id) {
            // Remove all edges connected to this node
            for edge_id in &node.connections {
                if let Some(edge) = self.edges.remove(*edge_id) {
                    // Remove the edge from the other node's connections
                    for other_node_id in edge.nodes {
                        if other_node_id != node_id
                            && let Some(other_node) = self.nodes.get_mut(other_node_id)
                        {
                            other_node.connections.retain(|id| id != edge_id);
                        }
                    }
                }
            }
            Some(node.plan)
        } else {
            None
        }
    }

    pub fn remove_edge(&mut self, edge_id: EdgeId) -> Option<Edge> {
        if let Some(edge) = self.edges.remove(edge_id) {
            // Remove the edge from both nodes' connections
            for node_id in edge.nodes {
                if let Some(node) = self.nodes.get_mut(node_id) {
                    node.connections.retain(|id| *id != edge_id);
                }
            }
            Some(edge)
        } else {
            None
        }
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        self.nodes.iter()
    }

    pub(crate) fn get_node(&self, key: NodeId) -> Option<&Node> {
        self.nodes.get(key)
    }

    pub(crate) fn get_edge(&self, key: EdgeId) -> Option<&Edge> {
        self.edges.get(key)
    }

    /// Returns the id of an edge directly connecting `a` and `b`, if one
    /// exists.
    fn find_edge_between(&self, a: NodeId, b: NodeId) -> Option<EdgeId> {
        let node_a = self.nodes.get(a)?;
        node_a.connections.iter().copied().find(|&eid| {
            self.edges
                .get(eid)
                .map(|e| e.nodes.contains(&b))
                .unwrap_or(false)
        })
    }

    /// Appends `pairs` to the given edge's `on` list.
    fn extend_edge_on(&mut self, edge_id: EdgeId, pairs: Vec<(Expr, Expr)>) {
        if let Some(edge) = self.edges.get_mut(edge_id) {
            edge.on.extend(pairs);
        }
    }

    /// Adds a "soft" theta-edge: an `Inner` edge with no equi-keys carrying a
    /// non-equi `filter` between `a` and `b` (see the `Edge` type doc).
    fn add_theta_edge(&mut self, a: NodeId, b: NodeId, filter: Expr) -> Option<EdgeId> {
        if self.nodes.contains_key(a) && self.nodes.contains_key(b) {
            let edge_id = self.edges.insert(Edge {
                nodes: [a, b],
                on: Vec::new(),
                filter: Some(filter),
                join_type: JoinType::Inner,
                null_equality: NullEquality::NullEqualsNothing,
            });
            if let Some(node) = self.nodes.get_mut(a) {
                node.connections.push(edge_id);
            }
            if let Some(node) = self.nodes.get_mut(b) {
                node.connections.push(edge_id);
            }
            Some(edge_id)
        } else {
            None
        }
    }

    /// AND-merges `pred` into an existing edge's `filter` (used when a non-equi
    /// predicate links a pair already connected by an equi-edge).
    fn set_or_extend_edge_filter(&mut self, edge_id: EdgeId, pred: Expr) {
        if let Some(edge) = self.edges.get_mut(edge_id) {
            edge.filter = Some(match edge.filter.take() {
                Some(existing) => existing.and(pred),
                None => pred,
            });
        }
    }

    /// Returns true if a path already connects `from` to `to`, treating
    /// edges as undirected. Used to detect cycles before adding a new
    /// edge; if a path exists, the new edge would close a cycle.
    fn path_exists(&self, from: NodeId, to: NodeId) -> bool {
        use std::collections::HashSet;
        if from == to {
            return true;
        }
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut stack: Vec<NodeId> = vec![from];
        while let Some(n) = stack.pop() {
            if !visited.insert(n) {
                continue;
            }
            if let Some(node) = self.nodes.get(n) {
                for &eid in &node.connections {
                    if let Some(edge) = self.edges.get(eid) {
                        for &neighbour in &edge.nodes {
                            if neighbour == n {
                                continue;
                            }
                            if neighbour == to {
                                return true;
                            }
                            if !visited.contains(&neighbour) {
                                stack.push(neighbour);
                            }
                        }
                    }
                }
            }
        }
        false
    }

    /// Collects all plain-column equi-pairs (`Expr::Column = Expr::Column`)
    /// from inner-join edges. These seed the transitive equivalence
    /// classes used by `rewire_pendants_to_selective_equivalent`.
    fn column_equivalence_pairs(&self) -> Vec<(Column, Column)> {
        let mut pairs = Vec::new();
        for (_id, edge) in self.edges.iter() {
            if edge.join_type != JoinType::Inner {
                continue;
            }
            for (l, r) in &edge.on {
                if let (Expr::Column(lc), Expr::Column(rc)) = (l, r) {
                    pairs.push((lc.clone(), rc.clone()));
                }
            }
        }
        pairs
    }

    /// Derive implied single-table predicates from multi-table disjunctions
    /// in the side-channel and push them onto the referenced tables' nodes.
    ///
    /// A disjunction `(A1 ∧ B1) ∨ (A2 ∧ B2) ∨ …`, where the `Ai` constrain
    /// table X and the `Bi` constrain table Y, logically implies
    /// `(A1 ∨ A2 ∨ …)` on X and `(B1 ∨ B2 ∨ …)` on Y — a necessary
    /// condition every surviving row must satisfy. Crediting it on the scan
    /// lets the cost model see a small filtered dimension (chbench q7:
    /// `nation n1 ∈ {JAPAN, CHINA}` ≈ 2 rows) and seed the left-deep build
    /// chain there instead of leaving it a pendant joined last.
    ///
    /// Only derived when *every* disjunct contributes at least one conjunct
    /// for the table; otherwise the implied predicate degenerates to `true`
    /// (no information). The original disjunction stays in the side-channel
    /// and is reapplied on top of the reordered plan, so results are
    /// unchanged — only estimates and scan selectivity improve. The derived
    /// `Filter` also materializes in the reconstructed plan, pruning the
    /// dimension at scan time.
    pub(crate) fn derive_implied_single_table_filters(&mut self) {
        use std::collections::HashMap;

        // node_id -> predicates to AND onto that node's plan.
        let mut derived: HashMap<NodeId, Vec<Expr>> = HashMap::new();

        for filter in &self.filters {
            let disjuncts = split_binary(filter, Operator::Or);
            // A single operand means `filter` is not a top-level OR.
            if disjuncts.len() < 2 {
                continue;
            }
            let per_disjunct_conjs: Vec<Vec<&Expr>> =
                disjuncts.iter().map(|d| split_conjunction(d)).collect();

            for (node_id, node) in self.nodes() {
                let schema = node.plan.schema();
                let mut node_disjuncts: Vec<Expr> =
                    Vec::with_capacity(per_disjunct_conjs.len());
                let mut all_disjuncts_contribute = true;

                for conjs in &per_disjunct_conjs {
                    let owned: Vec<Expr> = conjs
                        .iter()
                        .filter(|c| {
                            let cols = c.column_refs();
                            !cols.is_empty()
                                && check_all_columns_from_schema(
                                    &cols,
                                    schema.as_ref(),
                                )
                                .unwrap_or(false)
                        })
                        .map(|c| (*c).clone())
                        .collect();
                    // A disjunct with no node-local conjunct cannot be
                    // bounded for this table, so the disjunction implies
                    // nothing about it.
                    if owned.is_empty() {
                        all_disjuncts_contribute = false;
                        break;
                    }
                    if let Some(conj) = conjunction(owned) {
                        node_disjuncts.push(conj);
                    }
                }

                if !all_disjuncts_contribute {
                    continue;
                }
                if let Some(implied) = disjunction(node_disjuncts) {
                    derived.entry(node_id).or_default().push(implied);
                }
            }
        }

        for (node_id, preds) in derived {
            if let Some(node) = self.nodes.get_mut(node_id) {
                for pred in preds {
                    tracing::debug!(
                        node = node_id,
                        predicate = %pred,
                        "derived implied single-table filter (shrinks the dimension's estimated cardinality)"
                    );
                    let input = Arc::clone(&node.plan);
                    if let Ok(filter) = Filter::try_new(pred, input) {
                        node.plan = Arc::new(LogicalPlan::Filter(filter));
                    }
                }
            }
        }
    }

    /// Promote side-channel predicates that link *exactly two* relations by a
    /// non-equi condition into first-class "soft" theta-edges.
    ///
    /// `flatten_joins_recursive` can only turn equi-pairs into edges; every
    /// other cross-relation predicate (e.g. chbench q7's
    /// `(n1='JAPAN' ∧ n2='CHINA') ∨ (n1='CHINA' ∧ n2='JAPAN')`) is hoisted to
    /// the side-channel and would otherwise be reapplied as a single top-level
    /// `Filter` over the *whole* reordered join. With no edge linking them, IK84
    /// has no signal to keep the two relations adjacent, so the reconstruction
    /// strands one of them and bridges it with an empty-`on` cross join (which a
    /// downstream pass turns into a `NestedLoopJoinExec` over large subtrees —
    /// q7's hang).
    ///
    /// For a predicate whose columns resolve in exactly two nodes we either
    /// attach it to the existing equi-edge between them (so it becomes that
    /// join's residual `filter`, matching the native plan), or — when no edge
    /// exists and adding one would not close a cycle — create a theta-edge. The
    /// predicate is removed from the side-channel so it is applied exactly once.
    /// Predicates spanning ≠2 relations, or that would close a cycle, stay in
    /// the side-channel unchanged. Results are identical either way; only join
    /// order and where the predicate materializes change.
    ///
    /// Runs *after* `derive_implied_single_table_filters` (which reads the same
    /// disjunctions to shrink the per-table cardinalities) and *before*
    /// `rewire_pendants_to_selective_equivalent`.
    pub(crate) fn derive_theta_edges(&mut self) {
        let filters = std::mem::take(&mut self.filters);
        let mut remaining: Vec<Expr> = Vec::with_capacity(filters.len());

        for filter in filters {
            let cols = filter.column_refs();
            if cols.is_empty() {
                remaining.push(filter);
                continue;
            }

            // Nodes whose schema owns at least one of the predicate's columns.
            let owners: Vec<NodeId> = self
                .nodes()
                .filter(|(_, node)| {
                    let schema = node.plan.schema();
                    cols.iter().any(|c| schema.has_column(c))
                })
                .map(|(id, _)| id)
                .collect();

            // Must link exactly two relations, and every column must resolve in
            // one of them (no unowned/ambiguous column).
            let all_covered = owners.len() == 2
                && {
                    let sa = self.get_node(owners[0]).map(|n| n.plan.schema());
                    let sb = self.get_node(owners[1]).map(|n| n.plan.schema());
                    match (sa, sb) {
                        (Some(sa), Some(sb)) => cols
                            .iter()
                            .all(|c| sa.has_column(c) || sb.has_column(c)),
                        _ => false,
                    }
                };
            if !all_covered {
                remaining.push(filter);
                continue;
            }

            let (a, b) = (owners[0], owners[1]);
            if let Some(edge_id) = self.find_edge_between(a, b) {
                tracing::debug!(
                    left = a,
                    right = b,
                    predicate = %filter,
                    "theta predicate merged onto existing edge"
                );
                self.set_or_extend_edge_filter(edge_id, filter);
            } else if !self.path_exists(a, b) {
                tracing::debug!(
                    left = a,
                    right = b,
                    predicate = %filter,
                    "theta soft edge added (keeps the two relations adjacent)"
                );
                self.add_theta_edge(a, b, filter);
            } else {
                // Adding an edge would close a cycle; leave it in the
                // side-channel for top-level reapplication.
                remaining.push(filter);
            }
        }

        self.filters = remaining;
    }

    /// Re-anchors degree-1 inner-join pendants onto a smaller relation that
    /// shares the pendant's join key via the transitive equivalence class.
    ///
    /// Prototype, gated on the `REORDER_DERIVE_EDGES` env var.
    ///
    /// Motivation (chbench q9): `item` connects only via `ol_i_id = i_id`
    /// to the 30M fact `order_line`. In a left-deep plan a pendant must
    /// immediately follow its neighbour, so this forces the fact early.
    /// Because `ol_i_id = s_i_id` also holds, the equivalence class of the
    /// key is `{ol_i_id, s_i_id, i_id}`; `stock` (owning `s_i_id`) is far
    /// smaller than `order_line`. We rewire `item`'s edge to
    /// `i_id = s_i_id` (anchoring on `stock`) and demote the original
    /// equality to a hoisted safety filter. Removing a pendant's only edge
    /// isolates it, then re-attaching it as a leaf of another tree node
    /// keeps the graph acyclic.
    pub(crate) fn rewire_pendants_to_selective_equivalent(&mut self) {
        struct Rewire {
            old_edge: EdgeId,
            pendant: NodeId,
            new_anchor: NodeId,
            col_pendant: Column,
            col_anchor: Column,
            col_old: Column,
            join_type: JoinType,
            null_equality: NullEquality,
        }

        let pairs = self.column_equivalence_pairs();
        let mut actions: Vec<Rewire> = Vec::new();

        for (node_id, node) in self.nodes() {
            if node.connections.len() != 1 {
                continue;
            }
            let edge_id = node.connections[0];
            let Some(edge) = self.get_edge(edge_id) else {
                continue;
            };
            if edge.join_type != JoinType::Inner || edge.on.len() != 1 {
                continue;
            }
            let (Expr::Column(lc), Expr::Column(rc)) = (&edge.on[0].0, &edge.on[0].1)
            else {
                continue;
            };
            let pendant_schema = node.plan.schema();
            let (col_pendant, col_old) = if column_in_schema(lc, pendant_schema) {
                (lc.clone(), rc.clone())
            } else if column_in_schema(rc, pendant_schema) {
                (rc.clone(), lc.clone())
            } else {
                continue;
            };
            let Some(neighbour) =
                edge.nodes.iter().copied().find(|&n| n != node_id)
            else {
                continue;
            };
            let Some(neighbour_node) = self.get_node(neighbour) else {
                continue;
            };
            let Ok(neighbour_card) =
                super::cost::estimate_cardinality(&neighbour_node.plan, None)
            else {
                continue;
            };

            let class = transitive_class(&pairs, &col_old);

            // Smallest-cardinality other node owning a class column.
            let mut best: Option<(NodeId, Column, f64)> = None;
            for (cand_id, cand) in self.nodes() {
                if cand_id == node_id || cand_id == neighbour {
                    continue;
                }
                let schema = cand.plan.schema();
                for cc in &class {
                    if column_in_schema(cc, schema) {
                        if let Ok(card) =
                            super::cost::estimate_cardinality(&cand.plan, None)
                            && best.as_ref().map(|(_, _, b)| card < *b).unwrap_or(true)
                        {
                            best = Some((cand_id, cc.clone(), card));
                        }
                        break;
                    }
                }
            }

            if let Some((anchor, col_anchor, anchor_card)) = best
                && anchor_card < neighbour_card
            {
                actions.push(Rewire {
                    old_edge: edge_id,
                    pendant: node_id,
                    new_anchor: anchor,
                    col_pendant,
                    col_anchor,
                    col_old,
                    join_type: edge.join_type,
                    null_equality: edge.null_equality,
                });
            }
        }

        for a in actions {
            self.remove_edge(a.old_edge);
            // Keep the original equality as a redundant safety filter; the
            // new edge plus the existing key edges imply it transitively.
            self.add_filter(
                Expr::Column(a.col_pendant.clone())
                    .eq(Expr::Column(a.col_old.clone())),
            );
            self.add_edge(
                a.pendant,
                a.new_anchor,
                vec![(Expr::Column(a.col_pendant), Expr::Column(a.col_anchor))],
                a.join_type,
                a.null_equality,
            );
        }
    }
}

/// Returns true if `col` resolves within `schema`.
fn column_in_schema(col: &Column, schema: &datafusion_common::DFSchema) -> bool {
    let mut set = std::collections::HashSet::new();
    set.insert(col);
    check_all_columns_from_schema(&set, schema).unwrap_or(false)
}

/// Transitive closure of `seed`'s column-equivalence class over `pairs`.
fn transitive_class(pairs: &[(Column, Column)], seed: &Column) -> Vec<Column> {
    use std::collections::HashSet;
    let mut class: HashSet<Column> = HashSet::new();
    class.insert(seed.clone());
    let mut changed = true;
    while changed {
        changed = false;
        for (l, r) in pairs {
            let has_l = class.contains(l);
            let has_r = class.contains(r);
            if has_l && !has_r {
                class.insert(r.clone());
                changed = true;
            } else if has_r && !has_l {
                class.insert(l.clone());
                changed = true;
            }
        }
    }
    class.into_iter().collect()
}

/// Extracts the join subtree from a logical plan, separating it from wrapper operators.
///
/// This function traverses the plan tree from the root downward, collecting all non-join
/// operators until it finds the topmost join node. The join subtree (all consecutive joins)
/// is extracted and returned separately from the wrapper operators.
///
/// # Arguments
///
/// * `plan` - The logical plan to extract from
///
/// # Returns
///
/// Returns a tuple of (join_subtree, wrapper_operators) where:
/// - `join_subtree` is the topmost join and all joins beneath it
/// - `wrapper_operators` is a vector of non-join operators above the joins, in order from root to join
///
/// # Errors
///
/// Returns an error if the plan doesn't contain any joins.
pub(crate) fn extract_join_subtree(
    plan: LogicalPlan,
) -> Result<(LogicalPlan, Vec<LogicalPlan>)> {
    let mut wrappers = Vec::new();
    let mut current = plan;
    let original_display = current.display().to_string();

    // Descend through single-input non-join nodes until we find a join.
    // Wrappers that sit *between* joins are no longer rejected here; they are
    // handled inside `flatten_joins_recursive` (absorbed as opaque leaves or,
    // for `Filter` directly above a decomposable join, hoisted to the
    // side-channel). This pass only strips wrappers above the topmost join.
    loop {
        match current {
            LogicalPlan::Join(_) => {
                // Found the join subtree root
                return Ok((current, wrappers));
            }
            other => {
                let inputs = other.inputs();
                if inputs.is_empty() {
                    return plan_err!(
                        "Plan does not contain any join nodes: {}",
                        original_display
                    );
                }
                if inputs.len() != 1 {
                    return plan_err!(
                        "Join extraction only supports single-input operators, found {} inputs in: {}",
                        inputs.len(),
                        other.display()
                    );
                }

                let next = (*inputs[0]).clone();
                wrappers.push(other.clone());
                current = next;
            }
        }
    }
}

/// Reconstructs a logical plan by wrapping an optimized join plan with the original wrapper operators.
///
/// This function takes an optimized join plan and re-applies the wrapper operators (Filter, Sort,
/// Aggregate, etc.) that were removed during extraction. The wrappers are applied in reverse order
/// (innermost to outermost) to reconstruct the original plan structure.
///
/// # Arguments
///
/// * `join_plan` - The optimized join plan to wrap
/// * `wrappers` - Vector of wrapper operators in order from outermost to innermost (root to join)
///
/// # Returns
///
/// Returns the fully reconstructed logical plan with all wrapper operators reapplied.
///
/// # Errors
///
/// Returns an error if reconstructing any wrapper operator fails.
pub fn reconstruct_plan(
    join_plan: LogicalPlan,
    wrappers: Vec<LogicalPlan>,
) -> Result<LogicalPlan> {
    let mut current = join_plan;

    // Apply wrappers in reverse order (from innermost to outermost)
    for wrapper in wrappers.into_iter().rev() {
        // Use with_new_exprs to reconstruct the wrapper with the new input
        current = wrapper.with_new_exprs(wrapper.expressions(), vec![current])?;
    }

    Ok(current)
}

fn flatten_joins_recursive(plan: LogicalPlan, join_graph: &mut JoinGraph) -> Result<()> {
    match plan {
        // Inner joins decompose into the graph. (Cross joins are encoded as
        // Inner with an empty `on` list, which is also handled here: the
        // equi-key loop simply runs zero iterations and the children are
        // joined by absence of edges, matching cross-product connectivity.)
        // The join's `filter` clause is hoisted into the side-channel so the
        // enumerator can reapply it after reordering.
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
            if let Some(filter) = join.filter.clone() {
                for conj in split_conjunction_owned(filter) {
                    join_graph.add_filter(conj);
                }
            }

            flatten_joins_recursive(
                Arc::unwrap_or_clone(Arc::clone(&join.left)),
                join_graph,
            )?;
            flatten_joins_recursive(
                Arc::unwrap_or_clone(Arc::clone(&join.right)),
                join_graph,
            )?;

            // Group each equi-pair by which two nodes it connects. A
            // single `Join.on` can mix pairs that span different node-
            // pairs (e.g. an outer join in a bushy plan whose `on`
            // contains keys from disjoint sub-trees); putting all pairs
            // on every edge produces edges that reference columns
            // missing from their endpoints' schemas, and the resulting
            // multi-edge structure forms a cycle that IK84 can't
            // process.
            use std::collections::HashMap;
            let mut pairs_by_node_pair: HashMap<(NodeId, NodeId), Vec<(Expr, Expr)>> =
                HashMap::new();
            let mut insertion_order: Vec<(NodeId, NodeId)> = Vec::new();

            for (left_key, right_key) in &join.on {
                let left_columns = left_key.column_refs();
                let right_columns = right_key.column_refs();

                let matching_nodes: Vec<NodeId> = join_graph
                    .nodes()
                    .filter_map(|(node_id, node)| {
                        let schema = node.plan.schema();
                        let has_left =
                            check_all_columns_from_schema(&left_columns, schema.as_ref())
                                .unwrap_or(false);
                        let has_right = check_all_columns_from_schema(
                            &right_columns,
                            schema.as_ref(),
                        )
                        .unwrap_or(false);
                        if (has_left && !has_right) || (!has_left && has_right) {
                            Some(node_id)
                        } else {
                            None
                        }
                    })
                    .collect();

                if matching_nodes.len() != 2 {
                    return plan_err!(
                        "Could not find exactly two nodes for join predicate: {} = {} (found {} nodes)",
                        left_key,
                        right_key,
                        matching_nodes.len()
                    );
                }

                let mut endpoints = [matching_nodes[0], matching_nodes[1]];
                endpoints.sort();
                let key = (endpoints[0], endpoints[1]);
                if !pairs_by_node_pair.contains_key(&key) {
                    insertion_order.push(key);
                }
                pairs_by_node_pair
                    .entry(key)
                    .or_default()
                    .push((left_key.clone(), right_key.clone()));
            }

            for (node_a, node_b) in insertion_order {
                let pairs = pairs_by_node_pair.remove(&(node_a, node_b)).unwrap();

                // If a prior recursive call already connected these two
                // nodes by an edge, merge our pairs into it instead of
                // adding a parallel edge.
                if let Some(existing_edge_id) =
                    join_graph.find_edge_between(node_a, node_b)
                {
                    join_graph.extend_edge_on(existing_edge_id, pairs);
                    continue;
                }

                // Cycle check: adding this edge would close a cycle.
                // IK84 needs a tree, so demote the equi-pairs of this
                // group to side-channel filter conjuncts; they'll be
                // re-applied as a Filter above the reordered join.
                if join_graph.path_exists(node_a, node_b) {
                    for (l, r) in pairs {
                        join_graph.add_filter(l.eq(r));
                    }
                    continue;
                }

                join_graph.add_edge(
                    node_a,
                    node_b,
                    pairs,
                    join.join_type,
                    join.null_equality,
                );
            }

            Ok(())
        }
        // Semi/anti joins (Left and Right variants) decompose
        // asymmetrically: the preserved side participates in reordering
        // normally, while the filtering side becomes one opaque blob.
        // The semi/anti edge then connects the LHS node that owns the
        // join key(s) to the blob. This mirrors DuckDB's relation_manager
        // logic at relation_manager.cpp:334-346.
        //
        // Right{Semi,Anti} are normalized to Left{Semi,Anti} here by
        // flipping the join's children and on-keys.
        //
        // If the LHS key(s) span more than one already-extracted sub-
        // relation (multi-LHS semi), the resulting join graph would be
        // cyclic and IK84 cannot handle it. We fall back to opaque in
        // that case.
        LogicalPlan::Join(join)
            if matches!(
                join.join_type,
                JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
                    | JoinType::RightAnti
            ) =>
        {
            // The join-graph `Edge` carries only equi-keys (`on`), not a join
            // `filter`. A semi/anti join with a correlation filter — e.g. q21's
            // `NOT EXISTS (… AND l2.ol_delivery_d > l1.ol_delivery_d)` — cannot
            // be represented as an edge without dropping that filter and
            // corrupting the plan (the inner predicates collapse into a
            // side-channel `Filter` over a single relation, referencing columns
            // that no longer resolve). Treat such a join as opaque (one node,
            // left un-reordered) so the query stays correct. q21's cost is the
            // anti-join build side (a cardinality concern), not inner-join
            // ordering, so we lose little by not reordering through it.
            if join.filter.is_some() {
                join_graph.add_node(Arc::new(LogicalPlan::Join(join)));
                return Ok(());
            }

            // Pre-flight multi-LHS guard. We inspect the qualifiers on
            // the LHS key columns *before* recursing into the LHS
            // subtree, because we cannot cleanly undo a recursive flatten.
            // If the LHS key columns reference more than one distinct
            // table qualifier, we treat the whole join as opaque.
            let (lhs_child, rhs_child, semi_join_type, on) = match join.join_type {
                JoinType::RightSemi | JoinType::RightAnti => (
                    Arc::clone(&join.right),
                    Arc::clone(&join.left),
                    join.join_type.swap(),
                    join.on
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect::<Vec<_>>(),
                ),
                _ => (
                    Arc::clone(&join.left),
                    Arc::clone(&join.right),
                    join.join_type,
                    join.on.clone(),
                ),
            };

            // Collect the table qualifiers of every LHS key column. We
            // require exactly one distinct qualifier: zero means at
            // least one column is unqualified (can't safely identify the
            // owner pre-flight), more than one means multi-LHS. Either
            // case falls back to opaque.
            let lhs_qualifiers: std::collections::HashSet<_> = on
                .iter()
                .flat_map(|(lhs_key, _)| lhs_key.column_refs())
                .map(|c| c.relation.clone())
                .collect();
            if lhs_qualifiers.len() != 1 || lhs_qualifiers.contains(&None) {
                join_graph.add_node(Arc::new(LogicalPlan::Join(join)));
                return Ok(());
            }

            // Recurse into the LHS subtree, then attach the blob.
            flatten_joins_recursive(Arc::unwrap_or_clone(lhs_child), join_graph)?;
            let blob_id = join_graph.add_node(rhs_child);

            // Find the single LHS-side node that owns the join key(s).
            // With the single-qualifier guard above this should be a
            // unique owner per key, with all keys agreeing. Any deviation
            // is an internal-invariant error.
            let mut lhs_owner: Option<NodeId> = None;
            for (lhs_key, _) in &on {
                let lhs_cols = lhs_key.column_refs();
                let owners: Vec<NodeId> = join_graph
                    .nodes()
                    .filter(|(id, _)| *id != blob_id)
                    .filter_map(|(id, node)| {
                        check_all_columns_from_schema(
                            &lhs_cols,
                            node.plan.schema().as_ref(),
                        )
                        .unwrap_or(false)
                        .then_some(id)
                    })
                    .collect();
                if owners.len() != 1 {
                    return plan_err!(
                        "Semi/anti join LHS key {} has {} candidate owner(s) \
                         among extracted left-side nodes; expected exactly 1",
                        lhs_key,
                        owners.len()
                    );
                }
                let owner = owners[0];
                match lhs_owner {
                    None => lhs_owner = Some(owner),
                    Some(prev) if prev != owner => {
                        return plan_err!(
                            "Semi/anti join LHS keys span multiple sub-relations after \
                             extraction; this should have been caught by the multi-LHS guard"
                        );
                    }
                    _ => {}
                }
            }
            let lhs_owner = lhs_owner.ok_or_else(|| {
                plan_datafusion_err!(
                    "Semi/anti join has no equi-keys; cannot determine LHS owner"
                )
            })?;

            // Convention: nodes[0] = LHS owner, nodes[1] = blob.
            join_graph.add_edge(
                lhs_owner,
                blob_id,
                on,
                semi_join_type,
                join.null_equality,
            );
            Ok(())
        }
        // Other non-inner joins (Left/Right/Full/Mark) are not freely
        // reorderable, so the entire join subtree becomes one opaque leaf.
        LogicalPlan::Join(join) => {
            join_graph.add_node(Arc::new(LogicalPlan::Join(join)));
            Ok(())
        }
        // A `Filter` directly above a decomposable join is part of the join
        // region: hoist its conjuncts to the side-channel and recurse into
        // the join.
        LogicalPlan::Filter(filter)
            if matches!(
                filter.input.as_ref(),
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner
            ) =>
        {
            for conj in split_conjunction_owned(filter.predicate) {
                join_graph.add_filter(conj);
            }
            let inner = Arc::unwrap_or_clone(filter.input);
            flatten_joins_recursive(inner, join_graph)
        }
        // Anything else (Aggregate, Projection, Sort, Limit, Window, Filter
        // not over a decomposable join, base scans, ...) is absorbed as an
        // opaque leaf. Joins nested inside such a wrapper are intentionally
        // hidden from the enumerator (matches Databend's dphyp behavior).
        other => {
            join_graph.add_node(Arc::new(other));
            Ok(())
        }
    }
}

/// A simple Vec-based map that uses `Option<T>` for sparse storage
/// Keys are never reused once removed
pub(crate) struct VecMap<V>(Vec<Option<V>>);

impl<V> VecMap<V> {
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn insert(&mut self, value: V) -> usize {
        let idx = self.0.len();
        self.0.push(Some(value));
        idx
    }

    pub(crate) fn get(&self, key: usize) -> Option<&V> {
        self.0.get(key)?.as_ref()
    }

    pub(crate) fn get_mut(&mut self, key: usize) -> Option<&mut V> {
        self.0.get_mut(key)?.as_mut()
    }

    pub(crate) fn remove(&mut self, key: usize) -> Option<V> {
        self.0.get_mut(key)?.take()
    }

    pub(crate) fn contains_key(&self, key: usize) -> bool {
        self.0.get(key).and_then(|v| v.as_ref()).is_some()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, &V)> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.as_ref().map(|v| (idx, v)))
    }
}

