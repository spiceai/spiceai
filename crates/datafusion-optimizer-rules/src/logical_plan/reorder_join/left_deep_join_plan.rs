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

use std::{collections::HashSet, fmt::Debug, sync::Arc};

use datafusion_common::{Result, plan_datafusion_err, plan_err, tree_node::TreeNode};
use datafusion_expr::{
    Expr, Filter, JoinConstraint, JoinType, LogicalPlan, utils::split_conjunction,
};

use super::{
    cost::JoinCostEstimator,
    join_graph::{JoinGraph, NodeId},
};

/// Generates an optimized left-deep join plan from a logical plan using the Ibaraki-Kameda algorithm.
///
/// This function is the main entry point for join reordering optimization. It takes a logical plan
/// that may contain joins along with wrapper operators (filters, sorts, aggregations, etc.) and
/// produces an optimized plan with reordered joins while preserving the wrapper operators.
///
/// # Algorithm Overview
///
/// The optimization process consists of several steps:
///
/// 1. **Extraction**: Separates the join subtree from wrapper operators (filters, sorts, limits, etc.)
/// 2. **Graph Conversion**: Converts the join subtree into a query graph representation where:
///    - Nodes represent base relations (or opaque subtrees containing non-reorderable operators)
///    - Edges represent equi-join conditions between relations
///    - Non-equi predicates are collected in a side-channel filter list
/// 3. **Optimization**: Uses the Ibaraki-Kameda algorithm to find the optimal left-deep join ordering
///    by trying each node as a potential root and selecting the plan with the lowest estimated cost
/// 4. **Reconstruction**: Rebuilds the complete logical plan by reapplying side-channel filters and
///    the wrapper operators on top of the optimized join plan
///
/// # Left-Deep Join Plans
///
/// A left-deep join plan is a join tree where:
/// - Each join has a relation or previous join result on the left side
/// - Each join has a single relation on the right side
/// - This creates a linear "chain" of joins processed left-to-right
///
/// Example: `((A ⋈ B) ⋈ C) ⋈ D` is left-deep, while `(A ⋈ B) ⋈ (C ⋈ D)` is not.
///
/// Left-deep plans are preferred because they:
/// - Allow pipelining of intermediate results
/// - Work well with hash join implementations
/// - Have predictable memory usage patterns
///
/// # Arguments
///
/// * `plan` - The logical plan to optimize. Must contain at least one join node.
/// * `cost_estimator` - Cost estimator for calculating join costs, cardinality, and selectivity.
///   Used to compare different join orderings and select the optimal one.
///
/// # Returns
///
/// Returns a `LogicalPlan` with optimized join ordering. The plan structure is:
/// - Wrapper operators (filters, sorts, etc.) in their original positions
/// - Joins reordered to minimize estimated execution cost
/// - Join semantics preserved (same result set as input plan)
pub fn optimal_left_deep_join_plan(
    plan: LogicalPlan,
    cost_estimator: &dyn JoinCostEstimator,
) -> Result<LogicalPlan> {
    // No joins anywhere in the plan: nothing to reorder. Returning the
    // input unchanged lets callers wire this in unconditionally without
    // having to pre-check.
    if !plan.exists(|p| Ok(matches!(p, LogicalPlan::Join(_))))? {
        return Ok(plan);
    }

    // Convert join subtree to query graph
    let (query_graph, wrappers) = JoinGraph::try_from_logical_plan(plan)?;

    // Optimize the joins
    let mut optimized_joins =
        query_graph_to_optimal_left_deep_join_plan(&query_graph, cost_estimator)?;

    // Reapply side-channel filters (hoisted from `Join.filter` and from
    // `Filter` nodes that sat between joins) as a single Filter on top of
    // the reordered plan.
    //
    // Deduplicate before reapplying. `add_filter` is called from several sites
    // (hoisted `Join.filter`, between-join `Filter` nodes, cycle-broken edges,
    // the pendant-rewire safety filter), so the same conjunct can be collected
    // more than once. We also skip any conjunct already carried by a wrapper
    // `Filter`, since `reconstruct_plan` re-applies the wrappers on top —
    // applying it from both sides produces a redundant `p AND p` (observed as
    // `c_state LIKE 'A%' AND c_state LIKE 'A%'` on chbench q3), which bloats the
    // plan and skews the cost model's selectivity estimate for that relation.
    let mut seen: HashSet<Expr> = wrappers
        .iter()
        .filter_map(|w| match w {
            LogicalPlan::Filter(f) => Some(&f.predicate),
            _ => None,
        })
        .flat_map(split_conjunction)
        .cloned()
        .collect();
    let deduped: Vec<Expr> = query_graph
        .filters()
        .iter()
        .filter(|f| seen.insert((*f).clone()))
        .cloned()
        .collect();
    if let Some(combined) = deduped.into_iter().reduce(Expr::and) {
        optimized_joins =
            LogicalPlan::Filter(Filter::try_new(combined, Arc::new(optimized_joins))?);
    }

    // Reconstruct the full plan with wrappers
    super::join_graph::reconstruct_plan(optimized_joins, wrappers)
}

/// Generates an optimized linear join plan from a query graph using the Ibaraki-Kameda algorithm.
///
/// This function finds the optimal join ordering for a query by:
/// 1. Trying each node in the query graph as a potential root
/// 2. For each root, building a precedence tree and optimizing it through normalization/denormalization
/// 3. Selecting the plan with the lowest estimated cost
///
/// The optimization process uses the Ibaraki-Kameda algorithm, which arranges joins to minimize
/// intermediate result sizes by considering both cardinality and cost estimates.
///
/// # Algorithm Steps
///
/// For each candidate root node:
/// 1. **Construction**: Build a precedence tree from the query graph starting at that node
/// 2. **Normalization**: Transform the tree into a chain structure ordered by rank
/// 3. **Denormalization**: Split merged operations back into individual nodes while maintaining chain structure
/// 4. **Cost Comparison**: Compare the resulting plan's cost against the current best
pub fn query_graph_to_optimal_left_deep_join_plan(
    query_graph: &JoinGraph,
    cost_estimator: &dyn JoinCostEstimator,
) -> Result<LogicalPlan> {
    let mut best_graph: Option<PrecedenceTreeNode> = None;

    for (node_id, _) in query_graph.nodes() {
        let mut precedence_graph =
            PrecedenceTreeNode::from_query_graph(query_graph, node_id, cost_estimator)?;
        precedence_graph.normalize();
        precedence_graph.denormalize()?;

        // DEBUG (REORDER_DBG): per-root total IK84 cost + chain head. Lets us
        // see which candidate root the enumerator picks and why. See
        // `reorder_join/PROGRESS.md`.
        if std::env::var("REORDER_DBG").is_ok() {
            let head = precedence_graph.query_nodes[0].node_id;
            let head_name = query_graph
                .get_node(head)
                .map(|n| format!("{}", n.plan))
                .unwrap_or_default();
            let head_name = head_name.lines().next().unwrap_or("").to_string();
            eprintln!(
                "[root] id={node_id} cost={:?} head={head} ({head_name})",
                precedence_graph.cost()
            );
        }

        best_graph = match best_graph.take() {
            Some(current) => {
                let new_cost = precedence_graph.cost()?;
                if new_cost < current.cost()? {
                    Some(precedence_graph)
                } else {
                    Some(current)
                }
            }
            None => Some(precedence_graph),
        };
    }

    best_graph
        .ok_or_else(|| plan_datafusion_err!("No valid precedence graph found"))?
        .into_logical_plan(query_graph)
}

#[derive(Debug)]
struct QueryNode {
    node_id: NodeId,
    // T in [IbarakiKameda84]
    selectivity: f64,
    // C in [IbarakiKameda84]
    cost: f64,
}

impl QueryNode {
    fn rank(&self) -> f64 {
        if self.cost == 0.0 {
            0.0
        } else {
            (self.selectivity - 1.0) / self.cost
        }
    }
}

/// A node in the precedence tree for query optimization.
///
/// The precedence tree is a data structure used by the Ibaraki-Kameda algorithm for
/// optimizing join ordering in database queries. It can represent both arbitrary tree
/// structures and linear chain structures (where each node has at most one child).
struct PrecedenceTreeNode<'graph> {
    query_nodes: Vec<QueryNode>,
    children: Vec<PrecedenceTreeNode<'graph>>,
    query_graph: &'graph JoinGraph,
}

impl Debug for PrecedenceTreeNode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrecedenceTreeNode")
            .field("query_nodes", &self.query_nodes)
            .field("children", &self.children)
            .finish()
    }
}

impl<'graph> PrecedenceTreeNode<'graph> {
    /// Creates a precedence tree from a query graph.
    pub(crate) fn from_query_graph(
        graph: &'graph JoinGraph,
        root_id: NodeId,
        cost_estimator: &dyn JoinCostEstimator,
    ) -> Result<Self> {
        let mut remaining: HashSet<NodeId> = graph.nodes().map(|(x, _)| x).collect();
        remaining.remove(&root_id);
        PrecedenceTreeNode::from_query_node(
            root_id,
            None,
            graph,
            &mut remaining,
            cost_estimator,
        )
    }

    /// Recursively constructs a precedence tree node from a query graph node.
    ///
    /// `incoming` carries the IK84 T- and C-values contributed by the edge
    /// that brought us to this node; `None` marks the root (`T = |root|`,
    /// `C = 0`). For non-root nodes the parent's `filter_map` computes
    /// these before recursing, since only the parent has the `Edge` in
    /// scope.
    fn from_query_node(
        node_id: NodeId,
        incoming: Option<(f64, f64)>,
        query_graph: &'graph JoinGraph,
        remaining: &mut HashSet<NodeId>,
        cost_estimator: &dyn JoinCostEstimator,
    ) -> Result<Self> {
        let node = query_graph
            .get_node(node_id)
            .ok_or_else(|| plan_datafusion_err!("Root node not found"))?;

        let children = node
            .connections()
            .iter()
            .filter_map(|edge_id| {
                let edge = query_graph.get_edge(*edge_id)?;
                let other = edge
                    .nodes
                    .into_iter()
                    .find(|x| *x != node_id && remaining.contains(x))?;

                remaining.remove(&other);
                let other_plan = &query_graph.get_node(other)?.plan;
                let sel = cost_estimator.selectivity(edge, &node.plan, other_plan);
                // IK84 per-step T (cardinality multiplier) and C (cost).
                //   inner:    T = sel × |other|,  C = cost(sel, |other|)
                //   semi/anti: T = sel,           C = |blob|  (hash-table
                //     build cost — independent of traversal direction; the
                //     blob is `edge.nodes[1]` by convention, see the `Edge`
                //     doc in `join_graph.rs`)
                let (child_t, child_c) = match edge.join_type {
                    JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
                    | JoinType::RightAnti => {
                        let blob_plan = &query_graph.get_node(edge.nodes[1])?.plan;
                        let blob_card =
                            cost_estimator.cardinality(blob_plan, None).unwrap_or(1.0);
                        (sel, blob_card)
                    }
                    _ => {
                        let other_card =
                            cost_estimator.cardinality(other_plan, None).unwrap_or(1.0);
                        (sel * other_card, cost_estimator.cost(sel, other_card))
                    }
                };
                Some(PrecedenceTreeNode::from_query_node(
                    other,
                    Some((child_t, child_c)),
                    query_graph,
                    remaining,
                    cost_estimator,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let (t_value, c_value) = match incoming {
            Some(tc) => tc,
            None => {
                let root_card =
                    cost_estimator.cardinality(&node.plan, None).unwrap_or(1.0);
                (root_card, 0.0)
            }
        };

        Ok(PrecedenceTreeNode {
            query_nodes: vec![QueryNode {
                node_id,
                selectivity: t_value,
                cost: c_value,
            }],
            children,
            query_graph,
        })
    }

    /// Rank function according to IbarakiKameda84
    fn rank(&self) -> f64 {
        let (cardinality, cost) =
            self.query_nodes
                .iter()
                .fold((1.0, 0.0), |(cardinality, cost), node| {
                    let cost = cost + cardinality * node.cost;
                    let cardinality = cardinality * node.selectivity;
                    (cardinality, cost)
                });
        if cost == 0.0 {
            0.0
        } else {
            (cardinality - 1.0) / cost
        }
    }

    /// Normalizes the precedence tree into a linear chain structure.
    fn normalize(&mut self) {
        match self.children.len() {
            0 => (),
            1 => {
                if self.children[0].rank() < self.rank() {
                    let mut child = self.children.pop().unwrap();
                    self.query_nodes.append(&mut child.query_nodes);
                    self.children = child.children;
                    self.normalize();
                } else {
                    self.children[0].normalize();
                }
            }
            _ => {
                for child in &mut self.children {
                    child.normalize();
                }
                let child = std::mem::take(&mut self.children)
                    .into_iter()
                    .reduce(Self::merge)
                    .unwrap();
                self.children = vec![child];
            }
        }
    }

    /// Merges two precedence tree chains into a single chain.
    fn merge(self, other: PrecedenceTreeNode<'graph>) -> Self {
        let (mut first, second) = if self.rank() < other.rank() {
            (self, other)
        } else {
            (other, self)
        };
        if first.children.is_empty() {
            first.children = vec![second];
        } else {
            first.children = vec![first.children.pop().unwrap().merge(second)];
        }
        first
    }

    /// Denormalizes a normalized precedence tree by splitting merged query nodes.
    fn denormalize(&mut self) -> Result<()> {
        match self.children.len() {
            0 => (),
            1 => self.children[0].denormalize()?,
            _ => return plan_err!("Tree is not normalized"),
        }

        while self.query_nodes.len() > 1 {
            if self.children.is_empty() {
                let highest_rank_idx = self
                    .query_nodes
                    .iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| a.rank().partial_cmp(&b.rank()).unwrap())
                    .map(|(idx, _)| idx)
                    .unwrap();

                let node = self.query_nodes.remove(highest_rank_idx);

                self.children.push(PrecedenceTreeNode {
                    query_nodes: vec![node],
                    children: Vec::new(),
                    query_graph: self.query_graph,
                });
            } else {
                let child_id = self.children[0].query_nodes[0].node_id;
                let child_node = self.query_graph.get_node(child_id).unwrap();
                let neighbours = child_node.neighbours(child_id, self.query_graph);

                // Prefer a remaining merged node that is a graph-neighbour of
                // the current tail head (keeps adjacent chain pairs
                // edge-connected). If none qualifies — which happens on real
                // multi-node graphs once cycle-breaking has demoted edges to
                // the side-channel, or when rank-merging interleaved branches
                // non-contiguously — fall back to the highest-rank remaining
                // node so reconstruction can proceed instead of panicking.
                // `into_logical_plan` reapplies the missing predicate (cross
                // join + side-channel filter) for any non-adjacent step.
                let highest_rank_idx = self
                    .query_nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, node)| neighbours.contains(&node.node_id))
                    .max_by(|(_, a), (_, b)| a.rank().partial_cmp(&b.rank()).unwrap())
                    .map(|(idx, _)| idx)
                    .or_else(|| {
                        self.query_nodes
                            .iter()
                            .enumerate()
                            .max_by(|(_, a), (_, b)| {
                                a.rank().partial_cmp(&b.rank()).unwrap()
                            })
                            .map(|(idx, _)| idx)
                    })
                    .unwrap();

                let node = self.query_nodes.remove(highest_rank_idx);

                let child = std::mem::replace(
                    &mut self.children[0],
                    PrecedenceTreeNode {
                        query_nodes: vec![node],
                        children: Vec::new(),
                        query_graph: self.query_graph,
                    },
                );
                self.children[0].children = vec![child];
            };
        }
        Ok(())
    }

    /// Converts the precedence tree chain into a DataFusion `LogicalPlan`.
    pub(crate) fn into_logical_plan(
        self,
        query_graph: &JoinGraph,
    ) -> Result<LogicalPlan> {
        let current_node_id = self.query_nodes[0].node_id;
        let mut current_plan = query_graph
            .get_node(current_node_id)
            .ok_or_else(|| plan_datafusion_err!("Node {:?} not found", current_node_id))?
            .plan
            .as_ref()
            .clone();

        let mut processed_nodes = vec![current_node_id];

        let mut current_chain = &self;

        while !current_chain.children.is_empty() {
            let child = &current_chain.children[0];
            let next_node_id = child.query_nodes[0].node_id;

            let next_plan = query_graph
                .get_node(next_node_id)
                .ok_or_else(|| plan_datafusion_err!("Node {:?} not found", next_node_id))?
                .plan
                .as_ref()
                .clone();

            let next_node = query_graph.get_node(next_node_id).ok_or_else(|| {
                plan_datafusion_err!("Node {:?} not found", next_node_id)
            })?;

            // Collect ALL edges connecting `next_node` to the already-processed
            // set. A relation can equi-join several processed relations at once
            // — e.g. q7's `order_line` joins both `stock`
            // (ol_i_id=s_i_id, ol_supply_w_id=s_w_id) and `oorder`
            // (ol_w_id=o_w_id, …). Keeping only one edge silently drops the
            // others' keys, yielding an under-selective join that explodes at
            // runtime (q7 OOM) or leaves a dangling column reference for a later
            // rule (q21 "No field named stock.s_w_id"). We merge every connecting
            // edge's keys into this single join.
            let connecting: Vec<_> = processed_nodes
                .iter()
                .rev()
                .filter_map(|&processed_id| next_node.connection_with(processed_id, query_graph))
                .collect();

            // No connecting edge: the denormalize fallback pulled a node that
            // is not a graph-neighbour of the processed set (cycle-broken /
            // non-contiguous chain). Build an Inner cross join so the left-deep
            // reconstruction can finish; the missing equi-predicate is reapplied
            // at the top level via the side-channel `filters`.
            let Some(&primary) = connecting.first() else {
                let join = datafusion_expr::Join::try_new(
                    Arc::new(current_plan),
                    Arc::new(next_plan),
                    vec![],
                    None,
                    JoinType::Inner,
                    JoinConstraint::On,
                    datafusion_common::NullEquality::NullEqualsNothing,
                    false,
                )?;
                current_plan = LogicalPlan::Join(join);
                processed_nodes.push(next_node_id);
                current_chain = child;
                continue;
            };

            let next_schema = next_plan.schema();
            let column_in_schema = |col: &datafusion_common::Column,
                                    schema: &datafusion_common::DFSchema|
             -> bool {
                if let Some(relation) = &col.relation {
                    schema.iter().any(|(qualifier, field)| {
                        qualifier == Some(relation) && field.name() == col.name()
                    })
                } else {
                    schema.field_with_unqualified_name(&col.name).is_ok()
                }
            };
            let is_semi_anti = |jt: JoinType| {
                matches!(
                    jt,
                    JoinType::LeftSemi
                        | JoinType::LeftAnti
                        | JoinType::RightSemi
                        | JoinType::RightAnti
                )
            };

            let (on, join_type, null_equality) =
                if connecting.iter().all(|e| !is_semi_anti(e.join_type)) {
                    // All-inner: merge every connecting edge's equi-keys,
                    // orienting each pair so the current-plan column is the LEFT
                    // input and the next-node column is the RIGHT input
                    // (independent of which edge it came from).
                    let mut on: Vec<(Expr, Expr)> = Vec::new();
                    for e in &connecting {
                        for (a, b) in &e.on {
                            let a_refs = a.column_refs();
                            let a_is_next = !a_refs.is_empty()
                                && a_refs
                                    .iter()
                                    .all(|c| column_in_schema(c, next_schema.as_ref()));
                            if a_is_next {
                                on.push((b.clone(), a.clone()));
                            } else {
                                on.push((a.clone(), b.clone()));
                            }
                        }
                    }
                    (on, JoinType::Inner, primary.null_equality)
                } else {
                    // Semi/anti (or a mix): keep the single-primary-edge
                    // behavior, oriented by the `nodes[0] = preserved-LHS`
                    // invariant set up by `flatten_joins_recursive` — the
                    // preserved side must be the physical LEFT of the join.
                    let join_order_swapped = next_node_id == primary.nodes[0];
                    if join_order_swapped {
                        let swapped_on = primary
                            .on
                            .iter()
                            .map(|(left, right)| (right.clone(), left.clone()))
                            .collect();
                        (swapped_on, primary.join_type.swap(), primary.null_equality)
                    } else {
                        (primary.on.clone(), primary.join_type, primary.null_equality)
                    }
                };

            // Build the join. Schema is auto-derived; non-equi predicates were
            // already hoisted into the side-channel and are reapplied at the
            // top level by `optimal_left_deep_join_plan`.
            let join = datafusion_expr::Join::try_new(
                Arc::new(current_plan),
                Arc::new(next_plan),
                on,
                None,
                join_type,
                JoinConstraint::On,
                null_equality,
                false,
            )?;
            current_plan = LogicalPlan::Join(join);

            processed_nodes.push(next_node_id);
            current_chain = child;
        }

        Ok(current_plan)
    }

    fn cost(&self) -> Result<f64> {
        self.cost_recursive(self.query_nodes[0].selectivity, 0.0)
    }

    fn cost_recursive(&self, cardinality: f64, cost: f64) -> Result<f64> {
        let cost = match self.children.len() {
            0 => cost + cardinality * self.query_nodes[0].cost,
            1 => self.children[0].cost_recursive(
                cardinality * self.query_nodes[0].selectivity,
                cost + cardinality * self.query_nodes[0].cost,
            )?,
            _ => {
                return plan_err!(
                    "Cost calculation requires normalized tree with 0 or 1 children"
                );
            }
        };
        Ok(cost)
    }
}

