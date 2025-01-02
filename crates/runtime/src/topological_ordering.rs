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
#![allow(clippy::implicit_hasher)]
use std::collections::{HashMap, HashSet, VecDeque};

/// Construct a topological ordering of a directed acyclic graph (DAG) using
/// Kahn's algorithm.
///
/// `nodes` are represented as a [`HashMap`] where the key is the node and the value is a list of
/// nodes that have an edge to the key node.
///
///
/// ```
///     A -> B --> D
///          |     ^
///          |     |
///          +->C--+
/// ```
///   Order: D, C, B, A
///
/// ```rust
///     let mut nodes = HashMap::new();
///     nodes.insert("A", vec!["B".to_string()]);
///     nodes.insert("B", vec!["D".to_string(), "C".to_string()]);
///     nodes.insert("C", vec!["D".to_string()]);
///     nodes.insert("D", vec![]);
/// ```
///
/// Returns None if the graph would have a cycle.
///
/// See: `<https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm>`
#[must_use]
pub fn construct_topological_ordering<T: Eq + std::hash::Hash + Clone + std::fmt::Debug>(
    mut nodes: HashMap<T, Vec<T>>,
) -> Option<Vec<T>> {
    // Get nodes that have no incoming edges, they do not depend on any other node.
    let mut independent_nodes = nodes
        .iter()
        .filter_map(|(node, dependences)| {
            if dependences.is_empty() {
                Some(node.clone())
            } else {
                None
            }
        })
        .collect::<VecDeque<_>>();

    for n in &independent_nodes {
        nodes.remove(n);
    }

    let mut result = Vec::<T>::with_capacity(nodes.len());

    while let Some(dep) = independent_nodes.pop_front() {
        result.push(dep.clone());

        let mut to_remove = Vec::new();
        for (node, dependences) in &mut nodes {
            // Remove `dep` from dependencies for each node
            dependences.retain(|n| *n != dep);

            // If node is now empty, add it to independent nodes, and remove from `nodes`.
            if dependences.is_empty() {
                independent_nodes.push_back(node.clone());
                to_remove.push(node.clone());
            }
        }
        for n in &to_remove {
            nodes.remove(n);
        }
    }

    if nodes.is_empty() {
        Some(result)
    } else {
        // Cycle detected
        None
    }
}

/// Construct a topological ordering of `nodes` and any additional nodes that depend on `nodes` in a topological order.
///
/// `nodes` are represented as a [`HashMap`] where the key is the node and the value is a list of
/// nodes that have an edge to the key node.
///
/// `nodes` is a subset of the graph.
///
/// Returns None if the graph would have a cycle.
#[must_use]
pub fn construct_effected_in_topological_order<
    T: Eq + std::hash::Hash + Clone + std::fmt::Debug,
>(
    graph: HashMap<T, Vec<T>>,
    nodes: &[T],
) -> Option<Vec<T>> {
    // Construct map of node -> all nodes that depend on it
    let mut reverse_graph: HashMap<T, Vec<T>> = HashMap::new();
    for (node, dependences) in &graph {
        for dep in dependences {
            reverse_graph
                .entry(dep.clone())
                .or_default()
                .push(node.clone());
        }
    }

    // Find all effected nodes.
    let mut effected_nodes = nodes.iter().cloned().collect::<HashSet<T>>();
    let mut q = nodes.iter().cloned().collect::<VecDeque<T>>();

    while let Some(node) = q.pop_front() {
        if let Some(dependents) = reverse_graph.get(&node) {
            for dep in dependents {
                if effected_nodes.insert(dep.clone()) {
                    // Add to queue if insert was successful (not already in set).
                    q.push_back(dep.clone());
                }
            }
        }
    }

    // Construct topological ordering over effected nodes.
    let nodes = graph
        .into_iter()
        .filter_map(|(node, deps)| {
            if effected_nodes.contains(&node) {
                Some((
                    node,
                    // Filter out dependencies that are not in effected nodes subtree.
                    // These are nodes that impact nodes in the subtree, but are not affected by the change.
                    deps.into_iter()
                        .filter(|dep| effected_nodes.contains(dep))
                        .collect(),
                ))
            } else {
                None
            }
        })
        .collect::<HashMap<T, Vec<T>>>();

    construct_topological_ordering(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_topological_ordering() {
        let mut nodes = HashMap::new();
        nodes.insert("A", vec!["B"]);
        nodes.insert("B", vec!["C"]);
        nodes.insert("C", vec!["D"]);
        nodes.insert("D", vec![]);

        assert_eq!(
            construct_topological_ordering(nodes.clone()),
            Some(vec!["D", "C", "B", "A"])
        );

        assert_eq!(
            construct_effected_in_topological_order(nodes.clone(), &["D"]),
            Some(vec!["D", "C", "B", "A"])
        );

        assert_eq!(
            construct_effected_in_topological_order(nodes.clone(), &["B"]),
            Some(vec!["B", "A"])
        );

        assert_eq!(
            construct_effected_in_topological_order(nodes.clone(), &["A"]),
            Some(vec!["A"])
        );
        assert_eq!(
            construct_effected_in_topological_order(nodes.clone(), &[]),
            Some(vec![])
        );
    }

    #[test]
    fn test_disconnected_graph() {
        let mut nodes = HashMap::new();
        nodes.insert("A", vec!["B"]);
        nodes.insert("B", vec!["C"]);
        nodes.insert("C", vec!["D"]);
        nodes.insert("D", vec![]);
        nodes.insert("E", vec!["F"]);
        nodes.insert("F", vec!["G"]);
        nodes.insert("G", vec!["H"]);
        nodes.insert("H", vec![]);

        let result = construct_topological_ordering(nodes.clone()).expect("Should not be None");
        assert!(
            result == vec!["H", "D", "G", "C", "F", "B", "E", "A"]
                || result == vec!["D", "H", "C", "G", "B", "F", "A", "E"]
        );

        assert_eq!(
            construct_effected_in_topological_order(nodes.clone(), &["H"]),
            Some(vec!["H", "G", "F", "E"])
        );
    }

    #[test]
    fn test_many_edges() {
        let mut nodes = HashMap::new();
        nodes.insert("A", vec!["B"]);
        nodes.insert("B", vec!["D", "C"]);
        nodes.insert("C", vec!["D"]);
        nodes.insert("D", vec![]);

        assert_eq!(
            construct_topological_ordering(nodes),
            Some(vec!["D", "C", "B", "A"])
        );
    }

    #[test]
    fn test_construct_topological_ordering_cycle() {
        let mut nodes = HashMap::new();
        nodes.insert("A", vec!["B"]);
        nodes.insert("B", vec!["C"]);
        nodes.insert("C", vec!["A"]);

        assert_eq!(construct_topological_ordering(nodes.clone()), None);
        assert_eq!(
            construct_effected_in_topological_order(nodes.clone(), &["A"]),
            None
        );
    }
}
