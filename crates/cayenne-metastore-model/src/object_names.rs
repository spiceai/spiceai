// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Names of immutable objects created by several leased units. Each unit (the
//! writer `w0`, maintenance units `m<id>`) has its own lease generation in the
//! dataset state, raised by a head commit when a node takes the unit. A holder
//! creates objects by create-if-absent under a name built from the naming rule,
//! then commits a reference to them, guarded by its unit's generation. Nodes
//! have no identity of their own in names. Maintenance units can be retired and
//! new ones created; a new unit takes the next id from a counter in the state.
use stateright::{Model, Property};

/// Nodes that take unit leases.
const NODES: usize = 2;
/// Lease takeovers across all units, bounding generations.
const TAKES: usize = 3;
/// Maintenance units created after the initial one.
const CREATED: usize = 1;
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    /// Objects one holder creates under one generation.
    per_hold: u8,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Kind {
    Writer,
    Maintenance,
}
/// A unit's entry in the dataset state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Unit {
    kind: Kind,
    id: u8,
    generation: u8,
    active: bool,
}
/// The leased unit and generation that created an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Owner {
    kind: Kind,
    id: u8,
    generation: u8,
}
/// An object name: the fields the naming rule keeps, the rest zeroed.
type Name = (Option<Kind>, u8, u8, u8);
/// An object's bytes, unique per holding and counter; equal bytes are one write.
type Bytes = (u8, u8);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Hold {
    /// Index of the unit in the dataset state.
    slot: usize,
    generation: u8,
    /// The takeover that started this holding, which makes its bytes unique.
    token: u8,
    counter: u8,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Node {
    hold: Option<Hold>,
    /// Uploads not yet committed: name and the bytes this node sent.
    uploads: Vec<(Name, Bytes)>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    units: Vec<Unit>,
    /// Next maintenance unit id; ids are never reused.
    next_id: u8,
    takes: u8,
    created: u8,
    /// Immutable objects: name, the first bytes written, and who wrote them.
    store: Vec<(Name, Bytes, Owner)>,
    /// References the head holds: name and the digest (bytes) referenced.
    refs: Vec<(Name, Bytes)>,
    nodes: Vec<Node>,
    /// A create found its name holding different bytes.
    collision: bool,
    /// A holder's commit was refused by its unit's newer generation.
    fenced: bool,
    /// The slot of the unit created after another retired.
    replacement: Option<usize>,
    /// That unit created an object.
    replacement_created: bool,
    /// An upload whose response was lost was sent again.
    retried: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Take(usize, usize),
    Create(usize),
    Retry(usize),
    Commit(usize),
    Retire(usize),
    Allocate,
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            per_hold: u8::try_from(bound.max(1)).unwrap_or(1),
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    /// The naming rule: `<kind><unit>.<generation>-<counter>`. The controls drop
    /// the unit (generation-only names) or the generation (unit-only names).
    fn name(&self, owner: Owner, counter: u8) -> Name {
        if self.has("generation-only") {
            (None, 0, owner.generation, counter)
        } else if self.has("no-generation") {
            (Some(owner.kind), owner.id, 0, counter)
        } else {
            (Some(owner.kind), owner.id, owner.generation, counter)
        }
    }
    fn owner(s: &State, h: Hold) -> Owner {
        let u = s.units[h.slot];
        Owner {
            kind: u.kind,
            id: u.id,
            generation: h.generation,
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            units: vec![
                Unit {
                    kind: Kind::Writer,
                    id: 0,
                    generation: 0,
                    active: true,
                },
                Unit {
                    kind: Kind::Maintenance,
                    id: 0,
                    generation: 0,
                    active: true,
                },
            ],
            next_id: 1,
            takes: 0,
            created: 0,
            store: vec![],
            refs: vec![],
            nodes: vec![Node::default(); NODES],
            collision: false,
            fenced: false,
            replacement: None,
            replacement_created: false,
            retried: false,
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for i in 0..NODES {
            for u in 0..s.units.len() {
                a.push(Action::Take(i, u));
            }
            a.extend([Action::Create(i), Action::Retry(i), Action::Commit(i)]);
        }
        for u in 0..s.units.len() {
            a.push(Action::Retire(u));
        }
        a.push(Action::Allocate);
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut next = s.clone();
        match action {
            Action::Take(i, u) => {
                // A turnover is a head commit that raises the unit's generation.
                if !s.units[u].active || usize::from(s.takes) >= TAKES {
                    return None;
                }
                next.takes += 1;
                next.units[u].generation += 1;
                next.nodes[i].hold = Some(Hold {
                    slot: u,
                    generation: next.units[u].generation,
                    token: next.takes,
                    counter: 0,
                });
            }
            Action::Create(i) => {
                let h = s.nodes[i].hold?;
                if h.counter >= self.per_hold {
                    return None;
                }
                let owner = Self::owner(s, h);
                let name = self.name(owner, h.counter);
                let bytes = (h.token, h.counter);
                // Create-if-absent keeps the first bytes under a name.
                if let Some((_, b, _)) = s.store.iter().find(|(n, _, _)| *n == name) {
                    next.collision |= *b != bytes;
                } else {
                    next.store.push((name, bytes, owner));
                    next.store.sort_unstable();
                }
                next.replacement_created |= s.replacement == Some(h.slot);
                next.nodes[i].uploads.push((name, bytes));
                next.nodes[i].hold = Some(Hold {
                    counter: h.counter + 1,
                    ..h
                });
            }
            Action::Retry(i) => {
                // A lost response: the same name and bytes are sent again.
                let &(name, bytes) = s.nodes[i].uploads.first()?;
                if s.retried {
                    return None;
                }
                next.retried = true;
                if let Some((_, b, _)) = s.store.iter().find(|(n, _, _)| *n == name) {
                    next.collision |= *b != bytes;
                }
            }
            Action::Commit(i) => {
                let h = s.nodes[i].hold?;
                let &(name, bytes) = s.nodes[i].uploads.first()?;
                next.nodes[i].uploads.remove(0);
                let unit = s.units[h.slot];
                if !unit.active || unit.generation != h.generation {
                    // The guard on the unit's generation refuses a fenced holder.
                    next.fenced = true;
                    next.nodes[i].hold = None;
                    next.nodes[i].uploads.clear();
                } else if s.store.iter().any(|(n, b, _)| *n == name && *b == bytes) {
                    // The holder reads its upload back by digest before referencing it.
                    next.refs.push((name, bytes));
                    next.refs.sort_unstable();
                }
            }
            Action::Retire(u) => {
                let unit = s.units[u];
                if unit.kind != Kind::Maintenance || !unit.active {
                    return None;
                }
                next.units[u].active = false;
            }
            Action::Allocate => {
                if usize::from(s.created) >= CREATED {
                    return None;
                }
                next.created += 1;
                let reuse = self.has("reuse-unit-id").then(|| {
                    s.units
                        .iter()
                        .position(|u| u.kind == Kind::Maintenance && !u.active)
                });
                if let Some(Some(slot)) = reuse {
                    // The retired unit's state is cleared and its id handed out again.
                    next.units[slot].generation = 0;
                    next.units[slot].active = true;
                    next.replacement = Some(slot);
                    for n in &mut next.nodes {
                        if n.hold.is_some_and(|h| h.slot == slot) {
                            n.hold = None;
                            n.uploads.clear();
                        }
                    }
                } else {
                    next.units.push(Unit {
                        kind: Kind::Maintenance,
                        id: s.next_id,
                        generation: 0,
                        active: true,
                    });
                    next.next_id += 1;
                    if s.units.iter().any(|u| !u.active) {
                        next.replacement = Some(s.units.len());
                    }
                }
            }
        }
        (next != *s).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no name created with different bytes", |_, s: &State| {
                !s.collision
            }),
            Property::always(
                "referenced objects read back with their digest",
                |_, s: &State| {
                    s.refs
                        .iter()
                        .all(|r| s.store.iter().any(|(n, b, _)| (*n, *b) == *r))
                },
            ),
            Property::sometimes(
                "two units created objects at one generation",
                |_, s: &State| {
                    s.store.iter().any(|(_, _, a)| {
                        s.store.iter().any(|(_, _, b)| {
                            a.generation == b.generation && (a.kind, a.id) != (b.kind, b.id)
                        })
                    })
                },
            ),
            Property::sometimes(
                "successive holders of one unit created objects",
                |_, s: &State| {
                    s.store.iter().any(|(_, _, a)| {
                        s.store.iter().any(|(_, _, b)| {
                            (a.kind, a.id) == (b.kind, b.id) && a.generation != b.generation
                        })
                    })
                },
            ),
            Property::sometimes(
                "new unit created objects after a unit retired",
                |_, s: &State| s.replacement_created,
            ),
            Property::sometimes("fenced holder's commit refused", |_, s: &State| s.fenced),
            Property::sometimes("identical retry of an upload", |_, s: &State| s.retried),
            Property::sometimes("objects of two units referenced", |_, s: &State| {
                let owners: Vec<_> = s
                    .store
                    .iter()
                    .filter(|(n, b, _)| s.refs.contains(&(*n, *b)))
                    .map(|(_, _, o)| (o.kind, o.id))
                    .collect();
                owners.iter().any(|a| owners.iter().any(|b| a != b))
            }),
        ]
    }
}
