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
fn kind_letter(kind: Option<Kind>) -> &'static str {
    match kind {
        Some(Kind::Writer) => "w",
        Some(Kind::Maintenance) => "m",
        None => "",
    }
}
fn name_label((kind, id, generation, counter): Name) -> String {
    match kind {
        Some(_) => format!("{}{id}.{generation}-{counter}", kind_letter(kind)),
        None => format!("{generation}-{counter}"),
    }
}
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
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Take(i, u) => {
                format!("node {i} takes unit {u}'s lease (head commit raises its generation)")
            }
            Action::Create(i) => format!("node {i} creates its next object (create if absent)"),
            Action::Retry(i) => format!("node {i} resends an upload whose response was lost"),
            Action::Commit(i) => format!("node {i} commits a reference to its upload"),
            Action::Retire(u) => format!("maintenance unit {u} is retired"),
            Action::Allocate => "a new maintenance unit is created".into(),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        Some(crate::explorer::summary(&[
            "names: <kind><unit>.<generation>-<counter>; bytes: (holding, counter)".into(),
            format!(
                "takeovers {} of {TAKES}; units created {} of {CREATED}",
                s.takes, s.created
            ),
            crate::explorer::flags(&[
                ("BAD name collision (different bytes)", s.collision),
                ("holder fenced", s.fenced),
                ("replacement unit created an object", s.replacement_created),
                ("upload retried", s.retried),
            ]),
        ]))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Svg, Tone, row};
        let s = path.last_state();
        let mut svg = Svg::new();
        let mut lines: Vec<String> = s
            .units
            .iter()
            .enumerate()
            .map(|(slot, u)| {
                format!(
                    "unit {slot} = {}{} gen {}{}",
                    kind_letter(Some(u.kind)),
                    u.id,
                    u.generation,
                    if u.active { "" } else { " (retired)" }
                )
            })
            .collect();
        let refs_at = lines.len();
        lines.extend(
            s.refs
                .iter()
                .map(|(n, b)| format!("ref {} digest {b:?}", name_label(*n))),
        );
        svg.heading(0, 0, "HEAD REFERENCES -> IMMUTABLE OBJECTS");
        let head = svg.node(0, 22, 0, "HEAD", &lines, Tone::Live);
        let col = head.right() + 80;
        let mut y = 22;
        let mut placed = vec![];
        for (name, bytes, owner) in &s.store {
            let referenced = s.refs.iter().any(|(n, _)| n == name);
            let node = svg.node(
                col,
                y,
                0,
                &name_label(*name),
                &[format!(
                    "bytes {bytes:?} by {}{} gen {}",
                    kind_letter(Some(owner.kind)),
                    owner.id,
                    owner.generation
                )],
                if referenced { Tone::Live } else { Tone::Staged },
            );
            y = node.bottom() + 8;
            placed.push((*name, *bytes, node));
        }
        for (i, (name, digest)) in s.refs.iter().enumerate() {
            if let Some((_, bytes, node)) = placed.iter().find(|(n, _, _)| n == name) {
                svg.arrow(
                    (head.right(), head.line_y(refs_at + i)),
                    (node.x, node.mid_y()),
                    if bytes == digest {
                        Tone::Live
                    } else {
                        Tone::Bad
                    },
                    None,
                );
            } else {
                let node = svg.node(
                    col,
                    y,
                    0,
                    &format!("{} MISSING", name_label(*name)),
                    &[String::new()],
                    Tone::Bad,
                );
                y = node.bottom() + 8;
                svg.arrow(
                    (head.right(), head.line_y(refs_at + i)),
                    (node.x, node.mid_y()),
                    Tone::Bad,
                    None,
                );
            }
        }
        let boxes: Vec<(String, Vec<String>, Tone)> =
            s.nodes
                .iter()
                .enumerate()
                .map(|(i, node)| {
                    let mut lines = vec![match node.hold {
                        Some(h) => format!(
                            "holds unit {} at gen {} (next counter {})",
                            h.slot, h.generation, h.counter
                        ),
                        None => "holds nothing".into(),
                    }];
                    lines.extend(node.uploads.iter().map(|(n, b)| {
                        format!("uploaded, not referenced: {} {b:?}", name_label(*n))
                    }));
                    (format!("NODE {i}"), lines, Tone::Actor)
                })
                .collect();
        row(&mut svg, head.bottom().max(y) + 24, &boxes);
        Some(svg.finish())
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
