// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Drawings for the Stateright Explorer (`serve`). Each model's `as_svg`
//! draws from its own state fields only, never from a re-derivation of the
//! protocol, so a picture can show a state but cannot change what is checked.
//! The checker never calls this code.

use std::fmt::Write as _;

/// Approximate monospace advance at 12px, in tenths of a pixel.
const CHAR_W10: usize = 75;
const LINE_H: usize = 15;
const PAD: usize = 6;
const GAP: usize = 40;

/// How a box or arrow is drawn.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tone {
    /// Committed: referenced by the authoritative head.
    Live,
    /// Written but not referenced, or in flight.
    Staged,
    /// Collected or superseded.
    Gone,
    /// A reference to something missing, or a broken invariant.
    Bad,
    /// An actor, writer or reader.
    Actor,
}
impl Tone {
    fn class(self) -> &'static str {
        match self {
            Tone::Live => "cm-live",
            Tone::Staged => "cm-staged",
            Tone::Gone => "cm-gone",
            Tone::Bad => "cm-bad",
            Tone::Actor => "cm-actor",
        }
    }
}

/// A drawn box: arrows attach to its edges or to one of its text lines.
#[derive(Clone, Copy)]
pub(crate) struct Node {
    pub(crate) x: usize,
    pub(crate) y: usize,
    pub(crate) w: usize,
    pub(crate) h: usize,
}
impl Node {
    pub(crate) fn right(self) -> usize {
        self.x + self.w
    }
    pub(crate) fn bottom(self) -> usize {
        self.y + self.h
    }
    pub(crate) fn mid_y(self) -> usize {
        self.y + self.h / 2
    }
    /// The vertical middle of text line `i` (0 is the first line under the title).
    pub(crate) fn line_y(self, i: usize) -> usize {
        self.y + PAD + LINE_H + i * LINE_H + LINE_H / 2 + 2
    }
}

pub(crate) struct Svg {
    body: String,
    width: usize,
    height: usize,
}
impl Svg {
    pub(crate) fn new() -> Self {
        Self {
            body: String::new(),
            width: 0,
            height: 0,
        }
    }
    fn extend(&mut self, x: usize, y: usize) {
        self.width = self.width.max(x);
        self.height = self.height.max(y);
    }
    /// The width a box needs for `title` and `lines`.
    pub(crate) fn node_width(title: &str, lines: &[String]) -> usize {
        let chars = lines
            .iter()
            .map(|l| l.chars().count())
            .chain([title.chars().count()])
            .max()
            .unwrap_or(0);
        chars * CHAR_W10 / 10 + 2 * PAD
    }
    /// A titled box of text lines, at least `min_w` wide; staged boxes are dashed.
    pub(crate) fn node(
        &mut self,
        x: usize,
        y: usize,
        min_w: usize,
        title: &str,
        lines: &[String],
        tone: Tone,
    ) -> Node {
        let w = Self::node_width(title, lines).max(min_w);
        let h = 2 * PAD + LINE_H * (lines.len() + 1);
        let dash = if tone == Tone::Staged {
            " stroke-dasharray=\"5 3\""
        } else {
            ""
        };
        let _ = write!(
            self.body,
            "<rect x=\"{x}\" y=\"{y}\" width=\"{w}\" height=\"{h}\" rx=\"4\" class=\"cm-box {}\"{dash}/>",
            tone.class()
        );
        self.text(x + PAD, y + PAD + LINE_H - 3, title, "cm-title");
        for (i, line) in lines.iter().enumerate() {
            self.text(x + PAD, y + PAD + LINE_H * (i + 2) - 3, line, "cm-line");
        }
        self.extend(x + w, y + h);
        Node { x, y, w, h }
    }
    pub(crate) fn text(&mut self, x: usize, y: usize, text: &str, class: &str) {
        let _ = write!(
            self.body,
            "<text x=\"{x}\" y=\"{y}\" class=\"{class}\">{}</text>",
            escape(text)
        );
        self.extend(x + text.chars().count() * CHAR_W10 / 10, y + 4);
    }
    /// A section heading above `y`.
    pub(crate) fn heading(&mut self, x: usize, y: usize, text: &str) {
        self.text(x, y + LINE_H - 3, text, "cm-heading");
    }
    /// An arrow from `(x1, y1)` to `(x2, y2)`, bent once horizontally.
    pub(crate) fn arrow(
        &mut self,
        (x1, y1): (usize, usize),
        (x2, y2): (usize, usize),
        tone: Tone,
        label: Option<&str>,
    ) {
        let mid = if x2 > x1 { x1 + (x2 - x1) / 2 } else { x1 + 12 };
        let dash = if tone == Tone::Staged {
            " stroke-dasharray=\"5 3\""
        } else {
            ""
        };
        let _ = write!(
            self.body,
            "<path d=\"M{x1},{y1} H{mid} V{y2} H{x2}\" class=\"cm-arrow {}\" marker-end=\"url(#cm-head-{})\"{dash}/>",
            tone.class(),
            tone.class()
        );
        if let Some(label) = label {
            self.text(x1 + 4, y1.saturating_sub(3), label, "cm-small");
        }
        self.extend(x1.max(x2), y1.max(y2));
    }
    /// A horizontal arrow between two lanes of a timeline at height `y`.
    pub(crate) fn hline(&mut self, x1: usize, x2: usize, y: usize, tone: Tone) {
        let dash = if tone == Tone::Staged {
            " stroke-dasharray=\"5 3\""
        } else {
            ""
        };
        let _ = write!(
            self.body,
            "<path d=\"M{x1},{y} H{x2}\" class=\"cm-arrow {}\" marker-end=\"url(#cm-head-{})\"{dash}/>",
            tone.class(),
            tone.class()
        );
        self.extend(x1.max(x2), y);
    }
    pub(crate) fn vline(&mut self, x: usize, y1: usize, y2: usize) {
        let _ = write!(self.body, "<path d=\"M{x},{y1} V{y2}\" class=\"cm-lane\"/>");
        self.extend(x, y2);
    }
    pub(crate) fn dot(&mut self, x: usize, y: usize, tone: Tone) {
        let _ = write!(
            self.body,
            "<circle cx=\"{x}\" cy=\"{y}\" r=\"4\" class=\"cm-dot {}\"/>",
            tone.class()
        );
    }
    pub(crate) fn finish(mut self) -> String {
        let legend_y = self.height + 12;
        let mut x = 0;
        for (tone, label) in [
            (Tone::Live, "committed"),
            (Tone::Staged, "written, not referenced / in flight"),
            (Tone::Gone, "collected / superseded"),
            (Tone::Bad, "missing / violated"),
            (Tone::Actor, "actor"),
        ] {
            let _ = write!(
                self.body,
                "<rect x=\"{x}\" y=\"{legend_y}\" width=\"10\" height=\"10\" class=\"cm-box {}\"/>",
                tone.class()
            );
            self.text(x + 14, legend_y + 9, label, "cm-small");
            x += 14 + label.chars().count() * CHAR_W10 / 10 + 18;
        }
        let w = self.width.max(x) + 10;
        let h = legend_y + 16;
        let tones = [Tone::Live, Tone::Staged, Tone::Gone, Tone::Bad, Tone::Actor];
        let mut markers = String::new();
        for t in tones {
            let _ = write!(
                markers,
                "<marker id=\"cm-head-{c}\" markerWidth=\"8\" markerHeight=\"8\" refX=\"7\" refY=\"4\" orient=\"auto\"><path d=\"M0,0 L8,4 L0,8 z\" class=\"cm-arrowhead {c}\"/></marker>",
                c = t.class()
            );
        }
        format!(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w}\" height=\"{h}\" viewBox=\"-4 -4 {w} {h}\" font-family=\"ui-monospace, SFMono-Regular, Menlo, monospace\" font-size=\"12\"><style>{STYLE}</style><defs>{markers}</defs>{}</svg>",
            self.body
        )
    }
}

const STYLE: &str = "\
.cm-box{fill:#1d3a5c;stroke:#e7ecef;stroke-width:1.2}\
.cm-box.cm-staged{fill:#22405f;stroke:#f2e27a}\
.cm-box.cm-gone{fill:#1a2a3c;stroke:#6b7c8c}\
.cm-box.cm-bad{fill:#5a1f2a;stroke:#ff8a80;stroke-width:2}\
.cm-box.cm-actor{fill:#24524a;stroke:#9be7c4}\
.cm-title{fill:#f2e27a;font-weight:bold}\
.cm-line{fill:#e7ecef}\
.cm-small{fill:#bbbbbb;font-size:11px}\
.cm-heading{fill:#fff9d1;font-weight:bold;font-size:13px}\
.cm-arrow{fill:none;stroke-width:1.5}\
.cm-arrow.cm-live{stroke:#e7ecef}.cm-arrowhead.cm-live{fill:#e7ecef}\
.cm-arrow.cm-staged{stroke:#f2e27a}.cm-arrowhead.cm-staged{fill:#f2e27a}\
.cm-arrow.cm-gone{stroke:#6b7c8c}.cm-arrowhead.cm-gone{fill:#6b7c8c}\
.cm-arrow.cm-bad{stroke:#ff8a80}.cm-arrowhead.cm-bad{fill:#ff8a80}\
.cm-arrow.cm-actor{stroke:#9be7c4}.cm-arrowhead.cm-actor{fill:#9be7c4}\
.cm-lane{stroke:#2b5672;stroke-width:2}\
.cm-dot.cm-live{fill:#e7ecef}.cm-dot.cm-staged{fill:#f2e27a}.cm-dot.cm-gone{fill:#6b7c8c}\
.cm-dot.cm-bad{fill:#ff8a80}.cm-dot.cm-actor{fill:#9be7c4}\
";

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// A storage layout in the shape every head-log model shares: an authoritative
/// head holding an inline window and a batch index, immutable batch objects
/// the index points to, a checkpoint root below the floor, and objects that are
/// written but not (or no longer) referenced. Positions are zero-based record
/// numbers and ranges are half-open.
pub(crate) struct StoreView {
    pub(crate) title: String,
    /// Extra head fields (lease, generation, clock), shown under the title.
    pub(crate) head: Vec<String>,
    pub(crate) floor: usize,
    /// The first record number held inline.
    pub(crate) first: usize,
    pub(crate) window: Vec<String>,
    /// Batch ranges `[start, end)` the head references.
    pub(crate) index: Vec<(usize, usize)>,
    /// Created batch objects by start position.
    pub(crate) batches: Vec<(usize, Vec<String>)>,
    /// Cuts observed but not yet created as objects.
    pub(crate) pending: Vec<(usize, Vec<String>)>,
    /// The published checkpoint root: records `[0, floor)`.
    pub(crate) root: Vec<String>,
    /// A root written at a batch end that the next commit publishes.
    pub(crate) staged: Option<(usize, Vec<String>)>,
    /// Batch starts collected after the floor passed them.
    pub(crate) deleted: Vec<usize>,
    /// Captured heads (a running query, a reader's poll): name and the batch
    /// ranges it still needs.
    pub(crate) snapshots: Vec<(String, Vec<(usize, usize)>)>,
}

/// Records as short labels: `r<id>`, or the record kind where a model gives one.
pub(crate) fn records(
    rs: &[crate::core::Record],
    label: fn(crate::core::Record) -> String,
) -> Vec<String> {
    rs.iter().map(|r| label(*r)).collect()
}

pub(crate) fn record_id(r: crate::core::Record) -> String {
    format!("r{}", r.2)
}

impl StoreView {
    pub(crate) fn of(
        title: &str,
        store: &crate::core::Store,
        label: fn(crate::core::Record) -> String,
    ) -> Self {
        Self {
            title: title.into(),
            head: vec![],
            floor: store.floor,
            first: store.first,
            window: records(&store.window, label),
            index: store.index.clone(),
            batches: store
                .batches
                .iter()
                .map(|(start, rs)| (*start, records(rs, label)))
                .collect(),
            pending: store
                .pending
                .iter()
                .map(|(start, rs)| (*start, records(rs, label)))
                .collect(),
            root: records(&store.root[..store.floor.min(store.root.len())], label),
            staged: store
                .staged
                .as_ref()
                .map(|(c, rs)| (*c, records(rs, label))),
            deleted: store.deleted.clone(),
            snapshots: vec![],
        }
    }
    /// Adds a captured head (a query or a reader's poll) that pins its batches.
    pub(crate) fn pin(&mut self, name: &str, snapshot: Option<&crate::core::Store>) {
        if let Some(s) = snapshot {
            self.snapshots.push((
                format!("{name} (floor {}, inline from {})", s.floor, s.first),
                s.index.clone(),
            ));
        }
    }

    /// Draws the layout with its top-left corner at `(x, y)`; returns its bottom.
    pub(crate) fn draw(&self, svg: &mut Svg, x: usize, y: usize) -> usize {
        svg.heading(x, y, &self.title);
        let y = y + LINE_H + 6;
        let mut lines = self.head.clone();
        lines.push(format!(
            "floor {} (root holds [0, {}))",
            self.floor, self.floor
        ));
        let index_at = lines.len();
        for &(a, b) in &self.index {
            lines.push(format!("index [{a}, {b})"));
        }
        if self.index.is_empty() {
            lines.push("index: none".into());
        }
        let end = self.first + self.window.len();
        lines.push(format!(
            "inline [{}, {end}): {}",
            self.first,
            self.window.join(" ")
        ));
        let head = svg.node(x, y, 0, "HEAD (conditional object)", &lines, Tone::Live);

        // Snapshots stack under the head and point at the batches they pin.
        let mut left_bottom = head.bottom();
        let mut snapshot_nodes = vec![];
        for (name, index) in &self.snapshots {
            let rows: Vec<String> = if index.is_empty() {
                vec!["index: none".into()]
            } else {
                index
                    .iter()
                    .map(|(a, b)| format!("needs [{a}, {b})"))
                    .collect()
            };
            let node = svg.node(x, left_bottom + 14, 0, name, &rows, Tone::Actor);
            left_bottom = node.bottom();
            snapshot_nodes.push((node, index));
        }

        let col = x
            + head
                .w
                .max(snapshot_nodes.iter().map(|(n, _)| n.w).max().unwrap_or(0))
            + GAP
            + 20;
        let mut cy = y;
        // The published root.
        if self.floor > 0 || !self.root.is_empty() {
            let root = svg.node(
                col,
                cy,
                0,
                &format!("checkpoint root [0, {})", self.floor),
                &[self.root.join(" ")],
                Tone::Live,
            );
            svg.arrow(
                (head.right(), head.line_y(self.head.len())),
                (root.x, root.mid_y()),
                Tone::Live,
                None,
            );
            cy = root.bottom() + 10;
        }
        if let Some((c, rs)) = &self.staged {
            let staged = svg.node(
                col,
                cy,
                0,
                &format!("staged root [0, {c}) - next commit publishes"),
                &[rs.join(" ")],
                Tone::Staged,
            );
            cy = staged.bottom() + 10;
        }
        // Batch objects, referenced or not.
        let mut placed: Vec<(usize, Node)> = vec![];
        let mut starts: Vec<usize> = self
            .batches
            .iter()
            .map(|(s, _)| *s)
            .chain(self.deleted.iter().copied())
            .chain(self.index.iter().map(|(a, _)| *a))
            .chain(
                self.snapshots
                    .iter()
                    .flat_map(|(_, i)| i.iter().map(|(a, _)| *a)),
            )
            .collect();
        starts.sort_unstable();
        starts.dedup();
        for start in starts {
            let referenced = self.index.iter().any(|(a, _)| *a == start);
            let node = match self.batches.iter().find(|(s, _)| *s == start) {
                Some((_, rs)) => {
                    let (tone, note) = if referenced {
                        (Tone::Live, "")
                    } else if start < self.floor {
                        (Tone::Gone, " - below floor")
                    } else {
                        (Tone::Staged, " - unreferenced")
                    };
                    svg.node(
                        col,
                        cy,
                        0,
                        &format!("batch @{start} [{start}, {}){note}", start + rs.len()),
                        &[rs.join(" ")],
                        tone,
                    )
                }
                None if self.deleted.contains(&start) && !referenced => svg.node(
                    col,
                    cy,
                    0,
                    &format!("batch @{start} - collected"),
                    &["(deleted)".into()],
                    Tone::Gone,
                ),
                None => svg.node(
                    col,
                    cy,
                    0,
                    &format!("batch @{start} - MISSING"),
                    &["referenced but not stored".into()],
                    Tone::Bad,
                ),
            };
            cy = node.bottom() + 10;
            placed.push((start, node));
        }
        for (i, &(a, b)) in self.index.iter().enumerate() {
            if let Some((_, node)) = placed.iter().find(|(s, _)| *s == a) {
                let tone = if self.batches.iter().any(|(s, _)| *s == a) {
                    Tone::Live
                } else {
                    Tone::Bad
                };
                let label = format!("[{a}, {b})");
                svg.arrow(
                    (head.right(), head.line_y(index_at + i)),
                    (node.x, node.mid_y()),
                    tone,
                    Some(&label),
                );
            }
        }
        for (node, index) in &snapshot_nodes {
            for (i, (a, _)) in index.iter().enumerate() {
                if let Some((_, target)) = placed.iter().find(|(s, _)| s == a) {
                    let tone = if self.batches.iter().any(|(s, _)| s == a) {
                        Tone::Actor
                    } else {
                        Tone::Bad
                    };
                    svg.arrow(
                        (node.right(), node.line_y(i)),
                        (target.x, target.mid_y() + 4),
                        tone,
                        None,
                    );
                }
            }
        }
        // Cuts observed for upload whose batch is not stored (not yet created, or collected).
        for (start, rs) in &self.pending {
            if self.batches.iter().any(|(s, _)| s == start) {
                continue;
            }
            let node = svg.node(
                col,
                cy,
                0,
                &format!("observed cut @{start} - not stored"),
                &[rs.join(" ")],
                Tone::Staged,
            );
            cy = node.bottom() + 10;
        }
        cy.max(left_bottom)
    }
}

/// A sequence diagram of a trace: one lane per actor, one row per action.
/// Each event sits on its actor's lane and may point at another lane (the
/// object it writes, the instance it fences).
pub(crate) struct Timeline {
    pub(crate) lanes: Vec<String>,
    pub(crate) events: Vec<Event>,
}
pub(crate) struct Event {
    pub(crate) lane: usize,
    pub(crate) to: Option<usize>,
    pub(crate) label: String,
    pub(crate) tone: Tone,
}
impl Event {
    pub(crate) fn on(lane: usize, label: impl Into<String>) -> Self {
        Self {
            lane,
            to: None,
            label: label.into(),
            tone: Tone::Live,
        }
    }
    pub(crate) fn to(mut self, lane: usize) -> Self {
        self.to = Some(lane);
        self
    }
    pub(crate) fn tone(mut self, tone: Tone) -> Self {
        self.tone = tone;
        self
    }
}
impl Timeline {
    pub(crate) fn draw(&self, svg: &mut Svg, x: usize, y: usize) -> usize {
        const LANE_W: usize = 150;
        const ROW_H: usize = 22;
        svg.heading(x, y, "TRACE (one row per step)");
        let top = y + LINE_H + 10;
        for (i, lane) in self.lanes.iter().enumerate() {
            let cx = x + 40 + i * LANE_W;
            svg.text(
                cx.saturating_sub(lane.chars().count() * CHAR_W10 / 20),
                top + 10,
                lane,
                "cm-title",
            );
        }
        let first = top + 30;
        let bottom = first + self.events.len().max(1) * ROW_H;
        for i in 0..self.lanes.len() {
            svg.vline(x + 40 + i * LANE_W, top + 16, bottom);
        }
        let label_x = x + 40 + self.lanes.len().saturating_sub(1) * LANE_W + 40;
        for (row, event) in self.events.iter().enumerate() {
            let ey = first + row * ROW_H + ROW_H / 2;
            let ex = x + 40 + event.lane * LANE_W;
            svg.text(x, ey + 4, &format!("{}", row + 1), "cm-small");
            match event.to {
                Some(to) if to != event.lane => {
                    let tx = x + 40 + to * LANE_W;
                    let (from, end) = if tx > ex {
                        (ex + 5, tx - 5)
                    } else {
                        (ex - 5, tx + 5)
                    };
                    svg.dot(ex, ey, event.tone);
                    svg.hline(from, end, ey, event.tone);
                }
                _ => svg.dot(ex, ey, event.tone),
            }
            let label = format!("{}: {}", self.lanes[event.lane], event.label);
            svg.text(label_x, ey + 4, &label, "cm-line");
        }
        bottom
    }
}

/// The Explorer's text panel for a model that draws its state: the facts the
/// drawing leaves out, one per line. "Complete State?" still shows the raw state.
pub(crate) fn summary(lines: &[String]) -> String {
    let mut out = String::from("(drawing above; tick \"Complete State?\" for the raw state)\n\n");
    for line in lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// The witness and violation flags that are set, by name.
pub(crate) fn flags(named: &[(&str, bool)]) -> String {
    let set: Vec<&str> = named
        .iter()
        .filter(|(_, on)| *on)
        .map(|(n, _)| *n)
        .collect();
    if set.is_empty() {
        "flags: none".into()
    } else {
        format!("flags: {}", set.join(", "))
    }
}

/// Draws store layouts one under another.
pub(crate) fn stacked(views: &[StoreView]) -> String {
    stacked_with(&[], views)
}

/// Draws boxes of facts in a row, then store layouts one under another.
pub(crate) fn stacked_with(header: &[(String, Vec<String>, Tone)], views: &[StoreView]) -> String {
    let mut svg = Svg::new();
    let mut x = 0;
    let mut y = 0;
    for (title, lines, tone) in header {
        let node = svg.node(x, 0, 0, title, lines, *tone);
        x = node.right() + 16;
        y = y.max(node.bottom() + 20);
    }
    for view in views {
        y = view.draw(&mut svg, 0, y) + 24;
    }
    svg.finish()
}

/// Draws boxes left to right starting at `(0, y)`; returns the bottom of the row.
pub(crate) fn row(svg: &mut Svg, y: usize, boxes: &[(String, Vec<String>, Tone)]) -> usize {
    let mut x = 0;
    let mut bottom = y;
    for (title, lines, tone) in boxes {
        let node = svg.node(x, y, 0, title, lines, *tone);
        x = node.right() + 16;
        bottom = bottom.max(node.bottom());
    }
    bottom
}

/// Draws the current state as a row of boxes, then the trace as a timeline.
pub(crate) fn state_and_trace(
    boxes: &[(String, Vec<String>, Tone)],
    timeline: &Timeline,
) -> String {
    let mut svg = Svg::new();
    let y = if boxes.is_empty() {
        0
    } else {
        row(&mut svg, 0, boxes) + 24
    };
    timeline.draw(&mut svg, 0, y);
    svg.finish()
}

/// Each step of a path as (state before, action, state after).
pub(crate) fn steps<S: Clone, A>(path: stateright::Path<S, A>) -> Vec<(S, A, S)> {
    let pairs = path.into_vec();
    let mut out = Vec::with_capacity(pairs.len());
    let mut iter = pairs.into_iter().peekable();
    while let Some((before, action)) = iter.next() {
        if let (Some(action), Some((after, _))) = (action, iter.peek()) {
            out.push((before, action, after.clone()));
        }
    }
    out
}
