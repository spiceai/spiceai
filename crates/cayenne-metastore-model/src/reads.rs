// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Read freshness against real time. A writer completes commits; a reader
//! keeps a cached head with the time its GET was sent and answers queries from
//! it when it is fresh for the staleness bound S, otherwise it waits for a
//! newer GET. Several queries may share one GET. A GET observes the head when
//! it is sent, the worst case for freshness, and responses can arrive late and
//! out of order. With S = 0, events within one tick are ordered, so a query must
//! see a commit that completed earlier in its tick; with S > 0 times compare by
//! tick, a commit counting as more than S before a query from S + 1 ticks on.
//! With S = 0 the freshness property is linearizability for one
//! dataset's committed state; the token control serves any cached head at or
//! past the client's own commit number, which is read-your-writes only.
use stateright::{Model, Property};

/// Commits the writer completes.
const COMMITS: u8 = 2;
/// Head GETs the reader sends.
const GETS: u8 = 3;
/// Last tick of the discrete clock.
const CLOCK: u8 = 3;
/// Queries the reader answers.
const QUERIES: usize = 2;
/// A real time: the tick and the event's position within it. Positions are
/// kept only with S = 0, as dense ranks within each tick.
type Time = (u8, u8);
/// Whether an event at `t` happened no earlier than S before `start`.
fn within(t: Time, s: u8, start: Time) -> bool {
    (t.0 + s, t.1) >= start
}
/// Every time a state records.
fn times(s: &mut State) -> impl Iterator<Item = &mut Time> {
    let cache = [&mut s.cache.sent, &mut s.cache.received];
    let gets = s.gets.iter_mut().map(|(_, t)| t);
    let queries = s.queries.iter_mut().flatten().flat_map(|q| {
        let from = q.from.as_mut().map(|c| [&mut c.sent, &mut c.received]);
        std::iter::once(&mut q.start).chain(from.into_iter().flatten())
    });
    cache
        .into_iter()
        .chain(&mut s.completed)
        .chain(gets)
        .chain(queries)
}
/// Renumbers positions to dense ranks within each tick, which keeps their order,
/// or drops them when S > 0, and places the next event after the current tick's.
fn canonical(s: &mut State, bound: u8) {
    let mut seen: Vec<Time> = times(s).map(|t| *t).collect();
    seen.sort_unstable();
    seen.dedup();
    for t in times(s) {
        t.1 = if bound > 0 {
            0
        } else {
            let rank = seen.iter().filter(|o| o.0 == t.0 && o.1 < t.1).count();
            u8::try_from(rank).unwrap_or(u8::MAX)
        };
    }
    s.event = if bound > 0 {
        0
    } else {
        let current = seen.iter().filter(|o| o.0 == s.clock).count();
        u8::try_from(current).unwrap_or(u8::MAX)
    };
}
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    /// The staleness bound S in ticks.
    s: u8,
}
/// A cached or in-flight head: the commit it holds, when its GET was sent, and
/// when its response arrived.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Copy {
    commit: u8,
    sent: Time,
    received: Time,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Query {
    start: Time,
    /// The commit number the client got back for its own last write.
    token: u8,
    served: Option<u8>,
    /// The cached copy that answered it.
    from: Option<Copy>,
    /// A GET response arrived after the query started and before it was served.
    waited: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    clock: u8,
    /// Position the next event takes in this tick.
    event: u8,
    head: u8,
    /// Completion time of each commit, in commit order.
    completed: Vec<Time>,
    cache: Copy,
    /// GETs in flight: the head they observed and when they were sent.
    gets: Vec<(u8, Time)>,
    sent: u8,
    queries: Vec<Option<Query>>,
    /// A query was answered from the cache with no GET since it started.
    cached_answer: bool,
    /// One GET response answered two queries.
    shared: bool,
    /// A late response older than the cache was ignored.
    late_ignored: bool,
    /// A commit completed while a GET was in flight.
    overtaken: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Tick,
    Commit,
    Send,
    Deliver(usize),
    Start(usize),
    Serve(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            s: u8::try_from(bound).unwrap_or(u8::MAX),
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    /// Whether the cache may answer query `q`.
    fn fresh(&self, cache: Copy, q: Query) -> bool {
        if self.has("token-wait") {
            cache.commit >= q.token
        } else if self.has("response-time") {
            // Freshness wrongly counted from the response's arrival.
            within(cache.received, self.s, q.start)
        } else if self.has("tick-only") {
            // Freshness wrongly compared by tick alone, so a GET sent earlier in
            // the query's tick counts as sent after it started.
            cache.sent.0 + self.s >= q.start.0
        } else {
            // Freshness counts from when the GET was sent.
            within(cache.sent, self.s, q.start)
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let mut init = State {
            clock: 0,
            event: 0,
            head: 0,
            completed: vec![],
            cache: Copy::default(),
            gets: vec![],
            sent: 0,
            queries: vec![None; QUERIES],
            cached_answer: false,
            shared: false,
            late_ignored: false,
            overtaken: false,
        };
        canonical(&mut init, self.s);
        vec![init]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        a.extend([Action::Tick, Action::Commit, Action::Send]);
        for j in 0..s.gets.len() {
            a.push(Action::Deliver(j));
        }
        for q in 0..QUERIES {
            a.extend([Action::Start(q), Action::Serve(q)]);
        }
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut next = s.clone();
        let now = (s.clock, s.event);
        match action {
            Action::Tick => {
                if s.clock >= CLOCK {
                    return None;
                }
                next.clock += 1;
            }
            Action::Commit => {
                if s.head >= COMMITS {
                    return None;
                }
                next.head += 1;
                next.completed.push(now);
                next.overtaken |= !s.gets.is_empty();
            }
            Action::Send => {
                if s.sent >= GETS {
                    return None;
                }
                next.sent += 1;
                next.gets.push((s.head, now));
            }
            Action::Deliver(j) => {
                let (commit, sent) = s.gets[j];
                next.gets.remove(j);
                if commit >= s.cache.commit {
                    // A reader never adopts a lower head.
                    next.cache = Copy {
                        commit,
                        sent,
                        received: now,
                    };
                    for q in next.queries.iter_mut().flatten() {
                        if q.served.is_none() {
                            q.waited = true;
                        }
                    }
                } else {
                    next.late_ignored = true;
                }
            }
            Action::Start(q) => {
                if s.queries[q].is_some() {
                    return None;
                }
                next.queries[q] = Some(Query {
                    start: now,
                    // The client's own write is the first commit, once it completed,
                    // so the token precedes the query in real time.
                    token: u8::from(!s.completed.is_empty()),
                    served: None,
                    from: None,
                    waited: false,
                });
            }
            Action::Serve(q) => {
                let query = s.queries[q]?;
                if query.served.is_some() || !self.fresh(s.cache, query) {
                    return None;
                }
                next.cached_answer |= !query.waited;
                next.shared |= query.waited
                    && s.queries
                        .iter()
                        .flatten()
                        .any(|o| o.waited && o.from == Some(s.cache));
                next.queries[q] = Some(Query {
                    served: Some(s.cache.commit),
                    from: Some(s.cache),
                    ..query
                });
            }
        }
        if next == *s {
            return None;
        }
        // The event took the position after every recorded time in the tick.
        next.event = next.event.saturating_add(1);
        canonical(&mut next, self.s);
        Some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut p = vec![
            Property::always(
                "query includes every commit completed more than S before it started",
                |m: &Protocol, s: &State| {
                    s.queries.iter().flatten().all(|q| {
                        q.served.is_none_or(|v| {
                            let required = s
                                .completed
                                .iter()
                                .filter(|&&t| !within(t, m.s, q.start))
                                .count();
                            usize::from(v) >= required
                        })
                    })
                },
            ),
            Property::sometimes("query waits for a GET", |_, s: &State| {
                s.queries
                    .iter()
                    .flatten()
                    .any(|q| q.waited && q.served.is_some_and(|v| v > 0))
            }),
            Property::sometimes("two queries share one GET", |_, s: &State| s.shared),
            Property::sometimes("late GET response ignored", |_, s: &State| s.late_ignored),
            Property::sometimes(
                "commit completes while a GET is in flight",
                |_, s: &State| s.overtaken,
            ),
            Property::sometimes(
                "query served while a newer commit exists",
                |_, s: &State| {
                    s.queries
                        .iter()
                        .flatten()
                        .any(|q| q.served.is_some_and(|v| v < s.head))
                },
            ),
        ];
        if self.s == 0 || self.has("token-wait") {
            // Read-your-writes across nodes needs S = 0 or the token rule; it is
            // not promised under a staleness bound.
            p.push(Property::always(
                "query includes the client's own write",
                |_, s: &State| {
                    s.queries
                        .iter()
                        .flatten()
                        .all(|q| q.served.is_none_or(|v| v >= q.token))
                },
            ));
        }
        if self.s > 0 {
            p.push(Property::sometimes(
                "query answered from the cache within S",
                |_, s: &State| s.cached_answer,
            ));
        }
        p
    }
}
