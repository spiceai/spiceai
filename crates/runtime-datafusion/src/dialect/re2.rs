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

//! The regular-expression syntax RE2 — the engine `DuckDB` and `BigQuery`
//! embed — reads the way the kernel's `regex` crate does.
//!
//! The two engines are near relatives, and their differences are silent: a
//! pattern one reads differently changes *which rows match*, not whether the
//! query runs. A dialect that pushes a regexp call down can therefore only do
//! so for a pattern built from syntax with one reading in both engines, and
//! this module is the allow-list of that syntax, walked over the pattern's
//! syntax tree as parsed by `regex-syntax` — the crate the kernel compiles
//! with, so what is judged is what the kernel would run.
//!
//! What is admitted: literals (verbatim, `\.`-style escapes of punctuation,
//! `\xHH`, `\x{H..}`, `\n`-style specials), `.`, bracketed classes of literals
//! and ranges (negated or not), `?`/`*`/`+`/`{m,n}` repetitions whose nested
//! counted bounds multiply to at most [`RE2_MAX_REPETITION`], greedy or lazy,
//! alternation, indexed or non-capturing groups, the `^`/`$`/`\A`/`\z`
//! anchors, and the `i` flag. Everything else is refused, and
//! [`EngineDependentSyntax`] says why: Perl classes and word boundaries are
//! Unicode-aware in the kernel and ASCII-only in RE2 (`'\d'` over `xy١`
//! matches locally and not remotely); class-set operations and nested classes
//! are syntax RE2 does not have, so it reads `[a&&a]` as a class of `a` and
//! `&`; RE2 rejects the `x` flag and the `\u` escapes outright; and Unicode
//! properties, POSIX classes, named groups and the other flags have not been
//! measured to agree. `.`, `[^a]`, `[0-9]` and case-insensitive matching of
//! non-ASCII letters were measured to agree.
//!
//! A caller with a stricter need layers its own predicate on the returned
//! syntax tree — the `DuckDB` `regexp_count` rendering also requires a minimum
//! match length above zero, because the two engines count an empty match
//! differently.

use std::fmt;

use regex_syntax::ast::{
    AssertionKind, Ast, ClassSetBinaryOp, ClassSetItem, Flag, Flags, FlagsItemKind, GroupKind,
    HexLiteralKind, Literal, LiteralKind, Repetition, RepetitionKind, RepetitionRange,
    SpecialLiteralKind, Visitor, parse::Parser, visit,
};

/// RE2's cap on counted repetition (`kMaxRepeat` in `re2/parse.cc`): the
/// product of the `{n}`/`{n,m}` bounds along a nesting path may not exceed
/// it, so `(a{100}){11}` fails the query remotely with `invalid repetition
/// size` where the kernel compiles it.
pub(super) const RE2_MAX_REPETITION: u32 = 1000;

/// Why a pattern is outside what both engines read alike — see the module
/// doc for the reasoning behind each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EngineDependentSyntax {
    /// The kernel cannot parse it; local evaluation delivers the kernel's own
    /// error, which is the right one for the user to see.
    Unparseable,
    /// `\d`, `\w`, `\s` or a negation, bare or inside a bracketed class.
    PerlClass,
    /// `\b`, `\B` or one of the word-start/word-end assertions.
    WordBoundary,
    /// `\p{..}`, `\pL` or a negation, bare or inside a bracketed class.
    UnicodeProperty,
    /// `[[:alpha:]]` and the other POSIX classes.
    AsciiClass,
    /// `&&`, `--` or `~~` between class-set items.
    ClassSetOperation,
    /// A bracketed class inside a bracketed class, or an empty one.
    NestedOrEmptyClass,
    /// Any flag but `i`, or a negated flag.
    Flag,
    /// `(?P<name>..)` and `(?<name>..)`.
    NamedGroup,
    /// An escape outside the shared set: `\u..`, `\U..`, octal, or an
    /// escaped ordinary character.
    Escape,
    /// Counted repetitions whose nested product exceeds
    /// [`RE2_MAX_REPETITION`].
    RepetitionBound,
}

impl fmt::Display for EngineDependentSyntax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unparseable => "it is not a valid regular expression",
            Self::PerlClass => {
                "it uses a Perl class (`\\d`, `\\w`, `\\s`), which is Unicode-aware in DataFusion and ASCII-only in RE2"
            }
            Self::WordBoundary => {
                "it uses a word boundary, which is Unicode-aware in DataFusion and ASCII-only in RE2"
            }
            Self::UnicodeProperty => "it uses a Unicode property class, whose agreement is unmeasured",
            Self::AsciiClass => "it uses a POSIX class, whose agreement is unmeasured",
            Self::ClassSetOperation => {
                "it uses a class-set operation (`&&`, `--`, `~~`), which RE2 reads as literal punctuation"
            }
            Self::NestedOrEmptyClass => "it uses a nested or empty bracketed class, which RE2 has no syntax for",
            Self::Flag => "it uses a flag other than `i`, which RE2 reads differently or rejects",
            Self::NamedGroup => "it uses a named group, whose agreement is unmeasured",
            Self::Escape => "it uses an escape RE2 rejects or reads differently",
            Self::RepetitionBound => {
                "its nested counted repetitions multiply past RE2's limit of 1000"
            }
        })
    }
}

/// Parses `pattern` as the kernel would and returns its syntax tree if every
/// construct in it is one both engines read alike.
pub(super) fn engine_neutral_ast(pattern: &str) -> Result<Ast, EngineDependentSyntax> {
    let ast = Parser::new()
        .parse(pattern)
        .map_err(|_| EngineDependentSyntax::Unparseable)?;
    visit(&ast, EngineNeutralSyntax::default())?;
    Ok(ast)
}

/// Walks a pattern's syntax tree and fails on the first
/// [`EngineDependentSyntax`] it meets.
#[derive(Default)]
struct EngineNeutralSyntax {
    /// The product of the counted-repetition bounds enclosing the node being
    /// visited, one entry per enclosing repetition so `visit_post` can unwind.
    repetition_products: Vec<u32>,
}

impl EngineNeutralSyntax {
    fn literal(literal: &Literal) -> Result<(), EngineDependentSyntax> {
        match literal.kind {
            LiteralKind::Verbatim
            | LiteralKind::Meta
            | LiteralKind::HexFixed(HexLiteralKind::X)
            | LiteralKind::HexBrace(HexLiteralKind::X) => Ok(()),
            LiteralKind::Special(ref special) if *special != SpecialLiteralKind::Space => Ok(()),
            _ => Err(EngineDependentSyntax::Escape),
        }
    }

    fn flags(flags: &Flags) -> Result<(), EngineDependentSyntax> {
        if flags
            .items
            .iter()
            .all(|item| matches!(item.kind, FlagsItemKind::Flag(Flag::CaseInsensitive)))
        {
            Ok(())
        } else {
            Err(EngineDependentSyntax::Flag)
        }
    }

    /// Enters a repetition: multiplies the enclosing counted bounds by this
    /// one's (`?`, `*` and `+` count as 1, as in RE2) and refuses the pattern
    /// once the product passes [`RE2_MAX_REPETITION`].
    fn enter_repetition(&mut self, repetition: &Repetition) -> Result<(), EngineDependentSyntax> {
        let bound = match repetition.op.kind {
            RepetitionKind::Range(
                RepetitionRange::Exactly(n)
                | RepetitionRange::AtLeast(n)
                | RepetitionRange::Bounded(_, n),
            ) => n,
            RepetitionKind::ZeroOrOne | RepetitionKind::ZeroOrMore | RepetitionKind::OneOrMore => 1,
        };
        let enclosing = self.repetition_products.last().copied().unwrap_or(1);
        let product = enclosing.saturating_mul(bound.max(1));
        if product > RE2_MAX_REPETITION {
            return Err(EngineDependentSyntax::RepetitionBound);
        }
        self.repetition_products.push(product);
        Ok(())
    }
}

impl Visitor for EngineNeutralSyntax {
    type Output = ();
    type Err = EngineDependentSyntax;

    fn finish(self) -> Result<(), EngineDependentSyntax> {
        Ok(())
    }

    fn visit_pre(&mut self, ast: &Ast) -> Result<(), EngineDependentSyntax> {
        match ast {
            Ast::Empty(_)
            | Ast::Dot(_)
            | Ast::Alternation(_)
            | Ast::Concat(_)
            | Ast::ClassBracketed(_) => Ok(()),
            Ast::Literal(literal) => Self::literal(literal),
            Ast::Repetition(repetition) => self.enter_repetition(repetition),
            Ast::Flags(set) => Self::flags(&set.flags),
            Ast::Group(group) => match &group.kind {
                GroupKind::CaptureIndex(_) => Ok(()),
                GroupKind::NonCapturing(flags) => Self::flags(flags),
                GroupKind::CaptureName { .. } => Err(EngineDependentSyntax::NamedGroup),
            },
            // Anchors are the only assertions both engines read alike; every
            // other kind is a word boundary, today's and any added later.
            Ast::Assertion(assertion) => match assertion.kind {
                AssertionKind::StartLine
                | AssertionKind::EndLine
                | AssertionKind::StartText
                | AssertionKind::EndText => Ok(()),
                _ => Err(EngineDependentSyntax::WordBoundary),
            },
            Ast::ClassUnicode(_) => Err(EngineDependentSyntax::UnicodeProperty),
            Ast::ClassPerl(_) => Err(EngineDependentSyntax::PerlClass),
        }
    }

    fn visit_post(&mut self, ast: &Ast) -> Result<(), EngineDependentSyntax> {
        if matches!(ast, Ast::Repetition(_)) {
            self.repetition_products.pop();
        }
        Ok(())
    }

    fn visit_class_set_item_pre(
        &mut self,
        item: &ClassSetItem,
    ) -> Result<(), EngineDependentSyntax> {
        match item {
            ClassSetItem::Union(_) => Ok(()),
            ClassSetItem::Literal(literal) => Self::literal(literal),
            ClassSetItem::Range(range) => {
                Self::literal(&range.start).and_then(|()| Self::literal(&range.end))
            }
            ClassSetItem::Empty(_) | ClassSetItem::Bracketed(_) => {
                Err(EngineDependentSyntax::NestedOrEmptyClass)
            }
            ClassSetItem::Ascii(_) => Err(EngineDependentSyntax::AsciiClass),
            ClassSetItem::Unicode(_) => Err(EngineDependentSyntax::UnicodeProperty),
            ClassSetItem::Perl(_) => Err(EngineDependentSyntax::PerlClass),
        }
    }

    fn visit_class_set_binary_op_pre(
        &mut self,
        _op: &ClassSetBinaryOp,
    ) -> Result<(), EngineDependentSyntax> {
        Err(EngineDependentSyntax::ClassSetOperation)
    }
}
