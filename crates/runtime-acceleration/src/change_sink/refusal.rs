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

//! Proven pre-mutation refusals. Every unmarked failure requires reconciliation.
//!
//! A backend wraps an error with `before_mutation` when it is sure that the
//! failed write changed no storage and no index. The owner checks the mark with
//! `is_before_mutation`: a marked failure of a `Recovery::Rebuildable` write
//! does not stop the sink. Every other failure stops it.
//!
//! A coalesced scoped write can instead fail with `member_refusal`, which is
//! also a pre-mutation refusal. It names the first member, in arrival order,
//! that would be refused on its own, so the owner can apply the members
//! before it and refuse only that member.

use datafusion::error::DataFusionError;
use snafu::Snafu;

/// Marker error that wraps a pre-mutation refusal. Its display is the source
/// error unchanged.
#[derive(Debug, Snafu)]
#[snafu(display("{source}"))]
pub struct BeforeMutation {
    source: DataFusionError,
}

/// Mark a refusal only when this submission has not changed storage or indexes.
/// Planning a provider write is not proof: some wrappers mutate while planning.
#[must_use]
pub fn before_mutation(source: DataFusionError) -> DataFusionError {
    DataFusionError::External(Box::new(BeforeMutation { source }))
}

/// Inspect typed error sources, without relying on error messages or recovery policy.
#[must_use]
pub fn is_before_mutation(error: &DataFusionError) -> bool {
    let mut current: &(dyn std::error::Error + 'static) = error;
    loop {
        if current.is::<BeforeMutation>() || current.is::<MemberRefusal>() {
            return true;
        }
        let Some(source) = current.source() else {
            return false;
        };
        current = source;
    }
}

/// A coalesced write refused before mutation because its member at `position`,
/// in arrival order, is the first that would be refused had each member been
/// applied on its own. The members before it would all have been applied.
#[derive(Debug, Snafu)]
#[snafu(display("{source}"))]
pub struct MemberRefusal {
    position: usize,
    source: DataFusionError,
}

/// Refuse a coalesced write before mutation, naming its first refused member.
#[must_use]
pub fn member_refusal(position: usize, source: DataFusionError) -> DataFusionError {
    DataFusionError::External(Box::new(MemberRefusal { position, source }))
}

/// The arrival position of the first refused member, when a refusal names one.
#[must_use]
pub fn refused_member(error: &DataFusionError) -> Option<usize> {
    let mut current: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(refusal) = current.downcast_ref::<MemberRefusal>() {
            return Some(refusal.position);
        }
        current = current.source()?;
    }
}
