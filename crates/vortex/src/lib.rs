// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Connectors to enable [DataFusion](https://docs.rs/datafusion/latest/datafusion/) to read [`Vortex`](https://docs.rs/crate/vortex/latest) data.
#![deny(missing_docs)]
#![cfg_attr(
    test,
    expect(
        clippy::cast_possible_wrap,
        clippy::clone_on_ref_ptr,
        clippy::default_trait_access,
        clippy::doc_markdown,
        clippy::explicit_into_iter_loop,
        clippy::ignored_unit_patterns,
        clippy::items_after_statements,
        clippy::manual_let_else,
        clippy::needless_raw_string_hashes,
        clippy::redundant_closure_for_method_calls,
        clippy::uninlined_format_args,
        clippy::unreadable_literal,
        clippy::unwrap_used,
        reason = "vendored upstream tests intentionally keep upstream fixture style"
    )
)]
use std::fmt::Debug;

use datafusion_common::stats::Precision as DFPrecision;
use vortex::expr::stats::Precision;

mod convert;
mod persistent;

pub use convert::exprs::DefaultExpressionConvertor;
pub use convert::exprs::ExpressionConvertor;
pub use convert::exprs::ProcessedProjection;
pub use persistent::*;

/// Extension trait to convert Vortex [`Precision`](vortex::stats::Precision) values to `DataFusion` [`Precision`](datafusion_common::stats::Precision) values.
trait PrecisionExt<T>
where
    T: Debug + Clone + PartialEq + Eq + PartialOrd,
{
    /// Convert `Precision` to the `DataFusion` equivalent.
    fn to_df(self) -> DFPrecision<T>;
}

impl<T> PrecisionExt<T> for Precision<T>
where
    T: Debug + Clone + PartialEq + Eq + PartialOrd,
{
    fn to_df(self) -> DFPrecision<T> {
        match self {
            Precision::Exact(v) => DFPrecision::Exact(v),
            Precision::Inexact(v) => DFPrecision::Inexact(v),
            Precision::Absent => DFPrecision::Absent,
        }
    }
}

impl<T> PrecisionExt<T> for Option<Precision<T>>
where
    T: Debug + Clone + PartialEq + Eq + PartialOrd,
{
    fn to_df(self) -> DFPrecision<T> {
        match self {
            Some(v) => v.to_df(),
            None => DFPrecision::Absent,
        }
    }
}

#[cfg(test)]
mod common_tests {
    use std::sync::Arc;

    use datafusion::datasource::provider::DefaultTableFactory;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_common::GetExt;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use url::Url;

    use crate::ProjectionPushdown;
    use crate::VortexFormatFactory;
    use crate::VortexTableOptions;

    pub struct TestSessionContext {
        pub store: Arc<dyn ObjectStore>,
        pub session: SessionContext,
    }

    impl Default for TestSessionContext {
        fn default() -> Self {
            Self::new(false)
        }
    }

    impl TestSessionContext {
        /// Create a new test session context with the given projection pushdown setting.
        pub fn new(projection_pushdown: bool) -> Self {
            let store = Arc::new(InMemory::new());
            let opts = VortexTableOptions {
                projection_pushdown: ProjectionPushdown::from_bool(projection_pushdown),
                ..Default::default()
            };
            let factory = Arc::new(VortexFormatFactory::new().with_options(opts));
            let mut session_state_builder = SessionStateBuilder::new()
                .with_default_features()
                .with_table_factory(
                    factory.get_ext().to_uppercase(),
                    Arc::new(DefaultTableFactory::new()),
                )
                .with_object_store(&Url::try_from("file://").unwrap(), store.clone());

            if let Some(file_formats) = session_state_builder.file_formats() {
                file_formats.push(factory as _);
            }

            let session: SessionContext =
                SessionContext::new_with_state(session_state_builder.build()).enable_url_table();

            Self { store, session }
        }
    }
}
