//! Implements [`FromValue`] and [`FromRow`] derive macros.

extern crate proc_macro;

use crate::error::Error;
type Result<T> = std::result::Result<T, crate::error::Error>;

mod error;
mod warn;

mod from_row;
mod from_value;

/// Derives `FromValue`. See `mysql_common` crate-level docs for more info.
#[proc_macro_derive(FromValue, attributes(mysql))]
pub fn from_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: syn::DeriveInput = syn::parse(input).unwrap();
    match from_value::impl_from_value(&input) {
        Ok(gen) => gen.into(),
        // Render the error as a spanned `compile_error!`, the same user-visible
        // outcome as the former `proc-macro-error2` `abort!`.
        Err(e) => syn::Error::from(e).to_compile_error().into(),
    }
}

/// Derives `FromRow`. See `mysql_common` crate-level docs for more info.
#[proc_macro_derive(FromRow, attributes(mysql))]
pub fn from_row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: syn::DeriveInput = syn::parse(input).unwrap();
    match from_row::impl_from_row(&input) {
        Ok(gen) => gen.into(),
        Err(e) => syn::Error::from(e).to_compile_error().into(),
    }
}
