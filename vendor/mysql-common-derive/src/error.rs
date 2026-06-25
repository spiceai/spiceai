use proc_macro2::Span;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("expected a struct with a single unnamed field")]
    NotANewTypeStruct(Span),
    #[error("structs with named fields are not supported")]
    NamedFieldsNotSupported(Span),
    #[error("unit structs are not supported")]
    UnitStructsNotSupported(Span),
    #[error("structs with unnamed fields are not supported")]
    StructsWithUnnamedFieldsNotSupported(Span),
    #[error("unions are not supported")]
    UnionsNotSupported(Span),
    #[error("enums are not supported")]
    EnumsNotSupported(Span),
    #[error("non-unit variants are not supported")]
    NonUnitVariant(Span),
    #[error("unsupported discriminant")]
    UnsupportedDiscriminant(Span),
    #[error("add #[mysql(explicit_invalid)] attribute to allow")]
    ExplicitInvalid(Span),
    #[error("no suitable crate found, use #[mysql(crate = \"..\")] to specify the crate name")]
    NoCrateNameFound,
    #[error("multiple crates found, use #[mysql(crate = \"..\")] to specify the particular name")]
    MultipleCratesFound,
    #[error(transparent)]
    Syn(#[from] syn::Error),
    #[error(transparent)]
    Darling(#[from] darling::error::Error),
    #[error("conflicting attributes")]
    FromValueConflictingAttributes(Span, Span),
    #[error("representation won't fit into MySql integer")]
    UnsupportedRepresentation(Span),
    #[error("this attribute requires `{}` attribute", 0)]
    FromValueAttributeRequired(Span, &'static str),
    #[error("conflicting attributes")]
    FromRowConflictingAttributes(Span, Span),
    #[error("this attribute requires `{}` attribute", 0)]
    FromRowAttributeRequired(Span, &'static str),
}

// Render a derive error as a `syn::Error`, whose `to_compile_error()` emits a
// spanned `compile_error!{ .. }` — the same user-visible outcome as the former
// `proc-macro-error2` `Diagnostic`/`abort!` path, without that unmaintained
// dependency. Multi-span variants attach the secondary span via `combine`.
impl From<Error> for syn::Error {
    fn from(x: Error) -> syn::Error {
        match x {
            Error::UnionsNotSupported(span)
            | Error::EnumsNotSupported(span)
            | Error::NonUnitVariant(span)
            | Error::UnsupportedDiscriminant(span)
            | Error::ExplicitInvalid(span)
            | Error::NotANewTypeStruct(span)
            | Error::NamedFieldsNotSupported(span)
            | Error::UnitStructsNotSupported(span)
            | Error::UnsupportedRepresentation(span)
            | Error::StructsWithUnnamedFieldsNotSupported(span) => {
                syn::Error::new(span, format!("FromValue: {x}"))
            }
            Error::Syn(ref e) => syn::Error::new(e.span(), format!("FromValue: {x}")),
            Error::Darling(ref e) => syn::Error::new(e.span(), format!("FromValue: {x}")),
            Error::NoCrateNameFound => {
                syn::Error::new(Span::call_site(), format!("FromValue: {x}"))
            }
            Error::MultipleCratesFound => {
                syn::Error::new(Span::call_site(), format!("FromValue: {x}"))
            }
            Error::FromValueConflictingAttributes(s1, s2) => {
                let mut err = syn::Error::new(s1, format!("FromValue: {x}"));
                err.combine(syn::Error::new(s2, "conflicting attribute"));
                err
            }
            Error::FromValueAttributeRequired(s, _) => {
                syn::Error::new(s, format!("FromValue: {x}"))
            }
            Error::FromRowConflictingAttributes(s1, s2) => {
                let mut err = syn::Error::new(s1, format!("FromRow: {x}"));
                err.combine(syn::Error::new(s2, "conflicting attribute"));
                err
            }
            Error::FromRowAttributeRequired(s, _) => syn::Error::new(s, format!("FromRow: {x}")),
        }
    }
}
