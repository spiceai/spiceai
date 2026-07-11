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

//! `#[derive(TypedParams)]` — typed spicepod component parameters.
//!
//! Generates an implementation of `runtime_parameters::typed::TypedParams` that
//! deserializes the secret-injected string map produced by
//! `runtime_secrets::get_params_with_secrets` into a plain Rust struct, preserving
//! the runtime's parameter semantics: per-variant key prefixing, unknown-key
//! warnings with typo suggestions, deprecation warnings, string defaults, and
//! secret autoload from configured secret stores.
//!
//! # Attributes
//!
//! Container: `#[params(prefix = "openai")]` (required) — the component prefix
//! applied to every field key unless the field is marked `runtime`.
//!
//! Field `#[param(...)]` keys:
//! - `runtime` — the spicepod key is unprefixed (parity with `ParameterSpec::runtime`).
//! - `rename = "key"` — spicepod key differs from the field identifier.
//! - `alias = "key"` — additional accepted key; repeatable; prefixed like the field.
//! - `default = "value"` — string default, parsed through the same path as user input.
//! - `secret` — opt into secret autoload: when absent from the spicepod, the
//!   prefixed key is looked up in the configured secret stores. The field must be
//!   `SecretString` or `Option<SecretString>`.
//! - `parse_with = path` — custom parser `fn(&str) -> Result<T, impl Display>`
//!   used instead of `FromStr`.
//!
//! Field semantics derived from the type: `Option<T>` fields are optional; all
//! other fields are required (missing yields an error naming the user-facing,
//! prefixed key). Doc comments become the hint appended to missing-parameter
//! errors. `#[deprecated(note = "...")]` on a field emits a runtime warning when
//! the corresponding key is present.

use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Expr, ExprLit, Fields, Lit, Type, parse_macro_input};

#[proc_macro_derive(TypedParams, attributes(params, param))]
pub fn derive_typed_params(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct FieldSpec {
    ident: syn::Ident,
    /// Declared type with any outer `Option` stripped.
    inner_ty: Type,
    optional: bool,
    secret: bool,
    runtime: bool,
    rename: Option<String>,
    aliases: Vec<String>,
    default: Option<String>,
    parse_with: Option<syn::Path>,
    doc: String,
    /// `Some(note)` when the field carries `#[deprecated]`; empty note for the bare form.
    deprecated: Option<String>,
}

impl FieldSpec {
    /// The unprefixed parameter name (field identifier unless renamed).
    fn name(&self) -> String {
        self.rename
            .clone()
            .unwrap_or_else(|| self.ident.to_string())
    }

    /// The user-facing spicepod key: `{prefix}_{name}` for component params
    /// (unless the name already carries the prefix), bare for runtime params.
    fn user_key(&self, prefix: &str) -> String {
        apply_prefix(&self.name(), prefix, self.runtime)
    }
}

fn apply_prefix(name: &str, prefix: &str, runtime: bool) -> String {
    if runtime || name.starts_with(&format!("{prefix}_")) {
        name.to_string()
    } else {
        format!("{prefix}_{name}")
    }
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_ident = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new(
            input.generics.span(),
            "TypedParams does not support generic structs",
        ));
    }

    let prefix = parse_prefix(input)?;

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(
            input.span(),
            "TypedParams can only be derived for structs",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new(
            input.span(),
            "TypedParams requires named struct fields",
        ));
    };

    let specs = fields
        .named
        .iter()
        .map(parse_field)
        .collect::<syn::Result<Vec<_>>>()?;

    let mut field_stmts = Vec::new();
    let mut known_keys = Vec::new();
    let mut any_deprecated = false;

    for spec in &specs {
        let user_key = spec.user_key(&prefix);
        let alias_keys: Vec<String> = spec
            .aliases
            .iter()
            .map(|a| apply_prefix(a, &prefix, spec.runtime))
            .collect();
        known_keys.push(user_key.clone());
        known_keys.extend(alias_keys.clone());

        field_stmts.push(expand_field(spec, &prefix, &user_key, &alias_keys)?);
        any_deprecated |= spec.deprecated.is_some();
    }

    let field_idents: Vec<&syn::Ident> = specs.iter().map(|s| &s.ident).collect();
    let allow_deprecated = any_deprecated.then(|| quote! { #[allow(deprecated)] });

    Ok(quote! {
        #allow_deprecated
        impl ::runtime_parameters::typed::TypedParams for #struct_ident {
            const PREFIX: &'static str = #prefix;

            async fn try_from_params(
                component_name: &str,
                mut params: ::runtime_parameters::typed::__private::HashMap<
                    ::std::string::String,
                    ::runtime_parameters::typed::__private::SecretString,
                >,
                secrets: &::runtime_parameters::typed::__private::Arc<
                    ::runtime_parameters::typed::__private::RwLock<
                        ::runtime_parameters::typed::__private::Secrets,
                    >,
                >,
            ) -> ::std::result::Result<Self, ::runtime_parameters::typed::ParamsError> {
                #(#field_stmts)*
                ::runtime_parameters::typed::warn_leftover_keys(
                    component_name,
                    &params,
                    &[#(#known_keys),*],
                    #prefix,
                );
                ::std::result::Result::Ok(Self { #(#field_idents),* })
            }
        }
    })
}

/// Generates the statements producing one `let <field> = ...;` binding.
fn expand_field(
    spec: &FieldSpec,
    prefix: &str,
    user_key: &str,
    alias_keys: &[String],
) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &spec.ident;
    let inner_ty = &spec.inner_ty;
    let hint = if spec.doc.is_empty() {
        String::new()
    } else {
        format!(" {}", spec.doc)
    };

    // Deprecation warning (checked against the original user keys, before consumption).
    let deprecation = spec.deprecated.as_ref().map(|note| {
        let note = if note.is_empty() {
            None
        } else {
            Some(note.clone())
        };
        let note_tokens = if let Some(n) = note { quote! { ::std::option::Option::Some(#n) } } else { quote! { ::std::option::Option::None } };
        let all_keys = std::iter::once(user_key.to_string())
            .chain(alias_keys.iter().cloned())
            .collect::<Vec<_>>();
        quote! {
            for __key in [#(#all_keys),*] {
                if params.contains_key(__key) {
                    ::runtime_parameters::typed::warn_deprecated(component_name, __key, #note_tokens);
                }
            }
        }
    });

    // Raw lookup: primary key, then aliases in declaration order.
    let alias_removals = alias_keys
        .iter()
        .map(|a| quote! { .or_else(|| params.remove(#a)) });
    let mut raw = quote! {
        params.remove(#user_key) #(#alias_removals)*
    };

    // Secret autoload: absent + `#[param(secret)]` → look up the prefixed key in
    // the secret stores (parity with `Parameters::try_new`).
    if spec.secret {
        let autoload_key = apply_prefix(&spec.name(), prefix, false);
        raw = quote! {
            match #raw {
                ::std::option::Option::Some(__v) => ::std::option::Option::Some(__v),
                ::std::option::Option::None => ::runtime_parameters::typed::autoload_secret(
                    secrets,
                    component_name,
                    #autoload_key,
                )
                .await,
            }
        };
    }

    // String default, parsed through the same path as user-provided values.
    if let Some(default) = &spec.default {
        raw = quote! {
            #raw.or_else(|| ::std::option::Option::Some(
                ::runtime_parameters::typed::__private::SecretString::from(#default)
            ))
        };
    }

    let is_secret_string = type_is_secret_string(inner_ty);
    if spec.secret && !is_secret_string {
        return Err(syn::Error::new(
            ident.span(),
            "#[param(secret)] fields must be `SecretString` or `Option<SecretString>`",
        ));
    }
    if spec.parse_with.is_some() && is_secret_string {
        return Err(syn::Error::new(
            ident.span(),
            "#[param(parse_with)] cannot be combined with a `SecretString` field",
        ));
    }

    // Typing: `SecretString` passes through unexposed; everything else parses.
    let parse_value = if is_secret_string {
        quote! { __v }
    } else if let Some(parse_with) = &spec.parse_with {
        quote! {
            ::runtime_parameters::typed::parse_param_with(#user_key, &__v, #parse_with)?
        }
    } else {
        quote! {
            ::runtime_parameters::typed::parse_param::<#inner_ty>(#user_key, &__v)?
        }
    };

    let binding = if spec.optional {
        quote! {
            let #ident = match #raw {
                ::std::option::Option::Some(__v) => ::std::option::Option::Some(#parse_value),
                ::std::option::Option::None => ::std::option::Option::None,
            };
        }
    } else {
        quote! {
            let #ident = match #raw {
                ::std::option::Option::Some(__v) => #parse_value,
                ::std::option::Option::None => {
                    return ::std::result::Result::Err(
                        ::runtime_parameters::typed::ParamsError::MissingRequired {
                            user_key: #user_key.to_string(),
                            hint: #hint.to_string(),
                        },
                    );
                }
            };
        }
    };

    Ok(quote! {
        #deprecation
        #binding
    })
}

fn parse_prefix(input: &DeriveInput) -> syn::Result<String> {
    let mut prefix = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("params") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                prefix = Some(lit.value());
                Ok(())
            } else {
                Err(meta.error("unsupported #[params(...)] key; expected `prefix`"))
            }
        })?;
    }
    prefix.ok_or_else(|| {
        syn::Error::new(
            input.ident.span(),
            "TypedParams requires #[params(prefix = \"...\")]",
        )
    })
}

fn parse_field(field: &syn::Field) -> syn::Result<FieldSpec> {
    let ident = field
        .ident
        .clone()
        .ok_or_else(|| syn::Error::new(field.span(), "expected a named field"))?;

    let (optional, inner_ty) = strip_option(&field.ty);

    let mut spec = FieldSpec {
        ident,
        inner_ty,
        optional,
        secret: false,
        runtime: false,
        rename: None,
        aliases: Vec::new(),
        default: None,
        parse_with: None,
        doc: String::new(),
        deprecated: None,
    };

    let mut doc_lines = Vec::new();
    for attr in &field.attrs {
        if attr.path().is_ident("doc") {
            if let syn::Meta::NameValue(nv) = &attr.meta
                && let Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
                }) = &nv.value
            {
                doc_lines.push(s.value().trim().to_string());
            }
        } else if attr.path().is_ident("deprecated") {
            spec.deprecated = Some(parse_deprecated_note(attr)?);
        } else if attr.path().is_ident("param") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("runtime") {
                    spec.runtime = true;
                } else if meta.path.is_ident("secret") {
                    spec.secret = true;
                } else if meta.path.is_ident("rename") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    spec.rename = Some(lit.value());
                } else if meta.path.is_ident("alias") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    spec.aliases.push(lit.value());
                } else if meta.path.is_ident("default") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    spec.default = Some(lit.value());
                } else if meta.path.is_ident("parse_with") {
                    spec.parse_with = Some(meta.value()?.parse()?);
                } else {
                    return Err(meta.error(
                        "unsupported #[param(...)] key; expected one of \
                         `runtime`, `secret`, `rename`, `alias`, `default`, `parse_with`",
                    ));
                }
                Ok(())
            })?;
        }
    }
    spec.doc = doc_lines.join(" ").trim().to_string();

    if spec.optional && spec.default.is_some() {
        return Err(syn::Error::new(
            field.span(),
            "#[param(default)] on an `Option` field is contradictory: the default \
             would always apply, so the field could never be `None`. \
             Make the field non-optional instead.",
        ));
    }

    Ok(spec)
}

fn parse_deprecated_note(attr: &syn::Attribute) -> syn::Result<String> {
    match &attr.meta {
        syn::Meta::Path(_) => Ok(String::new()),
        syn::Meta::NameValue(nv) => {
            if let Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) = &nv.value
            {
                Ok(s.value())
            } else {
                Err(syn::Error::new(
                    nv.value.span(),
                    "expected a string literal note",
                ))
            }
        }
        syn::Meta::List(_) => {
            let mut note = String::new();
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("note") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    note = lit.value();
                } else if meta.path.is_ident("since") {
                    let _: syn::LitStr = meta.value()?.parse()?;
                } else {
                    return Err(meta.error("unsupported #[deprecated(...)] key"));
                }
                Ok(())
            })?;
            Ok(note)
        }
    }
}

/// Returns `(true, T)` for `Option<T>`, `(false, ty)` otherwise. Detection is
/// syntactic on the last path segment; type aliases for `Option` are not seen.
fn strip_option(ty: &Type) -> (bool, Type) {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
        && segment.ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
        && args.args.len() == 1
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return (true, inner.clone());
    }
    (false, ty.clone())
}

/// Syntactic check on the last path segment; type aliases are not seen.
fn type_is_secret_string(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "SecretString";
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn expand_str(input: DeriveInput) -> Result<String, String> {
        expand(&input)
            .map(|t| t.to_string())
            .map_err(|e| e.to_string())
    }

    #[test]
    fn missing_prefix_is_an_error() {
        let input: DeriveInput = parse_quote! {
            struct P { a: String }
        };
        let err = expand_str(input).expect_err("should require #[params(prefix)]");
        assert!(err.contains("prefix"), "unexpected error: {err}");
    }

    #[test]
    fn secret_on_non_secret_string_is_an_error() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x")]
            struct P {
                #[param(secret)]
                a: String,
            }
        };
        let err = expand_str(input).expect_err("secret requires SecretString");
        assert!(err.contains("SecretString"), "unexpected error: {err}");
    }

    #[test]
    fn default_on_option_is_an_error() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x")]
            struct P {
                #[param(default = "1")]
                a: Option<u32>,
            }
        };
        let err = expand_str(input).expect_err("default on Option is contradictory");
        assert!(err.contains("contradictory"), "unexpected error: {err}");
    }

    #[test]
    fn generic_struct_is_an_error() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x")]
            struct P<T> { a: T }
        };
        let err = expand_str(input).expect_err("generics unsupported");
        assert!(err.contains("generic"), "unexpected error: {err}");
    }

    #[test]
    fn parse_with_on_secret_string_is_an_error() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x")]
            struct P {
                #[param(parse_with = my_parser)]
                a: SecretString,
            }
        };
        let err = expand_str(input).expect_err("parse_with + SecretString invalid");
        assert!(err.contains("parse_with"), "unexpected error: {err}");
    }

    #[test]
    fn expands_component_and_runtime_keys() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "openai")]
            struct P {
                /// The OpenAI API key.
                #[param(secret)]
                api_key: Option<SecretString>,
                #[param(runtime, default = "https://api.openai.com/v1")]
                endpoint: String,
                /// The OpenAI organization ID.
                org_id: String,
            }
        };
        let out = expand_str(input).expect("valid struct should expand");
        assert!(out.contains("\"openai_api_key\""), "expansion: {out}");
        assert!(out.contains("\"endpoint\""), "expansion: {out}");
        assert!(out.contains("\"openai_org_id\""), "expansion: {out}");
        assert!(out.contains("autoload_secret"), "expansion: {out}");
        // Doc comments surface as the hint on required-field errors.
        assert!(
            out.contains("The OpenAI organization ID."),
            "doc hint missing: {out}"
        );
    }

    #[test]
    fn name_already_prefixed_is_not_double_prefixed() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "file")]
            struct P {
                file_format: Option<String>,
            }
        };
        let out = expand_str(input).expect("valid struct should expand");
        assert!(out.contains("\"file_format\""), "expansion: {out}");
        assert!(!out.contains("file_file_format"), "expansion: {out}");
    }
}
