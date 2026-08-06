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
//! Generates an implementation of `runtime_parameters_typed::TypedParams` that
//! deserializes the secret-injected string map produced by
//! `runtime_secrets::get_params_with_secrets` into a plain Rust struct, preserving
//! the runtime's parameter semantics: per-variant key prefixing, unknown-key
//! warnings with typo suggestions, deprecation warnings, string defaults, and
//! secret autoload from configured secret stores.
//!
//! # Attributes
//!
//! Container `#[params(...)]` keys:
//! - `prefix = "openai"` (required) — the component prefix applied to every field
//!   key unless the field is marked `runtime`.
//! - `deny_unknown` — make an unrecognized spicepod key a hard error
//!   (`ParamsError::UnknownParameter`) instead of a logged warning. Use for
//!   config that must fail fast on typos (e.g. secret stores).
//! - `passthrough = <PATH>` — a `&'static [runtime_parameters_typed::PassthroughParam]`
//!   of keys the component accepts but does not bind to a field. They are consumed
//!   (so they never trip the unknown-key warning), folded into the typo-suggestion
//!   candidate set, and deprecation-warned when present. Used for large shared
//!   groups whose values are read elsewhere off the raw params map (e.g. the
//!   OpenAI-compatible chat-completion overrides every model provider accepts).
//! - `emit_specs` — also generate an inherent
//!   `pub fn parameter_specs() -> Vec<runtime_parameters::ParameterSpec>` describing
//!   the fields (and any `passthrough` table) for JSON-schema generation.
//!
//! Field `#[param(...)]` keys:
//! - `runtime` — the spicepod key is unprefixed (parity with `ParameterSpec::runtime`).
//! - `rename = "key"` — spicepod key differs from the field identifier.
//! - `alias = "key"` — additional accepted key; repeatable; prefixed like the field.
//! - `default = "value"` — string default, parsed through the same path as user input.
//! - `autoload_secret` — opt into secret autoload: when absent from the spicepod, the
//!   prefixed key is looked up in the configured secret stores. The field must be
//!   `SecretString` or `Option<SecretString>`.
//! - `parse_with = path` — custom parser `fn(&str) -> Result<T, impl Display>`
//!   used instead of `FromStr`.
//! - `one_of = ["a", "b"]` — allowed values, surfaced by `emit_specs` schema
//!   generation only; runtime validation remains the field type's `FromStr`.
//!
//! Field semantics derived from the type: `Option<T>` fields are optional; all
//! other fields are required (missing yields an error naming the user-facing,
//! prefixed key). Doc comments become the hint appended to missing-parameter
//! errors. `#[deprecated(note = "...")]` on a field emits a runtime warning when
//! the corresponding key is present.

use proc_macro::TokenStream;
// NOTE: generated code targets the `runtime-parameters-typed` foundation leaf
// (path `::runtime_parameters_typed`), not `runtime-parameters` — so crates
// below `runtime-parameters` (e.g. `runtime-secrets`) can derive without a
// dependency cycle. Every deriving crate must depend on `runtime-parameters-typed`.
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
    autoload_secret: bool,
    runtime: bool,
    rename: Option<String>,
    aliases: Vec<String>,
    default: Option<String>,
    parse_with: Option<syn::Path>,
    doc: String,
    /// Allowed values, carried only for schema generation (`#[param(one_of = [...])]`).
    /// Runtime validation is the field type's `FromStr`.
    one_of: Vec<String>,
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

    let ContainerAttrs {
        prefix,
        deny_unknown,
        passthrough,
        emit_specs,
    } = parse_container(input)?;

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
    let expect_deprecated = any_deprecated.then(|| quote! { #[expect(deprecated)] });

    // `params` is only mutated (via `.remove`, or by `consume_passthrough`) when there
    // are fields to consume or a passthrough table; a struct with neither (e.g. a store
    // with no params) would otherwise trip `unused_mut`.
    let params_binding = if specs.is_empty() && passthrough.is_none() {
        quote! { params }
    } else {
        quote! { mut params }
    };

    // Leftover-key handling combines two independent axes:
    // - known-key set: the compile-time field/alias literals, extended at runtime with
    //   the passthrough table's prefixed keys when `#[params(passthrough = ...)]` is set
    //   (which also consumes them from `params` so they never look leftover).
    // - failure mode: warn (default) or fail fast (`#[params(deny_unknown)]`).
    let leftover = if let Some(path) = &passthrough {
        let check = if deny_unknown {
            quote! {
                ::runtime_parameters_typed::deny_leftover_keys(&params, &__known_refs)?;
            }
        } else {
            quote! {
                ::runtime_parameters_typed::warn_leftover_keys(
                    component_name,
                    &params,
                    &__known_refs,
                    #prefix,
                );
            }
        };
        quote! {
            let mut __known: ::std::vec::Vec<::std::string::String> =
                ::std::vec![ #( ::std::string::ToString::to_string(#known_keys) ),* ];
            __known.extend(::runtime_parameters_typed::consume_passthrough(
                component_name,
                &mut params,
                #prefix,
                #path,
            ));
            let __known_refs: ::std::vec::Vec<&str> =
                __known.iter().map(::std::string::String::as_str).collect();
            #check
        }
    } else if deny_unknown {
        quote! {
            ::runtime_parameters_typed::deny_leftover_keys(&params, &[#(#known_keys),*])?;
        }
    } else {
        quote! {
            ::runtime_parameters_typed::warn_leftover_keys(
                component_name,
                &params,
                &[#(#known_keys),*],
                #prefix,
            );
        }
    };

    let specs_impl = emit_specs.then(|| {
        let spec_pushes = specs.iter().map(field_spec_tokens);
        let passthrough_specs = passthrough.as_ref().map(|path| {
            quote! {
                for __p in #path {
                    let mut __s = if __p.prefixed {
                        ::runtime_parameters::ParameterSpec::component(__p.name)
                    } else {
                        ::runtime_parameters::ParameterSpec::runtime(__p.name)
                    };
                    if !__p.description.is_empty() {
                        __s = __s.description(__p.description);
                    }
                    if let ::std::option::Option::Some(__note) = __p.deprecated {
                        __s = __s.deprecated(__note);
                    }
                    specs.push(__s);
                }
            }
        });
        quote! {
            impl #struct_ident {
                /// Parameter specifications for this component, generated from the
                /// struct's fields (and any `passthrough` table). Used only for
                /// schema generation; runtime validation lives in `try_from_params`.
                #[must_use]
                pub fn parameter_specs() -> ::std::vec::Vec<::runtime_parameters::ParameterSpec> {
                    let mut specs: ::std::vec::Vec<::runtime_parameters::ParameterSpec> =
                        ::std::vec::Vec::new();
                    #(#spec_pushes)*
                    #passthrough_specs
                    specs
                }
            }
        }
    });

    Ok(quote! {
        #expect_deprecated
        impl ::runtime_parameters_typed::TypedParams for #struct_ident {
            const PREFIX: &'static str = #prefix;

            async fn try_from_params<__R: ::runtime_parameters_typed::SecretAutoload>(
                component_name: &str,
                #params_binding: ::runtime_parameters_typed::__private::HashMap<
                    ::std::string::String,
                    ::runtime_parameters_typed::__private::SecretString,
                >,
                secrets: &::runtime_parameters_typed::__private::Arc<
                    ::runtime_parameters_typed::__private::RwLock<__R>,
                >,
            ) -> ::std::result::Result<Self, ::runtime_parameters_typed::ParamsError> {
                #(#field_stmts)*
                #leftover
                ::std::result::Result::Ok(Self { #(#field_idents),* })
            }
        }

        #specs_impl
    })
}

/// Emits a `specs.push(ParameterSpec::...);` statement for one field
/// (schema generation only).
fn field_spec_tokens(spec: &FieldSpec) -> proc_macro2::TokenStream {
    let name = spec.name();
    let ctor = if spec.runtime {
        quote! { ::runtime_parameters::ParameterSpec::runtime(#name) }
    } else {
        quote! { ::runtime_parameters::ParameterSpec::component(#name) }
    };
    let mut builder = ctor;
    if !spec.optional && spec.default.is_none() {
        builder = quote! { #builder.required() };
    }
    if !spec.doc.is_empty() {
        let doc = &spec.doc;
        builder = quote! { #builder.description(#doc) };
    }
    if let Some(default) = &spec.default {
        builder = quote! { #builder.default(#default) };
    }
    if spec.autoload_secret {
        builder = quote! { #builder.secret() };
    }
    if !spec.one_of.is_empty() {
        let values = &spec.one_of;
        builder = quote! { #builder.one_of(&[#(#values),*]) };
    }
    if let Some(note) = &spec.deprecated
        && !note.is_empty()
    {
        builder = quote! { #builder.deprecated(#note) };
    }
    quote! { specs.push(#builder); }
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
                    ::runtime_parameters_typed::warn_deprecated(component_name, __key, #note_tokens);
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

    // Secret autoload: absent + `#[param(autoload_secret)]` → look up the prefixed key in
    // the secret stores (parity with `Parameters::try_new`).
    if spec.autoload_secret {
        let autoload_key = apply_prefix(&spec.name(), prefix, spec.runtime);
        raw = quote! {
            match #raw {
                ::std::option::Option::Some(__v) => ::std::option::Option::Some(__v),
                ::std::option::Option::None => ::runtime_parameters_typed::autoload_secret(
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
                ::runtime_parameters_typed::__private::SecretString::from(#default)
            ))
        };
    }

    let is_secret_string = type_is_secret_string(inner_ty);
    if spec.autoload_secret && !is_secret_string {
        return Err(syn::Error::new(
            ident.span(),
            "#[param(autoload_secret)] fields must be `SecretString` or `Option<SecretString>`",
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
            ::runtime_parameters_typed::parse_param_with(#user_key, &__v, #parse_with)?
        }
    } else {
        quote! {
            ::runtime_parameters_typed::parse_param::<#inner_ty>(#user_key, &__v)?
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
                        ::runtime_parameters_typed::ParamsError::MissingRequired {
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

/// Parsed `#[params(...)]` container attribute.
struct ContainerAttrs {
    prefix: String,
    /// `#[params(deny_unknown)]` — make an unrecognized spicepod key a hard error
    /// instead of a logged warning.
    deny_unknown: bool,
    /// `#[params(passthrough = <PATH>)]` — a `&'static [PassthroughParam]` of
    /// accepted-but-unbound keys (consumed, folded into typo suggestions, and
    /// deprecation-warned).
    passthrough: Option<syn::Path>,
    /// `#[params(emit_specs)]` — also generate an inherent
    /// `parameter_specs() -> Vec<ParameterSpec>` for schema generation.
    emit_specs: bool,
}

/// Parses the container attribute `#[params(prefix = "...", deny_unknown, passthrough =
/// <PATH>, emit_specs)]`. `prefix` is required; the rest are optional.
fn parse_container(input: &DeriveInput) -> syn::Result<ContainerAttrs> {
    let mut prefix = None;
    let mut deny_unknown = false;
    let mut passthrough = None;
    let mut emit_specs = false;
    for attr in &input.attrs {
        if !attr.path().is_ident("params") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                prefix = Some(lit.value());
                Ok(())
            } else if meta.path.is_ident("deny_unknown") {
                deny_unknown = true;
                Ok(())
            } else if meta.path.is_ident("passthrough") {
                passthrough = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("emit_specs") {
                emit_specs = true;
                Ok(())
            } else {
                Err(meta.error(
                    "unsupported #[params(...)] key; expected one of \
                     `prefix`, `deny_unknown`, `passthrough`, `emit_specs`",
                ))
            }
        })?;
    }
    let prefix = prefix.ok_or_else(|| {
        syn::Error::new(
            input.ident.span(),
            "TypedParams requires #[params(prefix = \"...\")]",
        )
    })?;
    Ok(ContainerAttrs {
        prefix,
        deny_unknown,
        passthrough,
        emit_specs,
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
        autoload_secret: false,
        runtime: false,
        rename: None,
        aliases: Vec::new(),
        default: None,
        parse_with: None,
        doc: String::new(),
        one_of: Vec::new(),
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
                } else if meta.path.is_ident("autoload_secret") {
                    spec.autoload_secret = true;
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
                } else if meta.path.is_ident("one_of") {
                    // `one_of = ["a", "b", ...]` — a bracketed list of string literals,
                    // carried through to schema generation only.
                    let value = meta.value()?;
                    let content;
                    syn::bracketed!(content in value);
                    let values = syn::punctuated::Punctuated::<syn::LitStr, syn::Token![,]>::parse_terminated(
                        &content,
                    )?;
                    spec.one_of = values.into_iter().map(|s| s.value()).collect();
                } else {
                    return Err(meta.error(
                        "unsupported #[param(...)] key; expected one of \
                         `runtime`, `autoload_secret`, `rename`, `alias`, `default`, `parse_with`, `one_of`",
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

    fn expand_str(input: &DeriveInput) -> Result<String, String> {
        expand(input)
            .map(|t| t.to_string())
            .map_err(|e| e.to_string())
    }

    #[test]
    fn missing_prefix_is_an_error() {
        let input: DeriveInput = parse_quote! {
            struct P { a: String }
        };
        let err = expand_str(&input).expect_err("should require #[params(prefix)]");
        assert!(err.contains("prefix"), "unexpected error: {err}");
    }

    #[test]
    fn secret_on_non_secret_string_is_an_error() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x")]
            struct P {
                #[param(autoload_secret)]
                a: String,
            }
        };
        let err = expand_str(&input).expect_err("secret requires SecretString");
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
        let err = expand_str(&input).expect_err("default on Option is contradictory");
        assert!(err.contains("contradictory"), "unexpected error: {err}");
    }

    #[test]
    fn generic_struct_is_an_error() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x")]
            struct P<T> { a: T }
        };
        let err = expand_str(&input).expect_err("generics unsupported");
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
        let err = expand_str(&input).expect_err("parse_with + SecretString invalid");
        assert!(err.contains("parse_with"), "unexpected error: {err}");
    }

    #[test]
    fn expands_component_and_runtime_keys() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "openai")]
            struct P {
                /// The OpenAI API key.
                #[param(autoload_secret)]
                api_key: Option<SecretString>,
                #[param(runtime, default = "https://api.openai.com/v1")]
                endpoint: String,
                /// The OpenAI organization ID.
                org_id: String,
            }
        };
        let out = expand_str(&input).expect("valid struct should expand");
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
    fn passthrough_generates_consume_call_and_runtime_known_keys() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "openai", passthrough = crate::common::OPENAI_COMMON)]
            struct P {
                #[param(autoload_secret)]
                api_key: Option<SecretString>,
            }
        };
        let out = expand_str(&input).expect("valid struct should expand");
        assert!(out.contains("consume_passthrough"), "expansion: {out}");
        assert!(
            out.contains("crate :: common :: OPENAI_COMMON"),
            "expansion: {out}"
        );
    }

    #[test]
    fn emit_specs_generates_parameter_specs_fn() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "openai", emit_specs)]
            struct P {
                /// The OpenAI API key.
                #[param(autoload_secret)]
                api_key: Option<SecretString>,
                #[param(runtime, default = "https://api.openai.com/v1")]
                endpoint: String,
                #[param(default = "tier1", one_of = ["free", "tier1"])]
                usage_tier: String,
            }
        };
        let out = expand_str(&input).expect("valid struct should expand");
        assert!(out.contains("fn parameter_specs"), "expansion: {out}");
        // secret from autoload_secret, description from doc, one_of carried through.
        assert!(out.contains(". secret ()"), "expansion: {out}");
        assert!(out.contains("The OpenAI API key."), "expansion: {out}");
        assert!(out.contains("one_of"), "expansion: {out}");
        // A required (non-Option, no default) field would be `.required()`; here all
        // fields are optional or defaulted, so no `.required()` is emitted.
        assert!(!out.contains(". required ()"), "expansion: {out}");
    }

    #[test]
    fn without_emit_specs_no_parameter_specs_fn() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "openai")]
            struct P { api_key: Option<SecretString> }
        };
        let out = expand_str(&input).expect("valid struct should expand");
        assert!(!out.contains("fn parameter_specs"), "expansion: {out}");
        // No passthrough → keep the compile-time literal known-key list.
        assert!(!out.contains("consume_passthrough"), "expansion: {out}");
    }

    #[test]
    fn deny_unknown_uses_fail_fast_leftover_handling() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x", deny_unknown)]
            struct P {
                a: Option<String>,
            }
        };
        let out = expand_str(&input).expect("valid struct should expand");
        assert!(out.contains("deny_leftover_keys"), "expansion: {out}");
        assert!(!out.contains("warn_leftover_keys"), "expansion: {out}");
    }

    #[test]
    fn default_warns_on_leftover_keys() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x")]
            struct P {
                a: Option<String>,
            }
        };
        let out = expand_str(&input).expect("valid struct should expand");
        assert!(out.contains("warn_leftover_keys"), "expansion: {out}");
        assert!(!out.contains("deny_leftover_keys"), "expansion: {out}");
    }

    #[test]
    fn unsupported_container_key_is_an_error() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "x", bogus)]
            struct P { a: Option<String> }
        };
        let err = expand_str(&input).expect_err("unknown container key should error");
        assert!(err.contains("deny_unknown"), "unexpected error: {err}");
    }

    #[test]
    fn passthrough_and_deny_unknown_combine() {
        // The two container attrs are orthogonal: a passthrough table's keys must still
        // be consumed (and folded into the dynamic known-key set) even when leftover
        // keys are a hard error rather than a warning.
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "openai", deny_unknown, passthrough = crate::common::OPENAI_COMMON)]
            struct P {
                #[param(autoload_secret)]
                api_key: Option<SecretString>,
            }
        };
        let out = expand_str(&input).expect("valid struct should expand");
        assert!(out.contains("consume_passthrough"), "expansion: {out}");
        assert!(
            out.contains("deny_leftover_keys"),
            "expansion should fail fast, not warn: {out}"
        );
        assert!(!out.contains("warn_leftover_keys"), "expansion: {out}");
    }

    #[test]
    fn name_already_prefixed_is_not_double_prefixed() {
        let input: DeriveInput = parse_quote! {
            #[params(prefix = "file")]
            struct P {
                file_format: Option<String>,
            }
        };
        let out = expand_str(&input).expect("valid struct should expand");
        assert!(out.contains("\"file_format\""), "expansion: {out}");
        assert!(!out.contains("file_file_format"), "expansion: {out}");
    }
}
