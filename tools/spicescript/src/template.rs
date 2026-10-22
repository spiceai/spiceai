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

//! Placeholder substitution and spicepod patches, both on the parsed YAML tree
//! rather than on text, so a substituted mapping or a patch can never produce
//! YAML that does not parse.

use anyhow::{Result, anyhow, bail};
use indexmap::IndexMap;
use serde_json::{Map, Value};

use crate::script::VariantSpec;

/// Replaces `${a.b.c}` placeholders, looked up as paths in `context`.
/// Placeholders containing `:` — Spice's own `${secrets:…}` and `${env:…}` —
/// are left for `spiced` to resolve.
///
/// A string that is exactly one placeholder takes the referenced value whole —
/// so `params: ${sources.src.params}` becomes a mapping. A placeholder inside a
/// longer string is rendered as text.
pub fn substitute(value: &Value, context: &Value) -> Result<Value> {
    Ok(match value {
        Value::String(s) => substitute_str(s, context)?,
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|v| substitute(v, context))
                .collect::<Result<_>>()?,
        ),
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                let v = substitute(v, context)?;
                // `$merge: ${…}` spreads a mapping placeholder into this mapping,
                // like a YAML merge key (which YAML resolves before placeholders); the
                // mapping's own keys win.
                if k == "$merge" {
                    let Value::Object(spread) = v else {
                        bail!("`$merge` must resolve to a mapping, got a {}", kind(&v));
                    };
                    for (sk, sv) in spread {
                        out.entry(sk).or_insert(sv);
                    }
                } else {
                    out.insert(substitute_text(k, context)?, v);
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    })
}

fn substitute_str(s: &str, context: &Value) -> Result<Value> {
    if let Some(path) = s.strip_prefix("${").and_then(|r| r.strip_suffix('}'))
        && !path.contains("${")
        && !path.contains('}')
        && !path.contains(':')
    {
        return lookup(context, path).cloned();
    }
    substitute_text(s, context).map(Value::String)
}

/// Renders every placeholder in `s` as text.
pub fn substitute_text(s: &str, context: &Value) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| anyhow!("unclosed placeholder in '{s}'"))?;
        if after[..end].contains(':') {
            out.push_str(&rest[start..=start + 2 + end]);
            rest = &after[end + 1..];
            continue;
        }
        match lookup(context, &after[..end])? {
            Value::String(v) => out.push_str(v),
            v @ (Value::Number(_) | Value::Bool(_)) => out.push_str(&v.to_string()),
            other => bail!(
                "placeholder '${{{}}}' is a {} and cannot be embedded in text '{s}'",
                &after[..end],
                kind(other)
            ),
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn lookup<'a>(context: &'a Value, path: &str) -> Result<&'a Value> {
    path.split('.').try_fold(context, |v, key| {
        v.get(key)
            .ok_or_else(|| anyhow!("unknown placeholder '${{{path}}}' (no '{key}')"))
    })
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "mapping",
    }
}

/// RFC 7386 JSON merge patch: mappings merge recursively, `null` deletes a
/// key, and anything else (lists included) replaces the target.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(target) = target else {
        unreachable!("target was just made a mapping");
    };
    for (key, value) in patch {
        if value.is_null() {
            target.remove(key);
        } else {
            merge_patch(target.entry(key.clone()).or_insert(Value::Null), value);
        }
    }
}

/// Applies a variant to a spicepod: the whole-spicepod patch first, then the
/// `*` dataset patch, then each named dataset's patch.
pub fn apply_variant(spicepod: &mut Value, variant: &VariantSpec) -> Result<()> {
    if let Some(patch) = &variant.spicepod {
        merge_patch(spicepod, patch);
    }
    patch_by_name(spicepod, "datasets", "dataset", &variant.datasets)?;
    patch_by_name(spicepod, "catalogs", "catalog", &variant.catalogs)
}

/// Applies merge patches to the entries of the spicepod list `key`, by each
/// entry's `name`; `*` patches every entry.
fn patch_by_name(
    spicepod: &mut Value,
    key: &str,
    kind: &str,
    patches: &IndexMap<String, Value>,
) -> Result<()> {
    if patches.is_empty() {
        return Ok(());
    }
    let entries = spicepod
        .get_mut(key)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow!("a variant patches {key}, but the spicepod has none"))?;
    for name in patches.keys().filter(|k| *k != "*") {
        if !entries
            .iter()
            .any(|d| d.get("name").and_then(Value::as_str) == Some(name))
        {
            bail!("a variant patches {kind} '{name}', which the spicepod does not define");
        }
    }
    for entry in entries {
        if let Some(patch) = patches.get("*") {
            merge_patch(entry, patch);
        }
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(patch) = name.and_then(|n| patches.get(&n)) {
            merge_patch(entry, patch);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn whole_placeholder_takes_the_value_and_embedded_renders_text() {
        let ctx = json!({ "sources": { "src": { "params": { "pg_port": "5432" } } }, "matrix": { "n": 3 } });
        let out = substitute(
            &json!({ "params": "${sources.src.params}", "from": "postgres:t_${matrix.n}" }),
            &ctx,
        )
        .expect("substitutes");
        assert_eq!(
            out,
            json!({ "params": { "pg_port": "5432" }, "from": "postgres:t_3" })
        );
        let err = substitute(&json!("x ${sources.src.params}"), &ctx).expect_err("mapping in text");
        assert!(err.to_string().contains("is a mapping"), "{err}");
        assert_eq!(
            substitute(
                &json!({ "a": "${secrets:PG_PASS}", "b": "x${env:HOME}/${matrix.n}" }),
                &ctx
            )
            .expect("leaves spice placeholders"),
            json!({ "a": "${secrets:PG_PASS}", "b": "x${env:HOME}/3" })
        );
        assert_eq!(
            substitute(
                &json!({ "$merge": "${sources.src.params}", "pg_port": "1", "x": "y" }),
                &ctx
            )
            .expect("spreads"),
            json!({ "pg_port": "1", "x": "y" })
        );
        let err = substitute(&json!("${nope}"), &ctx).expect_err("unknown");
        assert!(
            err.to_string().contains("unknown placeholder '${nope}'"),
            "{err}"
        );
    }

    #[test]
    fn variant_patches_every_dataset_then_the_named_one() {
        let mut pod = json!({ "datasets": [
            { "name": "a", "acceleration": { "engine": "cayenne", "params": { "x": "1" } } },
            { "name": "b", "acceleration": { "engine": "cayenne" } },
        ]});
        let variant: VariantSpec = serde_json::from_value(json!({ "datasets": {
            "*": { "acceleration": { "params": { "cayenne_metastore": "turso" } } },
            "a": { "acceleration": { "params": { "x": null } } },
        }}))
        .expect("variant");
        apply_variant(&mut pod, &variant).expect("applies");
        assert_eq!(
            pod,
            json!({ "datasets": [
                { "name": "a", "acceleration": { "engine": "cayenne", "params": { "cayenne_metastore": "turso" } } },
                { "name": "b", "acceleration": { "engine": "cayenne", "params": { "cayenne_metastore": "turso" } } },
            ]})
        );
        let bad: VariantSpec =
            serde_json::from_value(json!({ "datasets": { "c": {} } })).expect("variant");
        let err = apply_variant(&mut pod, &bad).expect_err("unknown dataset");
        assert!(err.to_string().contains("dataset 'c'"), "{err}");
    }
}
