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

//! Shared Cloud Connect contract tests.
//!
//! `proto/cloud_connect.proto` is the authoritative copy of the wire contract,
//! and `proto/cloud_connect_contract.json` pins the field numbers, types, and
//! presence semantics of the messages every implementation of the protocol
//! (this runtime, the Spice Cloud gateway/API, and the Kubernetes operator)
//! must agree on. The first half of this file checks the schema this crate
//! actually compiled against that fixture, so an incompatible edit to a shared
//! message fails here rather than as a wire break between deployed peers; the
//! mirror repositories run the same check against their own generated code.
//!
//! The second half proves the additive changes behave additively on the wire:
//! a peer built from the pre-change contract still decodes everything it knew,
//! and presence-carrying fields stay distinguishable from empty ones.

#![expect(
    clippy::expect_used,
    reason = "integration-test harness — readability over lint strictness"
)]

use std::collections::BTreeMap;

use prost::Message as _;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use runtime_cloud_connect::proto;

/// The compiled schema, emitted by the build script from the same
/// `compile_protos` invocation that generated the Rust types.
fn compiled_schema() -> FileDescriptorSet {
    let bytes: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/cloud_connect_descriptor.bin"));
    FileDescriptorSet::decode(bytes).expect("the emitted descriptor set decodes")
}

fn contract_fixture() -> serde_json::Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/proto/cloud_connect_contract.json"
    );
    let raw = std::fs::read_to_string(path).expect("read proto/cloud_connect_contract.json");
    serde_json::from_str(&raw).expect("the contract fixture parses as JSON")
}

/// One field, in the fixture's vocabulary:
/// `(number, type, presence, real_oneof_name)`.
type FieldShape = (i32, String, String, Option<String>);

/// Reduce a compiled enum to its wire-visible names and numbers.
fn enum_shape(enumeration: &EnumDescriptorProto) -> BTreeMap<String, i32> {
    enumeration
        .value
        .iter()
        .map(|value| (value.name().to_string(), value.number()))
        .collect()
}

/// Reduce a compiled message to the fixture's vocabulary, keyed by field name.
fn message_shape(message: &DescriptorProto, package: &str) -> BTreeMap<String, FieldShape> {
    message
        .field
        .iter()
        .map(|field| {
            (
                field.name().to_string(),
                (
                    field.number(),
                    field_type(field, message, package),
                    field_presence(field, message, package),
                    field_oneof(field, message),
                ),
            )
        })
        .collect()
}

/// The real oneof containing `field`, excluding proto3 optional's synthetic
/// oneof. Pinning this separately from presence ensures moving an envelope arm
/// to an ordinary singular message field cannot preserve the fixture shape.
fn field_oneof(field: &FieldDescriptorProto, message: &DescriptorProto) -> Option<String> {
    if field.proto3_optional() {
        return None;
    }
    let index = usize::try_from(field.oneof_index?).ok()?;
    message
        .oneof_decl
        .get(index)
        .map(|declaration| declaration.name().to_string())
}

/// The nested map-entry type backing `field`, when it is a protobuf map.
fn map_entry<'a>(
    field: &FieldDescriptorProto,
    message: &'a DescriptorProto,
    package: &str,
) -> Option<&'a DescriptorProto> {
    if field.label() != Label::Repeated || field.r#type() != Type::Message {
        return None;
    }
    let prefix = format!(".{package}.{}.", message.name());
    let nested_name = field.type_name().strip_prefix(&prefix)?;
    message
        .nested_type
        .iter()
        .find(|nested| nested.name() == nested_name)
        .filter(|nested| {
            nested
                .options
                .as_ref()
                .is_some_and(prost_types::MessageOptions::map_entry)
        })
}

fn field_type(field: &FieldDescriptorProto, message: &DescriptorProto, package: &str) -> String {
    if let Some(entry) = map_entry(field, message, package) {
        let key = entry
            .field
            .iter()
            .find(|f| f.number() == 1)
            .map_or_else(|| "?".to_string(), |f| scalar_name(f.r#type(), f));
        let value = entry
            .field
            .iter()
            .find(|f| f.number() == 2)
            .map_or_else(|| "?".to_string(), |f| scalar_name(f.r#type(), f));
        return format!("map<{key}, {value}>");
    }
    scalar_name(field.r#type(), field)
}

fn scalar_name(ty: Type, field: &FieldDescriptorProto) -> String {
    let qualified = || field.type_name().trim_start_matches('.').to_string();
    match ty {
        Type::Double => "double".to_string(),
        Type::Float => "float".to_string(),
        Type::Int64 => "int64".to_string(),
        Type::Uint64 => "uint64".to_string(),
        Type::Int32 => "int32".to_string(),
        Type::Fixed64 => "fixed64".to_string(),
        Type::Fixed32 => "fixed32".to_string(),
        Type::Bool => "bool".to_string(),
        Type::String => "string".to_string(),
        Type::Group => format!("group:{}", qualified()),
        Type::Message => format!("message:{}", qualified()),
        Type::Bytes => "bytes".to_string(),
        Type::Uint32 => "uint32".to_string(),
        Type::Enum => format!("enum:{}", qualified()),
        Type::Sfixed32 => "sfixed32".to_string(),
        Type::Sfixed64 => "sfixed64".to_string(),
        Type::Sint32 => "sint32".to_string(),
        Type::Sint64 => "sint64".to_string(),
    }
}

/// The fixture's presence vocabulary. A singular message field tracks presence
/// natively in proto3, so it is "explicit" with or without the `optional`
/// keyword — which is exactly the semantic the mirrors must preserve.
fn field_presence(
    field: &FieldDescriptorProto,
    message: &DescriptorProto,
    package: &str,
) -> String {
    if map_entry(field, message, package).is_some() {
        return "map".to_string();
    }
    if field.label() == Label::Repeated {
        return "repeated".to_string();
    }
    if field.proto3_optional() || field.r#type() == Type::Message || field.r#type() == Type::Group {
        return "explicit".to_string();
    }
    "implicit".to_string()
}

/// Every message named by the fixture must exist in the compiled schema with
/// exactly the fixture's fields — same numbers, same types, same presence, no
/// extras. An extra compiled field is a failure too: a new field on a shared
/// message must be added to the fixture in the same review, so the mirrors
/// hear about it.
#[test]
fn compiled_schema_matches_the_shared_contract_fixture() {
    let fixture = contract_fixture();
    let package = fixture["package"]
        .as_str()
        .expect("fixture names the package");
    let schema = compiled_schema();
    let file = schema
        .file
        .iter()
        .find(|file| file.package() == package)
        .unwrap_or_else(|| panic!("no compiled file for package {package}"));

    let messages = fixture["messages"]
        .as_object()
        .expect("fixture carries a messages map");
    assert!(
        !messages.is_empty(),
        "the fixture pins at least one message"
    );

    for (message_name, expected) in messages {
        let compiled = file
            .message_type
            .iter()
            .find(|message| message.name() == message_name.as_str())
            .unwrap_or_else(|| {
                panic!("shared message {message_name} is missing from the compiled schema")
            });
        let compiled_shape = message_shape(compiled, package);

        let expected_fields = expected["fields"]
            .as_array()
            .expect("fixture message carries a field list");
        let expected_shape: BTreeMap<String, FieldShape> = expected_fields
            .iter()
            .map(|field| {
                (
                    field["name"]
                        .as_str()
                        .expect("fixture field has a name")
                        .to_string(),
                    (
                        i32::try_from(
                            field["number"]
                                .as_i64()
                                .expect("fixture field has a number"),
                        )
                        .expect("fixture field number fits in i32"),
                        field["type"]
                            .as_str()
                            .expect("fixture field has a type")
                            .to_string(),
                        field["presence"]
                            .as_str()
                            .expect("fixture field has presence")
                            .to_string(),
                        field
                            .get("oneof")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string),
                    ),
                )
            })
            .collect();

        assert_eq!(
            compiled_shape, expected_shape,
            "{message_name} drifted from the shared contract fixture: change \
             proto/cloud_connect.proto and proto/cloud_connect_contract.json together, and never \
             reuse a field number"
        );
    }

    let enums = fixture["enums"]
        .as_object()
        .expect("fixture carries an enums map");
    assert!(!enums.is_empty(), "the fixture pins shared enum values");

    for (enum_name, expected) in enums {
        let compiled = file
            .enum_type
            .iter()
            .find(|enumeration| enumeration.name() == enum_name.as_str())
            .unwrap_or_else(|| {
                panic!("shared enum {enum_name} is missing from the compiled schema")
            });
        let expected_shape: BTreeMap<String, i32> = expected
            .as_array()
            .expect("fixture enum carries a value list")
            .iter()
            .map(|value| {
                (
                    value["name"]
                        .as_str()
                        .expect("fixture enum value has a name")
                        .to_string(),
                    i32::try_from(
                        value["number"]
                            .as_i64()
                            .expect("fixture enum value has a number"),
                    )
                    .expect("fixture enum value number fits in i32"),
                )
            })
            .collect();

        assert_eq!(
            enum_shape(compiled),
            expected_shape,
            "{enum_name} drifted from the shared contract fixture: never rename an enum value or reuse its number"
        );
    }
}

/// The package and negotiated protocol revision are the contract's outermost
/// invariants: the additive fields ride within `spice.cloud.v1` revision 1.
#[test]
fn package_and_protocol_version_are_unchanged() {
    let fixture = contract_fixture();
    assert_eq!(fixture["package"].as_str(), Some("spice.cloud.v1"));
    assert_eq!(fixture["protocol_version"].as_u64(), Some(1));
    assert_eq!(runtime_cloud_connect::PROTOCOL_VERSION, 1);
    let schema = compiled_schema();
    assert!(
        schema.file.iter().any(|f| f.package() == "spice.cloud.v1"),
        "the compiled schema must stay in spice.cloud.v1"
    );
}

fn full_heartbeat() -> proto::Heartbeat {
    proto::Heartbeat {
        identifier: "inst_contract".to_string(),
        sequence: 42,
        phase: proto::RuntimePhase::Ready as i32,
        warnings: vec!["a dataset is refreshing from acceleration".to_string()],
        active_datasets: 3,
        active_models: 1,
        active_spicepods: 1,
        runtime_versions: std::iter::once(("spiced".to_string(), "1.9.0".to_string())).collect(),
        standalone_runtime: Some(proto::StandaloneRuntimeStatus {
            restart_required: vec![
                "runtime.telemetry".to_string(),
                "runtime.memory_pool".to_string(),
            ],
        }),
    }
}

fn full_attach_app() -> proto::AttachApp {
    proto::AttachApp {
        app_id: Some("4002".to_string()),
        org_name: Some("acme".to_string()),
        app_name: Some("retail-analytics".to_string()),
        monitor_url: Some("https://spice.ai/acme/retail-analytics/monitor".to_string()),
    }
}

#[test]
fn heartbeat_round_trips_every_field() {
    let heartbeat = full_heartbeat();
    let decoded =
        proto::Heartbeat::decode(heartbeat.encode_to_vec().as_slice()).expect("decode heartbeat");
    assert_eq!(decoded, heartbeat);
    assert_eq!(
        decoded
            .standalone_runtime
            .expect("standalone detail survives the round trip")
            .restart_required,
        ["runtime.telemetry", "runtime.memory_pool"],
        "the restart list preserves order and content"
    );
}

#[test]
fn attach_app_round_trips_every_field() {
    let attach = full_attach_app();
    let decoded =
        proto::AttachApp::decode(attach.encode_to_vec().as_slice()).expect("decode attach_app");
    assert_eq!(decoded, attach);
}

/// "Absent" and "present but empty" are different wire states, and the control
/// plane reads them differently: no detail reported versus nothing requiring a
/// restart. Both directions of the codec must keep them apart.
#[test]
fn absent_standalone_detail_differs_from_a_present_empty_restart_list() {
    let absent = proto::Heartbeat {
        identifier: "inst_contract".to_string(),
        ..Default::default()
    };
    let present_empty = proto::Heartbeat {
        identifier: "inst_contract".to_string(),
        standalone_runtime: Some(proto::StandaloneRuntimeStatus {
            restart_required: Vec::new(),
        }),
        ..Default::default()
    };

    let absent_bytes = absent.encode_to_vec();
    let present_bytes = present_empty.encode_to_vec();
    assert_ne!(
        absent_bytes, present_bytes,
        "presence must be visible on the wire, not only in memory"
    );

    let absent_decoded =
        proto::Heartbeat::decode(absent_bytes.as_slice()).expect("decode absent detail");
    assert_eq!(absent_decoded.standalone_runtime, None);

    let present_decoded =
        proto::Heartbeat::decode(present_bytes.as_slice()).expect("decode empty detail");
    let detail = present_decoded
        .standalone_runtime
        .expect("an empty detail is still a present detail");
    assert!(detail.restart_required.is_empty());
}

/// The generated types a pre-change peer runs: `Heartbeat` before field 9 and
/// `AttachApp` before fields 2-4, frozen by hand from the prost output the
/// previous contract produced. (`phase` is held as a plain varint `i32`, which
/// is the same wire shape prost's `enumeration` attribute decodes.) These are
/// what "additive" is measured against, so they must never be updated to the
/// current schema.
mod frozen {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Heartbeat {
        #[prost(string, tag = "1")]
        pub identifier: String,
        #[prost(uint64, tag = "2")]
        pub sequence: u64,
        #[prost(int32, tag = "3")]
        pub phase: i32,
        #[prost(string, repeated, tag = "4")]
        pub warnings: Vec<String>,
        #[prost(uint32, tag = "5")]
        pub active_datasets: u32,
        #[prost(uint32, tag = "6")]
        pub active_models: u32,
        #[prost(uint32, tag = "7")]
        pub active_spicepods: u32,
        #[prost(map = "string, string", tag = "8")]
        pub runtime_versions: std::collections::HashMap<String, String>,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct AttachApp {
        #[prost(string, optional, tag = "1")]
        pub app_id: Option<String>,
    }
}

/// A pre-change peer must decode a heartbeat carrying the new field without
/// failure and retain every field it knows (1-8) — codec additivity for this
/// message. (Envelope arm stability is pinned by the contract fixture, and
/// how a peer *interprets* the fields is its own contract.)
#[test]
fn a_frozen_pre_change_decoder_retains_heartbeat_fields_1_to_8() {
    let heartbeat = full_heartbeat();
    let decoded = frozen::Heartbeat::decode(heartbeat.encode_to_vec().as_slice())
        .expect("a pre-change peer decodes a heartbeat carrying standalone detail");

    assert_eq!(decoded.identifier, heartbeat.identifier);
    assert_eq!(decoded.sequence, heartbeat.sequence);
    assert_eq!(decoded.phase, heartbeat.phase);
    assert_eq!(decoded.warnings, heartbeat.warnings);
    assert_eq!(decoded.active_datasets, heartbeat.active_datasets);
    assert_eq!(decoded.active_models, heartbeat.active_models);
    assert_eq!(decoded.active_spicepods, heartbeat.active_spicepods);
    assert_eq!(decoded.runtime_versions, heartbeat.runtime_versions);
}

#[test]
fn a_frozen_pre_change_decoder_retains_the_attach_app_id() {
    let attach = full_attach_app();
    let decoded = frozen::AttachApp::decode(attach.encode_to_vec().as_slice())
        .expect("a pre-change peer decodes an attachment carrying portal metadata");
    assert_eq!(decoded.app_id.as_deref(), Some("4002"));
}

/// The reverse direction: a message from a pre-change peer decodes with the
/// new types, with every new field absent — never defaulted to a present
/// value.
#[test]
fn a_pre_change_message_decodes_with_the_new_fields_absent() {
    let old_heartbeat = frozen::Heartbeat {
        identifier: "inst_contract".to_string(),
        sequence: 7,
        ..Default::default()
    };
    let heartbeat = proto::Heartbeat::decode(old_heartbeat.encode_to_vec().as_slice())
        .expect("decode a pre-change heartbeat");
    assert_eq!(heartbeat.identifier, "inst_contract");
    assert_eq!(heartbeat.sequence, 7);
    assert_eq!(heartbeat.standalone_runtime, None);

    let old_attach = frozen::AttachApp {
        app_id: Some("4002".to_string()),
    };
    let attach = proto::AttachApp::decode(old_attach.encode_to_vec().as_slice())
        .expect("decode a pre-change attachment");
    assert_eq!(attach.app_id.as_deref(), Some("4002"));
    assert_eq!(attach.org_name, None);
    assert_eq!(attach.app_name, None);
    assert_eq!(attach.monitor_url, None);
}

/// Field-number spot checks straight off the wire, independent of the
/// generated code and the descriptor: with exactly one field set, the first
/// byte of the encoding is that field's key, `(number << 3) | wire_type`.
#[test]
fn new_fields_encode_under_their_contract_numbers() {
    let detail_only = proto::Heartbeat {
        standalone_runtime: Some(proto::StandaloneRuntimeStatus::default()),
        ..Default::default()
    };
    assert_eq!(
        detail_only.encode_to_vec(),
        // Field 9, wire type 2 (length-delimited), empty submessage.
        [(9 << 3) | 2, 0x00],
        "Heartbeat.standalone_runtime must encode as field 9"
    );

    let restart = proto::StandaloneRuntimeStatus {
        restart_required: vec!["a".to_string()],
    };
    assert_eq!(
        restart.encode_to_vec(),
        // Field 1, wire type 2, one-byte string "a".
        [(1 << 3) | 2, 0x01, b'a'],
        "StandaloneRuntimeStatus.restart_required must encode as field 1"
    );

    for (number, attach) in [
        (
            1,
            proto::AttachApp {
                app_id: Some("a".to_string()),
                ..Default::default()
            },
        ),
        (
            2,
            proto::AttachApp {
                org_name: Some("a".to_string()),
                ..Default::default()
            },
        ),
        (
            3,
            proto::AttachApp {
                app_name: Some("a".to_string()),
                ..Default::default()
            },
        ),
        (
            4,
            proto::AttachApp {
                monitor_url: Some("a".to_string()),
                ..Default::default()
            },
        ),
    ] {
        assert_eq!(
            attach.encode_to_vec(),
            [(number << 3) | 2, 0x01, b'a'],
            "AttachApp field expected under number {number}"
        );
    }
}
