/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::fmt::{self, Display, Formatter};

use arrow::{
    array::{Int64Builder, RecordBatch, StringBuilder, StructBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};

pub enum Op {
    Create,
    Update,
    Delete,
    Read,
    Truncate,
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Op::Create => write!(f, "c"),
            Op::Update => write!(f, "u"),
            Op::Delete => write!(f, "d"),
            Op::Read => write!(f, "r"),
            Op::Truncate => write!(f, "t"),
        }
    }
}

pub struct ChangeEvent {
    pub table_schema: SchemaRef,
    pub op: Op,
    pub ts_ms: i64,
    pub source: Source,
    pub data: serde_json::Value,
}

pub struct Source {
    pub version: Option<String>,
    pub connector: Option<String>,
    pub name: Option<String>,
    pub ts_ms: i64,
    pub snapshot: Option<String>,
    pub db: String,
    pub schema: String,
    pub table: String,
    pub tx_id: i64,
    pub lsn: i64,
}

impl ChangeEvent {
    #[must_use]
    pub fn new(
        table_schema: SchemaRef,
        op: Op,
        ts_ms: i64,
        source: Source,
        data: serde_json::Value,
    ) -> Self {
        Self {
            table_schema,
            op,
            ts_ms,
            source,
            data,
        }
    }

    #[allow(clippy::missing_panics_doc)]
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn to_record_batch(changes: &[Self]) -> RecordBatch {
        assert!(!changes.is_empty());
        let table_schema = &changes[0].table_schema;
        let schema = changes_schema(&changes[0].table_schema);

        let mut struct_builder = StructBuilder::from_fields(schema.fields().clone(), changes.len());

        for change in changes {
            struct_builder.append(true);

            for (idx, field) in schema.fields().iter().enumerate() {
                match field.name().as_str() {
                    "op" => {
                        let Some(str_builder) = struct_builder.field_builder::<StringBuilder>(idx)
                        else {
                            todo!("Handle missing field builder");
                        };
                        str_builder.append_value(change.op.to_string());
                    }
                    "ts_ms" => {
                        let Some(int_builder) = struct_builder.field_builder::<Int64Builder>(idx)
                        else {
                            todo!("Handle missing field builder");
                        };
                        int_builder.append_value(change.ts_ms);
                    }
                    "source" => {
                        let Some(source_struct_builder) =
                            struct_builder.field_builder::<StructBuilder>(idx)
                        else {
                            todo!("Handle missing field builder");
                        };
                        source_struct_builder.append(true);

                        for source_field in table_schema.fields() {
                            match source_field.name().as_str() {
                                "version" => {
                                    let Some(str_builder) =
                                        source_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    if let Some(version) = &change.source.version {
                                        str_builder.append_value(version);
                                    } else {
                                        str_builder.append_null();
                                    }
                                }
                                "connector" => {
                                    let Some(str_builder) =
                                        source_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    if let Some(connector) = &change.source.connector {
                                        str_builder.append_value(connector);
                                    } else {
                                        str_builder.append_null();
                                    }
                                }
                                "name" => {
                                    let Some(str_builder) =
                                        source_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    if let Some(name) = &change.source.name {
                                        str_builder.append_value(name);
                                    } else {
                                        str_builder.append_null();
                                    }
                                }
                                "ts_ms" => {
                                    let Some(int_builder) =
                                        source_struct_builder.field_builder::<Int64Builder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    int_builder.append_value(change.source.ts_ms);
                                }
                                "snapshot" => {
                                    let Some(str_builder) =
                                        source_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    if let Some(snapshot) = &change.source.snapshot {
                                        str_builder.append_value(snapshot);
                                    } else {
                                        str_builder.append_null();
                                    }
                                }
                                "db" => {
                                    let Some(str_builder) =
                                        source_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    str_builder.append_value(change.source.db.clone());
                                }
                                "schema" => {
                                    let Some(str_builder) =
                                        source_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    str_builder.append_value(change.source.schema.clone());
                                }
                                "table" => {
                                    let Some(str_builder) =
                                        source_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    str_builder.append_value(change.source.table.clone());
                                }
                                "tx_id" => {
                                    let Some(int_builder) =
                                        source_struct_builder.field_builder::<Int64Builder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    int_builder.append_value(change.source.tx_id);
                                }
                                "lsn" => {
                                    let Some(int_builder) =
                                        source_struct_builder.field_builder::<Int64Builder>(idx)
                                    else {
                                        todo!("Handle missing field builder in source");
                                    };
                                    int_builder.append_value(change.source.lsn);
                                }
                                _ => {
                                    unimplemented!("Field not supported yet");
                                }
                            }
                        }
                    }
                    "data" => {
                        let Some(data_struct_builder) =
                            struct_builder.field_builder::<StructBuilder>(idx)
                        else {
                            todo!("Handle missing field builder");
                        };
                        data_struct_builder.append(true);
                        for field in table_schema.fields() {
                            let Some(value) = change.data.get(field.name()) else {
                                todo!("Handle missing field in struct");
                            };
                            match field.data_type() {
                                DataType::Utf8 => {
                                    let Some(value) = value.as_str() else {
                                        todo!("Handle non-string value in struct");
                                    };
                                    let Some(str_builder) =
                                        data_struct_builder.field_builder::<StringBuilder>(idx)
                                    else {
                                        todo!("Handle missing field builder in struct");
                                    };
                                    str_builder.append_value(value);
                                }
                                DataType::Int64 => {
                                    let Some(value) = value.as_i64() else {
                                        todo!("Handle non-i64 value in struct");
                                    };
                                    let Some(int_builder) =
                                        data_struct_builder.field_builder::<Int64Builder>(idx)
                                    else {
                                        todo!("Handle missing field builder in struct");
                                    };
                                    int_builder.append_value(value);
                                }
                                _ => {
                                    unimplemented!("Data type not supported yet");
                                }
                            }
                        }
                    }
                    _ => {
                        unimplemented!("Data type not supported yet");
                    }
                }
            }
        }

        let struct_array = struct_builder.finish();
        struct_array.into()
    }
}

fn changes_schema(table_schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("op", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
        Field::new(
            "source",
            DataType::Struct(
                vec![
                    Field::new("version", DataType::Utf8, true),
                    Field::new("connector", DataType::Utf8, true),
                    Field::new("name", DataType::Utf8, true),
                    Field::new("ts_ms", DataType::Int64, false),
                    Field::new("snapshot", DataType::Utf8, true),
                    Field::new("db", DataType::Utf8, false),
                    Field::new("schema", DataType::Utf8, false),
                    Field::new("table", DataType::Utf8, false),
                    Field::new("tx_id", DataType::Int64, false),
                    Field::new("lsn", DataType::Int64, false),
                ]
                .into(),
            ),
            false,
        ),
        Field::new(
            "data",
            DataType::Struct(table_schema.fields().clone()),
            true,
        ),
    ])
}
