/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::{
    arrow::struct_builder::StructBuilder,
    cdc::{changes_schema, ChangeBatch},
    debezium::{
        arrow::downcast_builder,
        change_event::{ChangeEvent, Op},
    },
};
use arrow::{
    array::{ArrayBuilder, ListBuilder, RecordBatch, StringBuilder},
    datatypes::SchemaRef,
};
use snafu::prelude::*;

/// Converts a `ChangeEvent` into a `ChangeBatch`
pub fn to_change_batch(
    table_schema: &SchemaRef,
    primary_key: &[String],
    change: &ChangeEvent,
) -> super::Result<ChangeBatch> {
    let schema = changes_schema(table_schema);

    let mut struct_builder = StructBuilder::from_fields(schema.fields().clone(), 1);

    struct_builder.append(true);

    for (idx, field) in schema.fields().iter().enumerate() {
        let field_builder = struct_builder.field_builder_array(idx);
        match field.name().as_str() {
            "op" => {
                let str_builder = downcast_builder::<StringBuilder>(field_builder)?;
                str_builder.append_value(change.payload.op.to_string());
            }
            "primary_keys" => {
                let list_builder =
                    downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(field_builder)?;
                if primary_key.is_empty() {
                    list_builder.append(false);
                } else {
                    let str_builder = downcast_builder::<StringBuilder>(list_builder.values())?;
                    for key in primary_key {
                        str_builder.append_value(key);
                    }
                    list_builder.append(true);
                }
            }
            "data" => {
                let change_data = match change.payload.op {
                    Op::Delete => change
                        .payload
                        .before
                        .clone()
                        .context(super::DeleteOpWithoutBeforeFieldSnafu)?,
                    _ => change.payload.after.clone(),
                };

                let data_struct_builder = downcast_builder::<StructBuilder>(field_builder)?;

                super::append_value_to_struct_builder(change_data, data_struct_builder)?;
            }
            _ => unreachable!("Unexpected field in changes schema {}", field.name()),
        }
    }

    let struct_array = struct_builder.finish();
    let record_batch: RecordBatch = struct_array.into();

    let Ok(change_batch) = ChangeBatch::try_new(record_batch) else {
        unreachable!(
            "We constructed the record batch with the correct schema, so this shouldn't fail"
        );
    };

    Ok(change_batch)
}
