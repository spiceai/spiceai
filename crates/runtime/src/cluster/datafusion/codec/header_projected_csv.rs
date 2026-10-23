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

//! Cluster serde for header-projected CSV/TSV scans.
//!
//! `HeaderProjectedCsvSource` has no `FileSource::try_to_proto` hook, so
//! `DataSourceExec` falls through to [`SpicePhysicalCodec`]. A stock `CsvScan`
//! would decode as positional `CsvSource` and always as `UNCOMPRESSED`. This
//! node keeps the header-name binding and the scan's `file_compression_type`.

use data_components::csv_header_projection::HeaderProjectedCsvSource;
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{DataFusionError, Result, exec_err, internal_datafusion_err};
use datafusion::execution::{FunctionRegistry, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::proto::{
    ExecutionPlanDecode, ExecutionPlanDecodeCtx, ExecutionPlanEncode, ExecutionPlanEncodeCtx,
};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{FileScanExecConf, PhysicalExprNode, PhysicalPlanNode};
use prost::Message;
use runtime_proto::{
    HeaderProjectedCsvScanExecNode, SpicePhysicalPlanNode, spice_physical_plan_node,
};
use std::str::FromStr;
use std::sync::Arc;

pub(super) fn encode_header_projected_csv(
    exec: &DataSourceExec,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Option<SpicePhysicalPlanNode>> {
    let Some((conf, source)) = exec.downcast_to_file_source::<HeaderProjectedCsvSource>() else {
        return Ok(None);
    };

    let encoder = SpicePlanEncoder {
        codec,
        proto_converter,
    };
    let encode_ctx = ExecutionPlanEncodeCtx::new(&encoder);
    let mut base_buf = Vec::new();
    conf.try_to_proto(&encode_ctx)?
        .encode(&mut base_buf)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let options = source.csv_options();
    Ok(Some(SpicePhysicalPlanNode {
        node: Some(spice_physical_plan_node::Node::HeaderProjectedCsvScan(
            HeaderProjectedCsvScanExecNode {
                base_conf: base_buf,
                has_header: source.has_header(),
                delimiter: proto_byte_to_string(options.delimiter, "delimiter")?,
                quote: proto_byte_to_string(options.quote, "quote")?,
                escape: options
                    .escape
                    .map(|escape| proto_byte_to_string(escape, "escape"))
                    .transpose()?,
                comment: options
                    .comment
                    .map(|comment| proto_byte_to_string(comment, "comment"))
                    .transpose()?,
                newlines_in_values: source.newlines_in_values(),
                truncate_rows: source.truncate_rows(),
                file_compression_type: proto_compression(conf.file_compression_type),
            },
        )),
    }))
}

pub(super) fn decode_header_projected_csv(
    node: &HeaderProjectedCsvScanExecNode,
    task_ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Arc<dyn ExecutionPlan>> {
    let base_conf = FileScanExecConf::decode(node.base_conf.as_slice()).map_err(|e| {
        DataFusionError::Internal(format!(
            "header-projected CSV scan is missing a valid FileScanExecConf: {e}"
        ))
    })?;

    let decode_context = PhysicalPlanDecodeContext::new(task_ctx, codec);
    let decoder = SpicePlanDecoder {
        ctx: &decode_context,
        proto_converter,
    };
    let decode_ctx = ExecutionPlanDecodeCtx::new(&decoder);

    let table_schema = FileScanConfig::parse_table_schema_from_proto(&base_conf)?;
    let file_compression_type = parse_compression(&node.file_compression_type)?;
    let csv_options = CsvOptions {
        has_header: Some(node.has_header),
        delimiter: proto_str_to_byte(&node.delimiter, "delimiter")?,
        quote: proto_str_to_byte(&node.quote, "quote")?,
        escape: node
            .escape
            .as_deref()
            .map(|escape| proto_str_to_byte(escape, "escape"))
            .transpose()?,
        comment: node
            .comment
            .as_deref()
            .map(|comment| proto_str_to_byte(comment, "comment"))
            .transpose()?,
        newlines_in_values: Some(node.newlines_in_values),
        truncated_rows: Some(node.truncate_rows),
        compression: file_compression_type.into(),
        ..Default::default()
    };
    let source = Arc::new(HeaderProjectedCsvSource::new(table_schema, csv_options));
    let conf = FileScanConfigBuilder::from(FileScanConfig::try_from_proto(
        &base_conf,
        &decode_ctx,
        source,
    )?)
    .with_file_compression_type(file_compression_type)
    .build();
    Ok(DataSourceExec::from_data_source(conf))
}

struct SpicePlanEncoder<'a> {
    codec: &'a dyn PhysicalExtensionCodec,
    proto_converter: &'a dyn PhysicalProtoConverterExtension,
}

impl ExecutionPlanEncode for SpicePlanEncoder<'_> {
    fn encode_plan(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<PhysicalPlanNode> {
        self.proto_converter
            .execution_plan_to_proto(plan, self.codec)
    }

    fn encode_expr(&self, expr: &Arc<dyn PhysicalExpr>) -> Result<PhysicalExprNode> {
        self.proto_converter
            .physical_expr_to_proto(expr, self.codec)
    }

    fn encode_udf(&self, udf: &ScalarUDF) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![];
        self.codec.try_encode_udf(udf, &mut buf)?;
        Ok((!buf.is_empty()).then_some(buf))
    }

    fn encode_udaf(&self, udaf: &AggregateUDF) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![];
        self.codec.try_encode_udaf(udaf, &mut buf)?;
        Ok((!buf.is_empty()).then_some(buf))
    }

    fn encode_udwf(&self, udwf: &WindowUDF) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![];
        self.codec.try_encode_udwf(udwf, &mut buf)?;
        Ok((!buf.is_empty()).then_some(buf))
    }
}

struct SpicePlanDecoder<'a, 'ctx> {
    ctx: &'a PhysicalPlanDecodeContext<'ctx>,
    proto_converter: &'a dyn PhysicalProtoConverterExtension,
}

impl ExecutionPlanDecode for SpicePlanDecoder<'_, '_> {
    fn decode_plan(&self, node: &PhysicalPlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        self.proto_converter.proto_to_execution_plan(node, self.ctx)
    }

    fn decode_plan_with_scalar_subquery_results(
        &self,
        node: &PhysicalPlanNode,
        results: ScalarSubqueryResults,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scoped_ctx = self.ctx.with_scalar_subquery_results(results);
        self.proto_converter
            .proto_to_execution_plan(node, &scoped_ctx)
    }

    fn decode_expr(
        &self,
        node: &PhysicalExprNode,
        input_schema: &arrow_schema::Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.proto_converter
            .proto_to_physical_expr(node, input_schema, self.ctx)
    }

    fn task_ctx(&self) -> &TaskContext {
        self.ctx.task_ctx()
    }

    fn decode_udf(&self, name: &str, payload: Option<&[u8]>) -> Result<Arc<ScalarUDF>> {
        match payload {
            Some(buf) => self.ctx.codec().try_decode_udf(name, buf),
            None => self
                .ctx
                .task_ctx()
                .udf(name)
                .or_else(|_| self.ctx.codec().try_decode_udf(name, &[])),
        }
    }

    fn decode_udaf(&self, name: &str, payload: Option<&[u8]>) -> Result<Arc<AggregateUDF>> {
        match payload {
            Some(buf) => self.ctx.codec().try_decode_udaf(name, buf),
            None => self
                .ctx
                .task_ctx()
                .udaf(name)
                .or_else(|_| self.ctx.codec().try_decode_udaf(name, &[])),
        }
    }

    fn decode_udwf(&self, name: &str, payload: Option<&[u8]>) -> Result<Arc<WindowUDF>> {
        match payload {
            Some(buf) => self.ctx.codec().try_decode_udwf(name, buf),
            None => self
                .ctx
                .task_ctx()
                .udwf(name)
                .or_else(|_| self.ctx.codec().try_decode_udwf(name, &[])),
        }
    }
}

fn proto_compression(compression: FileCompressionType) -> String {
    match *compression.get_variant() {
        CompressionTypeVariant::GZIP => "GZIP",
        CompressionTypeVariant::BZIP2 => "BZIP2",
        CompressionTypeVariant::XZ => "XZ",
        CompressionTypeVariant::ZSTD => "ZSTD",
        CompressionTypeVariant::UNCOMPRESSED => "UNCOMPRESSED",
    }
    .to_owned()
}

fn parse_compression(text: &str) -> Result<FileCompressionType> {
    if text.is_empty() {
        return Ok(FileCompressionType::UNCOMPRESSED);
    }
    FileCompressionType::from_str(text).map_err(|e| {
        DataFusionError::Internal(format!(
            "header-projected CSV scan has unknown file_compression_type '{text}': {e}"
        ))
    })
}

fn proto_byte_to_string(byte: u8, description: &str) -> Result<String> {
    let bytes = &[byte];
    let text = std::str::from_utf8(bytes).map_err(|_| {
        internal_datafusion_err!("Invalid CSV {description}: can not represent {bytes:0x?} as utf8")
    })?;
    Ok(text.to_owned())
}

fn proto_str_to_byte(text: &str, description: &str) -> Result<u8> {
    if text.len() != 1 {
        return exec_err!("Invalid CSV {description}: expected single character, got {text}");
    }
    Ok(text.as_bytes()[0])
}
