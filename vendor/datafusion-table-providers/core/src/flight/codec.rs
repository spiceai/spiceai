// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Codec to enable running in a distributed environment

use std::sync::Arc;

use crate::flight::exec::{FlightConfig, FlightExec};
use crate::flight::to_df_err;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

/// Physical extension codec for FlightExec
#[derive(Clone, Debug, Default)]
pub struct FlightPhysicalCodec {}

impl PhysicalExtensionCodec for FlightPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if inputs.is_empty() {
            let config: FlightConfig = serde_json::from_slice(buf).map_err(to_df_err)?;
            Ok(Arc::from(FlightExec::from(config)))
        } else {
            Err(DataFusionError::Internal(
                "FlightExec is not supposed to have any inputs".into(),
            ))
        }
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        if let Some(flight) = node.as_any().downcast_ref::<FlightExec>() {
            let mut bytes = serde_json::to_vec(flight.config()).map_err(to_df_err)?;
            buf.append(&mut bytes);
            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "This codec only supports the FlightExec physical plan".into(),
            ))
        }
    }
}
