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

use crate::Runtime;
use mcp_core::{
    handler::{PromptError, ResourceError},
    Content, ToolError,
};
use mcp_server;
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(Clone)]
pub struct RuntimeServer(Arc<Runtime>);

impl From<Arc<Runtime>> for RuntimeServer {
    fn from(rt: Arc<Runtime>) -> Self {
        Self(rt)
    }
}

impl mcp_server::Router for RuntimeServer {
    fn name(&self) -> String {
        todo!()
    }

    fn instructions(&self) -> String {
        todo!()
    }

    fn capabilities(&self) -> mcp_core::protocol::ServerCapabilities {
        todo!()
    }

    fn list_tools(&self) -> Vec<mcp_core::tool::Tool> {
        todo!()
    }

    fn call_tool(
        &self,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Content>, ToolError>> + Send + 'static>> {
        todo!()
    }

    fn list_resources(&self) -> Vec<mcp_core::resource::Resource> {
        todo!()
    }

    fn read_resource(
        &self,
        uri: &str,
    ) -> Pin<Box<dyn Future<Output = Result<String, ResourceError>> + Send + 'static>> {
        todo!()
    }

    fn list_prompts(&self) -> Vec<mcp_core::prompt::Prompt> {
        todo!()
    }

    fn get_prompt(
        &self,
        prompt_name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<String, PromptError>> + Send + 'static>> {
        todo!()
    }
}

pub(crate) mod codec {
    use tokio_util::codec::Decoder;

    /// Directly from `<https://github.com/modelcontextprotocol/rust-sdk/blob/main/examples/servers/src/common/jsonrpc_frame_codec.rs>`
    #[derive(Default)]
    pub struct JsonRpcFrameCodec;
    impl Decoder for JsonRpcFrameCodec {
        type Item = tokio_util::bytes::Bytes;
        type Error = tokio::io::Error;
        fn decode(
            &mut self,
            src: &mut tokio_util::bytes::BytesMut,
        ) -> Result<Option<Self::Item>, Self::Error> {
            if let Some(end) = src
                .iter()
                .enumerate()
                .find_map(|(idx, &b)| (b == b'\n').then_some(idx))
            {
                let line = src.split_to(end);
                let _char_next_line = src.split_to(1);
                Ok(Some(line.freeze()))
            } else {
                Ok(None)
            }
        }
    }
}
