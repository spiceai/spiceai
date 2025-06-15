/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::borrow::Cow;
use std::io::{self, Cursor};
use std::path::PathBuf;

use app::{App, AppBuilder};
use async_trait::async_trait;
use spicepod::reader::ReadableYaml;
use spicepod::{
    Spicepod,
    reader::{self, ReadablePath},
};

struct SpicepodString {
    spicepod_str: String,
}

impl SpicepodString {
    pub fn new(spicepod_str: Cow<'_, str>) -> Self {
        Self {
            spicepod_str: spicepod_str.into_owned(),
        }
    }
}

#[async_trait]
impl ReadablePath for SpicepodString {
    async fn open(&self, _path: PathBuf) -> reader::Result<Box<dyn io::Read + Send + Sync>> {
        Ok(Box::new(Cursor::new(self.spicepod_str.clone())))
    }
}

impl ReadableYaml for SpicepodString {}

#[allow(clippy::missing_panics_doc)]
#[allow(clippy::expect_used)]
pub async fn load_app_from_spicepod_str(spicepod_str: &str) -> anyhow::Result<App> {
    let spicepod_str = SpicepodString::new(Cow::Borrowed(spicepod_str));
    let spicepod = Spicepod::load_from(&spicepod_str, PathBuf::from("."))
        .await
        .expect("should be able to load spicepod");
    let app = AppBuilder::build_from_spicepod(spicepod, PathBuf::from("."))
        .await
        .expect("should be able to build app");
    Ok(app)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_load_app_from_spicepod_str() {
        let spicepod_str = "version: v1
kind: Spicepod
name: iceberg-ai-demo

datasets:
  - from: postgres:user_roles
    name: user_roles
    params:
      pg_host: aws-0-ap-northeast-2.pooler.supabase.com
      pg_port: 5432
      pg_user: ${ secrets:PG_USER }
      pg_pass: ${ secrets:PG_PASS }
      pg_db: ${ secrets:PG_DB }
      pg_sslmode: require
        ";
        let app = load_app_from_spicepod_str(spicepod_str)
            .await
            .expect("should be able to load app");
        assert_eq!(app.name, "iceberg-ai-demo");
        assert_eq!(app.datasets.len(), 1);
        assert_eq!(app.datasets[0].name, "user_roles");
        let params = app.datasets[0]
            .clone()
            .params
            .expect("params should be Some")
            .as_string_map();
        assert_eq!(params.len(), 6);
        assert_eq!(
            params["pg_host"],
            "aws-0-ap-northeast-2.pooler.supabase.com"
        );
        assert_eq!(params["pg_port"], "5432");
        assert_eq!(params["pg_user"], "${ secrets:PG_USER }");
        assert_eq!(params["pg_pass"], "${ secrets:PG_PASS }");
    }
}
