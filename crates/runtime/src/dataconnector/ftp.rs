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

use super::{AnyErrorResult, DataConnector, DataConnectorFactory};
use arrow::array::timezone::TzOffset;
use arrow_ipc::Date;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::ParquetReadOptions;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutOptions, PutResult,
};
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::io::Read;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, future::Future};
use suppaftp::FtpStream;
use tokio::io::AsyncWrite;
use tonic::IntoRequest;
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse URL {url}: {source}"))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display("{source}"))]
    UnableToGetReadProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToGetReadWriteProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToBuildObjectStore {
        source: object_store::Error,
    },

    ObjectStoreNotImplemented,

    #[snafu(display("{source}"))]
    UnableToBuildLogicalPlan {
        source: DataFusionError,
    },
}

pub struct FTP {
    secret: Option<Secret>,
    params: HashMap<String, String>,
}

impl DataConnectorFactory for FTP {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let ftp = Self {
                secret,
                params: params.as_ref().clone().map_or_else(HashMap::new, |x| x),
            };
            Ok(Arc::new(ftp) as Arc<dyn DataConnector>)
        })
    }
}

#[derive(Debug)]
pub struct FTPObjectStore {
    client: Arc<Mutex<FtpStream>>,
}

impl std::fmt::Display for FTPObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FTS")
    }
}

impl FTPObjectStore {
    pub fn new(client: FtpStream) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }
}

#[async_trait]
impl ObjectStore for FTPObjectStore {
    async fn put_opts(&self, _: &Path, _: Bytes, _: PutOptions) -> object_store::Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart(
        &self,
        _: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        unimplemented!()
    }

    async fn abort_multipart(&self, _: &Path, _: &MultipartId) -> object_store::Result<()> {
        unimplemented!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut client = self.client.lock().unwrap();
        let client = &mut *client;
        let location_string: Arc<str> = location.to_string().into();
        let object_meta = ObjectMeta {
            location: location.clone(),
            size: client.size(location_string.clone()).unwrap(),
            last_modified: client.mdtm(location_string.clone()).unwrap().and_utc(),
            e_tag: None,
            version: None,
        };
        let stream = client.retr_as_stream(location_string.clone()).unwrap();

        unimplemented!()
    }

    async fn delete(&self, _: &Path) -> object_store::Result<()> {
        unimplemented!()
    }

    fn delete_stream<'a>(
        &'a self,
        _: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        unimplemented!()
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let mut client = self.client.lock().unwrap();
        let client = &mut *client;
        client.cwd("taxi_trips").unwrap();
        let list = client.nlst(None).unwrap();

        let list = list
            .iter()
            .map(|x| {
                let path = Path::from(x.clone());

                Ok(ObjectMeta {
                    location: path,
                    size: client.size(x.clone()).unwrap(),
                    last_modified: client.mdtm(x.clone()).unwrap().and_utc(),
                    e_tag: None,
                    version: None,
                })
            })
            .collect::<Vec<_>>();
        futures::stream::iter(list.into_iter()).boxed()
    }

    fn list_with_offset(
        &self,
        _: Option<&Path>,
        _: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        unimplemented!()
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
        unimplemented!()
    }

    async fn copy(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        unimplemented!()
    }

    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        unimplemented!()
    }
}

impl FTP {
    fn get_object_store(
        &self,
        dataset: &Dataset,
    ) -> Option<AnyErrorResult<(Url, Arc<dyn ObjectStore + 'static>)>> {
        let result: AnyErrorResult<(Url, Arc<dyn ObjectStore + 'static>)> = (|| {
            let url = dataset.from.clone();
            let mut ftp_stream = FtpStream::connect("eu-central-1.sftpcloud.io:21").unwrap();
            let _ = ftp_stream.login("username", "password");
            let ftp_object_store = FTPObjectStore::new(ftp_stream);

            let ftp_url = Url::parse(&url).context(UnableToParseURLSnafu { url: url.clone() })?;
            Ok((ftp_url, Arc::new(ftp_object_store) as Arc<dyn ObjectStore>))
        })();

        Some(result)
    }
}

#[async_trait]
impl DataConnector for FTP {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let ctx = SessionContext::new();

        let (url, ftp) = self.get_object_store(dataset).unwrap().unwrap();

        let _ = ctx.runtime_env().register_object_store(&url, ftp);

        let df = ctx
            .read_parquet(&dataset.from, ParquetReadOptions::default())
            .await
            .unwrap();

        Ok(df.into_view())
    }
}
