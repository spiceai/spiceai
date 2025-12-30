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

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncReadExt;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{Attributes, GetRange, MultipartUpload, PutMultipartOptions, PutPayload};
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta, ObjectStore, PutOptions,
    PutResult, path::Path,
};
use suppaftp::AsyncFtpStream;
use suppaftp::types::FileType;

use super::common::generic_error;

const STORE_NAME: &str = "FTP";

#[derive(Debug)]
struct FTPClient {
    user: String,
    password: String,
    host: String,
    port: String,
    timeout: Option<Duration>,
}

impl FTPClient {
    fn new(
        user: String,
        password: String,
        host: String,
        port: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            user,
            password,
            host,
            port,
            timeout,
        }
    }

    async fn get_async_client(&self) -> object_store::Result<AsyncFtpStream> {
        let mut client = match self.timeout {
            Some(timeout) => {
                AsyncFtpStream::connect_timeout(
                    format!("{}:{}", self.host, self.port).parse().map_err(
                        |e: std::net::AddrParseError| object_store::Error::Generic {
                            store: "FTP",
                            source: e.into(),
                        },
                    )?,
                    timeout,
                )
                .await
            }
            None => AsyncFtpStream::connect(format!("{}:{}", self.host, self.port)).await,
        }
        .map_err(|e| object_store::Error::Generic {
            store: "FTP",
            source: e.into(),
        })?;
        client
            .login(&self.user, &self.password)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "FTP",
                source: e.into(),
            })?;

        Ok(client)
    }
}

#[derive(Debug)]
pub struct FTPObjectStore {
    client: Arc<FTPClient>,
}

impl std::fmt::Display for FTPObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FTP")
    }
}

impl FTPObjectStore {
    #[must_use]
    pub fn new(
        user: String,
        password: String,
        host: String,
        port: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            client: Arc::new(FTPClient::new(user, password, host, port, timeout)),
        }
    }

    /// List all files recursively starting from a given path
    async fn list_all_files(
        &self,
        location: Option<Path>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let mut client = self.client.get_async_client().await?;
        let path = location.map(|v| v.to_string());
        let mut queue = vec![path];
        let mut results = Vec::new();

        while let Some(path) = queue.pop() {
            let list =
                client
                    .nlst(path.as_deref())
                    .await
                    .map_err(|e| object_store::Error::NotFound {
                        path: path.clone().unwrap_or_else(|| "/".to_string()),
                        source: e.into(),
                    })?;

            for item in list {
                let children =
                    client
                        .nlst(Some(&item))
                        .await
                        .map_err(|e| object_store::Error::NotFound {
                            path: item.clone(),
                            source: e.into(),
                        })?;

                if children.is_empty() {
                    continue;
                }

                if children[0] == item {
                    // It's a file - get metadata
                    let size =
                        client
                            .size(&item)
                            .await
                            .map_err(|e| object_store::Error::NotFound {
                                path: item.clone(),
                                source: e.into(),
                            })?;
                    let last_modified =
                        client
                            .mdtm(&item)
                            .await
                            .map_err(|e| object_store::Error::NotFound {
                                path: item.clone(),
                                source: e.into(),
                            })?;

                    results.push(ObjectMeta {
                        location: Path::from(item),
                        size: u64::try_from(size).unwrap_or(0),
                        last_modified: last_modified.and_utc(),
                        e_tag: None,
                        version: None,
                    });
                } else {
                    // It's a directory - add to queue
                    queue.push(Some(item));
                }
            }
        }

        Ok(results)
    }
}

/// Read data from an FTP stream asynchronously
async fn read_ftp_data(
    mut client: AsyncFtpStream,
    location: String,
    start: usize,
    read_size: usize,
) -> object_store::Result<Vec<u8>> {
    client
        .transfer_type(FileType::Binary)
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?;

    client
        .resume_transfer(start)
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?;

    let mut stream = client
        .retr_as_stream(location)
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?;

    let mut result = Vec::with_capacity(read_size);
    let mut buf = vec![0; 4096];
    let mut total = 0;

    loop {
        if total >= read_size {
            break;
        }

        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| generic_error(STORE_NAME, e))?;

        if n == 0 {
            break;
        }

        let bytes_to_take = (read_size - total).min(n);
        result.extend_from_slice(&buf[..bytes_to_take]);
        total += n;
    }

    Ok(result)
}

#[async_trait]
impl ObjectStore for FTPObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let client = self.client.get_async_client().await?;

        let location_string = location.to_string();

        // Create a new client for metadata queries
        let mut meta_client = self.client.get_async_client().await?;

        let size: u64 = u64::try_from(meta_client.size(&location_string).await.map_err(|e| {
            object_store::Error::NotFound {
                path: location_string.clone(),
                source: e.into(),
            }
        })?)
        .unwrap_or(0);

        let last_modified = meta_client
            .mdtm(&location_string)
            .await
            .map_err(|e| object_store::Error::NotFound {
                path: location_string.clone(),
                source: e.into(),
            })?
            .and_utc();

        let object_meta = ObjectMeta {
            location: location.clone(),
            size,
            last_modified,
            e_tag: None,
            version: None,
        };

        let mut start = 0u64;
        let mut end = size;
        let mut data_to_read = size;

        if let Some(GetRange::Bounded(range)) = options.range {
            data_to_read = range.end - range.start;
            start = range.start;
            end = range.end;
        }

        #[expect(clippy::cast_possible_truncation)]
        let data = read_ftp_data(
            client,
            location_string,
            start as usize,
            data_to_read as usize,
        )
        .await?;

        let stream = futures::stream::once(async move { Ok(Bytes::from(data)) });

        Ok(GetResult {
            meta: object_meta,
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: Range { start, end },
            attributes: Attributes::default(),
        })
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

    fn list(
        &self,
        location: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let ftp_client = Arc::clone(&self.client);
        let location = location.map(ToOwned::to_owned);

        let fut = async move {
            let store = FTPObjectStore { client: ftp_client };
            match store.list_all_files(location).await {
                Ok(files) => futures::stream::iter(files.into_iter().map(Ok)).boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        futures::stream::once(fut).flatten().boxed()
    }

    fn list_with_offset(
        &self,
        _: Option<&Path>,
        _: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
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
