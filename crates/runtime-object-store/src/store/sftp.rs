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

use std::{
    io::{Read, Seek, SeekFrom},
    net::TcpStream,
    ops::Range,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use ssh2::Session;

use super::common::generic_error;

const STORE_NAME: &str = "SFTP";

#[derive(Debug)]
struct SFTPClient {
    user: String,
    password: String,
    host: String,
    port: String,
    timeout: Option<Duration>,
}

impl SFTPClient {
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

    fn get_client(&self) -> object_store::Result<Session> {
        let stream = match self.timeout {
            Some(timeout) => TcpStream::connect_timeout(
                &format!("{}:{}", self.host, self.port).parse().map_err(
                    |e: std::net::AddrParseError| object_store::Error::Generic {
                        store: "SFTP",
                        source: e.into(),
                    },
                )?,
                timeout,
            )
            .map_err(handle_error)?,
            None => {
                TcpStream::connect(format!("{}:{}", self.host, self.port)).map_err(handle_error)?
            }
        };
        let mut session = Session::new().map_err(handle_error)?;
        session.set_tcp_stream(stream);
        session.handshake().map_err(handle_error)?;
        session
            .userauth_password(&self.user, &self.password)
            .map_err(handle_error)?;

        Ok(session)
    }
}

#[derive(Debug)]
pub struct SFTPObjectStore {
    client: Arc<SFTPClient>,
}

impl std::fmt::Display for SFTPObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SFTP")
    }
}

impl SFTPObjectStore {
    #[must_use]
    pub fn new(
        user: String,
        password: String,
        host: String,
        port: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            client: Arc::new(SFTPClient::new(user, password, host, port, timeout)),
        }
    }
}

fn handle_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    error: T,
) -> object_store::Error {
    generic_error(STORE_NAME, error)
}

#[async_trait]
impl ObjectStore for SFTPObjectStore {
    async fn put_opts(
        &self,
        _: &Path,
        _: PutPayload,
        _: PutOptions,
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
        let client = Arc::clone(&self.client);
        let location = location.clone();

        // Perform all blocking operations in spawn_blocking, including reading the data
        let (object_meta, start, end, data) = tokio::task::spawn_blocking(move || {
            let client = client.get_client()?;
            let mut file = client
                .sftp()
                .map_err(handle_error)?
                .open(std::path::Path::new(location.as_ref()))
                .map_err(handle_error)?;

            let file_stat = file.stat().map_err(handle_error)?;
            let size = file_stat.size.ok_or_else(|| object_store::Error::Generic {
                store: STORE_NAME,
                source: "No size found for file".into(),
            })?;

            #[expect(clippy::cast_possible_wrap)]
            let last_modified = DateTime::from_timestamp(
                file_stat
                    .mtime
                    .ok_or_else(|| object_store::Error::Generic {
                        store: STORE_NAME,
                        source: "No modification time found for file".into(),
                    })? as i64,
                0,
            )
            .ok_or_else(|| object_store::Error::Generic {
                store: STORE_NAME,
                source: "Failed to construct DateTime".into(),
            })?;

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

            // Seek to start position
            file.seek(SeekFrom::Start(start)).map_err(handle_error)?;

            // Read all requested data
            #[expect(clippy::cast_possible_truncation)]
            let mut buffer = vec![0u8; data_to_read as usize];
            let mut total_read = 0;
            while total_read < buffer.len() {
                let n = file.read(&mut buffer[total_read..]).map_err(handle_error)?;
                if n == 0 {
                    break;
                }
                total_read += n;
            }
            buffer.truncate(total_read);

            Ok::<_, object_store::Error>((object_meta, start, end, buffer))
        })
        .await
        .map_err(|e| generic_error(STORE_NAME, e))??;

        let stream = futures::stream::once(async move { Ok(Bytes::from(data)) });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            meta: object_meta,
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
        let location = location
            .map(ToOwned::to_owned)
            .map_or("/".to_string(), |x| x.to_string());

        let client = Arc::clone(&self.client);

        let fut = async move {
            let result =
                tokio::task::spawn_blocking(move || {
                    let session = client.get_client()?;
                    let mut queue = vec![location];
                    let mut all_files = Vec::new();

                    while let Some(item) = queue.pop() {
                        let list = session
                            .sftp()
                            .map_err(handle_error)?
                            .readdir(std::path::Path::new(&item))
                            .map_err(handle_error)?;

                        for entry in list {
                            if entry.1.is_dir() {
                                queue.push(entry.0.to_string_lossy().to_string());
                            } else {
                                // Convert to ObjectMeta inside spawn_blocking
                                let path_str = entry.0.to_str().ok_or_else(|| {
                                    object_store::Error::Generic {
                                        store: STORE_NAME,
                                        source: "Failed to convert path".into(),
                                    }
                                })?;
                                let size =
                                    entry.1.size.ok_or_else(|| object_store::Error::Generic {
                                        store: STORE_NAME,
                                        source: "No size found for file".into(),
                                    })?;
                                #[expect(clippy::cast_possible_wrap)]
                                let last_modified = DateTime::from_timestamp(
                                    entry.1.mtime.ok_or_else(|| object_store::Error::Generic {
                                        store: STORE_NAME,
                                        source: "No modification time found for file".into(),
                                    })? as i64,
                                    0,
                                )
                                .ok_or_else(|| object_store::Error::Generic {
                                    store: STORE_NAME,
                                    source: "Failed to construct DateTime".into(),
                                })?;

                                all_files.push(ObjectMeta {
                                    location: Path::from(path_str),
                                    size,
                                    last_modified,
                                    e_tag: None,
                                    version: None,
                                });
                            }
                        }
                    }

                    Ok::<_, object_store::Error>(all_files)
                })
                .await;

            match result {
                Ok(Ok(files)) => futures::stream::iter(files.into_iter().map(Ok)).boxed(),
                Ok(Err(e)) => futures::stream::once(async move { Err(e) }).boxed(),
                Err(e) => {
                    futures::stream::once(async move { Err(generic_error(STORE_NAME, e)) }).boxed()
                }
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
