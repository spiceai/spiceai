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
    time::Duration,
};

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures::stream::BoxStream;
use object_store::{
    path::Path, Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use ssh2::Session;

#[derive(Debug)]
pub struct SFTPObjectStore {
    user: String,
    password: String,
    host: String,
    port: String,
    timeout: Option<Duration>,
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

fn handle_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    error: T,
) -> object_store::Error {
    object_store::Error::Generic {
        store: "SFTP",
        source: error.into(),
    }
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
        _opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let client = self.get_client()?;
        let mut file = client
            .sftp()
            .map_err(handle_error)?
            .open(std::path::Path::new(location.as_ref()))
            .map_err(handle_error)?;

        let object_meta = ObjectMeta {
            location: location.clone(),
            size: usize::try_from(file.stat().map_err(handle_error)?.size.ok_or_else(|| {
                object_store::Error::Generic {
                    store: "SFTP",
                    source: "No size found for file".into(),
                }
            })?)
            .map_err(handle_error)?,

            #[allow(clippy::cast_possible_wrap)]
            last_modified: DateTime::from_timestamp(
                file.stat()
                    .map_err(handle_error)?
                    .mtime
                    .ok_or(object_store::Error::Generic {
                        store: "SFTP",
                        source: "No modification time found for file".into(),
                    })? as i64,
                0,
            )
            .ok_or_else(|| object_store::Error::Generic {
                store: "SFTP",
                source: "Failed to construct DataTime".into(),
            })?,
            e_tag: None,
            version: None,
        };

        let mut start = 0;
        let mut end = object_meta.size;
        let mut data_to_read = end;

        if let Some(GetRange::Bounded(range)) = options.range {
            data_to_read = range.end - range.start;
            start = range.start;
            end = range.end;
        }

        let stream = stream! {
            file.seek(SeekFrom::Start(start as u64)).map_err(handle_error)?;

            let mut total = 0;
            let mut buf = vec![0; 4096];
            loop {
                if total > data_to_read {
                    break;
                }
                let mut n = file
                    .read(&mut buf)
                    .map_err(handle_error)?;

                total += n;
                if n == 0 {
                    break;
                }
                if total > data_to_read {
                    n -= total - data_to_read;
                }

                yield Ok(Bytes::copy_from_slice(&buf[..n]))
            }
        };

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

    fn list(&self, location: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let location = location
            .map(ToOwned::to_owned)
            .map_or("/".to_string(), |x| x.to_string());

        let stream = stream! {
            let client = self.get_client()?;

            let mut queue = vec![location];

            while let Some(item) = queue.pop() {
                let list = client
                    .sftp()
                    .map_err(handle_error)?
                    .readdir(std::path::Path::new(&item))
                    .map_err(handle_error)?;

                for entry in list {
                    if entry.1.is_dir() {
                        queue.push(entry.0.to_string_lossy().to_string());
                        continue;
                    }
                    yield Ok(ObjectMeta {
                        location: Path::from(entry.0.to_str().ok_or_else(|| object_store::Error::Generic {
                            store: "SFTP",
                            source: "Failed to convert path".into(),
                        })?),
                        size: usize::try_from(entry.1.size.ok_or_else(|| object_store::Error::Generic {
                            store: "SFTP",
                            source: "No size found for file".into(),
                        })?).map_err(handle_error)?,
                        #[allow(clippy::cast_possible_wrap)]
                        last_modified: DateTime::from_timestamp(entry.1.mtime.ok_or_else(|| object_store::Error::Generic {
                                store: "SFTP",
                                source: "No modification time found for file".into(),
                            })? as i64, 0)
                            .ok_or_else(|| object_store::Error::Generic {
                                store: "SFTP",
                                source: "Failed to construct DataTime".into(),
                            })?,
                        e_tag: None,
                        version: None,
                    })
                }
            }
        };

        Box::pin(stream)
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
