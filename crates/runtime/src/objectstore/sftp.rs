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

use std::{
    io::{Read, Seek, SeekFrom},
    net::TcpStream,
    ops::Range,
};

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartId,
    ObjectMeta, ObjectStore, PutOptions, PutResult,
};
use ssh2::Session;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct SFTPObjectStore {
    user: String,
    password: String,
    host: String,
    port: String,
}

impl std::fmt::Display for SFTPObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SFTP")
    }
}

impl SFTPObjectStore {
    #[must_use]
    pub fn new(user: String, password: String, host: String, port: String) -> Self {
        Self {
            user,
            password,
            host,
            port,
        }
    }

    fn get_client(&self) -> object_store::Result<Session> {
        let stream = TcpStream::connect(format!("{}:{}", self.host, self.port)).map_err(|e| {
            object_store::Error::Generic {
                store: "SFTP",
                source: e.into(),
            }
        })?;
        let mut session = Session::new().map_err(|e| object_store::Error::Generic {
            store: "SFTP",
            source: e.into(),
        })?;
        session.set_tcp_stream(stream);
        session
            .handshake()
            .map_err(|e| object_store::Error::Generic {
                store: "SFTP",
                source: e.into(),
            })?;
        session
            .userauth_password(&self.user, &self.password)
            .map_err(|e| object_store::Error::Generic {
                store: "SFTP",
                source: e.into(),
            })?;

        Ok(session)
    }
}

#[async_trait]
impl ObjectStore for SFTPObjectStore {
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
        let client = self.get_client()?;
        let mut file = client
            .sftp()
            .map_err(|e| object_store::Error::Generic {
                store: "SFTP",
                source: e.into(),
            })?
            .open(std::path::Path::new(location.as_ref()))
            .map_err(|e| object_store::Error::Generic {
                store: "SFTP",
                source: e.into(),
            })?;

        let object_meta = ObjectMeta {
            location: location.clone(),
            size: file
                .stat()
                .map_err(|e| object_store::Error::Generic {
                    store: "SFTP",
                    source: e.into(),
                })?
                .size
                .ok_or_else(|| object_store::Error::Generic {
                    store: "SFTP",
                    source: "No size found for file".into(),
                })? as usize,

            last_modified: DateTime::from_timestamp(
                file.stat()
                    .map_err(|e| object_store::Error::Generic {
                        store: "SFTP",
                        source: e.into(),
                    })?
                    .mtime
                    .ok_or_else(|| object_store::Error::Generic {
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
            file.seek(SeekFrom::Start(start as u64)).map_err(|e| {
                object_store::Error::Generic {
                    store: "SFTP",
                    source: e.into(),
                }
            })?;

            let mut total = 0;
            let mut buf = vec![0; 4096];
            loop {
                if total > data_to_read {
                    break;
                }
                let mut n = file
                    .read(&mut buf)
                    .map_err(|e| object_store::Error::Generic {
                        store: "SFTP",
                        source: e.into(),
                    })?;

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
            .map(|x| x.to_string())
            .unwrap_or("/".to_string());
        let stream = stream! {
            let client = self.get_client()?;

            let list = client
                .sftp()
                .map_err(|e| object_store::Error::Generic {
                    store: "SFTP",
                    source: e.into(),
                })?
                .readdir(std::path::Path::new(&location))
                .map_err(|e| object_store::Error::Generic {
                    store: "SFTP",
                    source: e.into(),
                })?;

            for entry in list {
                yield Ok(ObjectMeta {
                    location: Path::from(entry.0.to_str().ok_or_else(|| object_store::Error::Generic {
                        store: "SFTP",
                        source: "Failed to convert path".into(),
                    })?),
                    size: entry.1.size.ok_or_else(|| object_store::Error::Generic {
                        store: "SFTP",
                        source: "No size found for file".into(),
                    })? as usize,
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

        };

        Box::pin(Box::pin(stream))
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
