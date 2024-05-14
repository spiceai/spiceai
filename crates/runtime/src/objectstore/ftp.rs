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

use std::ops::Range;

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::AsyncReadExt;
use object_store::GetRange;
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta,
    ObjectStore, PutOptions, PutResult,
};
use suppaftp::AsyncFtpStream;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct FTPObjectStore {
    user: String,
    password: String,
    host: String,
    port: String,
}

impl std::fmt::Display for FTPObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FTS")
    }
}

impl FTPObjectStore {
    #[must_use]
    pub fn new(user: String, password: String, host: String, port: String) -> Self {
        Self {
            user,
            password,
            host,
            port,
        }
    }

    async fn get_async_client(&self) -> AsyncFtpStream {
        let mut client = AsyncFtpStream::connect(format!("{}:{}", self.host, self.port))
            .await
            .unwrap();
        let _ = client.login(&self.user, &self.password).await;

        client
    }

    fn walk_path(&self, location: Option<Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let stream = stream! {
            let mut client = self.get_async_client().await;
            let path = location.map(|v| v.to_string());
            let mut queue = vec![path];
            while let Some(path) = queue.pop() {
                let list = client.nlst(path.as_deref())
                    .await
                    .map_err(|e| object_store::Error::NotFound { path: path.unwrap_or("/".to_string()), source: e.into() })?;
                for item in list {
                    let children = client.nlst(Some(&item)).await.unwrap_or(vec![]);
                    if children.len() > 0 && children[0] != item {
                        queue.push(Some(item));
                    } else {
                        let meta = ObjectMeta {
                            location: Path::from(item.clone()),
                            size: client.size(&item).await.map_err(|e| object_store::Error::NotFound { path: item.clone(), source: e.into() })?,
                            last_modified: client.mdtm(&item).await.map_err(|e| object_store::Error::NotFound { path: item.clone(), source: e.into() })?.and_utc(),
                            e_tag: None,
                            version: None,
                        };
                        yield Ok(meta);
                    }
                }
            }
        };

        Box::pin(stream)
    }
}

fn pipe_stream(
    mut client: AsyncFtpStream,
    location: String,
    start: usize,
    read_size: usize,
) -> BoxStream<'static, std::result::Result<Bytes, object_store::Error>> {
    let stream = stream! {
        let mut total = 0;
        let mut buf = vec![0; 4096];

        client.resume_transfer(start).await.unwrap();
        let mut stream = client
            .retr_as_stream(location.clone())
            .await
            .unwrap();
        loop {
            if total > read_size {
                break;
            }
            let mut n = stream.read(&mut buf).await.map_err(|_| object_store::Error::NotImplemented
                {})?;
            total += n;
            if n == 0 {
                break;
            }
            if total > read_size {
                n -= total - read_size;
            }
            yield Ok(Bytes::copy_from_slice(&buf[..n]));
        }
    };
    Box::pin(stream)
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
        let mut client = self.get_async_client().await;

        let location_string = location.to_string();
        let object_meta = ObjectMeta {
            location: location.clone(),
            size: client.size(&location_string).await.unwrap(),
            last_modified: client.mdtm(&location_string).await.unwrap().and_utc(),
            e_tag: None,
            version: None,
        };

        let mut start = 0;
        let mut end = object_meta.size;
        let mut data_to_read = object_meta.size;

        if let Some(GetRange::Bounded(range)) = options.range {
            data_to_read = range.end - range.start;
            start = range.start;
            end = range.end;
        }

        Ok(GetResult {
            meta: object_meta.clone(),
            payload: GetResultPayload::Stream(pipe_stream(
                client,
                location_string,
                start,
                data_to_read,
            )),
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
        self.walk_path(location.map(|v| v.to_owned()))
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
