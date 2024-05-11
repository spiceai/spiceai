use std::{io::Read, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta,
    ObjectStore, PutOptions, PutResult,
};
use suppaftp::{AsyncFtpStream, FtpStream};
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
    pub fn new(user: String, password: String, host: String, port: String) -> Self {
        Self {
            user,
            password,
            host,
            port,
        }
    }

    fn get_client(&self) -> FtpStream {
        let mut client = FtpStream::connect(format!("{}:{}", self.host, self.port)).unwrap();
        let _ = client.login(&self.user, &self.password);
        client
    }

    async fn get_async_client(&self) -> AsyncFtpStream {
        let mut client = AsyncFtpStream::connect(format!("{}:{}", self.host, self.port))
            .await
            .unwrap();
        let _ = client.login(&self.user, &self.password).await;
        client
    }

    fn get_object_meta(&self, location: &String) -> Result<ObjectMeta, object_store::Error> {
        let mut client = self.get_client();

        let path = Path::from(location.clone());

        Ok(ObjectMeta {
            location: path,
            size: client.size(location.clone()).unwrap(),
            last_modified: client.mdtm(location.clone()).unwrap().and_utc(),
            e_tag: None,
            version: None,
        })
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

    async fn get_opts(&self, location: &Path, _: GetOptions) -> object_store::Result<GetResult> {
        let mut client = self.get_client();

        let location_string = location.to_string();
        let object_meta = self.get_object_meta(&location_string).unwrap();
        let stream = client.retr_as_stream(location_string.clone()).unwrap();
        // TODO: slow performance properly pipe to stream
        let stream = stream
            .bytes()
            .map(|x| {
                x.map(|x| Bytes::from(vec![x]))
                    .map_err(|_| object_store::Error::NotImplemented)
            })
            .collect::<Vec<_>>();

        Ok(GetResult {
            meta: object_meta.clone(),
            payload: GetResultPayload::Stream(futures::stream::iter(stream).boxed()),
            range: Range {
                start: 0,
                end: object_meta.size,
            },
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
        let mut client = self.get_client();

        let path = location.map(ToString::to_string);
        let list = client.nlst(path.as_deref()).unwrap();

        let list = list
            .iter()
            .map(|x| self.get_object_meta(x))
            .collect::<Vec<_>>();
        futures::stream::iter(list).boxed()
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
