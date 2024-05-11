use std::{
    io::Read,
    ops::Range,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta,
    ObjectStore, PutOptions, PutResult,
};
use suppaftp::FtpStream;
use tokio::io::AsyncWrite;

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

    async fn get_opts(&self, location: &Path, _: GetOptions) -> object_store::Result<GetResult> {
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
        // TODO: slow performance properly pipe to stream
        let stream = stream
            .bytes()
            .map(|x| {
                x.map(|x| Bytes::from(vec![x]))
                    .map_err(|_| object_store::Error::NotImplemented)
            })
            .collect::<Vec<_>>();
        let len = stream.len();

        Ok(GetResult {
            meta: object_meta,
            payload: GetResultPayload::Stream(futures::stream::iter(stream).boxed()),
            range: Range { start: 0, end: len },
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
        let mut client = self.client.lock().unwrap();
        let client = &mut *client;
        let path = location.map(|val| val.to_string());
        let list = client.nlst(path.as_deref()).unwrap();

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
