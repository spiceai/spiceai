use std::ops::Range;

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::AsyncReadExt;
use object_store::GetRange;
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

fn pipe_stream<T>(
    stream: T,
    read_size: usize,
) -> BoxStream<'static, std::result::Result<Bytes, object_store::Error>>
where
    T: AsyncReadExt + Unpin + Send + 'static,
{
    let stream = stream! {
        let mut stream = stream;
        let mut buf = vec![0; 4096];
        let mut total = 0;
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
        let object_meta = self.get_object_meta(&location_string).unwrap();
        let mut start = 0;
        let mut end = object_meta.size;

        let mut data_to_read = object_meta.size;

        match options.range {
            Some(GetRange::Bounded(range)) => {
                let _ = client.resume_transfer(range.start).await;
                data_to_read = range.end - range.start;
                end = range.end;
                start = range.start;
            }
            _ => {}
        }
        let stream = client
            .retr_as_stream(location_string.clone())
            .await
            .unwrap();

        Ok(GetResult {
            meta: object_meta.clone(),
            payload: GetResultPayload::Stream(pipe_stream(stream, data_to_read)),
            range: Range {
                start: start,
                end: end,
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
