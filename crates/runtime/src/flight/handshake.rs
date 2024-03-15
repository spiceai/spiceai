use std::pin::Pin;

use arrow_flight::HandshakeResponse;
use futures::Stream;
use tonic::{metadata::MetadataValue, Response, Status};
use uuid::Uuid;

type HandshakeResponseStream =
    Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;

pub(crate) fn handle() -> Result<Response<HandshakeResponseStream>, Status> {
    // THIS IS PLACEHOLDER NO-OP AUTH THAT DOES NOT CHECK THE PROVIDED TOKEN AND SIMPLY RETURNS A UUID.
    // TODO: Implement proper auth.
    let token = Uuid::new_v4().to_string();
    let result = HandshakeResponse {
        protocol_version: 0,
        payload: token.as_bytes().to_vec().into(),
    };
    let result = Ok(result);
    let output = futures::stream::iter(vec![result]);
    let str = format!("Bearer {token}");
    let mut resp: Response<HandshakeResponseStream> = Response::new(Box::pin(output));
    let md = MetadataValue::try_from(str)
        .map_err(|_| Status::internal("generated authorization could not be parsed"))?;
    resp.metadata_mut().insert("authorization", md);
    Ok(resp)
}
