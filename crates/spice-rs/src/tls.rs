use std::error::Error;
use tonic::{transport::Channel};
use tonic::transport::channel::{Endpoint, ClientTlsConfig};


pub fn system_tls_certificate() -> Result<tonic::transport::Certificate, Box<dyn Error>> {
    let certs = rustls_native_certs::load_native_certs()?;
    
    let concatenated_pems = certs.iter()
        .filter_map(|cert| {
            let mut buf = &cert.0[..];
            rustls_pemfile::certs(&mut buf).ok()?.pop()
        })
        .map(String::from_utf8)
        .collect::<Result<String, _>>()?;

    Ok(tonic::transport::Certificate::from_pem(concatenated_pems))
}


pub async fn new_tls_flight_channel(https_url: String) -> Result<Channel, Box<dyn Error>> {
    let endpoint_result = Endpoint::from_shared(https_url.clone());
    if endpoint_result.is_err() {
        return Err(endpoint_result.err().expect("").into())
    }
    let mut endpoint = endpoint_result.expect("");

    if https_url.starts_with("grpc+tls://") {
        match system_tls_certificate() {
            Err(e) => {return Err(e.into())},
            Ok(cert) => {
                let tls_config = ClientTlsConfig::new()
                    .ca_certificate(cert)
                    .domain_name(https_url.trim_start_matches("grpc+tls://"));
                endpoint = endpoint.tls_config(tls_config)?;
            }
        }
    }

    match endpoint.connect().await {
        Ok(c) => {Ok(c)},
        Err(e) => {Err(e.into())},
    }
}
