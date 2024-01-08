use std::error::Error;
use std::str::FromStr;
use tonic::transport::channel::{ClientTlsConfig, Endpoint};
use tonic::transport::Channel;

pub fn system_tls_certificate() -> Result<tonic::transport::Certificate, Box<dyn Error>> {
    // Load root certificates found in the platform’s native certificate store.
    let certs = rustls_native_certs::load_native_certs()?;

    let concatenated_pems = certs
        .iter()
        .filter_map(|cert| {
            let mut buf = &cert.0[..];
            rustls_pemfile::certs(&mut buf).ok()?.pop()
        })
        .map(String::from_utf8)
        .collect::<Result<String, _>>()?;

    Ok(tonic::transport::Certificate::from_pem(concatenated_pems))
}

pub async fn new_tls_flight_channel(https_url: &str) -> Result<Channel, Box<dyn Error>> {
    let mut endpoint = Endpoint::from_str(https_url)?;

    if https_url.starts_with("https://") {
        let cert = system_tls_certificate()?;
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(cert)
            .domain_name(https_url.trim_start_matches("https://"));
        endpoint = endpoint.tls_config(tls_config)?;
    }

    Ok(endpoint.connect().await?)
}
