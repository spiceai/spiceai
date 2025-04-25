use crate::{
    flight::{create_flight_client, start_spice_test_app},
    init_tracing,
    utils::test_request_context,
};

#[tokio::test]
async fn test_basic_binding() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let (channel, df) = start_spice_test_app(None, None).await?;

            let mut client = create_flight_client(channel, None)?;

            Ok(())
        })
        .await
}
