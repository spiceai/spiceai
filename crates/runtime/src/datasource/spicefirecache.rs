use futures_core::Stream;

pub struct SpiceFirecache {}

impl super::DataSource for SpiceFirecache {
    fn get_data(&self) -> impl Stream<Item = super::DataUpdate> {
        todo!()
    }
}
