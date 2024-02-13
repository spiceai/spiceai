# Spice.ai Extensibility

This document is an overview of all the interfaces and extension points in Spice.ai.

## DataConnector

A `DataConnector` is a component that represents the source of data to the Spice.ai runtime. Datasets specify which `DataConnector` to use via the `from` field in the dataset configuration. A `DataConnector` knows how to retrieve data, stream data updates, and writing data back to the source.

```rust
/// A `DataConnector` knows how to retrieve and modify data for a given dataset.
///
/// Implementing `get_all_data` is required, but `stream_data_updates` & `supports_data_streaming` is optional.
/// If `stream_data_updates` is not supported for a dataset, the runtime will fall back to polling `get_all_data` and returning a
/// `DataUpdate` that is constructed like:
///
/// ```rust
/// DataUpdate {
///    data: get_all_data(dataset),
///    update_type: UpdateType::Overwrite,
/// }
/// ```
pub trait DataConnector: Send + Sync {
    /// Create a new `DataConnector` with the given `AuthProvider`.
    fn new(
        auth_provider: AuthProvider,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self>>>>
    where
        Self: Sized;

    /// Returns true if the given dataset supports streaming by this `DataConnector`.
    fn supports_data_streaming(&self, _dataset: &Dataset) -> bool {
        false
    }

    /// Returns a stream of `DataUpdates` for the given dataset.
    fn stream_data_updates<'a>(&self, dataset: &Dataset) -> BoxStream<'a, DataUpdate> {
        panic!("stream_data_updates not implemented for {}", dataset.name)
    }

    /// Returns all data for the given dataset.
    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<RecordBatch>> + Send>>;

    /// Returns true if the given dataset supports writing data back to this `DataConnector`.
    fn supports_data_writes(&self, _dataset: &Dataset) -> bool {
        false
    }

    /// Adds data ingested locally back to the source.
    fn add_data(
        &self,
        dataset: &Dataset,
        _data: DataUpdate,
    ) -> Pin<Box<dyn Future<Output = Result<()>>>> {
        panic!("add_data not implemented for {}", dataset.name)
    }
}
```

## DataBackend

A `DataBackend` is a component the runtime uses to store accelerated data locally. The `acceleration` object in the dataset configuration specifies which `DataBackend` to use via the `engine` & `mode` fields.

i.e.
```yaml
acceleration:
  engine: duckdb # arrow
  mode: memory # file
```

```rust
pub type AddDataResult<'a> =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send + 'a>>;

pub trait DataBackend: Send + Sync {
    fn add_data(&self, data_update: DataUpdate) -> AddDataResult;
}
```

