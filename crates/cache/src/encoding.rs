/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use async_compression::Level;
use async_compression::tokio::bufread::ZstdDecoder;
use async_compression::tokio::write::ZstdEncoder as AsyncZstdEncoder;
use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use spicepod::component::caching::Encoding;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to serialize RecordBatch to Arrow IPC: {source}"))]
    FailedToSerialize { source: arrow::error::ArrowError },

    #[snafu(display("Failed to deserialize RecordBatch from Arrow IPC: {source}"))]
    FailedToDeserialize { source: arrow::error::ArrowError },

    #[snafu(display("Failed to compress data with zstd: {source}"))]
    FailedToCompress { source: std::io::Error },

    #[snafu(display("Failed to decompress data with zstd: {source}"))]
    FailedToDecompress { source: std::io::Error },

    #[snafu(display("No encoder specified for decoding cached data"))]
    NoEncoderSpecified,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Trait for encoding and decoding `RecordBatch` data.
#[async_trait]
pub trait Encoder: Send + Sync {
    /// Encode a vector of `RecordBatch`es into compressed bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or compression fails.
    async fn encode(&self, batches: &[RecordBatch]) -> Result<Vec<u8>>;

    /// Decode compressed bytes back into a vector of `RecordBatch`es.
    ///
    /// # Errors
    ///
    /// Returns an error if decompression or deserialization fails.
    async fn decode(&self, data: &[u8]) -> Result<Vec<RecordBatch>>;

    /// Returns a reference to self as `Any` for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Zstd encoder that compresses `RecordBatch` data.
#[derive(Debug, Clone, Copy)]
pub struct ZstdEncoder {
    compression_level: i32,
}

impl ZstdEncoder {
    /// Create a new Zstd encoder with the specified compression level.
    /// Level 0 uses the default (currently 3), levels 1-22 are valid (higher = better compression, slower).
    #[must_use]
    pub fn new(compression_level: i32) -> Self {
        Self { compression_level }
    }
}

impl Default for ZstdEncoder {
    fn default() -> Self {
        Self::new(6) // Zstd compression level 6
    }
}

#[async_trait]
impl Encoder for ZstdEncoder {
    async fn encode(&self, batches: &[RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // First, serialize to Arrow IPC format
        let schema = batches[0].schema();
        let mut ipc_buffer = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut ipc_buffer, &schema).context(FailedToSerializeSnafu)?;

            for batch in batches {
                writer.write(batch).context(FailedToSerializeSnafu)?;
            }

            writer.finish().context(FailedToSerializeSnafu)?;
        }

        let mut compressed_data = Vec::new();
        let mut encoder = AsyncZstdEncoder::with_quality(
            &mut compressed_data,
            Level::Precise(self.compression_level),
        );
        encoder
            .write_all(&ipc_buffer)
            .await
            .context(FailedToCompressSnafu)?;
        encoder.shutdown().await.context(FailedToCompressSnafu)?;
        Ok(compressed_data)
    }

    async fn decode(&self, data: &[u8]) -> Result<Vec<RecordBatch>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let mut decompressed = Vec::new();
        let mut decoder = ZstdDecoder::new(data);
        decoder
            .read_to_end(&mut decompressed)
            .await
            .context(FailedToDecompressSnafu)?;

        // Then deserialize from Arrow IPC format
        let cursor = Cursor::new(decompressed);
        let reader = StreamReader::try_new(cursor, None).context(FailedToDeserializeSnafu)?;

        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(FailedToDeserializeSnafu)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Create an encoder based on the encoding configuration.
#[must_use]
pub fn get_encoder(encoding: Encoding) -> Option<Arc<dyn Encoder>> {
    match encoding {
        Encoding::None => None,
        Encoding::Zstd => Some(Arc::new(ZstdEncoder::default())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            ],
        )
        .expect("valid record batch")
    }

    #[tokio::test]
    async fn test_zstd_encoder_roundtrip() {
        let encoder = ZstdEncoder::default();
        let original = vec![create_test_batch()];

        let encoded_data = encoder
            .encode(&original)
            .await
            .expect("encode should succeed");
        (!encoded_data.is_empty())
            .then_some(())
            .expect("encoded data should not be empty");

        let decoded = encoder
            .decode(&encoded_data)
            .await
            .expect("decode should succeed");
        (decoded.len() == original.len())
            .then_some(())
            .expect("decoded and original should have same length");
        (decoded[0].num_rows() == original[0].num_rows())
            .then_some(())
            .expect("decoded and original should have same num_rows");
        (decoded[0].num_columns() == original[0].num_columns())
            .then_some(())
            .expect("decoded and original should have same num_columns");
    }

    #[tokio::test]
    async fn test_zstd_encoder_empty() {
        let encoder = ZstdEncoder::default();
        let empty: Vec<RecordBatch> = vec![];

        let encoded_data = encoder.encode(&empty).await.expect("encode should succeed");
        encoded_data
            .is_empty()
            .then_some(())
            .expect("encoded empty data should be empty");

        let decoded = encoder
            .decode(&encoded_data)
            .await
            .expect("decode should succeed");
        decoded
            .is_empty()
            .then_some(())
            .expect("decoded empty data should be empty");
    }

    #[tokio::test]
    async fn test_zstd_compression_reduces_size() {
        // Create a batch with repetitive data that should compress well
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));

        let values: Vec<i32> = vec![42; 1000]; // Highly compressible
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])
            .expect("valid record batch");

        let zstd_encoder = ZstdEncoder::default();

        let uncompressed_size = batch.get_array_memory_size();
        let zstd_size = zstd_encoder
            .encode(std::slice::from_ref(&batch))
            .await
            .expect("encode should succeed")
            .len();

        // Zstd should compress this significantly
        assert!(
            zstd_size < uncompressed_size,
            "Zstd size ({zstd_size}) should be less than uncompressed size ({uncompressed_size})"
        );
    }
}
