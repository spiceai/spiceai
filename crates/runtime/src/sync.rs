use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Invalid length"))]
    InvalidLength { context: &'static str },

    #[snafu(display("Invalid magic bytes"))]
    InvalidMagicBytes,

    #[snafu(display("Invalid prefix"))]
    InvalidPrefix { source: std::string::FromUtf8Error },

    #[snafu(display("Invalid LSN"))]
    InvalidLogSequenceNumber,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub mod cursor {
    use super::*;

    const MAGIC_BYTES: [u8; 4] = [83, 80, 83, 67]; // ASCII for "SPSC" (SPice Sync Cursor)

    // Cursor is used to synchronize data between the
    // Spice.ai Runtime and the Spice.ai CLI.
    pub struct Cursor {
        version: u8,
        prefix: String,
        log_sequence_number: u64,
    }

    impl Cursor {
        pub fn parse(cursor: impl AsRef<[u8]>) -> Result<Self> {
            let cursor = cursor.as_ref();
            let mut iter = cursor.iter();

            // Check for magic bytes
            for &magic_byte in &MAGIC_BYTES {
                if iter.next() != Some(&magic_byte) {
                    return InvalidMagicBytesSnafu.fail();
                }
            }

            // Parse version
            let version = *iter.next().ok_or(Error::InvalidLength {
                context: "version: *iter.next().ok_or",
            })?;

            // Parse ASCII string prefix
            let prefix_length = *iter.next().ok_or(Error::InvalidLength {
                context: "prefix_length: *iter.next().ok_or",
            })?;
            let prefix_bytes: Vec<u8> = iter.by_ref().take(prefix_length.into()).copied().collect();
            let prefix = String::from_utf8(prefix_bytes)
                .context(InvalidPrefixSnafu)?
                .trim_matches(char::is_whitespace)
                .to_string();

            // Parse log sequence number (8 bytes, big endian)
            let log_sequence_number_bytes: Vec<u8> = iter.by_ref().take(8).copied().collect();
            if log_sequence_number_bytes.len() != 8 {
                return InvalidLengthSnafu {
                    context: "log_sequence_number_bytes.len() != 8",
                }
                .fail();
            }
            let log_sequence_number_bytes: [u8; 8] = match log_sequence_number_bytes.try_into() {
                Ok(bytes) => bytes,
                Err(_) => return InvalidLogSequenceNumberSnafu.fail(),
            };
            let log_sequence_number = u64::from_be_bytes(log_sequence_number_bytes);

            Ok(Cursor {
                version,
                prefix,
                log_sequence_number,
            })
        }

        pub fn to_bytes(&self) -> Result<Vec<u8>> {
            let mut bytes = Vec::with_capacity(28);

            // Add magic bytes
            bytes.extend_from_slice(&MAGIC_BYTES);

            // Add version byte
            bytes.push(self.version);

            // Add ASCII string prefix
            let prefix_bytes = self.prefix.as_bytes().to_vec();
            let prefix_length = match u8::try_from(prefix_bytes.len()) {
                Ok(length) => length,
                Err(_err) => {
                    return Err(Error::InvalidLength {
                        context: "prefix length > u8::MAX (255)",
                    })
                }
            };
            bytes.push(prefix_length);
            bytes.extend_from_slice(&prefix_bytes);

            // Add log sequence number (8 bytes, big endian)
            let log_sequence_number_bytes = self.log_sequence_number.to_be_bytes();
            bytes.extend_from_slice(&log_sequence_number_bytes);

            Ok(bytes)
        }
    }

    #[cfg(test)]
    #[allow(clippy::expect_used)]
    mod tests {
        use super::*;

        #[test]
        fn test_parse() {
            let cursor_test_bytes: Vec<u8> = vec![
                83, 80, 83, 67, // Magic bytes (SPSC)
                1,  // Version
                12, 115, 112, 105, 99, 101, 46, 97, 105, 46, 99, 108, 105, // Prefix
                255, 255, 255, 255, 255, 255, 0, 0, // Log sequence number
            ];
            let cursor = match Cursor::parse(&cursor_test_bytes) {
                Ok(cursor) => cursor,
                Err(err) => panic!("Unable to parse cursor: {err:?}"),
            };
            assert_eq!(cursor.version, 1);
            assert_eq!(cursor.prefix, "spice.ai.cli");
            assert_eq!(cursor.log_sequence_number, 0xFFFF_FFFF_FFFF_0000_u64);

            let cursor_bytes = cursor
                .to_bytes()
                .expect("Unable to convert cursor to bytes");

            assert_eq!(cursor_bytes, cursor_test_bytes);
        }
    }
}
