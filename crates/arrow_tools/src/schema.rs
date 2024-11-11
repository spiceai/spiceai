use datafusion::common::DFSchema;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Expected and actual number of fields in the query result don't match: expected {expected}, received {actual}"))]
    SchemaMismatchNumFields { expected: usize, actual: usize },

    #[snafu(display("Query returned an unexpected data type for column {name}: expected {expected}, received {actual}. Is the column data type supported by the data accelerator (https://docs.spiceai.org/reference/datatypes)?"))]
    SchemaMismatchDataType {
        name: String,
        expected: String,
        actual: String,
    },

    #[snafu(display("Failed to get field data type"))]
    UnableToGetFieldDataType {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Validates the fields between two Arrow schemas match, with a specific error about which field is mismatched.
///
/// # Errors
///
/// This function will return an error if the fields of the expected schema don't
/// match the fields of the actual schema.
pub fn verify_schema(
    expected: &arrow::datatypes::Fields,
    actual: &arrow::datatypes::Fields,
) -> Result<()> {
    if expected.len() != actual.len() {
        return SchemaMismatchNumFieldsSnafu {
            expected: expected.len(),
            actual: actual.len(),
        }
        .fail();
    }

    for idx in 0..expected.len() {
        let a = expected.get(idx).context(UnableToGetFieldDataTypeSnafu)?;
        let b = actual.get(idx).context(UnableToGetFieldDataTypeSnafu)?;

        let a_data_type = a.data_type();
        let b_data_type = b.data_type();

        if !DFSchema::datatype_is_semantically_equal(a_data_type, b_data_type) {
            return SchemaMismatchDataTypeSnafu {
                name: a.name(),
                expected: format!("{a_data_type}"),
                actual: format!("{b_data_type}"),
            }
            .fail();
        }
    }

    Ok(())
}
