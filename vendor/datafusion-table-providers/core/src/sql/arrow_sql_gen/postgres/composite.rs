use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use fallible_iterator::FallibleIterator;
use snafu::prelude::*;
use std::{fmt, ops::Range};
use tokio_postgres::{
    row::RowIndex,
    types::{Field, FromSql, Kind, Type, WrongType},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse composite type ranges: {source}"))]
    UnableToParseCompositeTypeRanges { source: std::io::Error },

    #[snafu(display("Unable to find column {column} in the fields {}", fields.join(", ")))]
    UnableToFindColumnInFields { column: String, fields: Vec<String> },

    #[snafu(display("{source}"))]
    UnableToConvertType { source: WrongType },

    #[snafu(display("Unable to conver raw bytes into expected type: {source}"))]
    UnableToConvertBytesToType {
        source: Box<dyn std::error::Error + Sync + Send>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `PostgreSQL` composite type.
/// Fields of a type can be accessed using `CompositeType::get` and `CompositeType::try_get` methods.
///
/// Adapted from <https://github.com/sfackler/rust-postgres/pull/565>
pub struct CompositeType<'a> {
    type_: Type,
    body: &'a [u8],
    ranges: Vec<Option<Range<usize>>>,
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
impl<'a> FromSql<'a> for CompositeType<'a> {
    fn from_sql(
        type_: &Type,
        body: &'a [u8],
    ) -> Result<CompositeType<'a>, Box<dyn std::error::Error + Sync + Send>> {
        match *type_.kind() {
            Kind::Composite(_) => {
                let fields: &[Field] = composite_type_fields(type_);
                if body.len() < 4 {
                    let message = format!("invalid composite type body length: {}", body.len());
                    return Err(message.into());
                }
                let num_fields: i32 = BigEndian::read_i32(&body[0..4]);
                if num_fields as usize != fields.len() {
                    let message =
                        format!("invalid field count: {} vs {}", num_fields, fields.len());
                    return Err(message.into());
                }
                let ranges = CompositeTypeRanges::new(&body[4..], body.len(), num_fields as u16)
                    .collect()
                    .context(UnableToParseCompositeTypeRangesSnafu)?;
                Ok(CompositeType {
                    type_: type_.clone(),
                    body,
                    ranges,
                })
            }
            _ => Err(format!("expected composite type, got {type_}").into()),
        }
    }
    fn accepts(ty: &Type) -> bool {
        matches!(*ty.kind(), Kind::Composite(_))
    }
}

fn composite_type_fields(type_: &Type) -> &[Field] {
    match type_.kind() {
        Kind::Composite(ref fields) => fields,
        _ => unreachable!(),
    }
}

impl CompositeType<'_> {
    /// Returns information about the fields of the composite type.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        composite_type_fields(&self.type_)
    }

    /// Determines if the composite contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of fields of the composite type.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields().len()
    }

    /// Deserializes a value from the composite type.
    ///
    /// The value can be specified either by its numeric index, or by its field name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<'b, I, T>(&'b self, idx: I) -> T
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'b>,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {idx}: {err}"),
        }
    }

    /// Like `CompositeType::get`, but returns a `Result` rather than panicking.
    ///
    /// # Errors
    ///
    /// Returns an error if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn try_get<'b, I, T>(&'b self, idx: I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'b>,
    {
        self.get_inner(&idx)
    }

    fn get_inner<'b, I, T>(&'b self, idx: &I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'b>,
    {
        let fields_vec = self
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();
        let idx = match idx.__idx(&fields_vec) {
            Some(idx) => idx,
            None => UnableToFindColumnInFieldsSnafu {
                column: idx.to_string(),
                fields: fields_vec,
            }
            .fail()?,
        };

        let ty = self.fields()[idx].type_();
        if !T::accepts(ty) {
            return Err(WrongType::new::<T>(ty.clone())).context(UnableToConvertTypeSnafu);
        }

        let buf = self.ranges[idx].clone().map(|r| &self.body[r]);
        FromSql::from_sql_nullable(ty, buf).context(UnableToConvertBytesToTypeSnafu)
    }
}

/// A fallible iterator over the fields of a composite type.
pub struct CompositeTypeRanges<'a> {
    buf: &'a [u8],
    len: usize,
    remaining: u16,
}

impl<'a> CompositeTypeRanges<'a> {
    /// Returns a fallible iterator over the fields of the composite type.
    #[inline]
    #[must_use]
    pub fn new(buf: &'a [u8], len: usize, remaining: u16) -> CompositeTypeRanges<'a> {
        CompositeTypeRanges {
            buf,
            len,
            remaining,
        }
    }
}

#[allow(clippy::cast_sign_loss)]
impl FallibleIterator for CompositeTypeRanges<'_> {
    type Item = Option<std::ops::Range<usize>>;
    type Error = std::io::Error;

    #[inline]
    fn next(&mut self) -> std::io::Result<Option<Option<std::ops::Range<usize>>>> {
        if self.remaining == 0 {
            if self.buf.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid buffer length: compositetyperanges is not empty",
            ));
        }

        self.remaining -= 1;

        // Binary format of a composite type:
        // [for each field]
        //     <OID of field's type: 4 bytes>
        //     [if value is NULL]
        //         <-1: 4 bytes>
        //     [else]
        //         <length of value: 4 bytes>
        //         <value: <length> bytes>
        //     [end if]
        // [end for]
        // https://www.postgresql.org/message-id/16CCB2D3-197E-4D9F-BC6F-9B123EA0D40D%40phlo.org
        // https://github.com/postgres/postgres/blob/29e321cdd63ea48fd0223447d58f4742ad729eb0/src/backend/utils/adt/rowtypes.c#L736

        let _oid = self.buf.read_i32::<BigEndian>()?;
        let len = self.buf.read_i32::<BigEndian>()?;
        if len < 0 {
            Ok(Some(None))
        } else {
            let len = len as usize;
            if self.buf.len() < len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                ));
            }
            let base = self.len - self.buf.len();
            self.buf = &self.buf[len..];
            Ok(Some(Some(base..base + len)))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.remaining as usize;
        (len, Some(len))
    }
}
