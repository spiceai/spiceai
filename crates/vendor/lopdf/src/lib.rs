#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![expect(clippy::pedantic)]
#![expect(clippy::allow_attributes)]
#![expect(clippy::expect_used)]
#![expect(clippy::unwrap_used)]
#![expect(clippy::redundant_clone)]
#![expect(clippy::equatable_if_let)]
#![allow(unfulfilled_lint_expectations)]
#![expect(dead_code)]

pub mod content;
pub mod encryption;
pub mod filters;
pub mod xobject;
pub mod xref;

#[macro_use]
mod object;
mod document;
mod incremental_document;

mod bookmarks;
mod cmap_section;
mod common_data_structures;
mod creator;
mod datetime;
mod destinations;
mod encodings;
mod error;
mod outlines;
mod processor;
mod toc;
mod writer;

mod load_options;
mod object_stream;
mod parser;
mod parser_aux;
mod reader;
mod save_options;

mod font;

pub use document::Document;
pub use object::{Dictionary, Object, ObjectId, Stream, StringFormat};

pub use bookmarks::Bookmark;
pub use common_data_structures::{decode_text_string, text_string};
pub use destinations::Destination;
pub use encodings::{Encoding, encode_utf8, encode_utf16_be};
pub use encryption::{EncryptionState, EncryptionVersion, Permissions};
pub use error::{Error, Result};
pub use incremental_document::IncrementalDocument;
pub use load_options::{FilterFunc, LoadOptions};
pub use object_stream::{ObjectStream, ObjectStreamBuilder, ObjectStreamConfig};
pub use outlines::Outline;
pub use reader::{PdfMetadata, Reader};
pub use save_options::{SaveOptions, SaveOptionsBuilder};
pub use toc::Toc;

pub use parser_aux::substr;
pub use parser_aux::substring;

pub use font::FontData;
