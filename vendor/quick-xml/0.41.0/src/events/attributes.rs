//! Xml Attributes module
//!
//! Provides an iterator over attributes key/value pairs

use crate::encoding::Decoder;
use crate::errors::Result as XmlResult;
use crate::escape::{escape, resolve_predefined_entity};
use crate::name::{LocalName, Namespace, NamespaceResolver, QName};
use crate::utils::{is_whitespace, Bytes};
use crate::XmlVersion;

use std::collections::HashSet;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{BuildHasherDefault, DefaultHasher, Hasher};
use std::iter::FusedIterator;
use std::{borrow::Cow, ops::Range};

/// A struct representing a key/value XML attribute.
///
/// Field `value` stores raw bytes, possibly containing escape-sequences. Most users will likely
/// want to access the value using one of the [`normalized_value`] and [`decoded_and_normalized_value`]
/// functions.
///
/// [`normalized_value`]: Self::normalized_value
/// [`decoded_and_normalized_value`]: Self::decoded_and_normalized_value
#[derive(Clone, Eq, PartialEq)]
pub struct Attribute<'a> {
    /// The key to uniquely define the attribute.
    ///
    /// If [`Attributes::with_checks`] is turned off, the key might not be unique.
    pub key: QName<'a>,
    /// The raw value of the attribute.
    pub value: Cow<'a, [u8]>,
}

impl<'a> Attribute<'a> {
    /// Returns the attribute value normalized as per [the XML specification] (or [for 1.0]).
    ///
    /// The document **must** be UTF-8 encoded, or pre-processed using [`DecodingReader`].
    ///
    /// The characters `\t`, `\r`, `\n` are replaced with whitespace characters (`0x20`).
    ///
    /// The following escape sequences are replaced with their unescaped equivalents:
    ///
    /// | Escape Sequence | Replacement
    /// |-----------------|------------
    /// | `&lt;`          | `<`
    /// | `&gt;`          | `>`
    /// | `&amp;`         | `&`
    /// | `&apos;`        | `'`
    /// | `&quot;`        | `"`
    ///
    /// This will allocate unless the raw attribute value does not require normalization.
    ///
    /// Note, although you may use this library to parse HTML, you cannot use this
    /// method to get HTML content, because its returns normalized value: the following
    /// sequences are translated into a single space (U+0020) character:
    ///
    /// - `\r\n`
    /// - `\r\x85` (only XML 1.1)
    /// - `\r`
    /// - `\n`
    /// - `\t`
    /// - `\x85` (only XML 1.1)
    /// - `\x2028` (only XML 1.1)
    ///
    /// The text in HTML normally is not normalized in any way; normalization is
    /// performed only in limited contexts and [only for] `\r\n` and `\r`.
    ///
    /// See also [`normalized_value_with()`](Self::normalized_value_with).
    ///
    /// <div style="background:rgba(120,145,255,0.45);padding:0.75em;">
    ///
    /// NOTE: If you are using this in a context where the input is not controlled,
    /// it is preferred to wrap the input stream in [`DecodingReader`] or to use
    /// [`decoded_and_normalized_value()`](Self::decoded_and_normalized_value) instead.
    ///
    /// </div>
    ///
    /// [the XML specification]: https://www.w3.org/TR/xml11/#AVNormalize
    /// [`DecodingReader`]: ../../encoding/struct.DecodingReader.html
    /// [for 1.0]: https://www.w3.org/TR/xml/#AVNormalize
    /// [only for]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn normalized_value(&self, version: XmlVersion) -> XmlResult<Cow<'a, str>> {
        // resolve_predefined_entity returns only non-recursive replacements, so depth=1 is enough
        self.normalized_value_with(version, 1, resolve_predefined_entity)
    }

    /// Returns the attribute value normalized as per [the XML specification] (or [for 1.0]),
    /// using a custom entity resolver.
    ///
    /// The document **must** be UTF-8 encoded, or pre-processed using [`DecodingReader`].
    ///
    /// Do not use this method with HTML attributes.
    ///
    /// The characters `\t`, `\r`, `\n` are replaced with whitespace characters (`0x20`).
    ///
    /// A function for resolving entities can be provided as `resolve_entity`.
    /// This method does not resolve any predefined entities, but you can use
    /// [`resolve_predefined_entity`] in your function.
    ///
    /// This will allocate unless the raw attribute value does not require normalization.
    ///
    /// Note, although you may use this library to parse HTML, you cannot use this
    /// method to get HTML content, because its returns normalized value: the following
    /// sequences are translated into a single space (U+0020) character:
    ///
    /// - `\r\n`
    /// - `\r\x85` (only XML 1.1)
    /// - `\r`
    /// - `\n`
    /// - `\t`
    /// - `\x85` (only XML 1.1)
    /// - `\x2028` (only XML 1.1)
    ///
    /// The text in HTML normally is not normalized in any way; normalization is
    /// performed only in limited contexts and [only for] `\r\n` and `\r`.
    ///
    /// See also [`normalized_value()`](Self::normalized_value).
    ///
    /// <div style="background:rgba(120,145,255,0.45);padding:0.75em;">
    ///
    /// NOTE: If you are using this in a context where the input is not controlled,
    /// it is preferred to wrap the input stream in [`DecodingReader`] or to use
    /// [`decoded_and_normalized_value_with()`](Self::decoded_and_normalized_value_with) instead.
    ///
    /// </div>
    ///
    /// # Parameters
    ///
    /// - `depth`: maximum number of nested entities that can be expanded. If expansion
    ///   chain will be more that this value, the function will return [`EscapeError::TooManyNestedEntities`]
    /// - `resolve_entity`: a function to resolve entity. This function could be called
    ///   multiple times on the same input and can return different values in each case
    ///   for the same input, although it is not recommended
    ///
    /// [the XML specification]: https://www.w3.org/TR/xml11/#AVNormalize
    /// [`DecodingReader`]: ../../encoding/struct.DecodingReader.html
    /// [for 1.0]: https://www.w3.org/TR/xml/#AVNormalize
    /// [only for]: https://html.spec.whatwg.org/#normalize-newlines
    /// [`EscapeError::TooManyNestedEntities`]: crate::escape::EscapeError::TooManyNestedEntities
    pub fn normalized_value_with<'entity>(
        &self,
        version: XmlVersion,
        depth: usize,
        resolve_entity: impl FnMut(&str) -> Option<&'entity str>,
    ) -> XmlResult<Cow<'a, str>> {
        use crate::encoding::EncodingError;
        use std::str::from_utf8;

        let decoded = match &self.value {
            Cow::Borrowed(bytes) => Cow::Borrowed(from_utf8(bytes).map_err(EncodingError::Utf8)?),
            // Convert to owned, because otherwise Cow will be bound with wrong lifetime
            Cow::Owned(bytes) => {
                Cow::Owned(from_utf8(bytes).map_err(EncodingError::Utf8)?.to_owned())
            }
        };

        match version.normalize_attribute_value(&decoded, depth, resolve_entity)? {
            // Because result is borrowed, no replacements was done and we can use original string
            Cow::Borrowed(_) => Ok(decoded),
            Cow::Owned(s) => Ok(s.into()),
        }
    }

    /// Decodes using a provided reader and returns the attribute value normalized
    /// as per [the XML specification] (or [for 1.0]).
    ///
    /// Do not use this method with HTML attributes.
    ///
    /// The characters `\t`, `\r`, `\n` are replaced with whitespace characters (`0x20`).
    ///
    /// The following escape sequences are replaced with their unescaped equivalents:
    ///
    /// | Escape Sequence | Replacement
    /// |-----------------|------------
    /// | `&lt;`          | `<`
    /// | `&gt;`          | `>`
    /// | `&amp;`         | `&`
    /// | `&apos;`        | `'`
    /// | `&quot;`        | `"`
    ///
    /// This will allocate unless the raw attribute value does not require normalization.
    ///
    /// Note, although you may use this library to parse HTML, you cannot use this
    /// method to get HTML content, because its returns normalized value: the following
    /// sequences are translated into a single space (U+0020) character:
    ///
    /// - `\r\n`
    /// - `\r\x85` (only XML 1.1)
    /// - `\r`
    /// - `\n`
    /// - `\t`
    /// - `\x85` (only XML 1.1)
    /// - `\x2028` (only XML 1.1)
    ///
    /// The text in HTML normally is not normalized in any way; normalization is
    /// performed only in limited contexts and [only for] `\r\n` and `\r`.
    ///
    /// See also [`decoded_and_normalized_value_with()`](#method.decoded_and_normalized_value_with)
    ///
    /// [the XML specification]: https://www.w3.org/TR/xml11/#AVNormalize
    /// [for 1.0]: https://www.w3.org/TR/xml/#AVNormalize
    /// [only for]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn decoded_and_normalized_value(
        &self,
        version: XmlVersion,
        decoder: Decoder,
    ) -> XmlResult<Cow<'a, str>> {
        // resolve_predefined_entity returns only non-recursive replacements, so depth=1 is enough
        self.decoded_and_normalized_value_with(version, decoder, 1, resolve_predefined_entity)
    }

    /// Decodes using a provided reader and returns the attribute value normalized
    /// as per [the XML specification] (or [for 1.0]), using a custom entity resolver.
    ///
    /// Do not use this method with HTML attributes.
    ///
    /// The characters `\t`, `\r`, `\n` are replaced with whitespace characters (`0x20`).
    ///
    /// A function for resolving entities can be provided as `resolve_entity`.
    /// This method does not resolve any predefined entities, but you can use
    /// [`resolve_predefined_entity`] in your function.
    ///
    /// This will allocate unless the raw attribute value does not require normalization.
    ///
    /// Note, although you may use this library to parse HTML, you cannot use this
    /// method to get HTML content, because its returns normalized value: the following
    /// sequences are translated into a single space (U+0020) character:
    ///
    /// - `\r\n`
    /// - `\r\x85` (only XML 1.1)
    /// - `\r`
    /// - `\n`
    /// - `\t`
    /// - `\x85` (only XML 1.1)
    /// - `\x2028` (only XML 1.1)
    ///
    /// The text in HTML normally is not normalized in any way; normalization is
    /// performed only in limited contexts and [only for] `\r\n` and `\r`.
    ///
    /// See also [`decoded_and_normalized_value()`](#method.decoded_and_normalized_value)
    ///
    /// # Parameters
    ///
    /// - `depth`: maximum number of nested entities that can be expanded. If expansion
    ///   chain will be more that this value, the function will return [`EscapeError::TooManyNestedEntities`]
    /// - `resolve_entity`: a function to resolve entity. This function could be called
    ///   multiple times on the same input and can return different values in each case
    ///   for the same input, although it is not recommended
    ///
    /// [the XML specification]: https://www.w3.org/TR/xml11/#AVNormalize
    /// [for 1.0]: https://www.w3.org/TR/xml/#AVNormalize
    /// [only for]: https://html.spec.whatwg.org/#normalize-newlines
    /// [`EscapeError::TooManyNestedEntities`]: crate::escape::EscapeError::TooManyNestedEntities
    pub fn decoded_and_normalized_value_with<'entity>(
        &self,
        version: XmlVersion,
        decoder: Decoder,
        depth: usize,
        resolve_entity: impl FnMut(&str) -> Option<&'entity str>,
    ) -> XmlResult<Cow<'a, str>> {
        let decoded = match &self.value {
            Cow::Borrowed(bytes) => decoder.decode(bytes)?,
            // Convert to owned, because otherwise Cow will be bound with wrong lifetime
            Cow::Owned(bytes) => decoder.decode(bytes)?.into_owned().into(),
        };

        match version.normalize_attribute_value(&decoded, depth, resolve_entity)? {
            // Because result is borrowed, no replacements was done and we can use original string
            Cow::Borrowed(_) => Ok(decoded),
            Cow::Owned(s) => Ok(s.into()),
        }
    }

    /// Returns the unescaped value.
    ///
    /// This is normally the value you are interested in. Escape sequences such as `&gt;` are
    /// replaced with their unescaped equivalents such as `>`.
    ///
    /// This will allocate if the value contains any escape sequences.
    ///
    /// See also [`unescape_value_with()`](Self::unescape_value_with)
    ///
    /// <div style="background:rgba(120,145,255,0.45);padding:0.75em;">
    ///
    /// NOTE: Because this method is available only if [`encoding`] feature is **not** enabled,
    /// should only be used by applications.
    /// Libs should use [`decoded_and_normalized_value()`](Self::decoded_and_normalized_value)
    /// instead, because if lib will be used in a project which depends on quick_xml with
    /// [`encoding`] feature enabled, the lib will fail to compile due to [feature unification].
    ///
    /// </div>
    ///
    /// [`encoding`]: ../../index.html#encoding
    /// [feature unification]: https://doc.rust-lang.org/cargo/reference/features.html#feature-unification
    #[cfg(any(doc, not(feature = "encoding")))]
    #[deprecated = "use `Self::normalized_value()`"]
    pub fn unescape_value(&self) -> XmlResult<Cow<'a, str>> {
        // resolve_predefined_entity returns only non-recursive replacements, so depth=1 is enough
        self.normalized_value_with(XmlVersion::Implicit1_0, 1, resolve_predefined_entity)
    }

    /// Decodes using UTF-8 then unescapes the value, using custom entities.
    ///
    /// This is normally the value you are interested in. Escape sequences such as `&gt;` are
    /// replaced with their unescaped equivalents such as `>`.
    /// A fallback resolver for additional custom entities can be provided via
    /// `resolve_entity`.
    ///
    /// This will allocate if the value contains any escape sequences.
    ///
    /// See also [`unescape_value()`](Self::unescape_value)
    ///
    /// <div style="background:rgba(120,145,255,0.45);padding:0.75em;">
    ///
    /// NOTE: Because this method is available only if [`encoding`] feature is **not** enabled,
    /// should only be used by applications.
    /// Libs should use [`decoded_and_normalized_value_with()`](Self::decoded_and_normalized_value_with)
    /// instead, because if lib will be used in a project which depends on quick_xml with
    /// [`encoding`] feature enabled, the lib will fail to compile due to [feature unification].
    ///
    /// </div>
    ///
    /// [`encoding`]: ../../index.html#encoding
    /// [feature unification]: https://doc.rust-lang.org/cargo/reference/features.html#feature-unification
    #[cfg(any(doc, not(feature = "encoding")))]
    #[deprecated = "use `Self::normalized_value_with()`"]
    #[inline]
    pub fn unescape_value_with<'entity>(
        &self,
        resolve_entity: impl FnMut(&str) -> Option<&'entity str>,
    ) -> XmlResult<Cow<'a, str>> {
        self.normalized_value_with(XmlVersion::Implicit1_0, 128, resolve_entity)
    }

    /// Decodes then unescapes the value.
    ///
    /// This will allocate if the value contains any escape sequences or in
    /// non-UTF-8 encoding.
    #[deprecated = "use `Self::decoded_and_normalized_value()`"]
    pub fn decode_and_unescape_value(&self, decoder: Decoder) -> XmlResult<Cow<'a, str>> {
        // resolve_predefined_entity returns only non-recursive replacements, so depth=1 is enough
        self.decoded_and_normalized_value_with(
            XmlVersion::Implicit1_0,
            decoder,
            1,
            resolve_predefined_entity,
        )
    }

    /// Decodes then unescapes the value with custom entities.
    ///
    /// This will allocate if the value contains any escape sequences or in
    /// non-UTF-8 encoding.
    #[deprecated = "use `Self::decoded_and_normalized_value_with()`"]
    pub fn decode_and_unescape_value_with<'entity>(
        &self,
        decoder: Decoder,
        resolve_entity: impl FnMut(&str) -> Option<&'entity str>,
    ) -> XmlResult<Cow<'a, str>> {
        self.decoded_and_normalized_value_with(
            XmlVersion::Implicit1_0,
            decoder,
            128,
            resolve_entity,
        )
    }

    /// If attribute value [represents] valid boolean values, returns `Some`, otherwise returns `None`.
    ///
    /// The valid boolean representations are only `"true"`, `"false"`, `"1"`, and `"0"`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::attributes::Attribute;
    ///
    /// let attr = Attribute::from(("attr", "false"));
    /// assert_eq!(attr.as_bool(), Some(false));
    ///
    /// let attr = Attribute::from(("attr", "0"));
    /// assert_eq!(attr.as_bool(), Some(false));
    ///
    /// let attr = Attribute::from(("attr", "true"));
    /// assert_eq!(attr.as_bool(), Some(true));
    ///
    /// let attr = Attribute::from(("attr", "1"));
    /// assert_eq!(attr.as_bool(), Some(true));
    ///
    /// let attr = Attribute::from(("attr", "not bool"));
    /// assert_eq!(attr.as_bool(), None);
    /// ```
    ///
    /// [represents]: https://www.w3.org/TR/xmlschema11-2/#boolean
    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self.value.as_ref() {
            b"1" | b"true" => Some(true),
            b"0" | b"false" => Some(false),
            _ => None,
        }
    }
}

impl<'a> Debug for Attribute<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Attribute")
            .field("key", &Bytes(self.key.as_ref()))
            .field("value", &Bytes(&self.value))
            .finish()
    }
}

impl<'a> From<(&'a [u8], &'a [u8])> for Attribute<'a> {
    /// Creates new attribute from raw bytes.
    /// Does not apply any transformation to both key and value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::attributes::Attribute;
    ///
    /// let features = Attribute::from(("features".as_bytes(), "Bells &amp; whistles".as_bytes()));
    /// assert_eq!(features.value, "Bells &amp; whistles".as_bytes());
    /// ```
    fn from(val: (&'a [u8], &'a [u8])) -> Attribute<'a> {
        Attribute {
            key: QName(val.0),
            value: Cow::from(val.1),
        }
    }
}

impl<'a> From<(&'a str, &'a str)> for Attribute<'a> {
    /// Creates new attribute from text representation.
    /// Key is stored as-is, but the value will be escaped.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::attributes::Attribute;
    ///
    /// let features = Attribute::from(("features", "Bells & whistles"));
    /// assert_eq!(features.value, "Bells &amp; whistles".as_bytes());
    /// ```
    fn from(val: (&'a str, &'a str)) -> Attribute<'a> {
        Attribute {
            key: QName(val.0.as_bytes()),
            value: match escape(val.1) {
                Cow::Borrowed(s) => Cow::Borrowed(s.as_bytes()),
                Cow::Owned(s) => Cow::Owned(s.into_bytes()),
            },
        }
    }
}

impl<'a> From<(&'a str, Cow<'a, str>)> for Attribute<'a> {
    /// Creates new attribute from text representation.
    /// Key is stored as-is, but the value will be escaped.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::borrow::Cow;
    /// use pretty_assertions::assert_eq;
    /// use quick_xml::events::attributes::Attribute;
    ///
    /// let features = Attribute::from(("features", Cow::Borrowed("Bells & whistles")));
    /// assert_eq!(features.value, "Bells &amp; whistles".as_bytes());
    /// ```
    fn from(val: (&'a str, Cow<'a, str>)) -> Attribute<'a> {
        Attribute {
            key: QName(val.0.as_bytes()),
            value: match escape(val.1) {
                Cow::Borrowed(s) => Cow::Borrowed(s.as_bytes()),
                Cow::Owned(s) => Cow::Owned(s.into_bytes()),
            },
        }
    }
}

impl<'a> From<Attr<&'a [u8]>> for Attribute<'a> {
    #[inline]
    fn from(attr: Attr<&'a [u8]>) -> Self {
        Self {
            key: attr.key(),
            value: Cow::Borrowed(attr.value()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Iterator over XML attributes.
///
/// Yields `Result<Attribute>`. An `Err` will be yielded if an attribute is malformed or duplicated.
/// The duplicate check can be turned off by calling [`with_checks(false)`].
///
/// When [`serialize`] feature is enabled, can be converted to serde's deserializer.
///
/// [`with_checks(false)`]: Self::with_checks
/// [`serialize`]: ../../index.html#serialize
#[derive(Clone)]
pub struct Attributes<'a> {
    /// Slice of `BytesStart` corresponding to attributes
    bytes: &'a [u8],
    /// Iterator state, independent from the actual source of bytes
    state: IterState,
    /// Encoding used for `bytes`
    decoder: Decoder,
}

impl<'a> Attributes<'a> {
    /// Internal constructor, used by `BytesStart`. Supplies data in reader's encoding
    #[inline]
    pub(crate) const fn wrap(buf: &'a [u8], pos: usize, html: bool, decoder: Decoder) -> Self {
        Self {
            bytes: buf,
            state: IterState::new(pos, html),
            decoder,
        }
    }

    /// Creates a new attribute iterator from a buffer, which recognizes only XML-style
    /// attributes, i. e. those which in the form `name = "value"` or `name = 'value'`.
    /// HTML style attributes (i. e. without quotes or only name) will return a error.
    ///
    /// # Parameters
    /// - `buf`: a buffer with a tag name and attributes, usually this is the whole
    ///   string between `<` and `>` (or `/>`) of a tag;
    /// - `pos`: a position in the `buf` where tag name is finished and attributes
    ///   is started. It is not necessary to point exactly to the end of a tag name,
    ///   although that is usually that. If it will be more than the `buf` length,
    ///   then the iterator will return `None`` immediately.
    ///
    /// # Example
    /// ```
    /// # use quick_xml::events::attributes::{Attribute, Attributes};
    /// # use pretty_assertions::assert_eq;
    /// #
    /// let mut iter = Attributes::new("tag-name attr1 = 'value1' attr2='value2' ", 9);
    /// //                              ^0       ^9
    /// assert_eq!(iter.next(), Some(Ok(Attribute::from(("attr1", "value1")))));
    /// assert_eq!(iter.next(), Some(Ok(Attribute::from(("attr2", "value2")))));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub const fn new(buf: &'a str, pos: usize) -> Self {
        Self::wrap(buf.as_bytes(), pos, false, Decoder::utf8())
    }

    /// Creates a new attribute iterator from a buffer, allowing HTML attribute syntax.
    ///
    /// # Parameters
    /// - `buf`: a buffer with a tag name and attributes, usually this is the whole
    ///   string between `<` and `>` (or `/>`) of a tag;
    /// - `pos`: a position in the `buf` where tag name is finished and attributes
    ///   is started. It is not necessary to point exactly to the end of a tag name,
    ///   although that is usually that. If it will be more than the `buf` length,
    ///   then the iterator will return `None`` immediately.
    ///
    /// # Example
    /// ```
    /// # use quick_xml::events::attributes::{Attribute, Attributes};
    /// # use pretty_assertions::assert_eq;
    /// #
    /// let mut iter = Attributes::html("tag-name attr1 = value1 attr2 ", 9);
    /// //                               ^0       ^9
    /// assert_eq!(iter.next(), Some(Ok(Attribute::from(("attr1", "value1")))));
    /// assert_eq!(iter.next(), Some(Ok(Attribute::from(("attr2", "")))));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub const fn html(buf: &'a str, pos: usize) -> Self {
        Self::wrap(buf.as_bytes(), pos, true, Decoder::utf8())
    }

    /// Changes whether attributes should be checked for uniqueness.
    ///
    /// The XML specification requires attribute keys in the same element to be unique. This check
    /// can be disabled to improve performance slightly.
    ///
    /// (`true` by default)
    pub fn with_checks(&mut self, val: bool) -> &mut Attributes<'a> {
        self.state.check_duplicates = val;
        self
    }

    /// Checks if the current tag has a [`xsi:nil`] attribute. This method ignores any errors in
    /// attributes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::Event;
    /// use quick_xml::name::QName;
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str("
    ///     <root xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>
    ///         <true xsi:nil='true'/>
    ///         <false xsi:nil='false'/>
    ///         <none/>
    ///         <non-xsi xsi:nil='true' xmlns:xsi='namespace'/>
    ///         <unbound-nil nil='true' xmlns='http://www.w3.org/2001/XMLSchema-instance'/>
    ///         <another-xmlns f:nil='true' xmlns:f='http://www.w3.org/2001/XMLSchema-instance'/>
    ///     </root>
    /// ");
    /// reader.config_mut().trim_text(true);
    ///
    /// macro_rules! check {
    ///     ($reader:expr, $name:literal, $value:literal) => {
    ///         let event = match $reader.read_event().unwrap() {
    ///             Event::Empty(e) => e,
    ///             e => panic!("Unexpected event {:?}", e),
    ///         };
    ///         assert_eq!(
    ///             (event.name(), event.attributes().has_nil($reader.resolver())),
    ///             (QName($name.as_bytes()), $value),
    ///         );
    ///     };
    /// }
    ///
    /// let root = match reader.read_event().unwrap() {
    ///     Event::Start(e) => e,
    ///     e => panic!("Unexpected event {:?}", e),
    /// };
    /// assert_eq!(root.attributes().has_nil(reader.resolver()), false);
    ///
    /// // definitely true
    /// check!(reader, "true",          true);
    /// // definitely false
    /// check!(reader, "false",         false);
    /// // absence of the attribute means that attribute is not set
    /// check!(reader, "none",          false);
    /// // attribute not bound to the correct namespace
    /// check!(reader, "non-xsi",       false);
    /// // attributes without prefix not bound to any namespace
    /// check!(reader, "unbound-nil",   false);
    /// // prefix can be any while it is bound to the correct namespace
    /// check!(reader, "another-xmlns", true);
    /// ```
    ///
    /// [`xsi:nil`]: https://www.w3.org/TR/xmlschema-1/#xsi_nil
    pub fn has_nil(&mut self, resolver: &NamespaceResolver) -> bool {
        use crate::name::ResolveResult::*;

        self.any(|attr| {
            if let Ok(attr) = attr {
                match resolver.resolve_attribute(attr.key) {
                    (
                        Bound(Namespace(b"http://www.w3.org/2001/XMLSchema-instance")),
                        LocalName(b"nil"),
                    ) => attr.as_bool().unwrap_or_default(),
                    _ => false,
                }
            } else {
                false
            }
        })
    }

    /// Get the decoder, used to decode bytes, read by the reader which produces
    /// this iterator, to the strings.
    ///
    /// When iterator was created manually or get from a manually created [`BytesStart`],
    /// encoding is UTF-8.
    ///
    /// If [`encoding`] feature is enabled and no encoding is specified in declaration,
    /// defaults to UTF-8.
    ///
    /// [`BytesStart`]: crate::events::BytesStart
    /// [`encoding`]: ../index.html#encoding
    #[inline]
    pub const fn decoder(&self) -> Decoder {
        self.decoder
    }
}

impl<'a> Debug for Attributes<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Attributes")
            .field("bytes", &Bytes(self.bytes))
            .field("state", &self.state)
            .field("decoder", &self.decoder)
            .finish()
    }
}

impl<'a> Iterator for Attributes<'a> {
    type Item = Result<Attribute<'a>, AttrError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.state.next(self.bytes) {
            None => None,
            Some(Ok(a)) => Some(Ok(a.map(|range| &self.bytes[range]).into())),
            Some(Err(e)) => Some(Err(e)),
        }
    }
}

impl<'a> FusedIterator for Attributes<'a> {}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Errors that can be raised during parsing attributes.
///
/// Recovery position in examples shows the position from which parsing of the
/// next attribute will be attempted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttrError {
    /// Attribute key was not followed by `=`, position relative to the start of
    /// the owning tag is provided.
    ///
    /// Example of input that raises this error:
    ///
    /// ```xml
    /// <tag key another="attribute"/>
    /// <!--     ^~~ error position, recovery position (8) -->
    /// ```
    ///
    /// This error can be raised only when the iterator is in XML mode.
    ExpectedEq(usize),
    /// Attribute value was not found after `=`, position relative to the start
    /// of the owning tag is provided.
    ///
    /// Example of input that raises this error:
    ///
    /// ```xml
    /// <tag key = />
    /// <!--       ^~~ error position, recovery position (10) -->
    /// ```
    ///
    /// This error can be returned only for the last attribute in the list,
    /// because otherwise any content after `=` will be threated as a value.
    /// The XML
    ///
    /// ```xml
    /// <tag key = another-key = "value"/>
    /// <!--                   ^ ^- recovery position (24) -->
    /// <!--                   '~~ error position (22) -->
    /// ```
    ///
    /// will be treated as `Attribute { key = b"key", value = b"another-key" }`
    /// and or [`Attribute`] is returned, or [`AttrError::UnquotedValue`] is raised,
    /// depending on the parsing mode.
    ExpectedValue(usize),
    /// Attribute value is not quoted, position relative to the start of the
    /// owning tag is provided.
    ///
    /// Example of input that raises this error:
    ///
    /// ```xml
    /// <tag key = value />
    /// <!--       ^    ^~~ recovery position (15) -->
    /// <!--       '~~ error position (10) -->
    /// ```
    ///
    /// This error can be raised only when the iterator is in XML mode.
    UnquotedValue(usize),
    /// Attribute value was not finished with a matching quote, position relative
    /// to the start of owning tag and a quote is provided. That position is always
    /// a last character in the tag content.
    ///
    /// Example of input that raises this error:
    ///
    /// ```xml
    /// <tag key = "value  />
    /// <tag key = 'value  />
    /// <!--               ^~~ error position, recovery position (18) -->
    /// ```
    ///
    /// This error can be returned only for the last attribute in the list,
    /// because all input was consumed during scanning for a quote.
    ExpectedQuote(usize, u8),
    /// An attribute with the same name was already encountered. Two parameters
    /// define (1) the error position relative to the start of the owning tag
    /// for a new attribute and (2) the start position of a previously encountered
    /// attribute with the same name.
    ///
    /// Example of input that raises this error:
    ///
    /// ```xml
    /// <tag key = 'value'  key="value2" attr3='value3' />
    /// <!-- ^              ^            ^~~ recovery position (32) -->
    /// <!-- |              '~~ error position (19) -->
    /// <!-- '~~ previous position (4) -->
    /// ```
    ///
    /// This error is returned only when [`Attributes::with_checks()`] is set
    /// to `true` (that is default behavior).
    Duplicated(usize, usize),
}

impl Display for AttrError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::ExpectedEq(pos) => write!(
                f,
                r#"position {}: attribute key must be directly followed by `=` or space"#,
                pos
            ),
            Self::ExpectedValue(pos) => write!(
                f,
                r#"position {}: `=` must be followed by an attribute value"#,
                pos
            ),
            Self::UnquotedValue(pos) => write!(
                f,
                r#"position {}: attribute value must be enclosed in `"` or `'`"#,
                pos
            ),
            Self::ExpectedQuote(pos, quote) => write!(
                f,
                r#"position {}: missing closing quote `{}` in attribute value"#,
                pos, *quote as char
            ),
            Self::Duplicated(pos1, pos2) => write!(
                f,
                r#"position {}: duplicated attribute, previous declaration at position {}"#,
                pos1, pos2
            ),
        }
    }
}

impl std::error::Error for AttrError {}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A struct representing a key/value XML or HTML [attribute].
///
/// [attribute]: https://www.w3.org/TR/xml11/#NT-Attribute
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Attr<T> {
    /// Attribute with value enclosed in double quotes (`"`). Attribute key and
    /// value provided. This is a canonical XML-style attribute.
    DoubleQ(T, T),
    /// Attribute with value enclosed in single quotes (`'`). Attribute key and
    /// value provided. This is an XML-style attribute.
    SingleQ(T, T),
    /// Attribute with value not enclosed in quotes. Attribute key and value
    /// provided. This is HTML-style attribute, it can be returned in HTML-mode
    /// parsing only. In an XML mode [`AttrError::UnquotedValue`] will be raised
    /// instead.
    ///
    /// Attribute value can be invalid according to the [HTML specification],
    /// in particular, it can contain `"`, `'`, `=`, `<`, and <code>&#96;</code>
    /// characters. The absence of the `>` character is nevertheless guaranteed,
    /// since the parser extracts [events] based on them even before the start
    /// of parsing attributes.
    ///
    /// [HTML specification]: https://html.spec.whatwg.org/#unquoted
    /// [events]: crate::events::Event::Start
    Unquoted(T, T),
    /// Attribute without value. Attribute key provided. This is HTML-style attribute,
    /// it can be returned in HTML-mode parsing only. In XML mode
    /// [`AttrError::ExpectedEq`] will be raised instead.
    Empty(T),
}

impl<T> Attr<T> {
    /// Maps an `Attr<T>` to `Attr<U>` by applying a function to a contained key and value.
    #[inline]
    pub fn map<U, F>(self, mut f: F) -> Attr<U>
    where
        F: FnMut(T) -> U,
    {
        match self {
            Attr::DoubleQ(key, value) => Attr::DoubleQ(f(key), f(value)),
            Attr::SingleQ(key, value) => Attr::SingleQ(f(key), f(value)),
            Attr::Empty(key) => Attr::Empty(f(key)),
            Attr::Unquoted(key, value) => Attr::Unquoted(f(key), f(value)),
        }
    }
}

impl<'a> Attr<&'a [u8]> {
    /// Returns the key value
    #[inline]
    pub const fn key(&self) -> QName<'a> {
        QName(match self {
            Attr::DoubleQ(key, _) => key,
            Attr::SingleQ(key, _) => key,
            Attr::Empty(key) => key,
            Attr::Unquoted(key, _) => key,
        })
    }
    /// Returns the attribute value. For [`Self::Empty`] variant an empty slice
    /// is returned according to the [HTML specification].
    ///
    /// [HTML specification]: https://www.w3.org/TR/2012/WD-html-markup-20120329/syntax.html#syntax-attr-empty
    #[inline]
    pub const fn value(&self) -> &'a [u8] {
        match self {
            Attr::DoubleQ(_, value) => value,
            Attr::SingleQ(_, value) => value,
            Attr::Empty(_) => &[],
            Attr::Unquoted(_, value) => value,
        }
    }
}

impl<T: AsRef<[u8]>> Debug for Attr<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Attr::DoubleQ(key, value) => f
                .debug_tuple("Attr::DoubleQ")
                .field(&Bytes(key.as_ref()))
                .field(&Bytes(value.as_ref()))
                .finish(),
            Attr::SingleQ(key, value) => f
                .debug_tuple("Attr::SingleQ")
                .field(&Bytes(key.as_ref()))
                .field(&Bytes(value.as_ref()))
                .finish(),
            Attr::Empty(key) => f
                .debug_tuple("Attr::Empty")
                // Comment to prevent formatting and keep style consistent
                .field(&Bytes(key.as_ref()))
                .finish(),
            Attr::Unquoted(key, value) => f
                .debug_tuple("Attr::Unquoted")
                .field(&Bytes(key.as_ref()))
                .field(&Bytes(value.as_ref()))
                .finish(),
        }
    }
}

/// Unpacks attribute key and value into tuple of this two elements.
/// `None` value element is returned only for [`Attr::Empty`] variant.
impl<T> From<Attr<T>> for (T, Option<T>) {
    #[inline]
    fn from(attr: Attr<T>) -> Self {
        match attr {
            Attr::DoubleQ(key, value) => (key, Some(value)),
            Attr::SingleQ(key, value) => (key, Some(value)),
            Attr::Empty(key) => (key, None),
            Attr::Unquoted(key, value) => (key, Some(value)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type AttrResult = Result<Attr<Range<usize>>, AttrError>;

#[derive(Clone, Copy, Debug)]
enum State {
    /// Iteration finished, iterator will return `None` to all [`IterState::next`]
    /// requests.
    Done,
    /// The last attribute returned was deserialized successfully. Contains an
    /// offset from which next attribute should be searched.
    Next(usize),
    /// The last attribute returns [`AttrError::UnquotedValue`], offset pointed
    /// to the beginning of the value. Recover should skip a value
    SkipValue(usize),
    /// The last attribute returns [`AttrError::Duplicated`], offset pointed to
    /// the equal (`=`) sign. Recover should skip it and a value
    SkipEqValue(usize),
}

/// Number of attributes a start tag may have before the duplicate-name check
/// switches from a direct linear scan of the previously seen names to a hash
/// pre-filter (see [`IterState::check_for_duplicates`]).
///
/// Real-world start tags carry only a handful of attributes -- the busiest
/// element in our benchmark corpus (`tests/documents/players.xml`) has 22 --
/// where the scan is faster than hashing and needs no allocation. Larger tags
/// are where the scan became the O(N²) CPU-DoS of [#969], so above this count we
/// pay for a hash set to keep the whole tag O(N). The value sits just above the
/// measured linear-vs-hash crossover.
///
/// [#969]: https://github.com/tafia/quick-xml/issues/969
const SMALL_ATTRIBUTE_COUNT: usize = 32;

/// A no-op [`Hasher`] for the `key_hashes` set, whose values are already 64-bit
/// hashes of attribute names; re-hashing them with the default SipHash would be
/// wasted work. Only `write_u64` is ever exercised (via `u64`'s `Hash` impl).
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _: &[u8]) {
        // The set only ever stores `u64` keys, which route through `write_u64`.
        unreachable!("IdentityHasher only supports u64 keys")
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

/// Hashes a single attribute name. A fresh [`DefaultHasher`] per name keeps each
/// hash independent (so it is also DoS-resistant on untrusted input).
#[inline]
fn hash_name(name: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(name);
    hasher.finish()
}

/// External iterator over spans of attribute key and value
#[derive(Clone, Debug)]
pub(crate) struct IterState {
    /// Iteration state that determines what actions should be done before the
    /// actual parsing of the next attribute
    state: State,
    /// If `true`, enables ability to parse unquoted values and key-only (empty)
    /// attributes
    html: bool,
    /// If `true`, checks for duplicate names
    check_duplicates: bool,
    /// If `check_duplicates` is set, contains the ranges of already parsed attribute
    /// names. We store a ranges instead of slices to able to report a previous
    /// attribute position
    keys: Vec<Range<usize>>,
    /// 64-bit hashes of the byte content of `keys`, used as an O(1) pre-filter
    /// once a start tag declares more than `SMALL_ATTRIBUTE_COUNT` attributes, so
    /// the duplicate check stays O(N) over the whole tag instead of O(N²). The
    /// values are already hashes, so the set stores them with `IdentityHasher`
    /// instead of re-hashing. Allocated only when the threshold is crossed, so
    /// small tags (and [`IterState::new`]) stay allocation-free and `const`.
    key_hashes: Option<HashSet<u64, BuildHasherDefault<IdentityHasher>>>,
}

impl IterState {
    pub const fn new(offset: usize, html: bool) -> Self {
        Self {
            state: State::Next(offset),
            html,
            check_duplicates: true,
            keys: Vec::new(),
            key_hashes: None,
        }
    }

    /// Recover from an error that could have been made on a previous step.
    /// Returns an offset from which parsing should continue.
    /// If there no input left, returns `None`.
    fn recover(&self, slice: &[u8]) -> Option<usize> {
        match self.state {
            State::Done => None,
            State::Next(offset) => Some(offset),
            State::SkipValue(offset) => self.skip_value(slice, offset),
            State::SkipEqValue(offset) => self.skip_eq_value(slice, offset),
        }
    }

    /// Skip all characters up to first space symbol or end-of-input
    #[inline]
    #[allow(clippy::manual_map)]
    fn skip_value(&self, slice: &[u8], offset: usize) -> Option<usize> {
        let mut iter = (offset..).zip(slice[offset..].iter());

        match iter.find(|(_, &b)| is_whitespace(b)) {
            // Input: `    key  =  value `
            //                     |    ^
            //                offset    e
            Some((e, _)) => Some(e),
            // Input: `    key  =  value`
            //                     |    ^
            //                offset    e = len()
            None => None,
        }
    }

    /// Skip all characters up to first space symbol or end-of-input
    #[inline]
    fn skip_eq_value(&self, slice: &[u8], offset: usize) -> Option<usize> {
        let mut iter = (offset..).zip(slice[offset..].iter());

        // Skip all up to the quote and get the quote type
        let quote = match iter.find(|(_, &b)| !is_whitespace(b)) {
            // Input: `    key  =  "`
            //                  |  ^
            //             offset
            Some((_, b'"')) => b'"',
            // Input: `    key  =  '`
            //                  |  ^
            //             offset
            Some((_, b'\'')) => b'\'',

            // Input: `    key  =  x`
            //                  |  ^
            //             offset
            Some((offset, _)) => return self.skip_value(slice, offset),
            // Input: `    key  =  `
            //                  |  ^
            //             offset
            None => return None,
        };

        match iter.find(|(_, &b)| b == quote) {
            // Input: `    key  =  "   "`
            //                         ^
            Some((e, b'"')) => Some(e),
            // Input: `    key  =  '   '`
            //                         ^
            Some((e, _)) => Some(e),

            // Input: `    key  =  "   `
            // Input: `    key  =  '   `
            //                         ^
            // Closing quote not found
            None => None,
        }
    }

    /// Checks that the attribute name `key` (a range into `slice`) was not seen
    /// earlier in the same start tag, recording it for subsequent checks.
    ///
    /// Small tags use a direct linear scan of [`Self::keys`]: for a handful of
    /// attributes that beats hashing and needs no allocation, which is the
    /// overwhelmingly common case. Once a tag declares more than
    /// `SMALL_ATTRIBUTE_COUNT` attributes -- where the scan would become the
    /// O(N²) CPU-DoS of [#969] -- it switches to a hash pre-filter that keeps the
    /// whole tag O(N).
    ///
    /// [#969]: https://github.com/tafia/quick-xml/issues/969
    #[inline]
    fn check_for_duplicates(
        &mut self,
        slice: &[u8],
        key: Range<usize>,
    ) -> Result<Range<usize>, AttrError> {
        if self.check_duplicates {
            if self.keys.len() >= SMALL_ATTRIBUTE_COUNT {
                return self.check_for_duplicates_hashed(slice, key);
            }
            if let Some(prev) = self
                .keys
                .iter()
                .find(|r| slice[(*r).clone()] == slice[key.clone()])
            {
                return Err(AttrError::Duplicated(key.start, prev.start));
            }
            self.keys.push(key.clone());
        }
        Ok(key)
    }

    /// Cold path of [`Self::check_for_duplicates`] for start tags with many
    /// attributes: a [`HashSet`] of 64-bit name hashes acts as an O(1) pre-filter
    /// so iterating N attributes is O(N) rather than O(N²).
    #[cold]
    fn check_for_duplicates_hashed(
        &mut self,
        slice: &[u8],
        key: Range<usize>,
    ) -> Result<Range<usize>, AttrError> {
        let keys = &self.keys;
        let key_hashes = self.key_hashes.get_or_insert_with(|| {
            // First time over the threshold: seed the set with the names already
            // collected during the linear phase so the pre-filter knows them.
            let mut set = HashSet::with_capacity_and_hasher(
                keys.len() * 2,
                BuildHasherDefault::<IdentityHasher>::default(),
            );
            for r in keys {
                set.insert(hash_name(&slice[r.clone()]));
            }
            set
        });
        // A fresh hash proves the name is new. On a hit (a real duplicate, or the
        // astronomically rare 64-bit collision) fall back to the linear scan to
        // recover the exact previous position for `AttrError::Duplicated`.
        if !key_hashes.insert(hash_name(&slice[key.clone()])) {
            if let Some(prev) = self
                .keys
                .iter()
                .find(|r| slice[(*r).clone()] == slice[key.clone()])
            {
                return Err(AttrError::Duplicated(key.start, prev.start));
            }
        }
        self.keys.push(key.clone());
        Ok(key)
    }

    /// # Parameters
    ///
    /// - `slice`: content of the tag, used for checking for duplicates
    /// - `key`: Range of key in slice, if iterator in HTML mode
    /// - `offset`: Position of error if iterator in XML mode
    #[inline]
    fn key_only(&mut self, slice: &[u8], key: Range<usize>, offset: usize) -> Option<AttrResult> {
        Some(if self.html {
            self.check_for_duplicates(slice, key).map(Attr::Empty)
        } else {
            Err(AttrError::ExpectedEq(offset))
        })
    }

    #[inline]
    fn double_q(&mut self, key: Range<usize>, value: Range<usize>) -> Option<AttrResult> {
        self.state = State::Next(value.end + 1); // +1 for `"`

        Some(Ok(Attr::DoubleQ(key, value)))
    }

    #[inline]
    fn single_q(&mut self, key: Range<usize>, value: Range<usize>) -> Option<AttrResult> {
        self.state = State::Next(value.end + 1); // +1 for `'`

        Some(Ok(Attr::SingleQ(key, value)))
    }

    pub fn next(&mut self, slice: &[u8]) -> Option<AttrResult> {
        let mut iter = match self.recover(slice) {
            Some(offset) => (offset..).zip(slice[offset..].iter()),
            None => return None,
        };

        // Index where next key started
        let start_key = match iter.find(|(_, &b)| !is_whitespace(b)) {
            // Input: `    key`
            //             ^
            Some((s, _)) => s,
            // Input: `    `
            //             ^
            None => {
                // Because we reach end-of-input, stop iteration on next call
                self.state = State::Done;
                return None;
            }
        };
        // Span of a key
        let (key, offset) = match iter.find(|(_, &b)| b == b'=' || is_whitespace(b)) {
            // Input: `    key=`
            //             |  ^
            //             s  e
            Some((e, b'=')) => (start_key..e, e),

            // Input: `    key `
            //                ^
            Some((e, _)) => match iter.find(|(_, &b)| !is_whitespace(b)) {
                // Input: `    key  =`
                //             |  | ^
                //     start_key  e
                Some((offset, b'=')) => (start_key..e, offset),
                // Input: `    key  x`
                //             |  | ^
                //     start_key  e
                // If HTML-like attributes is allowed, this is the result, otherwise error
                Some((offset, _)) => {
                    // In any case, recovering is not required
                    self.state = State::Next(offset);
                    return self.key_only(slice, start_key..e, offset);
                }
                // Input: `    key  `
                //             |  | ^
                //     start_key  e
                // If HTML-like attributes is allowed, this is the result, otherwise error
                None => {
                    // Because we reach end-of-input, stop iteration on next call
                    self.state = State::Done;
                    return self.key_only(slice, start_key..e, slice.len());
                }
            },

            // Input: `    key`
            //             |  ^
            //             s  e = len()
            // If HTML-like attributes is allowed, this is the result, otherwise error
            None => {
                // Because we reach end-of-input, stop iteration on next call
                self.state = State::Done;
                let e = slice.len();
                return self.key_only(slice, start_key..e, e);
            }
        };

        let key = match self.check_for_duplicates(slice, key) {
            Err(e) => {
                self.state = State::SkipEqValue(offset);
                return Some(Err(e));
            }
            Ok(key) => key,
        };

        ////////////////////////////////////////////////////////////////////////

        // Gets the position of quote and quote type
        let (start_value, quote) = match iter.find(|(_, &b)| !is_whitespace(b)) {
            // Input: `    key  =  "`
            //                     ^
            Some((s, b'"')) => (s + 1, b'"'),
            // Input: `    key  =  '`
            //                     ^
            Some((s, b'\'')) => (s + 1, b'\''),

            // Input: `    key  =  x`
            //                     ^
            // If HTML-like attributes is allowed, this is the start of the value
            Some((s, _)) if self.html => {
                // We do not check validity of attribute value characters as required
                // according to https://html.spec.whatwg.org/#unquoted. It can be done
                // during validation phase
                let end = match iter.find(|(_, &b)| is_whitespace(b)) {
                    // Input: `    key  =  value `
                    //                     |    ^
                    //                     s    e
                    Some((e, _)) => e,
                    // Input: `    key  =  value`
                    //                     |    ^
                    //                     s    e = len()
                    None => slice.len(),
                };
                self.state = State::Next(end);
                return Some(Ok(Attr::Unquoted(key, s..end)));
            }
            // Input: `    key  =  x`
            //                     ^
            Some((s, _)) => {
                self.state = State::SkipValue(s);
                return Some(Err(AttrError::UnquotedValue(s)));
            }

            // Input: `    key  =  `
            //                     ^
            None => {
                // Because we reach end-of-input, stop iteration on next call
                self.state = State::Done;
                return Some(Err(AttrError::ExpectedValue(slice.len())));
            }
        };

        match iter.find(|(_, &b)| b == quote) {
            // Input: `    key  =  "   "`
            //                         ^
            Some((e, b'"')) => self.double_q(key, start_value..e),
            // Input: `    key  =  '   '`
            //                         ^
            Some((e, _)) => self.single_q(key, start_value..e),

            // Input: `    key  =  "   `
            // Input: `    key  =  '   `
            //                         ^
            // Closing quote not found
            None => {
                // Because we reach end-of-input, stop iteration on next call
                self.state = State::Done;
                Some(Err(AttrError::ExpectedQuote(slice.len(), quote)))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks, how parsing of XML-style attributes works. Each attribute should
/// have a value, enclosed in single or double quotes.
#[cfg(test)]
mod xml {
    use super::*;
    use pretty_assertions::assert_eq;

    mod attribute_value_normalization {
        use super::*;
        use crate::errors::Error;
        use crate::escape::EscapeError::*;
        use crate::XmlVersion::*;
        use pretty_assertions::assert_eq;

        /// Empty values returned are unchanged
        #[test]
        fn empty() {
            let raw_value = "".as_bytes();
            let attr = Attribute::from(("foo".as_bytes(), raw_value));

            let value = attr
                .decoded_and_normalized_value(Implicit1_0, Decoder::utf8())
                .unwrap();
            assert_eq!(value, "");
            // assert_eq! does not check if value is borrowed, but this is important
            assert!(matches!(value, Cow::Borrowed(_)));

            let value = attr
                .decoded_and_normalized_value(Explicit1_0, Decoder::utf8())
                .unwrap();
            assert_eq!(value, "");
            // assert_eq! does not check if value is borrowed, but this is important
            assert!(matches!(value, Cow::Borrowed(_)));

            let value = attr
                .decoded_and_normalized_value(Explicit1_1, Decoder::utf8())
                .unwrap();
            assert_eq!(value, "");
            // assert_eq! does not check if value is borrowed, but this is important
            assert!(matches!(value, Cow::Borrowed(_)));
        }

        /// Already normalized values are returned unchanged
        #[test]
        fn already_normalized() {
            let raw_value = "foobar123".as_bytes();
            let attr = Attribute::from(("foo".as_bytes(), raw_value));

            let value = attr
                .decoded_and_normalized_value(Implicit1_0, Decoder::utf8())
                .unwrap();
            assert_eq!(value, "foobar123");
            // assert_eq! does not check if value is borrowed, but this is important
            assert!(matches!(value, Cow::Borrowed(_)));

            let value = attr
                .decoded_and_normalized_value(Explicit1_0, Decoder::utf8())
                .unwrap();
            assert_eq!(value, "foobar123");
            // assert_eq! does not check if value is borrowed, but this is important
            assert!(matches!(value, Cow::Borrowed(_)));

            let value = attr
                .decoded_and_normalized_value(Explicit1_1, Decoder::utf8())
                .unwrap();
            assert_eq!(value, "foobar123");
            // assert_eq! does not check if value is borrowed, but this is important
            assert!(matches!(value, Cow::Borrowed(_)));
        }

        /// Return, tab, and newline characters (0xD, 0x9, 0xA) must be substituted with
        /// a space character, \r\n and \r\u{85} should be replaced by one space in 1.1
        #[test]
        fn space_replacement() {
            let raw_value = "\r\nfoo\u{85}\u{2028}\rbar\tbaz\n\ndelta\n\r\u{85}".as_bytes();
            let attr = Attribute::from(("foo".as_bytes(), raw_value));

            assert_eq!(
                attr.decoded_and_normalized_value(Implicit1_0, Decoder::utf8())
                    .unwrap(),
                " foo\u{85}\u{2028} bar baz  delta  \u{85}"
            );
            assert_eq!(
                attr.decoded_and_normalized_value(Explicit1_0, Decoder::utf8())
                    .unwrap(),
                " foo\u{85}\u{2028} bar baz  delta  \u{85}"
            );
            assert_eq!(
                attr.decoded_and_normalized_value(Explicit1_1, Decoder::utf8())
                    .unwrap(),
                " foo   bar baz  delta  "
            );
        }

        /// Entities must be terminated
        #[test]
        fn unterminated_entity() {
            let raw_value = "abc&quotdef".as_bytes();
            let attr = Attribute::from(("foo".as_bytes(), raw_value));

            match attr.decoded_and_normalized_value(Implicit1_0, Decoder::utf8()) {
                Err(Error::Escape(err)) => assert_eq!(err, UnterminatedEntity(3..11)),
                x => panic!("Expected Err(Escape(_)), got {:?}", x),
            }

            match attr.decoded_and_normalized_value(Explicit1_0, Decoder::utf8()) {
                Err(Error::Escape(err)) => assert_eq!(err, UnterminatedEntity(3..11)),
                x => panic!("Expected Err(Escape(_)), got {:?}", x),
            }

            match attr.decoded_and_normalized_value(Explicit1_1, Decoder::utf8()) {
                Err(Error::Escape(err)) => assert_eq!(err, UnterminatedEntity(3..11)),
                x => panic!("Expected Err(Escape(_)), got {:?}", x),
            }
        }

        /// Unknown entities raise error
        #[test]
        fn unrecognized_entity() {
            let raw_value = "abc&unkn;def".as_bytes();
            let attr = Attribute::from(("foo".as_bytes(), raw_value));

            match attr.decoded_and_normalized_value(Implicit1_0, Decoder::utf8()) {
                // TODO: is this divergence between range behavior of UnterminatedEntity
                // and UnrecognizedEntity appropriate? existing unescape code behaves the same.  (see: start index)
                Err(Error::Escape(err)) => {
                    assert_eq!(err, UnrecognizedEntity(4..8, "unkn".to_owned()))
                }
                x => panic!("Expected Err(Escape(err)), got {:?}", x),
            }
            match attr.decoded_and_normalized_value(Explicit1_0, Decoder::utf8()) {
                // TODO: is this divergence between range behavior of UnterminatedEntity
                // and UnrecognizedEntity appropriate? existing unescape code behaves the same.  (see: start index)
                Err(Error::Escape(err)) => {
                    assert_eq!(err, UnrecognizedEntity(4..8, "unkn".to_owned()))
                }
                x => panic!("Expected Err(Escape(err)), got {:?}", x),
            }
            match attr.decoded_and_normalized_value(Explicit1_1, Decoder::utf8()) {
                // TODO: is this divergence between range behavior of UnterminatedEntity
                // and UnrecognizedEntity appropriate? existing unescape code behaves the same.  (see: start index)
                Err(Error::Escape(err)) => {
                    assert_eq!(err, UnrecognizedEntity(4..8, "unkn".to_owned()))
                }
                x => panic!("Expected Err(Escape(err)), got {:?}", x),
            }
        }

        /// custom entity replacement works, entity replacement text processed recursively
        #[test]
        fn entity_replacement() {
            let raw_value = "&d;&d;A&a;&#x20;&a;B&da;".as_bytes();
            let attr = Attribute::from(("foo".as_bytes(), raw_value));
            fn custom_resolver(ent: &str) -> Option<&'static str> {
                match ent {
                    "d" => Some("&#xD;"),
                    "a" => Some("&#xA;"),
                    "da" => Some("&#xD;&#xA;"),
                    _ => None,
                }
            }

            assert_eq!(
                attr.decoded_and_normalized_value_with(
                    Implicit1_0,
                    Decoder::utf8(),
                    5,
                    &custom_resolver
                )
                .unwrap(),
                "\r\rA\n \nB\r\n"
            );
            assert_eq!(
                attr.decoded_and_normalized_value_with(
                    Explicit1_0,
                    Decoder::utf8(),
                    5,
                    &custom_resolver
                )
                .unwrap(),
                "\r\rA\n \nB\r\n"
            );
            assert_eq!(
                attr.decoded_and_normalized_value_with(
                    Explicit1_1,
                    Decoder::utf8(),
                    5,
                    &custom_resolver
                )
                .unwrap(),
                "\r\rA\n \nB\r\n"
            );
        }

        #[test]
        fn char_references() {
            // character literal references are substituted without being replaced by spaces
            let raw_value = "&#xd;&#xd;A&#xa;&#xa;B&#xd;&#xa;".as_bytes();
            let attr = Attribute::from(("foo".as_bytes(), raw_value));

            assert_eq!(
                attr.decoded_and_normalized_value(Implicit1_0, Decoder::utf8())
                    .unwrap(),
                "\r\rA\n\nB\r\n"
            );
            assert_eq!(
                attr.decoded_and_normalized_value(Explicit1_0, Decoder::utf8())
                    .unwrap(),
                "\r\rA\n\nB\r\n"
            );
            assert_eq!(
                attr.decoded_and_normalized_value(Explicit1_1, Decoder::utf8())
                    .unwrap(),
                "\r\rA\n\nB\r\n"
            );
        }
    }

    /// Checked attribute is the single attribute
    mod single {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Attribute have a value enclosed in single quotes
        #[test]
        fn single_quoted() {
            let mut iter = Attributes::new(r#"tag key='value'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value enclosed in double quotes
        #[test]
        fn double_quoted() {
            let mut iter = Attributes::new(r#"tag key="value""#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value, not enclosed in quotes
        #[test]
        fn unquoted() {
            let mut iter = Attributes::new(r#"tag key=value"#, 3);
            //                                0       ^ = 8

            assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(8))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Only attribute key is present
        #[test]
        fn key_only() {
            let mut iter = Attributes::new(r#"tag key"#, 3);
            //                                0      ^ = 7

            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(7))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key is started with an invalid symbol (a single quote in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_start_invalid() {
            let mut iter = Attributes::new(r#"tag 'key'='value'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'key'"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key contains an invalid symbol (an ampersand in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_contains_invalid() {
            let mut iter = Attributes::new(r#"tag key&jey='value'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key&jey"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute value is missing after `=`
        #[test]
        fn missed_value() {
            let mut iter = Attributes::new(r#"tag key="#, 3);
            //                                0       ^ = 8

            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedValue(8))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }

    /// Checked attribute is the first attribute in the list of many attributes
    mod first {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Attribute have a value enclosed in single quotes
        #[test]
        fn single_quoted() {
            let mut iter = Attributes::new(r#"tag key='value' regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value enclosed in double quotes
        #[test]
        fn double_quoted() {
            let mut iter = Attributes::new(r#"tag key="value" regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value, not enclosed in quotes
        #[test]
        fn unquoted() {
            let mut iter = Attributes::new(r#"tag key=value regular='attribute'"#, 3);
            //                                0       ^ = 8

            assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(8))));
            // check error recovery
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Only attribute key is present
        #[test]
        fn key_only() {
            let mut iter = Attributes::new(r#"tag key regular='attribute'"#, 3);
            //                                0       ^ = 8

            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(8))));
            // check error recovery
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key is started with an invalid symbol (a single quote in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_start_invalid() {
            let mut iter = Attributes::new(r#"tag 'key'='value' regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'key'"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key contains an invalid symbol (an ampersand in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_contains_invalid() {
            let mut iter = Attributes::new(r#"tag key&jey='value' regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key&jey"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute value is missing after `=`.
        #[test]
        fn missed_value() {
            let mut iter = Attributes::new(r#"tag key= regular='attribute'"#, 3);
            //                                0        ^ = 9

            assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(9))));
            // Because we do not check validity of keys and values during parsing,
            // "error='recovery'" is considered, as unquoted attribute value and
            // skipped during recovery and iteration finished
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);

            ////////////////////////////////////////////////////////////////////

            let mut iter = Attributes::new(r#"tag key= regular= 'attribute'"#, 3);
            //                                0        ^ = 9               ^ = 29

            // In that case "regular=" considered as unquoted value
            assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(9))));
            // In that case "'attribute'" considered as a key, because we do not check
            // validity of key names
            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(29))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);

            ////////////////////////////////////////////////////////////////////

            let mut iter = Attributes::new(r#"tag key= regular ='attribute'"#, 3);
            //                                0        ^ = 9               ^ = 29

            // In that case "regular" considered as unquoted value
            assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(9))));
            // In that case "='attribute'" considered as a key, because we do not check
            // validity of key names
            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(29))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);

            ////////////////////////////////////////////////////////////////////

            let mut iter = Attributes::new(r#"tag key= regular = 'attribute'"#, 3);
            //                                0        ^ = 9     ^ = 19     ^ = 30

            assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(9))));
            // In that case second "=" considered as a key, because we do not check
            // validity of key names
            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(19))));
            // In that case "'attribute'" considered as a key, because we do not check
            // validity of key names
            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(30))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }

    /// Copy of single, but with additional spaces in markup
    mod sparsed {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Attribute have a value enclosed in single quotes
        #[test]
        fn single_quoted() {
            let mut iter = Attributes::new(r#"tag key = 'value' "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value enclosed in double quotes
        #[test]
        fn double_quoted() {
            let mut iter = Attributes::new(r#"tag key = "value" "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value, not enclosed in quotes
        #[test]
        fn unquoted() {
            let mut iter = Attributes::new(r#"tag key = value "#, 3);
            //                                0         ^ = 10

            assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(10))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Only attribute key is present
        #[test]
        fn key_only() {
            let mut iter = Attributes::new(r#"tag key "#, 3);
            //                                0       ^ = 8

            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(8))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key is started with an invalid symbol (a single quote in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_start_invalid() {
            let mut iter = Attributes::new(r#"tag 'key' = 'value' "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'key'"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key contains an invalid symbol (an ampersand in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_contains_invalid() {
            let mut iter = Attributes::new(r#"tag key&jey = 'value' "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key&jey"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute value is missing after `=`
        #[test]
        fn missed_value() {
            let mut iter = Attributes::new(r#"tag key = "#, 3);
            //                                0         ^ = 10

            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedValue(10))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }

    /// Checks that duplicated attributes correctly reported and recovering is
    /// possible after that
    mod duplicated {
        use super::*;

        mod with_check {
            use super::*;
            use pretty_assertions::assert_eq;

            /// Attribute have a value enclosed in single quotes
            #[test]
            fn single_quoted() {
                let mut iter = Attributes::new(r#"tag key='value' key='dup' another=''"#, 3);
                //                                0   ^ = 4       ^ = 16

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::Duplicated(16, 4))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value enclosed in double quotes
            #[test]
            fn double_quoted() {
                let mut iter = Attributes::new(r#"tag key='value' key="dup" another=''"#, 3);
                //                                0   ^ = 4       ^ = 16

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::Duplicated(16, 4))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value, not enclosed in quotes
            #[test]
            fn unquoted() {
                let mut iter = Attributes::new(r#"tag key='value' key=dup another=''"#, 3);
                //                                0   ^ = 4       ^ = 16

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::Duplicated(16, 4))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Only attribute key is present
            #[test]
            fn key_only() {
                let mut iter = Attributes::new(r#"tag key='value' key another=''"#, 3);
                //                                0                   ^ = 20

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(20))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Once a start tag declares more than `SMALL_ATTRIBUTE_COUNT`
            /// attributes the duplicate check switches to its hash-based path. A
            /// duplicate of a name first seen during the earlier linear phase must
            /// still be detected, with the original position reported. Regression
            /// cover for the cold path of [#969].
            ///
            /// [#969]: https://github.com/tafia/quick-xml/issues/969
            #[test]
            fn duplicate_past_hash_threshold() {
                let dup = SMALL_ATTRIBUTE_COUNT / 2;
                let n = SMALL_ATTRIBUTE_COUNT + 8;

                let mut source = String::from("tag");
                let mut positions = Vec::with_capacity(n);
                for i in 0..n {
                    source.push(' ');
                    positions.push(source.len());
                    source.push_str(&format!("k{:04}=''", i));
                }
                // Repeat the name first seen at `positions[dup]` (linear phase).
                source.push(' ');
                let dup_pos = source.len();
                source.push_str(&format!("k{:04}=''", dup));

                let mut iter = Attributes::new(&source, 3);
                for _ in 0..n {
                    assert!(matches!(iter.next(), Some(Ok(_))));
                }
                assert_eq!(
                    iter.next(),
                    Some(Err(AttrError::Duplicated(dup_pos, positions[dup])))
                );
            }
        }

        /// Check for duplicated names is disabled
        mod without_check {
            use super::*;
            use pretty_assertions::assert_eq;

            /// Attribute have a value enclosed in single quotes
            #[test]
            fn single_quoted() {
                let mut iter = Attributes::new(r#"tag key='value' key='dup' another=''"#, 3);
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"dup"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value enclosed in double quotes
            #[test]
            fn double_quoted() {
                let mut iter = Attributes::new(r#"tag key='value' key="dup" another=''"#, 3);
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"dup"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value, not enclosed in quotes
            #[test]
            fn unquoted() {
                let mut iter = Attributes::new(r#"tag key='value' key=dup another=''"#, 3);
                //                                0                   ^ = 20
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::UnquotedValue(20))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Only attribute key is present
            #[test]
            fn key_only() {
                let mut iter = Attributes::new(r#"tag key='value' key another=''"#, 3);
                //                                0                   ^ = 20
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::ExpectedEq(20))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }
        }
    }

    #[test]
    fn mixed_quote() {
        let mut iter = Attributes::new(r#"tag a='a' b = "b" c='cc"cc' d="dd'dd""#, 3);

        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"a"),
                value: Cow::Borrowed(b"a"),
            }))
        );
        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"b"),
                value: Cow::Borrowed(b"b"),
            }))
        );
        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"c"),
                value: Cow::Borrowed(br#"cc"cc"#),
            }))
        );
        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"d"),
                value: Cow::Borrowed(b"dd'dd"),
            }))
        );
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}

/// Checks, how parsing of HTML-style attributes works. Each attribute can be
/// in three forms:
/// - XML-like: have a value, enclosed in single or double quotes
/// - have a value, do not enclosed in quotes
/// - without value, key only
#[cfg(test)]
mod html {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Checked attribute is the single attribute
    mod single {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Attribute have a value enclosed in single quotes
        #[test]
        fn single_quoted() {
            let mut iter = Attributes::html(r#"tag key='value'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value enclosed in double quotes
        #[test]
        fn double_quoted() {
            let mut iter = Attributes::html(r#"tag key="value""#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value, not enclosed in quotes
        #[test]
        fn unquoted() {
            let mut iter = Attributes::html(r#"tag key=value"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Only attribute key is present
        #[test]
        fn key_only() {
            let mut iter = Attributes::html(r#"tag key"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(&[]),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key is started with an invalid symbol (a single quote in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_start_invalid() {
            let mut iter = Attributes::html(r#"tag 'key'='value'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'key'"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key contains an invalid symbol (an ampersand in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_contains_invalid() {
            let mut iter = Attributes::html(r#"tag key&jey='value'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key&jey"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute value is missing after `=`
        #[test]
        fn missed_value() {
            let mut iter = Attributes::html(r#"tag key="#, 3);
            //                                0       ^ = 8

            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedValue(8))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }

    /// Checked attribute is the first attribute in the list of many attributes
    mod first {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Attribute have a value enclosed in single quotes
        #[test]
        fn single_quoted() {
            let mut iter = Attributes::html(r#"tag key='value' regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value enclosed in double quotes
        #[test]
        fn double_quoted() {
            let mut iter = Attributes::html(r#"tag key="value" regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value, not enclosed in quotes
        #[test]
        fn unquoted() {
            let mut iter = Attributes::html(r#"tag key=value regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Only attribute key is present
        #[test]
        fn key_only() {
            let mut iter = Attributes::html(r#"tag key regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(&[]),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key is started with an invalid symbol (a single quote in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_start_invalid() {
            let mut iter = Attributes::html(r#"tag 'key'='value' regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'key'"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key contains an invalid symbol (an ampersand in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_contains_invalid() {
            let mut iter = Attributes::html(r#"tag key&jey='value' regular='attribute'"#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key&jey"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"regular"),
                    value: Cow::Borrowed(b"attribute"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute value is missing after `=`
        #[test]
        fn missed_value() {
            let mut iter = Attributes::html(r#"tag key= regular='attribute'"#, 3);

            // Because we do not check validity of keys and values during parsing,
            // "regular='attribute'" is considered as unquoted attribute value
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"regular='attribute'"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);

            ////////////////////////////////////////////////////////////////////

            let mut iter = Attributes::html(r#"tag key= regular= 'attribute'"#, 3);

            // Because we do not check validity of keys and values during parsing,
            // "regular=" is considered as unquoted attribute value
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"regular="),
                }))
            );
            // Because we do not check validity of keys and values during parsing,
            // "'attribute'" is considered as key-only attribute
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'attribute'"),
                    value: Cow::Borrowed(&[]),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);

            ////////////////////////////////////////////////////////////////////

            let mut iter = Attributes::html(r#"tag key= regular ='attribute'"#, 3);

            // Because we do not check validity of keys and values during parsing,
            // "regular" is considered as unquoted attribute value
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"regular"),
                }))
            );
            // Because we do not check validity of keys and values during parsing,
            // "='attribute'" is considered as key-only attribute
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"='attribute'"),
                    value: Cow::Borrowed(&[]),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);

            ////////////////////////////////////////////////////////////////////

            let mut iter = Attributes::html(r#"tag key= regular = 'attribute'"#, 3);
            //                                 0        ^ = 9     ^ = 19     ^ = 30

            // Because we do not check validity of keys and values during parsing,
            // "regular" is considered as unquoted attribute value
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"regular"),
                }))
            );
            // Because we do not check validity of keys and values during parsing,
            // "=" is considered as key-only attribute
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"="),
                    value: Cow::Borrowed(&[]),
                }))
            );
            // Because we do not check validity of keys and values during parsing,
            // "'attribute'" is considered as key-only attribute
            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'attribute'"),
                    value: Cow::Borrowed(&[]),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }

    /// Copy of single, but with additional spaces in markup
    mod sparsed {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Attribute have a value enclosed in single quotes
        #[test]
        fn single_quoted() {
            let mut iter = Attributes::html(r#"tag key = 'value' "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value enclosed in double quotes
        #[test]
        fn double_quoted() {
            let mut iter = Attributes::html(r#"tag key = "value" "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute have a value, not enclosed in quotes
        #[test]
        fn unquoted() {
            let mut iter = Attributes::html(r#"tag key = value "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Only attribute key is present
        #[test]
        fn key_only() {
            let mut iter = Attributes::html(r#"tag key "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key"),
                    value: Cow::Borrowed(&[]),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key is started with an invalid symbol (a single quote in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_start_invalid() {
            let mut iter = Attributes::html(r#"tag 'key' = 'value' "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"'key'"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Key contains an invalid symbol (an ampersand in this test).
        /// Because we do not check validity of keys and values during parsing,
        /// that invalid attribute will be returned
        #[test]
        fn key_contains_invalid() {
            let mut iter = Attributes::html(r#"tag key&jey = 'value' "#, 3);

            assert_eq!(
                iter.next(),
                Some(Ok(Attribute {
                    key: QName(b"key&jey"),
                    value: Cow::Borrowed(b"value"),
                }))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        /// Attribute value is missing after `=`
        #[test]
        fn missed_value() {
            let mut iter = Attributes::html(r#"tag key = "#, 3);
            //                                 0         ^ = 10

            assert_eq!(iter.next(), Some(Err(AttrError::ExpectedValue(10))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }

    /// Checks that duplicated attributes correctly reported and recovering is
    /// possible after that
    mod duplicated {
        use super::*;

        mod with_check {
            use super::*;
            use pretty_assertions::assert_eq;

            /// Attribute have a value enclosed in single quotes
            #[test]
            fn single_quoted() {
                let mut iter = Attributes::html(r#"tag key='value' key='dup' another=''"#, 3);
                //                                 0   ^ = 4       ^ = 16

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::Duplicated(16, 4))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value enclosed in double quotes
            #[test]
            fn double_quoted() {
                let mut iter = Attributes::html(r#"tag key='value' key="dup" another=''"#, 3);
                //                                 0   ^ = 4       ^ = 16

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::Duplicated(16, 4))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value, not enclosed in quotes
            #[test]
            fn unquoted() {
                let mut iter = Attributes::html(r#"tag key='value' key=dup another=''"#, 3);
                //                                 0   ^ = 4       ^ = 16

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::Duplicated(16, 4))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Only attribute key is present
            #[test]
            fn key_only() {
                let mut iter = Attributes::html(r#"tag key='value' key another=''"#, 3);
                //                                 0   ^ = 4       ^ = 16

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(iter.next(), Some(Err(AttrError::Duplicated(16, 4))));
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }
        }

        /// Check for duplicated names is disabled
        mod without_check {
            use super::*;
            use pretty_assertions::assert_eq;

            /// Attribute have a value enclosed in single quotes
            #[test]
            fn single_quoted() {
                let mut iter = Attributes::html(r#"tag key='value' key='dup' another=''"#, 3);
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"dup"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value enclosed in double quotes
            #[test]
            fn double_quoted() {
                let mut iter = Attributes::html(r#"tag key='value' key="dup" another=''"#, 3);
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"dup"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Attribute have a value, not enclosed in quotes
            #[test]
            fn unquoted() {
                let mut iter = Attributes::html(r#"tag key='value' key=dup another=''"#, 3);
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"dup"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }

            /// Only attribute key is present
            #[test]
            fn key_only() {
                let mut iter = Attributes::html(r#"tag key='value' key another=''"#, 3);
                iter.with_checks(false);

                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(b"value"),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"key"),
                        value: Cow::Borrowed(&[]),
                    }))
                );
                assert_eq!(
                    iter.next(),
                    Some(Ok(Attribute {
                        key: QName(b"another"),
                        value: Cow::Borrowed(b""),
                    }))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(iter.next(), None);
            }
        }
    }

    #[test]
    fn mixed_quote() {
        let mut iter = Attributes::html(r#"tag a='a' b = "b" c='cc"cc' d="dd'dd""#, 3);

        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"a"),
                value: Cow::Borrowed(b"a"),
            }))
        );
        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"b"),
                value: Cow::Borrowed(b"b"),
            }))
        );
        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"c"),
                value: Cow::Borrowed(br#"cc"cc"#),
            }))
        );
        assert_eq!(
            iter.next(),
            Some(Ok(Attribute {
                key: QName(b"d"),
                value: Cow::Borrowed(b"dd'dd"),
            }))
        );
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}
