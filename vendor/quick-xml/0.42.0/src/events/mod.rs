//! Defines zero-copy XML events used throughout this library.
//!
//! A XML event often represents part of a XML element.
//! They occur both during reading and writing and are
//! usually used with the stream-oriented API.
//!
//! For example, the XML element
//! ```xml
//! <name attr="value">Inner text</name>
//! ```
//! consists of the three events `Start`, `Text` and `End`.
//! They can also represent other parts in an XML document like the
//! XML declaration. Each Event usually contains further information,
//! like the tag name, the attribute or the inner text.
//!
//! See [`Event`] for a list of all possible events.
//!
//! # Reading
//! When reading a XML stream, the events are emitted by [`Reader::read_event`]
//! and [`Reader::read_event_into`]. You must listen
//! for the different types of events you are interested in.
//!
//! See [`Reader`] for further information.
//!
//! # Writing
//! When writing the XML document, you must create the XML element
//! by constructing the events it consists of and pass them to the writer
//! sequentially.
//!
//! See [`Writer`] for further information.
//!
//! [`Reader::read_event`]: crate::reader::Reader::read_event
//! [`Reader::read_event_into`]: crate::reader::Reader::read_event_into
//! [`Reader`]: crate::reader::Reader
//! [`Writer`]: crate::writer::Writer
//! [`Event`]: crate::events::Event

pub mod attributes;

#[cfg(feature = "encoding")]
use encoding_rs::Encoding;
use std::borrow::Cow;
use std::fmt::{self, Debug, Formatter};
use std::iter::FusedIterator;
use std::mem::replace;
use std::ops::Deref;

use crate::XmlVersion;
use crate::encoding::EncodingError;
use crate::errors::{Error, IllFormedError};
use crate::escape::{
    EscapeError, escape, minimal_escape, normalize_xml10_eols, normalize_xml11_eols, parse_number,
    partial_escape,
};
use crate::name::{LocalName, QName};
use crate::utils::{self, name_len, trim_xml_end, trim_xml_start, write_cow_string};
use attributes::{AttrError, Attribute, Attributes};

/// Opening tag data (`Event::Start`), with optional attributes: `<name attr="value">`.
///
/// The name can be accessed using the [`name`] or [`local_name`] methods.
/// An iterator over the attributes is returned by the [`attributes`] method.
///
/// This event implements `Deref<Target = str>`. The `deref()` implementation
/// returns the content of this event between `<` and `>` or `/>`:
///
/// ```
/// # use quick_xml::events::{BytesStart, Event};
/// # use quick_xml::reader::Reader;
/// # use pretty_assertions::assert_eq;
/// // Remember, that \ at the end of string literal strips
/// // all space characters to the first non-space character
/// let mut reader = Reader::from_str("\
///     <element a1 = 'val1' a2=\"val2\" />\
///     <element a1 = 'val1' a2=\"val2\" >"
/// );
/// let content = "element a1 = 'val1' a2=\"val2\" ";
/// let event = BytesStart::from_content(content, 7);
///
/// assert_eq!(reader.read_event().unwrap(), Event::Empty(event.borrow()));
/// assert_eq!(reader.read_event().unwrap(), Event::Start(event.borrow()));
/// // deref coercion of &BytesStart to &str
/// assert_eq!(event.as_ref(), content);
/// ```
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [`name`]: Self::name
/// [`local_name`]: Self::local_name
/// [`attributes`]: Self::attributes
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BytesStart<'i> {
    /// content of the element
    pub(crate) buf: Cow<'i, str>,
    /// end of the element name, the name starts at that the start of `buf`
    pub(crate) name_len: usize,
}

impl<'i> BytesStart<'i> {
    /// Internal constructor, used by `Reader`. Supplies data in reader's encoding
    #[inline]
    pub(crate) const fn wrap(content: &'i str, name_len: usize) -> Self {
        BytesStart {
            buf: Cow::Borrowed(content),
            name_len,
        }
    }

    /// Creates a new `BytesStart` from the given name.
    ///
    /// # Warning
    ///
    /// `name` must be a valid name.
    #[inline]
    pub fn new<C: Into<Cow<'i, str>>>(name: C) -> Self {
        let buf: Cow<'i, str> = name.into();
        BytesStart {
            name_len: buf.len(),
            buf,
        }
    }

    /// Creates a new `BytesStart` from the given content (name + attributes).
    ///
    /// # Warning
    ///
    /// `&content[..name_len]` must be a valid name, and the remainder of `content`
    /// must be correctly-formed attributes. Neither are checked, it is possible
    /// to generate invalid XML if `content` or `name_len` are incorrect.
    #[inline]
    pub fn from_content<C: Into<Cow<'i, str>>>(content: C, name_len: usize) -> Self {
        BytesStart {
            buf: content.into(),
            name_len,
        }
    }

    /// Converts the event into an owned event.
    pub fn into_owned(self) -> BytesStart<'static> {
        BytesStart {
            buf: Cow::Owned(self.buf.into_owned()),
            name_len: self.name_len,
        }
    }

    /// Converts the event into an owned event without taking ownership of Event
    pub fn to_owned(&self) -> BytesStart<'static> {
        BytesStart {
            buf: Cow::Owned(self.buf.clone().into_owned()),
            name_len: self.name_len,
        }
    }

    /// Converts the event into a borrowed event. Most useful when paired with [`to_end`].
    ///
    /// # Example
    ///
    /// ```
    /// use quick_xml::events::{BytesStart, Event};
    /// # use quick_xml::writer::Writer;
    /// # use quick_xml::Error;
    ///
    /// struct SomeStruct<'i> {
    ///     attrs: BytesStart<'i>,
    ///     // ...
    /// }
    /// # impl<'i> SomeStruct<'i> {
    /// # fn example(&self) -> Result<(), Error> {
    /// # let mut writer = Writer::new(Vec::new());
    ///
    /// writer.write_event(Event::Start(self.attrs.borrow()))?;
    /// // ...
    /// writer.write_event(Event::End(self.attrs.to_end()))?;
    /// # Ok(())
    /// # }}
    /// ```
    ///
    /// [`to_end`]: Self::to_end
    pub fn borrow(&self) -> BytesStart<'_> {
        BytesStart {
            buf: Cow::Borrowed(&self.buf),
            name_len: self.name_len,
        }
    }

    /// Creates new paired close tag
    #[inline]
    pub fn to_end(&self) -> BytesEnd<'_> {
        BytesEnd::from(self.name())
    }

    /// Gets the undecoded raw tag name, as present in the input stream.
    #[inline]
    pub fn name(&self) -> QName<'_> {
        QName(&self.buf[..self.name_len])
    }

    /// Gets the undecoded raw local tag name (excluding namespace) as present
    /// in the input stream.
    ///
    /// All content up to and including the first `:` character is removed from the tag name.
    #[inline]
    pub fn local_name(&self) -> LocalName<'_> {
        self.name().into()
    }

    /// Edit the name of the BytesStart in-place
    ///
    /// # Warning
    ///
    /// `name` must be a valid name.
    pub fn set_name(&mut self, name: &str) -> &mut BytesStart<'i> {
        let s = self.buf.to_mut();
        s.replace_range(..self.name_len, name);
        self.name_len = name.len();
        self
    }
}

/// Attribute-related methods
impl<'i> BytesStart<'i> {
    /// Consumes `self` and yield a new `BytesStart` with additional attributes from an iterator.
    ///
    /// The yielded items must be convertible to [`Attribute`] using `Into`.
    pub fn with_attributes<'a, I>(mut self, attributes: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<Attribute<'a>>,
    {
        self.extend_attributes(attributes);
        self
    }

    /// Add additional attributes to this tag using an iterator.
    ///
    /// The yielded items must be convertible to [`Attribute`] using `Into`.
    pub fn extend_attributes<'a, I>(&mut self, attributes: I) -> &mut BytesStart<'i>
    where
        I: IntoIterator,
        I::Item: Into<Attribute<'a>>,
    {
        for attr in attributes {
            self.push_attribute(attr);
        }
        self
    }

    /// Adds an attribute to this element.
    pub fn push_attribute<'a, A>(&mut self, attr: A)
    where
        A: Into<Attribute<'a>>,
    {
        self.buf.to_mut().push(' ');
        self.push_attr(attr.into());
    }

    /// Remove all attributes from the ByteStart
    pub fn clear_attributes(&mut self) -> &mut BytesStart<'i> {
        self.buf.to_mut().truncate(self.name_len);
        self
    }

    /// Returns an iterator over the attributes of this tag.
    pub fn attributes(&self) -> Attributes<'_> {
        Attributes::wrap(&self.buf, self.name_len, false)
    }

    /// Returns an iterator over the HTML-like attributes of this tag (no mandatory quotes or `=`).
    pub fn html_attributes(&self) -> Attributes<'_> {
        Attributes::wrap(&self.buf, self.name_len, true)
    }

    /// Gets the undecoded raw string with the attributes of this tag as a `&str`,
    /// including the whitespace after the tag name if there is any.
    #[inline]
    pub fn attributes_raw(&self) -> &str {
        &self.buf[self.name_len..]
    }

    /// Try to get an attribute
    pub fn try_get_attribute<'a>(
        &'a self,
        attr_name: &str,
    ) -> Result<Option<Attribute<'a>>, AttrError> {
        for a in self.attributes().with_checks(false) {
            let a = a?;
            if a.key.as_ref() == attr_name {
                return Ok(Some(a));
            }
        }
        Ok(None)
    }

    /// Adds an attribute to this element.
    pub(crate) fn push_attr<'a>(&mut self, attr: Attribute<'a>) {
        let s = self.buf.to_mut();
        s.push_str(attr.key.as_ref());
        s.push_str("=\"");
        // FIXME: need to escape attribute content
        s.push_str(&attr.value);
        s.push('"');
    }

    /// Adds new line in existing element
    pub(crate) fn push_newline(&mut self) {
        self.buf.to_mut().push('\n');
    }

    /// Adds indentation in existing element
    pub(crate) fn push_indent(&mut self, indent: &str) {
        self.buf.to_mut().push_str(indent);
    }
}

impl<'i> Debug for BytesStart<'i> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BytesStart {{ buf: ")?;
        write_cow_string(f, &self.buf)?;
        write!(f, ", name_len: {} }}", self.name_len)
    }
}

impl<'i> Deref for BytesStart<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.buf
    }
}

impl AsRef<str> for BytesStart<'_> {
    fn as_ref(&self) -> &str {
        self
    }
}

#[cfg(feature = "arbitrary")]
impl<'i> arbitrary::Arbitrary<'i> for BytesStart<'i> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'i>) -> arbitrary::Result<Self> {
        let s = <&str>::arbitrary(u)?;
        if s.is_empty() || !s.chars().all(char::is_alphanumeric) {
            return Err(arbitrary::Error::IncorrectFormat);
        }
        let mut result = Self::new(s);
        result.extend_attributes(Vec::<(&str, &str)>::arbitrary(u)?);
        Ok(result)
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <&str as arbitrary::Arbitrary>::size_hint(depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Closing tag data (`Event::End`): `</name>`.
///
/// The name can be accessed using the [`name`] or [`local_name`] methods.
///
/// This event implements `Deref<Target = str>`. The `deref()` implementation
/// returns the content of this event between `</` and `>`.
///
/// Note, that inner text will not contain `>` character inside:
///
/// ```
/// # use quick_xml::events::{BytesEnd, Event};
/// # use quick_xml::reader::Reader;
/// # use pretty_assertions::assert_eq;
/// let mut reader = Reader::from_str(r#"<element></element a1 = 'val1' a2="val2" >"#);
/// // Note, that this entire string considered as a .name()
/// let content = "element a1 = 'val1' a2=\"val2\" ";
/// let event = BytesEnd::new(content);
///
/// reader.config_mut().trim_markup_names_in_closing_tags = false;
/// reader.config_mut().check_end_names = false;
/// reader.read_event().unwrap(); // Skip `<element>`
///
/// assert_eq!(reader.read_event().unwrap(), Event::End(event.borrow()));
/// assert_eq!(event.name().as_ref(), content);
/// // deref coercion of &BytesEnd to &str
/// assert_eq!(event.as_ref(), content);
/// ```
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [`name`]: Self::name
/// [`local_name`]: Self::local_name
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BytesEnd<'i> {
    name: Cow<'i, str>,
}

impl<'i> BytesEnd<'i> {
    /// Internal constructor, used by `Reader`. Supplies data in reader's encoding
    #[inline]
    pub(crate) const fn wrap(name: Cow<'i, str>) -> Self {
        BytesEnd { name }
    }

    /// Creates a new `BytesEnd` borrowing a slice.
    ///
    /// # Warning
    ///
    /// `name` must be a valid name.
    #[inline]
    pub fn new<C: Into<Cow<'i, str>>>(name: C) -> Self {
        Self::wrap(name.into())
    }

    /// Converts the event into an owned event.
    pub fn into_owned(self) -> BytesEnd<'static> {
        BytesEnd {
            name: Cow::Owned(self.name.into_owned()),
        }
    }

    /// Converts the event into a borrowed event.
    #[inline]
    pub fn borrow(&self) -> BytesEnd<'_> {
        BytesEnd {
            name: Cow::Borrowed(&self.name),
        }
    }

    /// Gets the undecoded raw tag name, as present in the input stream.
    #[inline]
    pub fn name(&self) -> QName<'_> {
        QName(&self.name)
    }

    /// Gets the undecoded raw local tag name (excluding namespace) as present
    /// in the input stream.
    ///
    /// All content up to and including the first `:` character is removed from the tag name.
    #[inline]
    pub fn local_name(&self) -> LocalName<'_> {
        self.name().into()
    }
}

impl<'i> Debug for BytesEnd<'i> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BytesEnd {{ name: ")?;
        write_cow_string(f, &self.name)?;
        write!(f, " }}")
    }
}

impl<'i> Deref for BytesEnd<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.name
    }
}

impl AsRef<str> for BytesEnd<'_> {
    fn as_ref(&self) -> &str {
        self
    }
}

impl<'i> From<QName<'i>> for BytesEnd<'i> {
    #[inline]
    fn from(name: QName<'i>) -> Self {
        Self::wrap(Cow::Borrowed(name.into_inner()))
    }
}

#[cfg(feature = "arbitrary")]
impl<'i> arbitrary::Arbitrary<'i> for BytesEnd<'i> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'i>) -> arbitrary::Result<Self> {
        Ok(Self::new(<&str>::arbitrary(u)?))
    }
    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <&str as arbitrary::Arbitrary>::size_hint(depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Data from various events (most notably, `Event::Text`).
///
/// This event implements `Deref<Target = str>`. The `deref()` implementation
/// returns the content of this event. In case of comment this is everything
/// between `<!--` and `-->` and the text of comment may not contain `-->` inside
/// (if [`Config::check_comments`] is set to `true`).
/// In case of DTD this is everything between `<!DOCTYPE` + spaces and closing `>`
/// (i.e. in case of DTD the first character is never space):
///
/// ```
/// # use quick_xml::events::{BytesText, Event};
/// # use quick_xml::reader::Reader;
/// # use pretty_assertions::assert_eq;
/// // Remember, that \ at the end of string literal strips
/// // all space characters to the first non-space character
/// let mut reader = Reader::from_str("\
///     <!DOCTYPE comment or text >\
///     comment or text \
///     <!--comment or text -->"
/// );
/// let content = "comment or text ";
/// let event = BytesText::new(content);
///
/// assert_eq!(reader.read_event().unwrap(), Event::DocType(event.borrow()));
/// assert_eq!(reader.read_event().unwrap(), Event::Text(event.borrow()));
/// assert_eq!(reader.read_event().unwrap(), Event::Comment(event.borrow()));
/// // deref coercion of &BytesText to &str
/// assert_eq!(event.as_ref(), content);
/// ```
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [`Config::check_comments`]: crate::reader::Config::check_comments
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BytesText<'i> {
    /// Escaped content of the event
    content: Cow<'i, str>,
}

impl<'i> BytesText<'i> {
    /// Creates a new `BytesText` from a string as it appeared in the XML source.
    #[inline]
    pub(crate) const fn wrap(content: &'i str) -> Self {
        Self {
            content: Cow::Borrowed(content),
        }
    }

    /// Creates a new `BytesText` from a raw string as it appeared in the XML source.
    ///
    /// # Warning
    ///
    /// `content` is not checked to not contain markup or entity references. Be warned
    /// that writing such event may result to invalid XML if your content contains not
    /// defined entity references or invalid XML markup.
    ///
    /// `content` may have any EOLs, they will be normalized when using [`xml_content()`] getters.
    ///
    /// [`xml_content()`]: Self::xml_content
    #[inline]
    pub fn from_escaped<C: Into<Cow<'i, str>>>(content: C) -> Self {
        Self {
            content: content.into(),
        }
    }

    /// Creates a new `BytesText` from a string.
    ///
    /// # Warning
    ///
    /// `content` will be escaped using the [`escape`] function, but that may change
    /// in the future, because events produced by the reader never contains `&` or `<`,
    /// and escaping of `>`, `"` and `'` is not required. If you want to preserve exact
    /// content, use [`from_escaped()`] method, but be warned that writing such event
    /// may result to invalid XML if your content contains not defined entity references
    /// or invalid XML markup.
    ///
    /// `content` may have any EOLs, they will be normalized when using [`xml_content()`] getters.
    ///
    /// [`escape`]: crate::escape::escape
    /// [`from_escaped()`]: Self::from_escaped
    /// [`xml_content()`]: Self::xml_content
    #[inline]
    pub fn new(content: &'i str) -> Self {
        Self::from_escaped(escape(content))
    }

    /// Ensures that all data is owned to extend the object's lifetime if
    /// necessary.
    #[inline]
    pub fn into_owned(self) -> BytesText<'static> {
        BytesText {
            content: Cow::Owned(self.content.into_owned()),
        }
    }

    /// Extracts the inner `Cow` from the `BytesText` event container.
    #[inline]
    pub fn into_inner(self) -> Cow<'i, str> {
        self.content
    }

    /// Converts the event into a borrowed event.
    #[inline]
    pub fn borrow(&self) -> BytesText<'_> {
        BytesText {
            content: Cow::Borrowed(&self.content),
        }
    }

    /// Returns the content of the XML 1.0 or HTML event with EOL normalization applied.
    ///
    /// This will allocate if EOL normalization is required.
    ///
    /// Note, that this method should be used only if event represents XML 1.0 or HTML content,
    /// because rules for normalizing EOLs for [XML 1.0] / [HTML] and [XML 1.1] differs.
    ///
    /// This method also can be used to get HTML content, because rules the same.
    ///
    /// [XML 1.0]: https://www.w3.org/TR/xml/#sec-line-ends
    /// [XML 1.1]: https://www.w3.org/TR/xml11/#sec-line-ends
    /// [HTML]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn xml10_content(&self) -> Cow<'i, str> {
        match &self.content {
            Cow::Borrowed(s) => normalize_xml10_eols(s),
            Cow::Owned(s) => Cow::Owned(normalize_xml10_eols(s).into_owned()),
        }
    }

    /// Returns the content of the XML 1.1 event with EOL normalization applied.
    ///
    /// This will allocate if EOL normalization is required.
    ///
    /// Note, that this method should be used only if event represents XML 1.1 content,
    /// because rules for normalizing EOLs for [XML 1.0], [XML 1.1] and [HTML] differs.
    ///
    /// To get HTML content use [`xml10_content()`](Self::xml10_content).
    ///
    /// [XML 1.0]: https://www.w3.org/TR/xml/#sec-line-ends
    /// [XML 1.1]: https://www.w3.org/TR/xml11/#sec-line-ends
    /// [HTML]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn xml11_content(&self) -> Cow<'i, str> {
        match &self.content {
            Cow::Borrowed(s) => normalize_xml11_eols(s),
            Cow::Owned(s) => Cow::Owned(normalize_xml11_eols(s).into_owned()),
        }
    }

    /// Returns the content of the XML event with EOL normalization applied
    /// according to the specified version.
    ///
    /// This will allocate if EOL normalization is required.
    #[inline]
    pub fn xml_content(&self, version: XmlVersion) -> Cow<'i, str> {
        match version {
            XmlVersion::Explicit1_1 => self.xml11_content(),
            _ => self.xml10_content(),
        }
    }

    /// Alias for [`xml10_content()`](Self::xml10_content).
    #[inline]
    pub fn html_content(&self) -> Cow<'i, str> {
        self.xml10_content()
    }

    /// Removes leading XML whitespace bytes from text content.
    ///
    /// Returns `true` if content is empty after that
    pub fn inplace_trim_start(&mut self) -> bool {
        self.content = trim_cow(
            replace(&mut self.content, Cow::Borrowed("")),
            trim_xml_start,
        );
        self.content.is_empty()
    }

    /// Removes trailing XML whitespace bytes from text content.
    ///
    /// Returns `true` if content is empty after that
    pub fn inplace_trim_end(&mut self) -> bool {
        self.content = trim_cow(replace(&mut self.content, Cow::Borrowed("")), trim_xml_end);
        self.content.is_empty()
    }
}

impl<'i> Debug for BytesText<'i> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BytesText {{ content: ")?;
        write_cow_string(f, &self.content)?;
        write!(f, " }}")
    }
}

impl<'i> Deref for BytesText<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.content
    }
}

impl AsRef<str> for BytesText<'_> {
    fn as_ref(&self) -> &str {
        self
    }
}

#[cfg(feature = "arbitrary")]
impl<'i> arbitrary::Arbitrary<'i> for BytesText<'i> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'i>) -> arbitrary::Result<Self> {
        let s = <&str>::arbitrary(u)?;
        if !s.chars().all(char::is_alphanumeric) {
            return Err(arbitrary::Error::IncorrectFormat);
        }
        Ok(Self::new(s))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <&str as arbitrary::Arbitrary>::size_hint(depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// CDATA content contains unescaped data from the reader. If you want to write them as a text,
/// [convert](Self::escape) it to [`BytesText`].
///
/// This event implements `Deref<Target = str>`. The `deref()` implementation
/// returns the content of this event between `<![CDATA[` and `]]>`.
///
/// Note, that inner text will not contain `]]>` sequence inside:
///
/// ```
/// # use quick_xml::events::{BytesCData, Event};
/// # use quick_xml::reader::Reader;
/// # use pretty_assertions::assert_eq;
/// let mut reader = Reader::from_str("<![CDATA[ CDATA section ]]>");
/// let content = " CDATA section ";
/// let event = BytesCData::new(content);
///
/// assert_eq!(reader.read_event().unwrap(), Event::CData(event.borrow()));
/// // deref coercion of &BytesCData to &str
/// assert_eq!(event.as_ref(), content);
/// ```
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BytesCData<'i> {
    content: Cow<'i, str>,
}

impl<'i> BytesCData<'i> {
    /// Creates a new `BytesCData` from a string.
    #[inline]
    pub(crate) const fn wrap(content: &'i str) -> Self {
        Self {
            content: Cow::Borrowed(content),
        }
    }

    /// Creates a new `BytesCData` from a string.
    ///
    /// # Warning
    ///
    /// `content` must not contain the `]]>` sequence. You can use
    /// [`BytesCData::escaped`] to escape the content instead.
    #[inline]
    pub fn new<C: Into<Cow<'i, str>>>(content: C) -> Self {
        Self {
            content: content.into(),
        }
    }

    /// Creates an iterator of `BytesCData` from a string.
    ///
    /// If a string contains `]]>`, it needs to be split into multiple `CDATA`
    /// sections, splitting the `]]` and `>` characters, because the CDATA closing
    /// sequence cannot be escaped. This iterator yields a `BytesCData` instance
    /// for each of those sections.
    ///
    /// # Examples
    ///
    /// ```
    /// # use quick_xml::events::BytesCData;
    /// # use pretty_assertions::assert_eq;
    /// let content = "";
    /// let cdata = BytesCData::escaped(content).collect::<Vec<_>>();
    /// assert_eq!(cdata, &[BytesCData::new("")]);
    ///
    /// let content = "Certain tokens like ]]> can be difficult and <invalid>";
    /// let cdata = BytesCData::escaped(content).collect::<Vec<_>>();
    /// assert_eq!(cdata, &[
    ///     BytesCData::new("Certain tokens like ]]"),
    ///     BytesCData::new("> can be difficult and <invalid>"),
    /// ]);
    ///
    /// let content = "foo]]>bar]]>baz]]>quux";
    /// let cdata = BytesCData::escaped(content).collect::<Vec<_>>();
    /// assert_eq!(cdata, &[
    ///     BytesCData::new("foo]]"),
    ///     BytesCData::new(">bar]]"),
    ///     BytesCData::new(">baz]]"),
    ///     BytesCData::new(">quux"),
    /// ]);
    /// ```
    #[inline]
    pub const fn escaped(content: &'i str) -> CDataIterator<'i> {
        CDataIterator {
            inner: utils::CDataIterator::new(content),
        }
    }

    /// Ensures that all data is owned to extend the object's lifetime if
    /// necessary.
    #[inline]
    pub fn into_owned(self) -> BytesCData<'static> {
        BytesCData {
            content: Cow::Owned(self.content.into_owned()),
        }
    }

    /// Extracts the inner `Cow` from the `BytesCData` event container.
    #[inline]
    pub fn into_inner(self) -> Cow<'i, str> {
        self.content
    }

    /// Converts the event into a borrowed event.
    #[inline]
    pub fn borrow(&self) -> BytesCData<'_> {
        BytesCData {
            content: Cow::Borrowed(&self.content),
        }
    }

    /// Converts this CDATA content to an escaped version, that can be written
    /// as an usual text in XML.
    ///
    /// This function performs following replacements:
    ///
    /// | Character | Replacement
    /// |-----------|------------
    /// | `<`       | `&lt;`
    /// | `>`       | `&gt;`
    /// | `&`       | `&amp;`
    /// | `'`       | `&apos;`
    /// | `"`       | `&quot;`
    pub fn escape(self) -> Result<BytesText<'i>, EncodingError> {
        Ok(match self.content {
            Cow::Borrowed(s) => BytesText::from_escaped(escape(s)),
            Cow::Owned(s) => BytesText::from_escaped(escape(&s).into_owned()),
        })
    }

    /// Converts this CDATA content to an escaped version, that can be written
    /// as an usual text in XML.
    ///
    /// In XML text content, it is allowed (though not recommended) to leave
    /// the quote special characters `"` and `'` unescaped.
    ///
    /// This function performs following replacements:
    ///
    /// | Character | Replacement
    /// |-----------|------------
    /// | `<`       | `&lt;`
    /// | `>`       | `&gt;`
    /// | `&`       | `&amp;`
    pub fn partial_escape(self) -> Result<BytesText<'i>, EncodingError> {
        Ok(match self.content {
            Cow::Borrowed(s) => BytesText::from_escaped(partial_escape(s)),
            Cow::Owned(s) => BytesText::from_escaped(partial_escape(&s).into_owned()),
        })
    }

    /// Converts this CDATA content to an escaped version, that can be written
    /// as an usual text in XML. This method escapes only those characters that
    /// must be escaped according to the [specification].
    ///
    /// This function performs following replacements:
    ///
    /// | Character | Replacement
    /// |-----------|------------
    /// | `<`       | `&lt;`
    /// | `&`       | `&amp;`
    ///
    /// [specification]: https://www.w3.org/TR/xml11/#syntax
    pub fn minimal_escape(self) -> Result<BytesText<'i>, EncodingError> {
        Ok(match self.content {
            Cow::Borrowed(s) => BytesText::from_escaped(minimal_escape(s)),
            Cow::Owned(s) => BytesText::from_escaped(minimal_escape(&s).into_owned()),
        })
    }

    /// Returns the content of the CDATA section of the XML 1.0 or HTML event
    /// with EOL normalization applied.
    ///
    /// This will allocate if EOL normalization is required.
    ///
    /// Note, that this method should be used only if event represents XML 1.0 or HTML content,
    /// because rules for normalizing EOLs for [XML 1.0] / [HTML] and [XML 1.1] differs.
    ///
    /// This method also can be used to get HTML content, because rules the same.
    ///
    /// [XML 1.0]: https://www.w3.org/TR/xml/#sec-line-ends
    /// [XML 1.1]: https://www.w3.org/TR/xml11/#sec-line-ends
    /// [HTML]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn xml10_content(&self) -> Cow<'i, str> {
        match &self.content {
            Cow::Borrowed(s) => normalize_xml10_eols(s),
            Cow::Owned(s) => Cow::Owned(normalize_xml10_eols(s).into_owned()),
        }
    }

    /// Returns the content of the CDATA section of the XML 1.1 event
    /// with EOL normalization applied.
    ///
    /// This will allocate if EOL normalization is required.
    ///
    /// Note, that this method should be used only if event represents XML 1.1 content,
    /// because rules for normalizing EOLs for [XML 1.0], [XML 1.1] and [HTML] differs.
    ///
    /// To get HTML content use [`xml10_content()`](Self::xml10_content).
    ///
    /// [XML 1.0]: https://www.w3.org/TR/xml/#sec-line-ends
    /// [XML 1.1]: https://www.w3.org/TR/xml11/#sec-line-ends
    /// [HTML]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn xml11_content(&self) -> Cow<'i, str> {
        match &self.content {
            Cow::Borrowed(s) => normalize_xml11_eols(s),
            Cow::Owned(s) => Cow::Owned(normalize_xml11_eols(s).into_owned()),
        }
    }

    /// Returns the content of the CDATA section with EOL normalization applied
    /// according to the specified version.
    ///
    /// This will allocate if EOL normalization is required.
    #[inline]
    pub fn xml_content(&self, version: XmlVersion) -> Cow<'i, str> {
        match version {
            XmlVersion::Explicit1_1 => self.xml11_content(),
            _ => self.xml10_content(),
        }
    }

    /// Alias for [`xml10_content()`](Self::xml10_content).
    #[inline]
    pub fn html_content(&self) -> Cow<'i, str> {
        self.xml10_content()
    }
}

impl<'i> Debug for BytesCData<'i> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BytesCData {{ content: ")?;
        write_cow_string(f, &self.content)?;
        write!(f, " }}")
    }
}

impl<'i> Deref for BytesCData<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.content
    }
}

impl AsRef<str> for BytesCData<'_> {
    fn as_ref(&self) -> &str {
        self
    }
}

#[cfg(feature = "arbitrary")]
impl<'i> arbitrary::Arbitrary<'i> for BytesCData<'i> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'i>) -> arbitrary::Result<Self> {
        Ok(Self::new(<&str>::arbitrary(u)?))
    }
    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <&str as arbitrary::Arbitrary>::size_hint(depth)
    }
}

/// Iterator over `CDATA` sections in a string.
///
/// This iterator is created by the [`BytesCData::escaped`] method.
#[derive(Debug, Clone)]
pub struct CDataIterator<'a> {
    inner: utils::CDataIterator<'a>,
}

impl<'a> Iterator for CDataIterator<'a> {
    type Item = BytesCData<'a>;

    fn next(&mut self) -> Option<BytesCData<'a>> {
        self.inner.next().map(BytesCData::wrap)
    }
}

impl FusedIterator for CDataIterator<'_> {}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// [Processing instructions][PI] (PIs) allow documents to contain instructions for applications.
///
/// This event implements `Deref<Target = str>`. The `deref()` implementation
/// returns the content of this event between `<?` and `?>`.
///
/// Note, that inner text will not contain `?>` sequence inside:
///
/// ```
/// # use quick_xml::events::{BytesPI, Event};
/// # use quick_xml::reader::Reader;
/// # use pretty_assertions::assert_eq;
/// let mut reader = Reader::from_str("<?processing instruction >:-<~ ?>");
/// let content = "processing instruction >:-<~ ";
/// let event = BytesPI::new(content);
///
/// assert_eq!(reader.read_event().unwrap(), Event::PI(event.borrow()));
/// // deref coercion of &BytesPI to &str
/// assert_eq!(event.as_ref(), content);
/// ```
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [PI]: https://www.w3.org/TR/xml11/#sec-pi
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BytesPI<'i> {
    content: BytesStart<'i>,
}

impl<'i> BytesPI<'i> {
    /// Creates a new `BytesPI` from a string.
    #[inline]
    pub(crate) const fn wrap(content: &'i str, target_len: usize) -> Self {
        Self {
            content: BytesStart::wrap(content, target_len),
        }
    }

    /// Creates a new `BytesPI` from a string.
    ///
    /// # Warning
    ///
    /// `content` must not contain the `?>` sequence.
    #[inline]
    pub fn new<C: Into<Cow<'i, str>>>(content: C) -> Self {
        let buf: Cow<'i, str> = content.into();
        let name_len = name_len(buf.as_bytes());
        Self {
            content: BytesStart { buf, name_len },
        }
    }

    /// Ensures that all data is owned to extend the object's lifetime if
    /// necessary.
    #[inline]
    pub fn into_owned(self) -> BytesPI<'static> {
        BytesPI {
            content: self.content.into_owned(),
        }
    }

    /// Extracts the inner `Cow` from the `BytesPI` event container.
    #[inline]
    pub fn into_inner(self) -> Cow<'i, str> {
        self.content.buf
    }

    /// Converts the event into a borrowed event.
    #[inline]
    pub fn borrow(&self) -> BytesPI<'_> {
        BytesPI {
            content: self.content.borrow(),
        }
    }

    /// A target used to identify the application to which the instruction is directed.
    ///
    /// # Example
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::BytesPI;
    ///
    /// let instruction = BytesPI::new(r#"xml-stylesheet href="style.css""#);
    /// assert_eq!(instruction.target(), "xml-stylesheet");
    /// ```
    #[inline]
    pub fn target(&self) -> &str {
        self.content.name().0
    }

    /// Content of the processing instruction. Contains everything between target
    /// name and the end of the instruction. A direct consequence is that the first
    /// character is always a space character.
    ///
    /// # Example
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::BytesPI;
    ///
    /// let instruction = BytesPI::new(r#"xml-stylesheet href="style.css""#);
    /// assert_eq!(instruction.content(), r#" href="style.css""#);
    /// ```
    #[inline]
    pub fn content(&self) -> &str {
        self.content.attributes_raw()
    }

    /// A view of the processing instructions' content as a list of key-value pairs.
    ///
    /// Key-value pairs are used in some processing instructions, for example in
    /// `<?xml-stylesheet?>`.
    ///
    /// Returned iterator does not validate attribute values as may required by
    /// target's rules. For example, it doesn't check that substring `?>` is not
    /// present in the attribute value. That shouldn't be the problem when event
    /// is produced by the reader, because reader detects end of processing instruction
    /// by the first `?>` sequence, as required by the specification, and therefore
    /// this sequence cannot appear inside it.
    ///
    /// # Example
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use std::borrow::Cow;
    /// use quick_xml::events::attributes::Attribute;
    /// use quick_xml::events::BytesPI;
    /// use quick_xml::name::QName;
    ///
    /// let instruction = BytesPI::new(r#"xml-stylesheet href="style.css""#);
    /// for attr in instruction.attributes() {
    ///     assert_eq!(attr, Ok(Attribute {
    ///         key: QName("href"),
    ///         value: Cow::Borrowed("style.css"),
    ///     }));
    /// }
    /// ```
    #[inline]
    pub fn attributes(&self) -> Attributes<'_> {
        self.content.attributes()
    }
}

impl<'i> Debug for BytesPI<'i> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BytesPI {{ content: ")?;
        write_cow_string(f, &self.content.buf)?;
        write!(f, " }}")
    }
}

impl<'i> Deref for BytesPI<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.content.buf
    }
}

impl AsRef<str> for BytesPI<'_> {
    fn as_ref(&self) -> &str {
        self
    }
}

#[cfg(feature = "arbitrary")]
impl<'i> arbitrary::Arbitrary<'i> for BytesPI<'i> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'i>) -> arbitrary::Result<Self> {
        Ok(Self::new(<&str>::arbitrary(u)?))
    }
    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <&str as arbitrary::Arbitrary>::size_hint(depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// An XML declaration (`Event::Decl`).
///
/// [W3C XML 1.1 Prolog and Document Type Declaration](http://w3.org/TR/xml11/#sec-prolog-dtd)
///
/// This event implements `Deref<Target = str>`. The `deref()` implementation
/// returns the content of this event between `<?` and `?>`.
///
/// Note, that inner text will not contain `?>` sequence inside:
///
/// ```
/// # use quick_xml::events::{BytesDecl, BytesStart, Event};
/// # use quick_xml::reader::Reader;
/// # use pretty_assertions::assert_eq;
/// let mut reader = Reader::from_str("<?xml version = '1.0' ?>");
/// let content = "xml version = '1.0' ";
/// let event = BytesDecl::from_start(BytesStart::from_content(content, 3));
///
/// assert_eq!(reader.read_event().unwrap(), Event::Decl(event.borrow()));
/// // deref coercion of &BytesDecl to &str
/// assert_eq!(event.as_ref(), content);
/// ```
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BytesDecl<'i> {
    content: BytesStart<'i>,
}

impl<'i> BytesDecl<'i> {
    /// Constructs a new `XmlDecl` from the (mandatory) _version_ (should be `1.0` or `1.1`),
    /// the optional _encoding_ (e.g., `UTF-8`) and the optional _standalone_ (`yes` or `no`)
    /// attribute.
    ///
    /// Does not escape any of its inputs. Always uses double quotes to wrap the attribute values.
    /// The caller is responsible for escaping attribute values. Shouldn't usually be relevant since
    /// the double quote character is not allowed in any of the attribute values.
    pub fn new(
        version: &str,
        encoding: Option<&str>,
        standalone: Option<&str>,
    ) -> BytesDecl<'static> {
        // Compute length of the buffer based on supplied attributes
        // ' encoding=""'   => 12
        let encoding_attr_len = if let Some(xs) = encoding {
            12 + xs.len()
        } else {
            0
        };
        // ' standalone=""' => 14
        let standalone_attr_len = if let Some(xs) = standalone {
            14 + xs.len()
        } else {
            0
        };
        // 'xml version=""' => 14
        let mut buf = String::with_capacity(14 + encoding_attr_len + standalone_attr_len);

        buf.push_str("xml version=\"");
        buf.push_str(version);

        if let Some(encoding_val) = encoding {
            buf.push_str("\" encoding=\"");
            buf.push_str(encoding_val);
        }

        if let Some(standalone_val) = standalone {
            buf.push_str("\" standalone=\"");
            buf.push_str(standalone_val);
        }
        buf.push('"');

        BytesDecl {
            content: BytesStart::from_content(buf, 3),
        }
    }

    /// Creates a `BytesDecl` from a `BytesStart`
    pub const fn from_start(start: BytesStart<'i>) -> Self {
        Self { content: start }
    }

    /// Gets xml version, excluding quotes (`'` or `"`).
    ///
    /// According to the [grammar], the version *must* be the first thing in the declaration.
    /// This method tries to extract the first thing in the declaration and return it.
    /// In case of multiple attributes value of the first one is returned.
    ///
    /// If version is missed in the declaration, or the first thing is not a version,
    /// [`IllFormedError::MissingDeclVersion`] will be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use quick_xml::errors::{Error, IllFormedError};
    /// use quick_xml::events::{BytesDecl, BytesStart};
    ///
    /// // <?xml version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" version='1.1'", 0));
    /// assert_eq!(decl.version().unwrap(), "1.1");
    ///
    /// // <?xml version='1.0' version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" version='1.0' version='1.1'", 0));
    /// assert_eq!(decl.version().unwrap(), "1.0");
    ///
    /// // <?xml encoding='utf-8'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" encoding='utf-8'", 0));
    /// match decl.version() {
    ///     Err(Error::IllFormed(IllFormedError::MissingDeclVersion(Some(key)))) => assert_eq!(key, "encoding"),
    ///     _ => assert!(false),
    /// }
    ///
    /// // <?xml encoding='utf-8' version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" encoding='utf-8' version='1.1'", 0));
    /// match decl.version() {
    ///     Err(Error::IllFormed(IllFormedError::MissingDeclVersion(Some(key)))) => assert_eq!(key, "encoding"),
    ///     _ => assert!(false),
    /// }
    ///
    /// // <?xml?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content("", 0));
    /// match decl.version() {
    ///     Err(Error::IllFormed(IllFormedError::MissingDeclVersion(None))) => {},
    ///     _ => assert!(false),
    /// }
    /// ```
    ///
    /// [grammar]: https://www.w3.org/TR/xml11/#NT-XMLDecl
    pub fn version(&self) -> Result<Cow<'_, str>, Error> {
        // The version *must* be the first thing in the declaration.
        match self.content.attributes().with_checks(false).next() {
            Some(Ok(a)) if a.key.as_ref() == "version" => Ok(a.value),
            // first attribute was not "version"
            Some(Ok(a)) => {
                let found = a.key.as_ref().to_string();
                Err(Error::IllFormed(IllFormedError::MissingDeclVersion(Some(
                    found,
                ))))
            }
            // error parsing attributes
            Some(Err(e)) => Err(e.into()),
            // no attributes
            None => Err(Error::IllFormed(IllFormedError::MissingDeclVersion(None))),
        }
    }

    /// Gets xml encoding, excluding quotes (`'` or `"`).
    ///
    /// Although according to the [grammar] encoding must appear before `"standalone"`
    /// and after `"version"`, this method does not check that. The first occurrence
    /// of the attribute will be returned even if there are several. Also, method does
    /// not restrict symbols that can forming the encoding, so the returned encoding
    /// name may not correspond to the grammar.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::borrow::Cow;
    /// use quick_xml::Error;
    /// use quick_xml::events::{BytesDecl, BytesStart};
    ///
    /// // <?xml version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" version='1.1'", 0));
    /// assert!(decl.encoding().is_none());
    ///
    /// // <?xml encoding='utf-8'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" encoding='utf-8'", 0));
    /// match decl.encoding() {
    ///     Some(Ok(Cow::Borrowed(encoding))) => assert_eq!(encoding, "utf-8"),
    ///     _ => assert!(false),
    /// }
    ///
    /// // <?xml encoding='something_WRONG' encoding='utf-8'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" encoding='something_WRONG' encoding='utf-8'", 0));
    /// match decl.encoding() {
    ///     Some(Ok(Cow::Borrowed(encoding))) => assert_eq!(encoding, "something_WRONG"),
    ///     _ => assert!(false),
    /// }
    /// ```
    ///
    /// [grammar]: https://www.w3.org/TR/xml11/#NT-XMLDecl
    pub fn encoding(&self) -> Option<Result<Cow<'_, str>, AttrError>> {
        self.content
            .try_get_attribute("encoding")
            .map(|a| a.map(|a| a.value))
            .transpose()
    }

    /// Gets xml standalone, excluding quotes (`'` or `"`).
    ///
    /// Although according to the [grammar] standalone flag must appear after `"version"`
    /// and `"encoding"`, this method does not check that. The first occurrence of the
    /// attribute will be returned even if there are several. Also, method does not
    /// restrict symbols that can forming the value, so the returned flag name may not
    /// correspond to the grammar.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::borrow::Cow;
    /// use quick_xml::Error;
    /// use quick_xml::events::{BytesDecl, BytesStart};
    ///
    /// // <?xml version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" version='1.1'", 0));
    /// assert!(decl.standalone().is_none());
    ///
    /// // <?xml standalone='yes'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" standalone='yes'", 0));
    /// match decl.standalone() {
    ///     Some(Ok(Cow::Borrowed(encoding))) => assert_eq!(encoding, "yes"),
    ///     _ => assert!(false),
    /// }
    ///
    /// // <?xml standalone='something_WRONG' encoding='utf-8'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" standalone='something_WRONG' encoding='utf-8'", 0));
    /// match decl.standalone() {
    ///     Some(Ok(Cow::Borrowed(flag))) => assert_eq!(flag, "something_WRONG"),
    ///     _ => assert!(false),
    /// }
    /// ```
    ///
    /// [grammar]: https://www.w3.org/TR/xml11/#NT-XMLDecl
    pub fn standalone(&self) -> Option<Result<Cow<'_, str>, AttrError>> {
        self.content
            .try_get_attribute("standalone")
            .map(|a| a.map(|a| a.value))
            .transpose()
    }

    /// Gets XML version as typified enumeration.
    ///
    /// According to the [grammar], the version *must* be the first thing in the declaration.
    /// This method tries to extract the first thing in the declaration and return it.
    /// In case of multiple attributes value of the first one is returned.
    ///
    /// If version is missed in the declaration, or the first thing is not a version,
    /// [`IllFormedError::MissingDeclVersion`] will be returned.
    ///
    /// If version is not 1.0 or 1.1, [`IllFormedError::UnknownVersion`] will be returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use quick_xml::XmlVersion;
    /// use quick_xml::errors::{Error, IllFormedError};
    /// use quick_xml::events::{BytesDecl, BytesStart};
    ///
    /// // <?xml version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" version='1.1'", 0));
    /// assert_eq!(decl.xml_version().unwrap(), XmlVersion::Explicit1_1);
    ///
    /// // <?xml version='1.0' version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" version='1.0' version='1.1'", 0));
    /// assert_eq!(decl.xml_version().unwrap(), XmlVersion::Explicit1_0);
    ///
    /// // <?xml version='1.2'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" version='1.2'", 0));
    /// match decl.xml_version() {
    ///     Err(Error::IllFormed(IllFormedError::UnknownVersion)) => {},
    ///     _ => assert!(false),
    /// }
    ///
    /// // <?xml encoding='utf-8'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" encoding='utf-8'", 0));
    /// match decl.xml_version() {
    ///     Err(Error::IllFormed(IllFormedError::MissingDeclVersion(Some(key)))) => assert_eq!(key, "encoding"),
    ///     _ => assert!(false),
    /// }
    ///
    /// // <?xml encoding='utf-8' version='1.1'?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content(" encoding='utf-8' version='1.1'", 0));
    /// match decl.xml_version() {
    ///     Err(Error::IllFormed(IllFormedError::MissingDeclVersion(Some(key)))) => assert_eq!(key, "encoding"),
    ///     _ => assert!(false),
    /// }
    ///
    /// // <?xml?>
    /// let decl = BytesDecl::from_start(BytesStart::from_content("", 0));
    /// match decl.xml_version() {
    ///     Err(Error::IllFormed(IllFormedError::MissingDeclVersion(None))) => {},
    ///     _ => assert!(false),
    /// }
    /// ```
    ///
    /// [grammar]: https://www.w3.org/TR/xml11/#NT-XMLDecl
    pub fn xml_version(&self) -> Result<XmlVersion, Error> {
        let v = self.version()?;
        match v.as_ref() {
            "1.0" => Ok(XmlVersion::Explicit1_0),
            "1.1" => Ok(XmlVersion::Explicit1_1),
            _ => Err(Error::IllFormed(IllFormedError::UnknownVersion)),
        }
    }

    /// Gets the actual encoding using [_get an encoding_](https://encoding.spec.whatwg.org/#concept-encoding-get)
    /// algorithm.
    ///
    /// If encoding in not known, or `encoding` key was not found, returns `None`.
    /// In case of duplicated `encoding` key, encoding, corresponding to the first
    /// one, is returned.
    #[cfg(feature = "encoding")]
    pub fn encoder(&self) -> Option<&'static Encoding> {
        self.encoding()
            .and_then(|e| e.ok())
            .and_then(|e| Encoding::for_label(e.as_bytes()))
    }

    /// Converts the event into an owned event.
    pub fn into_owned(self) -> BytesDecl<'static> {
        BytesDecl {
            content: self.content.into_owned(),
        }
    }

    /// Converts the event into a borrowed event.
    #[inline]
    pub fn borrow(&self) -> BytesDecl<'_> {
        BytesDecl {
            content: self.content.borrow(),
        }
    }
}

impl<'i> Deref for BytesDecl<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.content.buf
    }
}

impl AsRef<str> for BytesDecl<'_> {
    fn as_ref(&self) -> &str {
        self
    }
}

#[cfg(feature = "arbitrary")]
impl<'i> arbitrary::Arbitrary<'i> for BytesDecl<'i> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'i>) -> arbitrary::Result<Self> {
        Ok(Self::new(
            <&str>::arbitrary(u)?,
            Option::<&str>::arbitrary(u)?,
            Option::<&str>::arbitrary(u)?,
        ))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <&str as arbitrary::Arbitrary>::size_hint(depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Character or general entity reference (`Event::GeneralRef`): `&ref;` or `&#<number>;`.
///
/// This event implements `Deref<Target = str>`. The `deref()` implementation
/// returns the content of this event between `&` and `;`:
///
/// ```
/// # use quick_xml::events::{BytesRef, Event};
/// # use quick_xml::reader::Reader;
/// # use pretty_assertions::assert_eq;
/// let mut reader = Reader::from_str(r#"&entity;"#);
/// let content = "entity";
/// let event = BytesRef::new(content);
///
/// assert_eq!(reader.read_event().unwrap(), Event::GeneralRef(event.borrow()));
/// // deref coercion of &BytesRef to &str
/// assert_eq!(event.as_ref(), content);
/// ```
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct BytesRef<'i> {
    content: Cow<'i, str>,
}

impl<'i> BytesRef<'i> {
    /// Internal constructor, used by `Reader`. Supplies data in reader's encoding
    #[inline]
    pub(crate) const fn wrap(content: &'i str) -> Self {
        Self {
            content: Cow::Borrowed(content),
        }
    }

    /// Creates a new `BytesRef` borrowing a slice.
    ///
    /// # Warning
    ///
    /// `name` must be a valid name.
    #[inline]
    pub fn new<C: Into<Cow<'i, str>>>(name: C) -> Self {
        Self {
            content: name.into(),
        }
    }

    /// Converts the event into an owned event.
    pub fn into_owned(self) -> BytesRef<'static> {
        BytesRef {
            content: Cow::Owned(self.content.into_owned()),
        }
    }

    /// Extracts the inner `Cow` from the `BytesRef` event container.
    #[inline]
    pub fn into_inner(self) -> Cow<'i, str> {
        self.content
    }

    /// Converts the event into a borrowed event.
    #[inline]
    pub fn borrow(&self) -> BytesRef<'_> {
        BytesRef {
            content: Cow::Borrowed(&self.content),
        }
    }

    /// Returns the content of the event as a string.
    ///
    /// Returns the content of the XML 1.0 or HTML event with EOL normalization applied.
    ///
    /// This will allocate if EOL normalization is required.
    ///
    /// Note, that this method should be used only if event represents XML 1.0 or HTML content,
    /// because rules for normalizing EOLs for [XML 1.0] / [HTML] and [XML 1.1] differs.
    ///
    /// This method also can be used to get HTML content, because rules the same.
    ///
    /// [XML 1.0]: https://www.w3.org/TR/xml/#sec-line-ends
    /// [XML 1.1]: https://www.w3.org/TR/xml11/#sec-line-ends
    /// [HTML]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn xml10_content(&self) -> Cow<'i, str> {
        match &self.content {
            Cow::Borrowed(s) => normalize_xml10_eols(s),
            Cow::Owned(s) => Cow::Owned(normalize_xml10_eols(s).into_owned()),
        }
    }

    /// Returns the content of the XML 1.1 event with EOL normalization applied.
    ///
    /// This will allocate if EOL normalization is required.
    ///
    /// Note, that this method should be used only if event represents XML 1.1 content,
    /// because rules for normalizing EOLs for [XML 1.0] / [HTML] and [XML 1.1] differs.
    ///
    /// To get HTML content use [`xml10_content()`](Self::xml10_content).
    ///
    /// [XML 1.0]: https://www.w3.org/TR/xml/#sec-line-ends
    /// [XML 1.1]: https://www.w3.org/TR/xml11/#sec-line-ends
    /// [HTML]: https://html.spec.whatwg.org/#normalize-newlines
    pub fn xml11_content(&self) -> Cow<'i, str> {
        match &self.content {
            Cow::Borrowed(s) => normalize_xml11_eols(s),
            Cow::Owned(s) => Cow::Owned(normalize_xml11_eols(s).into_owned()),
        }
    }

    /// Returns the content with EOL normalization applied according to the
    /// specified version.
    ///
    /// This will allocate if EOL normalization is required.
    #[inline]
    pub fn xml_content(&self, version: XmlVersion) -> Cow<'i, str> {
        match version {
            XmlVersion::Explicit1_1 => self.xml11_content(),
            _ => self.xml10_content(),
        }
    }

    /// Alias for [`xml10_content()`](Self::xml10_content).
    #[inline]
    pub fn html_content(&self) -> Cow<'i, str> {
        self.xml10_content()
    }

    /// Returns `true` if the specified reference represents the character reference
    /// (`&#<number>;`).
    ///
    /// ```
    /// # use quick_xml::events::BytesRef;
    /// # use pretty_assertions::assert_eq;
    /// assert_eq!(BytesRef::new("#x30").is_char_ref(), true);
    /// assert_eq!(BytesRef::new("#49" ).is_char_ref(), true);
    /// assert_eq!(BytesRef::new("lt"  ).is_char_ref(), false);
    /// ```
    pub fn is_char_ref(&self) -> bool {
        self.content.starts_with('#')
    }

    /// If this reference represents character reference, then resolves it and
    /// returns the character, otherwise returns `None`.
    ///
    /// This method does not check if character is allowed for XML, in other words,
    /// well-formedness constraint [WFC: Legal Char] is not enforced.
    /// The character `0x0`, however, will return `EscapeError::InvalidCharRef`.
    ///
    /// ```
    /// # use quick_xml::events::BytesRef;
    /// # use pretty_assertions::assert_eq;
    /// assert_eq!(BytesRef::new("#x30").resolve_char_ref().unwrap(), Some('0'));
    /// assert_eq!(BytesRef::new("#49" ).resolve_char_ref().unwrap(), Some('1'));
    /// assert_eq!(BytesRef::new("lt"  ).resolve_char_ref().unwrap(), None);
    /// ```
    ///
    /// [WFC: Legal Char]: https://www.w3.org/TR/xml11/#wf-Legalchar
    pub fn resolve_char_ref(&self) -> Result<Option<char>, Error> {
        if let Some(num) = self.content.strip_prefix('#') {
            let ch = parse_number(num).map_err(EscapeError::InvalidCharRef)?;
            return Ok(Some(ch));
        }
        Ok(None)
    }
}

impl<'i> Debug for BytesRef<'i> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BytesRef {{ content: ")?;
        write_cow_string(f, &self.content)?;
        write!(f, " }}")
    }
}

impl<'i> Deref for BytesRef<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.content
    }
}

impl AsRef<str> for BytesRef<'_> {
    fn as_ref(&self) -> &str {
        self
    }
}

#[cfg(feature = "arbitrary")]
impl<'i> arbitrary::Arbitrary<'i> for BytesRef<'i> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'i>) -> arbitrary::Result<Self> {
        Ok(Self::new(<&str>::arbitrary(u)?))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <&str as arbitrary::Arbitrary>::size_hint(depth)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Event emitted by [`Reader::read_event_into`].
///
/// # Lifetime
///
/// `'i` (stands of "input") is a lifetime of the original buffer from which event was parsed.
/// In particular, when reader was created from a string, this is lifetime of the string.
/// If event come from a buffered reader, this is lifetime of the user-provided buffer.
/// If such event need to outlive the single parsing loop iteration, take ownership of the data
/// using [`.into_owned()`].
///
/// [`Reader::read_event_into`]: crate::reader::Reader::read_event_into
/// [`.into_owned()`]: Self::into_owned
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Event<'i> {
    /// Start tag (with attributes) `<tag attr="value">`.
    Start(BytesStart<'i>),
    /// End tag `</tag>`.
    End(BytesEnd<'i>),
    /// Empty element tag (with attributes) `<tag attr="value" />`.
    Empty(BytesStart<'i>),
    /// Escaped character data between tags.
    Text(BytesText<'i>),
    /// Unescaped character data stored in `<![CDATA[...]]>`.
    CData(BytesCData<'i>),
    /// Comment `<!-- ... -->`.
    Comment(BytesText<'i>),
    /// XML declaration `<?xml ...?>`.
    Decl(BytesDecl<'i>),
    /// Processing instruction `<?...?>`.
    PI(BytesPI<'i>),
    /// Document type definition data (DTD) stored in `<!DOCTYPE ...>`.
    DocType(BytesText<'i>),
    /// General reference `&entity;` in the textual data. Can be either an entity
    /// reference, or a character reference.
    GeneralRef(BytesRef<'i>),
    /// End of XML document.
    Eof,
}

impl<'i> Event<'i> {
    /// Converts the event to an owned version, untied to the lifetime of
    /// buffer used when reading but incurring a new, separate allocation.
    pub fn into_owned(self) -> Event<'static> {
        match self {
            Event::Start(e) => Event::Start(e.into_owned()),
            Event::End(e) => Event::End(e.into_owned()),
            Event::Empty(e) => Event::Empty(e.into_owned()),
            Event::Text(e) => Event::Text(e.into_owned()),
            Event::Comment(e) => Event::Comment(e.into_owned()),
            Event::CData(e) => Event::CData(e.into_owned()),
            Event::Decl(e) => Event::Decl(e.into_owned()),
            Event::PI(e) => Event::PI(e.into_owned()),
            Event::DocType(e) => Event::DocType(e.into_owned()),
            Event::GeneralRef(e) => Event::GeneralRef(e.into_owned()),
            Event::Eof => Event::Eof,
        }
    }

    /// Converts the event into a borrowed event.
    #[inline]
    pub fn borrow(&self) -> Event<'_> {
        match self {
            Event::Start(e) => Event::Start(e.borrow()),
            Event::End(e) => Event::End(e.borrow()),
            Event::Empty(e) => Event::Empty(e.borrow()),
            Event::Text(e) => Event::Text(e.borrow()),
            Event::Comment(e) => Event::Comment(e.borrow()),
            Event::CData(e) => Event::CData(e.borrow()),
            Event::Decl(e) => Event::Decl(e.borrow()),
            Event::PI(e) => Event::PI(e.borrow()),
            Event::DocType(e) => Event::DocType(e.borrow()),
            Event::GeneralRef(e) => Event::GeneralRef(e.borrow()),
            Event::Eof => Event::Eof,
        }
    }
}

impl<'i> Deref for Event<'i> {
    type Target = str;

    fn deref(&self) -> &str {
        match *self {
            Event::Start(ref e) | Event::Empty(ref e) => e,
            Event::End(ref e) => e,
            Event::Text(ref e) => e,
            Event::Decl(ref e) => e,
            Event::PI(ref e) => e,
            Event::CData(ref e) => e,
            Event::Comment(ref e) => e,
            Event::DocType(ref e) => e,
            Event::GeneralRef(ref e) => e,
            Event::Eof => "",
        }
    }
}

impl<'i> AsRef<Event<'i>> for Event<'i> {
    fn as_ref(&self) -> &Event<'i> {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

fn trim_cow<'a, F>(value: Cow<'a, str>, trim: F) -> Cow<'a, str>
where
    F: for<'s> FnOnce(&'s str) -> &'s str,
{
    match value {
        Cow::Borrowed(s) => Cow::Borrowed(trim(s)),
        Cow::Owned(s) => {
            let trimmed = trim(&s);
            if trimmed.len() != s.len() {
                Cow::Owned(trimmed.to_owned())
            } else {
                Cow::Owned(s)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn bytestart_create() {
        let b = BytesStart::new("test");
        assert_eq!(b.len(), 4);
        assert_eq!(b.name(), QName("test"));
    }

    #[test]
    fn bytestart_set_name() {
        let mut b = BytesStart::new("test");
        assert_eq!(b.len(), 4);
        assert_eq!(b.name(), QName("test"));
        assert_eq!(b.attributes_raw(), "");
        b.push_attribute(("x", "a"));
        assert_eq!(b.len(), 10);
        assert_eq!(b.attributes_raw(), " x=\"a\"");
        b.set_name("g");
        assert_eq!(b.len(), 7);
        assert_eq!(b.name(), QName("g"));
    }

    #[test]
    fn bytestart_clear_attributes() {
        let mut b = BytesStart::new("test");
        b.push_attribute(("x", "y\"z"));
        b.push_attribute(("x", "y\"z"));
        b.clear_attributes();
        assert!(b.attributes().next().is_none());
        assert_eq!(b.len(), 4);
        assert_eq!(b.name(), QName("test"));
    }
}
