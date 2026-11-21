//! A reader that manages namespace declarations found in the input and able
//! to resolve [qualified names] to [expanded names].
//!
//! [qualified names]: https://www.w3.org/TR/xml-names11/#dt-qualname
//! [expanded names]: https://www.w3.org/TR/xml-names11/#dt-expname

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::Deref;
use std::path::Path;

use crate::errors::Result;
use crate::events::{BytesText, Event};
use crate::name::{NamespaceResolver, QName, ResolveResult};
use crate::reader::{Config, Reader, Span, XmlSource};

/// A low level encoding-agnostic XML event reader that performs namespace resolution.
///
/// Consumes a [`BufRead`] and streams XML `Event`s.
#[derive(Debug, Clone)]
pub struct NsReader<R> {
    /// An XML reader
    pub(super) reader: Reader<R>,
    /// A buffer to manage namespaces
    pub(super) ns_resolver: NamespaceResolver,
    /// We cannot pop data from the namespace stack until returned `Empty` or `End`
    /// event will be processed by the user, so we only mark that we should that
    /// in the next [`Self::read_event_impl()`] call.
    pending_pop: bool,
}

/// Builder methods
impl<R> NsReader<R> {
    /// Creates a `NsReader` that reads from a reader.
    #[inline]
    pub fn from_reader(reader: R) -> Self {
        Self::new(Reader::from_reader(reader))
    }

    /// Returns reference to the parser configuration
    #[inline]
    pub const fn config(&self) -> &Config {
        self.reader.config()
    }

    /// Returns mutable reference to the parser configuration
    #[inline]
    pub fn config_mut(&mut self) -> &mut Config {
        self.reader.config_mut()
    }
}

/// Private methods
impl<R> NsReader<R> {
    #[inline]
    fn new(reader: Reader<R>) -> Self {
        Self {
            reader,
            ns_resolver: NamespaceResolver::default(),
            pending_pop: false,
        }
    }

    fn read_event_impl<'i, B>(&mut self, buf: B) -> Result<Event<'i>>
    where
        R: XmlSource<'i, B>,
    {
        self.pop();
        let event = self.reader.read_event_impl(buf);
        self.process_event(event)
    }

    pub(super) fn pop(&mut self) {
        if self.pending_pop {
            self.ns_resolver.pop();
            self.pending_pop = false;
        }
    }

    pub(super) fn process_event<'i>(&mut self, event: Result<Event<'i>>) -> Result<Event<'i>> {
        match event {
            Ok(Event::Start(e)) => {
                self.ns_resolver.push(&e)?;
                Ok(Event::Start(e))
            }
            Ok(Event::Empty(e)) => {
                self.ns_resolver.push(&e)?;
                // notify next `read_event_impl()` invocation that it needs to pop this
                // namespace scope
                self.pending_pop = true;
                Ok(Event::Empty(e))
            }
            Ok(Event::End(e)) => {
                // notify next `read_event_impl()` invocation that it needs to pop this
                // namespace scope
                self.pending_pop = true;
                Ok(Event::End(e))
            }
            e => e,
        }
    }
}

/// Getters
impl<R> NsReader<R> {
    /// Consumes `NsReader` returning the underlying reader
    ///
    /// See the [`Reader::into_inner`] for examples
    #[inline]
    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }

    /// Gets a mutable reference to the underlying reader.
    pub fn get_mut(&mut self) -> &mut R {
        self.reader.get_mut()
    }

    /// Returns a storage of namespace bindings associated with this reader.
    #[inline]
    pub const fn resolver(&self) -> &NamespaceResolver {
        &self.ns_resolver
    }

    /// Returns a mutable reference to the storage of namespace bindings
    /// associated with this reader.
    ///
    /// Useful for configuring the resolver, e.g. to change the
    /// [per-element namespace-declaration limit](NamespaceResolver::set_max_declarations_per_element).
    #[inline]
    pub fn resolver_mut(&mut self) -> &mut NamespaceResolver {
        &mut self.ns_resolver
    }
}

impl<R: BufRead> NsReader<R> {
    /// Reads the next event into given buffer.
    ///
    /// This method manages namespaces but doesn't resolve them automatically.
    /// You should call [`resolver().resolve_element()`] if you want to get a namespace.
    ///
    /// You also can use [`read_resolved_event_into()`] instead if you want to resolve
    /// namespace as soon as you get an event.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::Event;
    /// use quick_xml::name::{Namespace, ResolveResult::*};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str(r#"
    ///     <x:tag1 xmlns:x="www.xxxx" xmlns:y="www.yyyy" att1 = "test">
    ///        <y:tag2><!--Test comment-->Test</y:tag2>
    ///        <y:tag2>Test 2</y:tag2>
    ///     </x:tag1>
    /// "#);
    /// reader.config_mut().trim_text(true);
    ///
    /// let mut count = 0;
    /// let mut buf = Vec::new();
    /// let mut txt = Vec::new();
    /// loop {
    ///     match reader.read_event_into(&mut buf).unwrap() {
    ///         Event::Start(e) => {
    ///             count += 1;
    ///             let (ns, local) = reader.resolver().resolve_element(e.name());
    ///             match local.as_ref() {
    ///                 b"tag1" => assert_eq!(ns, Bound(Namespace(b"www.xxxx"))),
    ///                 b"tag2" => assert_eq!(ns, Bound(Namespace(b"www.yyyy"))),
    ///                 _ => unreachable!(),
    ///             }
    ///         }
    ///         Event::Text(e) => {
    ///             txt.push(e.decode().unwrap().into_owned())
    ///         }
    ///         Event::Eof => break,
    ///         _ => (),
    ///     }
    ///     buf.clear();
    /// }
    /// assert_eq!(count, 3);
    /// assert_eq!(txt, vec!["Test".to_string(), "Test 2".to_string()]);
    /// ```
    ///
    /// [`resolver().resolve_element()`]: NamespaceResolver::resolve_element
    /// [`read_resolved_event_into()`]: Self::read_resolved_event_into
    #[inline]
    pub fn read_event_into<'b>(&mut self, buf: &'b mut Vec<u8>) -> Result<Event<'b>> {
        self.read_event_impl(buf)
    }

    /// Reads the next event into given buffer and resolves its namespace (if applicable).
    ///
    /// Namespace is resolved only for [`Start`], [`Empty`] and [`End`] events.
    /// For all other events the concept of namespace is not defined, so
    /// a [`ResolveResult::Unbound`] is returned.
    ///
    /// If you are not interested in namespaces, you can use [`read_event_into()`]
    /// which will not automatically resolve namespaces for you.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::Event;
    /// use quick_xml::name::{Namespace, QName, ResolveResult::*};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str(r#"
    ///     <x:tag1 xmlns:x="www.xxxx" xmlns:y="www.yyyy" att1 = "test">
    ///        <y:tag2><!--Test comment-->Test</y:tag2>
    ///        <y:tag2>Test 2</y:tag2>
    ///     </x:tag1>
    /// "#);
    /// reader.config_mut().trim_text(true);
    ///
    /// let mut count = 0;
    /// let mut buf = Vec::new();
    /// let mut txt = Vec::new();
    /// loop {
    ///     match reader.read_resolved_event_into(&mut buf).unwrap() {
    ///         (Bound(Namespace(b"www.xxxx")), Event::Start(e)) => {
    ///             count += 1;
    ///             assert_eq!(e.local_name(), QName(b"tag1").into());
    ///         }
    ///         (Bound(Namespace(b"www.yyyy")), Event::Start(e)) => {
    ///             count += 1;
    ///             assert_eq!(e.local_name(), QName(b"tag2").into());
    ///         }
    ///         (_, Event::Start(_)) => unreachable!(),
    ///
    ///         (_, Event::Text(e)) => {
    ///             txt.push(e.decode().unwrap().into_owned())
    ///         }
    ///         (_, Event::Eof) => break,
    ///         _ => (),
    ///     }
    ///     buf.clear();
    /// }
    /// assert_eq!(count, 3);
    /// assert_eq!(txt, vec!["Test".to_string(), "Test 2".to_string()]);
    /// ```
    ///
    /// [`Start`]: Event::Start
    /// [`Empty`]: Event::Empty
    /// [`End`]: Event::End
    /// [`read_event_into()`]: Self::read_event_into
    #[inline]
    pub fn read_resolved_event_into<'b>(
        &mut self,
        buf: &'b mut Vec<u8>,
    ) -> Result<(ResolveResult<'_>, Event<'b>)> {
        let event = self.read_event_impl(buf)?;
        Ok(self.ns_resolver.resolve_event(event))
    }

    /// Reads until end element is found using provided buffer as intermediate
    /// storage for events content. This function is supposed to be called after
    /// you already read a [`Start`] event.
    ///
    /// Returns a span that cover content between `>` of an opening tag and `<` of
    /// a closing tag or an empty slice, if [`expand_empty_elements`] is set and
    /// this method was called after reading expanded [`Start`] event.
    ///
    /// Manages nested cases where parent and child elements have the _literally_
    /// same name.
    ///
    /// If a corresponding [`End`] event is not found, an error of type [`IllFormed`]
    /// will be returned. In particularly, that error will be returned if you call
    /// this method without consuming the corresponding [`Start`] event first.
    ///
    /// If your reader created from a string slice or byte array slice, it is
    /// better to use [`read_to_end()`] method, because it will not copy bytes
    /// into intermediate buffer.
    ///
    /// The provided `buf` buffer will be filled only by one event content at time.
    /// Before reading of each event the buffer will be cleared. If you know an
    /// appropriate size of each event, you can preallocate the buffer to reduce
    /// number of reallocations.
    ///
    /// The `end` parameter should contain name of the end element _in the reader
    /// encoding_. It is good practice to always get that parameter using
    /// [`BytesStart::to_end()`] method.
    ///
    /// # Namespaces
    ///
    /// While the `NsReader` does namespace resolution, namespaces does not
    /// change the algorithm for comparing names. Although the names `a:name`
    /// and `b:name` where both prefixes `a` and `b` resolves to the same namespace,
    /// are semantically equivalent, `</b:name>` cannot close `<a:name>`, because
    /// according to [the specification]
    ///
    /// > The end of every element that begins with a **start-tag** MUST be marked
    /// > by an **end-tag** containing a name that echoes the element's type as
    /// > given in the **start-tag**
    ///
    /// # Examples
    ///
    /// This example shows, how you can skip XML content after you read the
    /// start event.
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::{BytesStart, Event};
    /// use quick_xml::name::{Namespace, ResolveResult};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str(r#"
    ///     <outer xmlns="namespace 1">
    ///         <inner xmlns="namespace 2">
    ///             <outer></outer>
    ///         </inner>
    ///         <inner>
    ///             <inner></inner>
    ///             <inner/>
    ///             <outer></outer>
    ///             <p:outer xmlns:p="ns"></p:outer>
    ///             <outer/>
    ///         </inner>
    ///     </outer>
    /// "#);
    /// reader.config_mut().trim_text(true);
    /// let mut buf = Vec::new();
    ///
    /// let ns = Namespace(b"namespace 1");
    /// let start = BytesStart::from_content(r#"outer xmlns="namespace 1""#, 5);
    /// let end   = start.to_end().into_owned();
    ///
    /// // First, we read a start event...
    /// assert_eq!(
    ///     reader.read_resolved_event_into(&mut buf).unwrap(),
    ///     (ResolveResult::Bound(ns), Event::Start(start))
    /// );
    ///
    /// // ...then, we could skip all events to the corresponding end event.
    /// // This call will correctly handle nested <outer> elements.
    /// // Note, however, that this method does not handle namespaces.
    /// reader.read_to_end_into(end.name(), &mut buf).unwrap();
    ///
    /// // At the end we should get an Eof event, because we ate the whole XML
    /// assert_eq!(
    ///     reader.read_resolved_event_into(&mut buf).unwrap(),
    ///     (ResolveResult::Unbound, Event::Eof)
    /// );
    /// ```
    ///
    /// [`Start`]: Event::Start
    /// [`End`]: Event::End
    /// [`IllFormed`]: crate::errors::Error::IllFormed
    /// [`read_to_end()`]: Self::read_to_end
    /// [`BytesStart::to_end()`]: crate::events::BytesStart::to_end
    /// [`expand_empty_elements`]: Config::expand_empty_elements
    /// [the specification]: https://www.w3.org/TR/xml11/#dt-etag
    #[inline]
    pub fn read_to_end_into(&mut self, end: QName, buf: &mut Vec<u8>) -> Result<Span> {
        // According to the https://www.w3.org/TR/xml11/#dt-etag, end name should
        // match literally the start name. See `Config::check_end_names` documentation
        let result = self.reader.read_to_end_into(end, buf)?;
        // read_to_end_into will consume closing tag. Because nobody can access to its
        // content anymore, we directly pop namespace of the opening tag
        self.ns_resolver.pop();
        Ok(result)
    }

    /// Reads content between start and end tags, including any markup using
    /// provided buffer as intermediate storage for events content. This function
    /// is supposed to be called after you already read a [`Start`] event.
    ///
    /// Manages nested cases where parent and child elements have the _literally_
    /// same name.
    ///
    /// This method does not unescape read data, instead it returns content
    /// "as is" of the XML document. This is because it has no idea what text
    /// it reads, and if, for example, it contains CDATA section, attempt to
    /// unescape it content will spoil data.
    ///
    /// If your reader created from a string slice or byte array slice, it is
    /// better to use [`read_text()`] method, because it will not copy bytes
    /// into intermediate buffer.
    ///
    /// # Examples
    ///
    /// This example shows, how you can read a HTML content from your XML document.
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// # use std::borrow::Cow;
    /// use quick_xml::events::{BytesStart, Event};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_reader("
    ///     <html>
    ///         <title>This is a HTML text</title>
    ///         <p>Usual XML rules does not apply inside it
    ///         <p>For example, elements not needed to be &quot;closed&quot;
    ///     </html>
    /// ".as_bytes());
    /// reader.config_mut().trim_text(true);
    ///
    /// let start = BytesStart::new("html");
    /// let end   = start.to_end().into_owned();
    ///
    /// let mut buf = Vec::new();
    ///
    /// // First, we read a start event...
    /// assert_eq!(reader.read_event_into(&mut buf).unwrap(), Event::Start(start));
    /// // ...and disable checking of end names because we expect HTML further...
    /// reader.config_mut().check_end_names = false;
    ///
    /// // ...then, we could read text content until close tag.
    /// // This call will correctly handle nested <html> elements.
    /// let text = reader.read_text_into(end.name(), &mut buf).unwrap();
    /// let text = text.decode().unwrap();
    /// assert_eq!(text, r#"
    ///         <title>This is a HTML text</title>
    ///         <p>Usual XML rules does not apply inside it
    ///         <p>For example, elements not needed to be &quot;closed&quot;
    ///     "#);
    /// assert!(matches!(text, Cow::Borrowed(_)));
    ///
    /// // Now we can enable checks again
    /// reader.config_mut().check_end_names = true;
    ///
    /// // At the end we should get an Eof event, because we ate the whole XML
    /// assert_eq!(reader.read_event_into(&mut buf).unwrap(), Event::Eof);
    /// ```
    ///
    /// [`Start`]: Event::Start
    /// [`read_text()`]: Self::read_text()
    #[inline]
    pub fn read_text_into<'b>(
        &mut self,
        end: QName,
        buf: &'b mut Vec<u8>,
    ) -> Result<BytesText<'b>> {
        // According to the https://www.w3.org/TR/xml11/#dt-etag, end name should
        // match literally the start name. See `Self::check_end_names` documentation
        let result = self.reader.read_text_into(end, buf)?;
        // read_text_into will consume closing tag. Because nobody can access to its
        // content anymore, we directly pop namespace of the opening tag
        self.ns_resolver.pop();
        Ok(result)
    }
}

impl NsReader<BufReader<File>> {
    /// Creates an XML reader from a file path.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self::new(Reader::from_file(path)?))
    }
}

impl<'i> NsReader<&'i [u8]> {
    /// Creates an XML reader from a string slice.
    #[inline]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &'i str) -> Self {
        Self::new(Reader::from_str(s))
    }

    /// Reads the next event, borrow its content from the input buffer.
    ///
    /// This method manages namespaces but doesn't resolve them automatically.
    /// You should call [`resolver().resolve_element()`] if you want to get a namespace.
    ///
    /// You also can use [`read_resolved_event()`] instead if you want to resolve namespace
    /// as soon as you get an event.
    ///
    /// There is no asynchronous `read_event_async()` version of this function,
    /// because it is not necessary -- the contents are already in memory and no IO
    /// is needed, therefore there is no potential for blocking.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::Event;
    /// use quick_xml::name::{Namespace, ResolveResult::*};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str(r#"
    ///     <x:tag1 xmlns:x="www.xxxx" xmlns:y="www.yyyy" att1 = "test">
    ///        <y:tag2><!--Test comment-->Test</y:tag2>
    ///        <y:tag2>Test 2</y:tag2>
    ///     </x:tag1>
    /// "#);
    /// reader.config_mut().trim_text(true);
    ///
    /// let mut count = 0;
    /// let mut txt = Vec::new();
    /// loop {
    ///     match reader.read_event().unwrap() {
    ///         Event::Start(e) => {
    ///             count += 1;
    ///             let (ns, local) = reader.resolver().resolve_element(e.name());
    ///             match local.as_ref() {
    ///                 b"tag1" => assert_eq!(ns, Bound(Namespace(b"www.xxxx"))),
    ///                 b"tag2" => assert_eq!(ns, Bound(Namespace(b"www.yyyy"))),
    ///                 _ => unreachable!(),
    ///             }
    ///         }
    ///         Event::Text(e) => {
    ///             txt.push(e.decode().unwrap().into_owned())
    ///         }
    ///         Event::Eof => break,
    ///         _ => (),
    ///     }
    /// }
    /// assert_eq!(count, 3);
    /// assert_eq!(txt, vec!["Test".to_string(), "Test 2".to_string()]);
    /// ```
    ///
    /// [`resolver().resolve_element()`]: NamespaceResolver::resolve_element
    /// [`read_resolved_event()`]: Self::read_resolved_event
    #[inline]
    pub fn read_event(&mut self) -> Result<Event<'i>> {
        self.read_event_impl(())
    }

    /// Reads the next event, borrow its content from the input buffer, and resolves
    /// its namespace (if applicable).
    ///
    /// Namespace is resolved only for [`Start`], [`Empty`] and [`End`] events.
    /// For all other events the concept of namespace is not defined, so
    /// a [`ResolveResult::Unbound`] is returned.
    ///
    /// If you are not interested in namespaces, you can use [`read_event()`]
    /// which will not automatically resolve namespaces for you.
    ///
    /// There is no asynchronous `read_resolved_event_async()` version of this function,
    /// because it is not necessary -- the contents are already in memory and no IO
    /// is needed, therefore there is no potential for blocking.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::Event;
    /// use quick_xml::name::{Namespace, QName, ResolveResult::*};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str(r#"
    ///     <x:tag1 xmlns:x="www.xxxx" xmlns:y="www.yyyy" att1 = "test">
    ///        <y:tag2><!--Test comment-->Test</y:tag2>
    ///        <y:tag2>Test 2</y:tag2>
    ///     </x:tag1>
    /// "#);
    /// reader.config_mut().trim_text(true);
    ///
    /// let mut count = 0;
    /// let mut txt = Vec::new();
    /// loop {
    ///     match reader.read_resolved_event().unwrap() {
    ///         (Bound(Namespace(b"www.xxxx")), Event::Start(e)) => {
    ///             count += 1;
    ///             assert_eq!(e.local_name(), QName(b"tag1").into());
    ///         }
    ///         (Bound(Namespace(b"www.yyyy")), Event::Start(e)) => {
    ///             count += 1;
    ///             assert_eq!(e.local_name(), QName(b"tag2").into());
    ///         }
    ///         (_, Event::Start(_)) => unreachable!(),
    ///
    ///         (_, Event::Text(e)) => {
    ///             txt.push(e.decode().unwrap().into_owned())
    ///         }
    ///         (_, Event::Eof) => break,
    ///         _ => (),
    ///     }
    /// }
    /// assert_eq!(count, 3);
    /// assert_eq!(txt, vec!["Test".to_string(), "Test 2".to_string()]);
    /// ```
    ///
    /// [`Start`]: Event::Start
    /// [`Empty`]: Event::Empty
    /// [`End`]: Event::End
    /// [`read_event()`]: Self::read_event
    #[inline]
    pub fn read_resolved_event(&mut self) -> Result<(ResolveResult<'_>, Event<'i>)> {
        let event = self.read_event_impl(())?;
        Ok(self.ns_resolver.resolve_event(event))
    }

    /// Reads until end element is found. This function is supposed to be called
    /// after you already read a [`Start`] event.
    ///
    /// Returns a span that cover content between `>` of an opening tag and `<` of
    /// a closing tag or an empty slice, if [`expand_empty_elements`] is set and
    /// this method was called after reading expanded [`Start`] event.
    ///
    /// Manages nested cases where parent and child elements have the _literally_
    /// same name.
    ///
    /// If a corresponding [`End`] event is not found, an error of type [`IllFormed`]
    /// will be returned. In particularly, that error will be returned if you call
    /// this method without consuming the corresponding [`Start`] event first.
    ///
    /// The `end` parameter should contain name of the end element _in the reader
    /// encoding_. It is good practice to always get that parameter using
    /// [`BytesStart::to_end()`] method.
    ///
    /// There is no asynchronous `read_to_end_async()` version of this function,
    /// because it is not necessary -- the contents are already in memory and no IO
    /// is needed, therefore there is no potential for blocking.
    ///
    /// # Namespaces
    ///
    /// While the `NsReader` does namespace resolution, namespaces does not
    /// change the algorithm for comparing names. Although the names `a:name`
    /// and `b:name` where both prefixes `a` and `b` resolves to the same namespace,
    /// are semantically equivalent, `</b:name>` cannot close `<a:name>`, because
    /// according to [the specification]
    ///
    /// > The end of every element that begins with a **start-tag** MUST be marked
    /// > by an **end-tag** containing a name that echoes the element's type as
    /// > given in the **start-tag**
    ///
    /// # Examples
    ///
    /// This example shows, how you can skip XML content after you read the
    /// start event.
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::events::{BytesStart, Event};
    /// use quick_xml::name::{Namespace, ResolveResult};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str(r#"
    ///     <outer xmlns="namespace 1">
    ///         <inner xmlns="namespace 2">
    ///             <outer></outer>
    ///         </inner>
    ///         <inner>
    ///             <inner></inner>
    ///             <inner/>
    ///             <outer></outer>
    ///             <p:outer xmlns:p="ns"></p:outer>
    ///             <outer/>
    ///         </inner>
    ///     </outer>
    /// "#);
    /// reader.config_mut().trim_text(true);
    ///
    /// let ns = Namespace(b"namespace 1");
    /// let start = BytesStart::from_content(r#"outer xmlns="namespace 1""#, 5);
    /// let end   = start.to_end().into_owned();
    ///
    /// // First, we read a start event...
    /// assert_eq!(
    ///     reader.read_resolved_event().unwrap(),
    ///     (ResolveResult::Bound(ns), Event::Start(start))
    /// );
    ///
    /// // ...then, we could skip all events to the corresponding end event.
    /// // This call will correctly handle nested <outer> elements.
    /// // Note, however, that this method does not handle namespaces.
    /// reader.read_to_end(end.name()).unwrap();
    ///
    /// // At the end we should get an Eof event, because we ate the whole XML
    /// assert_eq!(
    ///     reader.read_resolved_event().unwrap(),
    ///     (ResolveResult::Unbound, Event::Eof)
    /// );
    /// ```
    ///
    /// [`Start`]: Event::Start
    /// [`End`]: Event::End
    /// [`IllFormed`]: crate::errors::Error::IllFormed
    /// [`BytesStart::to_end()`]: crate::events::BytesStart::to_end
    /// [`expand_empty_elements`]: Config::expand_empty_elements
    /// [the specification]: https://www.w3.org/TR/xml11/#dt-etag
    #[inline]
    pub fn read_to_end(&mut self, end: QName) -> Result<Span> {
        // According to the https://www.w3.org/TR/xml11/#dt-etag, end name should
        // match literally the start name. See `Config::check_end_names` documentation
        let result = self.reader.read_to_end(end)?;
        // read_to_end will consume closing tag. Because nobody can access to its
        // content anymore, we directly pop namespace of the opening tag
        self.ns_resolver.pop();
        Ok(result)
    }

    /// Reads content between start and end tags, including any markup. This
    /// function is supposed to be called after you already read a [`Start`] event.
    ///
    /// Manages nested cases where parent and child elements have the _literally_
    /// same name.
    ///
    /// This method does not unescape read data, instead it returns content
    /// "as is" of the XML document. This is because it has no idea what text
    /// it reads, and if, for example, it contains CDATA section, attempt to
    /// unescape it content will spoil data.
    ///
    /// Any text will be decoded using the XML current [`decoder()`].
    ///
    /// Actually, this method perform the following code:
    ///
    /// ```ignore
    /// let span = reader.read_to_end(end)?;
    /// let text = reader.decoder().decode(&reader.inner_slice[span]);
    /// ```
    ///
    /// # Examples
    ///
    /// This example shows, how you can read a HTML content from your XML document.
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// # use std::borrow::Cow;
    /// use quick_xml::events::{BytesStart, Event};
    /// use quick_xml::reader::NsReader;
    ///
    /// let mut reader = NsReader::from_str(r#"
    ///     <html>
    ///         <title>This is a HTML text</title>
    ///         <p>Usual XML rules does not apply inside it
    ///         <p>For example, elements not needed to be &quot;closed&quot;
    ///     </html>
    /// "#);
    /// reader.config_mut().trim_text(true);
    ///
    /// let start = BytesStart::new("html");
    /// let end   = start.to_end().into_owned();
    ///
    /// // First, we read a start event...
    /// assert_eq!(reader.read_event().unwrap(), Event::Start(start));
    /// // ...and disable checking of end names because we expect HTML further...
    /// reader.config_mut().check_end_names = false;
    ///
    /// // ...then, we could read text content until close tag.
    /// // This call will correctly handle nested <html> elements.
    /// let text = reader.read_text(end.name()).unwrap();
    /// let text = text.decode().unwrap();
    /// assert_eq!(text, r#"
    ///         <title>This is a HTML text</title>
    ///         <p>Usual XML rules does not apply inside it
    ///         <p>For example, elements not needed to be &quot;closed&quot;
    ///     "#);
    /// assert!(matches!(text, Cow::Borrowed(_)));
    ///
    /// // Now we can enable checks again
    /// reader.config_mut().check_end_names = true;
    ///
    /// // At the end we should get an Eof event, because we ate the whole XML
    /// assert_eq!(reader.read_event().unwrap(), Event::Eof);
    /// ```
    ///
    /// [`Start`]: Event::Start
    /// [`decoder()`]: Reader::decoder()
    #[inline]
    pub fn read_text(&mut self, end: QName) -> Result<BytesText<'i>> {
        // According to the https://www.w3.org/TR/xml11/#dt-etag, end name should
        // match literally the start name. See `Self::check_end_names` documentation
        let result = self.reader.read_text(end)?;
        // read_text will consume closing tag. Because nobody can access to its
        // content anymore, we directly pop namespace of the opening tag
        self.ns_resolver.pop();
        Ok(result)
    }
}

impl<R> Deref for NsReader<R> {
    type Target = Reader<R>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}
