//! Module for handling names according to the W3C [Namespaces in XML 1.1 (Second Edition)][spec]
//! specification
//!
//! [spec]: https://www.w3.org/TR/xml-names11

use crate::events::attributes::Attribute;
use crate::events::{BytesStart, Event};
use std::fmt::{self, Debug, Formatter};
use std::iter::FusedIterator;

/// Some namespace was invalid
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceError {
    /// Specified namespace prefix is unknown, cannot resolve namespace for it
    UnknownPrefix(String),
    /// Attempts to bind the `xml` prefix to something other than `http://www.w3.org/XML/1998/namespace`.
    ///
    /// `xml` prefix can be bound only to `http://www.w3.org/XML/1998/namespace`.
    ///
    /// Contains the namespace to which `xml` tried to be bound.
    InvalidXmlPrefixBind(String),
    /// Attempts to bind the `xmlns` prefix.
    ///
    /// `xmlns` prefix is always bound to `http://www.w3.org/2000/xmlns/` and cannot be bound
    /// to any other namespace or even to `http://www.w3.org/2000/xmlns/`.
    ///
    /// Contains the namespace to which `xmlns` tried to be bound.
    InvalidXmlnsPrefixBind(String),
    /// Attempts to bind some prefix (except `xml`) to `http://www.w3.org/XML/1998/namespace`.
    ///
    /// Only `xml` prefix can be bound to `http://www.w3.org/XML/1998/namespace`.
    ///
    /// Contains the prefix that is tried to be bound.
    InvalidPrefixForXml(String),
    /// Attempts to bind some prefix to `http://www.w3.org/2000/xmlns/`.
    ///
    /// `http://www.w3.org/2000/xmlns/` cannot be bound to any prefix, even to `xmlns`.
    ///
    /// Contains the prefix that is tried to be bound.
    InvalidPrefixForXmlns(String),
    /// The total number of `xmlns` / `xmlns:*` namespace bindings in scope exceeded
    /// the configured [`NamespaceResolver::max_namespace_bindings`] limit. Contains
    /// the configured limit.
    ///
    /// This bounds the work done by [`NamespaceResolver`] (and hence by [`NsReader`](crate::reader::NsReader))
    /// on untrusted input by capping both the heap allocated and the cost of prefix
    /// resolution (which scans the binding stack).
    TooManyBindings(usize),
    /// The document nested elements more deeply than the namespace resolver's
    /// depth counter (a `u16`) can track. This bounds stack / scope-bookkeeping
    /// work on untrusted input. Contains the depth limit that was exceeded.
    TooDeeplyNested(usize),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UnknownPrefix(prefix) => {
                write!(f, "unknown namespace prefix '{}'", prefix)
            }
            Self::InvalidXmlPrefixBind(namespace) => {
                write!(
                    f,
                    "the namespace prefix 'xml' cannot be bound to '{}'",
                    namespace
                )
            }
            Self::InvalidXmlnsPrefixBind(namespace) => {
                write!(
                    f,
                    "the namespace prefix 'xmlns' cannot be bound to '{}'",
                    namespace
                )
            }
            Self::InvalidPrefixForXml(prefix) => {
                write!(
                    f,
                    "the namespace prefix '{}' cannot be bound to 'http://www.w3.org/XML/1998/namespace'",
                    prefix
                )
            }
            Self::InvalidPrefixForXmlns(prefix) => {
                write!(
                    f,
                    "the namespace prefix '{}' cannot be bound to 'http://www.w3.org/2000/xmlns/'",
                    prefix
                )
            }
            Self::TooManyBindings(limit) => {
                write!(
                    f,
                    "more than {} namespace bindings in scope; \
                     raise the limit with NamespaceResolver::set_max_namespace_bindings",
                    limit,
                )
            }
            Self::TooDeeplyNested(limit) => {
                write!(
                    f,
                    "document nests elements deeper than the supported limit of {}",
                    limit
                )
            }
        }
    }
}

impl std::error::Error for NamespaceError {}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A [qualified name] of an element or an attribute, including an optional
/// namespace [prefix](Prefix) and a [local name](LocalName).
///
/// [qualified name]: https://www.w3.org/TR/xml-names11/#dt-qualname
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde-types", derive(serde::Deserialize, serde::Serialize))]
pub struct QName<'a>(pub &'a str);
impl<'a> QName<'a> {
    /// Converts this name to an internal slice representation.
    #[inline(always)]
    pub const fn into_inner(self) -> &'a str {
        self.0
    }

    /// Returns local part of this qualified name.
    ///
    /// All content up to and including the first `:` character is removed from
    /// the tag name.
    ///
    /// # Examples
    ///
    /// ```
    /// # use quick_xml::name::QName;
    /// let simple = QName("simple-name");
    /// assert_eq!(simple.local_name().into_inner(), "simple-name");
    ///
    /// let qname = QName("namespace:simple-name");
    /// assert_eq!(qname.local_name().into_inner(), "simple-name");
    /// ```
    pub fn local_name(&self) -> LocalName<'a> {
        LocalName(self.index().map_or(self.0, |i| &self.0[i + 1..]))
    }

    /// Returns namespace part of this qualified name or `None` if namespace part
    /// is not defined (symbol `':'` not found).
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::convert::AsRef;
    /// # use quick_xml::name::QName;
    /// let simple = QName("simple-name");
    /// assert_eq!(simple.prefix(), None);
    ///
    /// let qname = QName("prefix:simple-name");
    /// assert_eq!(qname.prefix().map(|n| n.into_inner()), Some("prefix"));
    /// ```
    pub fn prefix(&self) -> Option<Prefix<'a>> {
        self.index().map(|i| Prefix(&self.0[..i]))
    }

    /// The same as `(qname.local_name(), qname.prefix())`, but does only one
    /// lookup for a `':'` symbol.
    pub fn decompose(&self) -> (LocalName<'a>, Option<Prefix<'a>>) {
        match self.index() {
            None => (LocalName(self.0), None),
            Some(i) => (LocalName(&self.0[i + 1..]), Some(Prefix(&self.0[..i]))),
        }
    }

    /// If that `QName` represents `"xmlns"` series of names, returns `Some`,
    /// otherwise `None` is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use quick_xml::name::{QName, PrefixDeclaration};
    /// let qname = QName("xmlns");
    /// assert_eq!(qname.as_namespace_binding(), Some(PrefixDeclaration::Default));
    ///
    /// let qname = QName("xmlns:prefix");
    /// assert_eq!(qname.as_namespace_binding(), Some(PrefixDeclaration::Named("prefix")));
    ///
    /// // Be aware that this method does not check the validity of the prefix - it can be empty!
    /// let qname = QName("xmlns:");
    /// assert_eq!(qname.as_namespace_binding(), Some(PrefixDeclaration::Named("")));
    ///
    /// let qname = QName("other-name");
    /// assert_eq!(qname.as_namespace_binding(), None);
    ///
    /// // https://www.w3.org/TR/xml-names11/#xmlReserved
    /// let qname = QName("xmlns-reserved-name");
    /// assert_eq!(qname.as_namespace_binding(), None);
    /// ```
    pub fn as_namespace_binding(&self) -> Option<PrefixDeclaration<'a>> {
        if self.0.starts_with("xmlns") {
            return match self.0.as_bytes().get(5) {
                None => Some(PrefixDeclaration::Default),
                Some(&b':') => Some(PrefixDeclaration::Named(&self.0[6..])),
                _ => None,
            };
        }
        None
    }

    /// Returns the index in the name where prefix ended
    #[inline(always)]
    fn index(&self) -> Option<usize> {
        self.0.find(':')
    }
}

impl<'a> Debug for QName<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "QName({})", self.0)
    }
}
impl<'a> AsRef<str> for QName<'a> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A [local (unqualified) name] of an element or an attribute, i.e. a name
/// without [prefix](Prefix).
///
/// [local (unqualified) name]: https://www.w3.org/TR/xml-names11/#dt-localname
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde-types", derive(serde::Deserialize, serde::Serialize))]
pub struct LocalName<'a>(pub(crate) &'a str);
impl<'a> LocalName<'a> {
    /// Converts this name to an internal slice representation.
    #[inline(always)]
    pub const fn into_inner(self) -> &'a str {
        self.0
    }
}

impl<'a> Debug for LocalName<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "LocalName({})", self.0)
    }
}

impl<'a> AsRef<str> for LocalName<'a> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl<'a> From<QName<'a>> for LocalName<'a> {
    /// Creates `LocalName` from a [`QName`]
    ///
    /// # Examples
    ///
    /// ```
    /// # use quick_xml::name::{LocalName, QName};
    ///
    /// let local: LocalName = QName("unprefixed").into();
    /// assert_eq!(local.into_inner(), "unprefixed");
    ///
    /// let local: LocalName = QName("some:prefix").into();
    /// assert_eq!(local.into_inner(), "prefix");
    /// ```
    #[inline]
    fn from(name: QName<'a>) -> Self {
        Self(name.index().map_or(name.0, |i| &name.0[i + 1..]))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A [namespace prefix] part of the [qualified name](QName) of an element tag
/// or an attribute: a `prefix` in `<prefix:local-element-name>` or
/// `prefix:local-attribute-name="attribute value"`.
///
/// [namespace prefix]: https://www.w3.org/TR/xml-names11/#dt-prefix
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde-types", derive(serde::Deserialize, serde::Serialize))]
pub struct Prefix<'a>(&'a str);
impl<'a> Prefix<'a> {
    /// Extracts internal slice
    #[inline(always)]
    pub const fn into_inner(self) -> &'a str {
        self.0
    }

    /// Checks if this prefix is a special prefix `xml`.
    #[inline(always)]
    pub const fn is_xml(&self) -> bool {
        matches!(self.0.as_bytes(), b"xml")
    }

    /// Checks if this prefix is a special prefix `xmlns`.
    #[inline(always)]
    pub const fn is_xmlns(&self) -> bool {
        matches!(self.0.as_bytes(), b"xmlns")
    }
}

impl<'a> Debug for Prefix<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Prefix({})", self.0)
    }
}

impl<'a> AsRef<str> for Prefix<'a> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A namespace prefix declaration, `xmlns` or `xmlns:<name>`, as defined in
/// [XML Schema specification](https://www.w3.org/TR/xml-names11/#ns-decl)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PrefixDeclaration<'a> {
    /// XML attribute binds a default namespace. Corresponds to `xmlns` in `xmlns="..."`
    Default,
    /// XML attribute binds a specified prefix to a namespace. Corresponds to a
    /// `prefix` in `xmlns:prefix="..."`, which is stored as payload of this variant.
    Named(&'a str),
}

impl<'a> Debug for PrefixDeclaration<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("PrefixDeclaration::Default"),
            Self::Named(prefix) => {
                write!(f, "PrefixDeclaration::Named({})", prefix)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// A [namespace name] that is declared in a `xmlns[:prefix]="namespace name"`.
///
/// [namespace name]: https://www.w3.org/TR/xml-names11/#dt-NSName
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde-types", derive(serde::Deserialize, serde::Serialize))]
pub struct Namespace<'a>(pub &'a str);
impl<'a> Namespace<'a> {
    /// Converts this namespace to an internal slice representation.
    ///
    /// This is [non-normalized] attribute value, i.e. any entity references is not
    /// expanded and space characters are not removed. This means, that different
    /// string slices, returned from this method, can represent the same namespace
    /// and would be treated by parser as identical.
    ///
    /// For example, if the entity **eacute** has been defined to be **é**,
    /// the empty tags below all contain namespace declarations binding the
    /// prefix `p` to the same [IRI reference], `http://example.org/rosé`.
    ///
    /// ```xml
    /// <p:foo xmlns:p="http://example.org/rosé" />
    /// <p:foo xmlns:p="http://example.org/ros&#xe9;" />
    /// <p:foo xmlns:p="http://example.org/ros&#xE9;" />
    /// <p:foo xmlns:p="http://example.org/ros&#233;" />
    /// <p:foo xmlns:p="http://example.org/ros&eacute;" />
    /// ```
    ///
    /// This is because XML entity references are expanded during attribute value
    /// normalization.
    ///
    /// [non-normalized]: https://www.w3.org/TR/xml11/#AVNormalize
    /// [IRI reference]: https://datatracker.ietf.org/doc/html/rfc3987
    #[inline(always)]
    pub const fn into_inner(self) -> &'a str {
        self.0
    }
    //TODO: implement value normalization and use it when comparing namespaces
}

impl<'a> Debug for Namespace<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Namespace({})", self.0)
    }
}

impl<'a> AsRef<str> for Namespace<'a> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Result of [prefix] resolution which creates by [`NamespaceResolver::resolve`],
/// [`NsReader::read_resolved_event`] and
/// [`NsReader::read_resolved_event_into`] methods.
///
/// [prefix]: Prefix
/// [`NsReader::read_resolved_event`]: crate::reader::NsReader::read_resolved_event
/// [`NsReader::read_resolved_event_into`]: crate::reader::NsReader::read_resolved_event_into
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ResolveResult<'ns> {
    /// Qualified name does not contain prefix, and resolver does not define
    /// default namespace, so name is not bound to any namespace
    Unbound,
    /// [`Prefix`] resolved to the specified namespace
    Bound(Namespace<'ns>),
    /// Specified prefix was not found in scope
    Unknown(String),
}

impl<'ns> Debug for ResolveResult<'ns> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Unbound => write!(f, "Unbound"),
            Self::Bound(ns) => write!(f, "Bound({:?})", ns),
            Self::Unknown(p) => write!(f, "Unknown({})", p),
        }
    }
}

impl<'ns> TryFrom<ResolveResult<'ns>> for Option<Namespace<'ns>> {
    type Error = NamespaceError;

    /// Try to convert this result to an optional namespace and returns
    /// [`NamespaceError::UnknownPrefix`] if this result represents unknown prefix
    fn try_from(result: ResolveResult<'ns>) -> Result<Self, NamespaceError> {
        use ResolveResult::*;

        match result {
            Unbound => Ok(None),
            Bound(ns) => Ok(Some(ns)),
            Unknown(p) => Err(NamespaceError::UnknownPrefix(p)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// An entry that contains index into the buffer with namespace bindings.
///
/// Defines a mapping from *[namespace prefix]* to *[namespace name]*.
/// If prefix is empty, defines a *default namespace* binding that applies to
/// unprefixed element names (unprefixed attribute names do not bind to any
/// namespace and they processing is dependent on the element in which their
/// defined).
///
/// [namespace prefix]: https://www.w3.org/TR/xml-names11/#dt-prefix
/// [namespace name]: https://www.w3.org/TR/xml-names11/#dt-NSName
#[derive(Debug, Clone)]
struct NamespaceBinding {
    /// Index of the namespace in the buffer
    start: usize,
    /// Length of the prefix
    /// * if greater than zero, then binds this namespace to the slice
    ///   `[start..start + prefix_len]` in the buffer.
    /// * else defines the current default namespace.
    prefix_len: usize,
    /// The length of a namespace name (the URI) of this namespace declaration.
    /// Name started just after prefix and extend for `value_len` bytes.
    ///
    /// The XML standard [specifies] that an empty namespace value 'removes' a namespace declaration
    /// for the extent of its scope. For prefix declarations that's not very interesting, but it is
    /// vital for default namespace declarations. With `xmlns=""` you can revert back to the default
    /// behaviour of leaving unqualified element names unqualified.
    ///
    /// [specifies]: https://www.w3.org/TR/xml-names11/#scoping
    value_len: usize,
    /// Level of nesting at which this namespace was declared. The declaring element is included,
    /// i.e., a declaration on the document root has `level = 1`.
    /// This is used to pop the namespace when the element gets closed.
    level: u16,
}

impl NamespaceBinding {
    /// Get the namespace prefix, bound to this namespace declaration, or `None`,
    /// if this declaration is for default namespace (`xmlns="..."`).
    #[inline]
    const fn prefix<'b>(&self, ns_buffer: &'b str) -> Option<Prefix<'b>> {
        if self.prefix_len == 0 {
            None
        } else {
            // We use split_at to get [start..start + prefix_len]
            // in a constant way
            let (_, prefix) = ns_buffer.split_at(self.start);
            let (prefix, _) = prefix.split_at(self.prefix_len);
            Some(Prefix(prefix))
        }
    }

    /// Gets the namespace name (the URI) slice out of namespace buffer
    ///
    /// Returns `None` if namespace for this prefix was explicitly removed from
    /// scope, using `xmlns[:prefix]=""`
    #[inline]
    const fn namespace<'ns>(&self, buffer: &'ns str) -> ResolveResult<'ns> {
        if self.value_len == 0 {
            ResolveResult::Unbound
        } else {
            // We use split_at to get [start + prefix_len..start + prefix_len + value_len]
            // in a constant way
            let (_, ns) = buffer.split_at(self.start + self.prefix_len);
            let (ns, _) = ns.split_at(self.value_len);
            ResolveResult::Bound(Namespace(ns))
        }
    }
}

/// A storage for currently defined namespace bindings, which is used to resolve
/// prefixes into namespaces.
///
/// Holds all internal logic to push/pop namespaces with their levels.
#[derive(Clone, Debug)]
pub struct NamespaceResolver {
    /// Buffer that contains names of namespace prefixes (the part between `xmlns:`
    /// and an `=`) and namespace values.
    buffer: String,
    /// A stack of namespace bindings to prefixes that currently in scope
    bindings: Vec<NamespaceBinding>,
    /// The number of open tags at the moment. We need to keep track of this to know which namespace
    /// declarations to remove when we encounter an `End` event.
    nesting_level: u16,
    /// Maximum number of user-declared `xmlns` / `xmlns:*` namespace bindings
    /// allowed in scope at once, not counting the two reserved bindings for
    /// `xml` and `xmlns`. See [`set_max_namespace_bindings`](Self::set_max_namespace_bindings).
    max_namespace_bindings: usize,
}

/// Default limit on the number of `xmlns` / `xmlns:*` namespace bindings allowed in scope at
/// once in a [`NamespaceResolver`], not counting the two reserved bindings (`xml` and `xmlns`)
/// that are always present.
///
/// Real-world XML dialects (XHTML, SVG, SOAP, RSS, RRDP, ...) declare a handful of namespaces,
/// almost always on the root element; 128 is significantly more than what most legitimate documents
/// would declare, while bounding both the heap allocated and the cost of prefix resolution
/// (which scans the binding stack).
pub const DEFAULT_MAX_NAMESPACE_BINDINGS: usize = 128;

/// The number of namespace bindings pre-loaded by [`NamespaceResolver::default()`]
/// (`xml` and `xmlns`). Subtracted from `bindings.len()` when checking against
/// the user-facing [`max_namespace_bindings`](NamespaceResolver::max_namespace_bindings)
/// limit, so these built-in bindings don't count against the user's limit.
const BUILTIN_NAMESPACE_BINDINGS: usize = 2;

/// This constant defines one the of [reserved namespaces] for the xml standard.
///
/// The prefix `xml` is by definition bound to the namespace name
/// `http://www.w3.org/XML/1998/namespace`. It may, but need not, be declared, and must not be
/// undeclared or bound to any other namespace name. Other prefixes must not be bound to this
/// namespace name, and it must not be declared as the default namespace.
///
/// [reserved namespaces]: https://www.w3.org/TR/xml-names11/#xmlReserved
const RESERVED_NAMESPACE_XML: (Prefix, Namespace) = (
    Prefix("xml"),
    Namespace("http://www.w3.org/XML/1998/namespace"),
);
/// This constant defines one of the [reserved namespaces] for the xml standard.
///
/// The prefix `xmlns` is used only to declare namespace bindings and is by definition bound
/// to the namespace name `http://www.w3.org/2000/xmlns/`. It must not be declared or
/// undeclared. Other prefixes must not be bound to this namespace name, and it must not be
/// declared as the default namespace. Element names must not have the prefix `xmlns`.
///
/// [reserved namespaces]: https://www.w3.org/TR/xml-names11/#xmlReserved
const RESERVED_NAMESPACE_XMLNS: (Prefix, Namespace) =
    (Prefix("xmlns"), Namespace("http://www.w3.org/2000/xmlns/"));

impl Default for NamespaceResolver {
    fn default() -> Self {
        let mut buffer = String::new();
        let mut bindings = Vec::new();
        for ent in &[RESERVED_NAMESPACE_XML, RESERVED_NAMESPACE_XMLNS] {
            let prefix = ent.0.into_inner();
            let uri = ent.1.into_inner();
            bindings.push(NamespaceBinding {
                start: buffer.len(),
                prefix_len: prefix.len(),
                value_len: uri.len(),
                level: 0,
            });
            buffer.push_str(prefix);
            buffer.push_str(uri);
        }

        Self {
            buffer,
            bindings,
            nesting_level: 0,
            max_namespace_bindings: DEFAULT_MAX_NAMESPACE_BINDINGS,
        }
    }
}

impl NamespaceResolver {
    /// Adds new binding of prefix to namespace, returns the result of operation.
    ///
    /// Binding will be added on current nesting level and will be removed, when
    /// level will be [popped out].
    ///
    /// The operation may fail if you try to (re-)declare reserved prefixes `xml` and `xmlns`.
    ///
    /// Note, that method does not check if namespace was already added on that level.
    /// Use `resolver.bindings_of(resolver.level()).any()` if you want to check that.
    /// New definition will be added and replace the old.
    ///
    /// Implementation detail: memory occupied by old binding of that level still will be used.
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// # use quick_xml::name::{Namespace, NamespaceResolver, PrefixDeclaration, QName, ResolveResult};
    /// #
    /// let mut resolver = NamespaceResolver::default();
    /// // names without prefix are unbound by default
    /// assert_eq!(
    ///     resolver.resolve_element(QName("name")).0,
    ///     ResolveResult::Unbound,
    /// );
    /// // names with undeclared prefix are unknown
    /// assert_eq!(
    ///     resolver.resolve_element(QName("ns:name")).0,
    ///     ResolveResult::Unknown("ns".to_string()),
    /// );
    ///
    /// resolver.add(PrefixDeclaration::Default, Namespace("example.com"));
    /// resolver.add(PrefixDeclaration::Named("ns"), Namespace("my:namespace"));
    ///
    /// assert_eq!(
    ///     resolver.resolve_element(QName("name")).0,
    ///     ResolveResult::Bound(Namespace("example.com")),
    /// );
    /// assert_eq!(
    ///     resolver.resolve_element(QName("ns:name")).0,
    ///     ResolveResult::Bound(Namespace("my:namespace")),
    /// );
    ///
    /// // adding empty namespace clears the binding
    /// resolver.add(PrefixDeclaration::Default, Namespace(""));
    /// resolver.add(PrefixDeclaration::Named("ns"), Namespace(""));
    ///
    /// assert_eq!(
    ///     resolver.resolve_element(QName("name")).0,
    ///     ResolveResult::Unbound,
    /// );
    /// assert_eq!(
    ///     resolver.resolve_element(QName("ns:name")).0,
    ///     ResolveResult::Unknown("ns".to_string()),
    /// );
    /// ```
    /// [popped out]: Self::pop
    pub fn add(
        &mut self,
        prefix: PrefixDeclaration,
        namespace: Namespace,
    ) -> Result<(), NamespaceError> {
        let level = self.nesting_level;
        match prefix {
            PrefixDeclaration::Default => {
                if self
                    .bindings
                    .len()
                    .saturating_sub(BUILTIN_NAMESPACE_BINDINGS)
                    >= self.max_namespace_bindings
                {
                    return Err(NamespaceError::TooManyBindings(self.max_namespace_bindings));
                }
                let start = self.buffer.len();
                self.buffer.push_str(namespace.0);
                self.bindings.push(NamespaceBinding {
                    start,
                    prefix_len: 0,
                    value_len: namespace.0.len(),
                    level,
                });
            }
            PrefixDeclaration::Named("xml") => {
                if namespace != RESERVED_NAMESPACE_XML.1 {
                    // error, `xml` prefix explicitly set to different value
                    return Err(NamespaceError::InvalidXmlPrefixBind(
                        namespace.0.to_string(),
                    ));
                }
                // don't add another NamespaceEntry for the `xml` namespace prefix
            }
            PrefixDeclaration::Named("xmlns") => {
                // error, `xmlns` prefix explicitly set
                return Err(NamespaceError::InvalidXmlnsPrefixBind(
                    namespace.0.to_string(),
                ));
            }
            PrefixDeclaration::Named(prefix) => {
                if namespace == RESERVED_NAMESPACE_XML.1 {
                    // error, non-`xml` prefix set to xml uri
                    return Err(NamespaceError::InvalidPrefixForXml(prefix.to_string()));
                } else if namespace == RESERVED_NAMESPACE_XMLNS.1 {
                    // error, non-`xmlns` prefix set to xmlns uri
                    return Err(NamespaceError::InvalidPrefixForXmlns(prefix.to_string()));
                }

                if self
                    .bindings
                    .len()
                    .saturating_sub(BUILTIN_NAMESPACE_BINDINGS)
                    >= self.max_namespace_bindings
                {
                    return Err(NamespaceError::TooManyBindings(self.max_namespace_bindings));
                }
                let start = self.buffer.len();
                self.buffer.push_str(prefix);
                self.buffer.push_str(namespace.0);
                self.bindings.push(NamespaceBinding {
                    start,
                    prefix_len: prefix.len(),
                    value_len: namespace.0.len(),
                    level,
                });
            }
        }
        Ok(())
    }

    /// Begins a new scope and add to it all [namespace bindings] that found in
    /// the specified start element.
    ///
    /// [namespace bindings]: https://www.w3.org/TR/xml-names11/#dt-NSDecl
    pub fn push(&mut self, start: &BytesStart) -> Result<(), NamespaceError> {
        self.nesting_level = self
            .nesting_level
            .checked_add(1)
            .ok_or(NamespaceError::TooDeeplyNested(u16::MAX as usize))?;
        // adds new namespaces for attributes starting with 'xmlns:' and for the 'xmlns'
        // (default namespace) attribute.
        for a in start.attributes().with_checks(false) {
            if let Ok(Attribute { key: k, value: v }) = a {
                if let Some(prefix) = k.as_namespace_binding() {
                    self.add(prefix, Namespace(&v))?;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Returns the maximum number of user-declared `xmlns` / `xmlns:*` namespace
    /// bindings allowed in scope at once (not counting the two reserved bindings
    /// for `xml` and `xmlns`).
    ///
    /// Defaults to [`DEFAULT_MAX_NAMESPACE_BINDINGS`].
    #[inline]
    pub const fn max_namespace_bindings(&self) -> usize {
        self.max_namespace_bindings
    }

    /// Sets the maximum number of user-declared `xmlns` / `xmlns:*` namespace bindings
    /// allowed in scope at once. The two reserved bindings (`xml` and `xmlns`) do not
    /// count toward this limit.
    ///
    /// [`add`](Self::add) is called by [`push`](Self::push), which is called by
    /// [`NsReader`](crate::reader::NsReader) for every `Start`/`Empty` event *before* the event
    /// is returned to the caller. This limit bounds both the heap allocated for namespace
    /// bindings and the cost of prefix resolution (which scans the binding stack). See
    /// <https://github.com/tafia/quick-xml/issues/970> and <https://github.com/tafia/quick-xml/issues/980>.
    ///
    /// Pass `usize::MAX` to disable the limit.
    #[inline]
    pub fn set_max_namespace_bindings(&mut self, limit: usize) -> &mut Self {
        self.max_namespace_bindings = limit;
        self
    }

    /// Ends a top-most scope by popping all [namespace bindings], that was added by
    /// last call to [`Self::push()`] and [`Self::add()`].
    ///
    /// [namespace bindings]: https://www.w3.org/TR/xml-names11/#dt-NSDecl
    #[inline]
    pub fn pop(&mut self) {
        self.set_level(self.nesting_level.saturating_sub(1));
    }

    /// Runs action as if all namespaces from the specified `start` element were added
    /// to the resolver, but without actually changing the resolver state.
    pub fn with<F, R>(&mut self, start: &BytesStart, mut action: F) -> Result<R, NamespaceError>
    where
        F: FnMut(&Self) -> R,
    {
        self.push(start)?;
        let result = action(self);
        self.pop();
        Ok(result)
    }

    /// Sets new number of [`push`] calls that were not followed by [`pop`] calls.
    ///
    /// When set to value lesser than current [`level`], behaves as if [`pop`]
    /// will be called until the level reaches the corresponding value.
    ///
    /// When set to value bigger than current [`level`] just increases internal
    /// counter. You may need to call [`pop`] more times that required before.
    ///
    /// # Example
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// # use quick_xml::events::BytesStart;
    /// # use quick_xml::name::{Namespace, NamespaceResolver, PrefixDeclaration, QName, ResolveResult};
    /// #
    /// let mut resolver = NamespaceResolver::default();
    ///
    /// assert_eq!(resolver.level(), 0);
    ///
    /// resolver.push(&BytesStart::new("tag"));
    /// assert_eq!(resolver.level(), 1);
    ///
    /// resolver.set_level(10);
    /// assert_eq!(resolver.level(), 10);
    ///
    /// resolver.pop();
    /// assert_eq!(resolver.level(), 9);
    ///
    /// resolver.set_level(0);
    /// assert_eq!(resolver.level(), 0);
    ///
    /// // pop from empty resolver does nothing
    /// resolver.pop();
    /// assert_eq!(resolver.level(), 0);
    /// ```
    ///
    /// [`push`]: Self::push
    /// [`pop`]: Self::pop
    /// [`level`]: Self::level
    pub fn set_level(&mut self, level: u16) {
        self.nesting_level = level;
        // from the back (most deeply nested scope), look for the first scope that is still valid
        match self.bindings.iter().rposition(|n| n.level <= level) {
            // none of the namespaces are valid, remove all of them
            None => {
                self.buffer.clear();
                self.bindings.clear();
            }
            // drop all namespaces past the last valid namespace
            Some(last_valid_pos) => {
                if let Some(len) = self.bindings.get(last_valid_pos + 1).map(|n| n.start) {
                    self.buffer.truncate(len);
                    self.bindings.truncate(last_valid_pos + 1);
                }
            }
        }
    }

    /// Resolves a potentially qualified **element name** or **attribute name**
    /// into _(namespace name, local name)_.
    ///
    /// _Qualified_ names have the form `local-name` or `prefix:local-name` where the `prefix`
    /// is defined on any containing XML element via `xmlns:prefix="the:namespace:uri"`.
    /// The namespace prefix can be defined on the same element as the name in question.
    ///
    /// The method returns following results depending on the `name` shape, `attribute` flag
    /// and the presence of the default namespace on element or any of its parents:
    ///
    /// |use_default|`xmlns="..."`|QName              |ResolveResult          |LocalName
    /// |-----------|-------------|-------------------|-----------------------|------------
    /// |`false`    |_(any)_      |`local-name`       |[`Unbound`]            |`local-name`
    /// |`false`    |_(any)_      |`prefix:local-name`|[`Bound`] / [`Unknown`]|`local-name`
    /// |`true`     |Not defined  |`local-name`       |[`Unbound`]            |`local-name`
    /// |`true`     |Defined      |`local-name`       |[`Bound`] (to `xmlns`) |`local-name`
    /// |`true`     |_(any)_      |`prefix:local-name`|[`Bound`] / [`Unknown`]|`local-name`
    ///
    /// # Parameters
    /// - `name`: probably qualified name to resolve;
    /// - `use_default`: whether to try to translate `None` prefix to the currently default namespace
    ///   (bound using `xmlns="default namespace"`) or return [`ResolveResult::Unbound`].
    ///   For attribute names this should be set to `false` and for element names to `true`.
    ///
    /// # Lifetimes
    ///
    /// - `'n`: lifetime of a name. Returned local name will be bound to the same
    ///   lifetime as the name in question.
    /// - returned namespace name will be bound to the resolver itself
    ///
    /// [`Bound`]: ResolveResult::Bound
    /// [`Unbound`]: ResolveResult::Unbound
    /// [`Unknown`]: ResolveResult::Unknown
    #[inline]
    pub fn resolve<'n>(
        &self,
        name: QName<'n>,
        use_default: bool,
    ) -> (ResolveResult<'_>, LocalName<'n>) {
        let (local_name, prefix) = name.decompose();
        (self.resolve_prefix(prefix, use_default), local_name)
    }

    /// Convenient method to call `resolve(name, true)`. May be used to clearly
    /// express that we want to resolve an element name, and not an attribute name.
    #[inline]
    pub fn resolve_element<'n>(&self, name: QName<'n>) -> (ResolveResult<'_>, LocalName<'n>) {
        self.resolve(name, true)
    }

    /// Convenient method to call `resolve(name, false)`. May be used to clearly
    /// express that we want to resolve an attribute name, and not an element name.
    #[inline]
    pub fn resolve_attribute<'n>(&self, name: QName<'n>) -> (ResolveResult<'_>, LocalName<'n>) {
        self.resolve(name, false)
    }

    /// Finds a [namespace name] for a given event, if applicable.
    ///
    /// Namespace is resolved only for [`Start`], [`Empty`] and [`End`] events.
    /// For all other events the concept of namespace is not defined, so
    /// a [`ResolveResult::Unbound`] is returned.
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
    ///     let event = reader.read_event().unwrap();
    ///     match reader.resolver().resolve_event(event) {
    ///         (Bound(Namespace("www.xxxx")), Event::Start(e)) => {
    ///             count += 1;
    ///             assert_eq!(e.local_name(), QName("tag1").into());
    ///         }
    ///         (Bound(Namespace("www.yyyy")), Event::Start(e)) => {
    ///             count += 1;
    ///             assert_eq!(e.local_name(), QName("tag2").into());
    ///         }
    ///         (_, Event::Start(_)) => unreachable!(),
    ///
    ///         (_, Event::Text(e)) => {
    ///             txt.push(e.into_inner().into_owned())
    ///         }
    ///         (_, Event::Eof) => break,
    ///         _ => (),
    ///     }
    /// }
    /// assert_eq!(count, 3);
    /// assert_eq!(txt, vec!["Test".to_string(), "Test 2".to_string()]);
    /// ```
    ///
    /// [namespace name]: https://www.w3.org/TR/xml-names11/#dt-NSName
    /// [`Empty`]: Event::Empty
    /// [`Start`]: Event::Start
    /// [`End`]: Event::End
    pub fn resolve_event<'i>(&self, event: Event<'i>) -> (ResolveResult<'_>, Event<'i>) {
        use Event::*;

        match event {
            Empty(e) => (self.resolve_prefix(e.name().prefix(), true), Empty(e)),
            Start(e) => (self.resolve_prefix(e.name().prefix(), true), Start(e)),
            End(e) => (self.resolve_prefix(e.name().prefix(), true), End(e)),
            e => (ResolveResult::Unbound, e),
        }
    }

    /// Resolves given optional prefix (usually got from [`QName`]) into a corresponding namespace.
    ///
    /// # Parameters
    /// - `prefix`: prefix to resolve, usually result of [`QName::prefix()`];
    /// - `use_default`: whether to try to translate `None` prefix to the currently default namespace
    ///   (bound using `xmlns="default namespace"`) or return [`ResolveResult::Unbound`].
    ///   For attribute names this should be set to `false` and for element names to `true`.
    pub fn resolve_prefix(&self, prefix: Option<Prefix>, use_default: bool) -> ResolveResult<'_> {
        // Find the last defined binding that corresponds to the given prefix
        let mut iter = self.bindings.iter().rev();
        match (prefix, use_default) {
            // Attribute name has no explicit prefix -> Unbound
            (None, false) => ResolveResult::Unbound,
            // Element name has no explicit prefix -> find nearest xmlns binding
            (None, true) => match iter.find(|n| n.prefix_len == 0) {
                Some(n) => n.namespace(&self.buffer),
                None => ResolveResult::Unbound,
            },
            // Attribute or element name with explicit prefix
            (Some(p), _) => match iter.find(|n| n.prefix(&self.buffer) == prefix) {
                Some(n) if n.value_len != 0 => n.namespace(&self.buffer),
                // Not found or binding reset (corresponds to `xmlns:p=""`)
                _ => ResolveResult::Unknown(p.into_inner().to_string()),
            },
        }
    }

    /// Returns all the bindings currently in effect except the default `xml` and `xmlns` bindings.
    ///
    /// # Examples
    ///
    /// This example shows what results the returned iterator would return after
    /// reading each event of a simple XML.
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::name::{Namespace, PrefixDeclaration};
    /// use quick_xml::NsReader;
    ///
    /// let src = "<root>
    ///   <a xmlns=\"a1\" xmlns:a=\"a2\">
    ///     <b xmlns=\"b1\" xmlns:b=\"b2\">
    ///       <c/>
    ///     </b>
    ///     <d/>
    ///   </a>
    /// </root>";
    /// let mut reader = NsReader::from_str(src);
    /// reader.config_mut().trim_text(true);
    /// // No bindings at the beginning
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![]);
    ///
    /// reader.read_resolved_event()?; // <root>
    /// // No bindings declared on root
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![]);
    ///
    /// reader.read_resolved_event()?; // <a>
    /// // Two bindings declared on "a"
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Default, Namespace("a1")),
    ///     (PrefixDeclaration::Named("a"), Namespace("a2"))
    /// ]);
    ///
    /// reader.read_resolved_event()?; // <b>
    /// // The default prefix got overridden and new "b" prefix
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Named("a"), Namespace("a2")),
    ///     (PrefixDeclaration::Default, Namespace("b1")),
    ///     (PrefixDeclaration::Named("b"), Namespace("b2"))
    /// ]);
    ///
    /// reader.read_resolved_event()?; // <c/>
    /// // Still the same
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Named("a"), Namespace("a2")),
    ///     (PrefixDeclaration::Default, Namespace("b1")),
    ///     (PrefixDeclaration::Named("b"), Namespace("b2"))
    /// ]);
    ///
    /// reader.read_resolved_event()?; // </b>
    /// // Still the same
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Named("a"), Namespace("a2")),
    ///     (PrefixDeclaration::Default, Namespace("b1")),
    ///     (PrefixDeclaration::Named("b"), Namespace("b2"))
    /// ]);
    ///
    /// reader.read_resolved_event()?; // <d/>
    /// // </b> got closed so back to the bindings declared on <a>
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Default, Namespace("a1")),
    ///     (PrefixDeclaration::Named("a"), Namespace("a2"))
    /// ]);
    ///
    /// reader.read_resolved_event()?; // </a>
    /// // Still the same
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Default, Namespace("a1")),
    ///     (PrefixDeclaration::Named("a"), Namespace("a2"))
    /// ]);
    ///
    /// reader.read_resolved_event()?; // </root>
    /// // <a> got closed
    /// assert_eq!(reader.resolver().bindings().collect::<Vec<_>>(), vec![]);
    /// # quick_xml::Result::Ok(())
    /// ```
    #[inline]
    pub const fn bindings(&self) -> NamespaceBindingsIter<'_> {
        NamespaceBindingsIter {
            resolver: self,
            // We initialize the cursor to 2 to skip the two default namespaces xml: and xmlns:
            cursor: 2,
        }
    }

    /// Returns all the bindings on the specified level, including the default
    /// `xml` and `xmlns` bindings.
    ///
    /// # Parameters
    /// - `level`: the nesting level of an XML tag. The document without tags has
    ///   level 0, at which default bindings are declared. The root tag has level 1
    ///   and all other tags has levels > 1. If specify level more than [current], the
    ///   empty iterator is returned.
    ///
    /// # Examples
    ///
    /// This example shows what results the returned iterator would return on each
    /// level after reaning some events of a simple XML.
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// use quick_xml::name::{Namespace, PrefixDeclaration};
    /// use quick_xml::NsReader;
    ///
    /// let src = "<root>
    ///   <a xmlns=\"a1\" xmlns:a=\"a2\">
    ///     <b xmlns=\"b1\" xmlns:b=\"b2\">
    ///       <c/>
    ///     </b>
    ///     <d/>
    ///   </a>
    /// </root>";
    /// let mut reader = NsReader::from_str(src);
    /// reader.config_mut().trim_text(true);
    /// reader.read_resolved_event()?; // <root>
    /// reader.read_resolved_event()?; // <a>
    /// reader.read_resolved_event()?; // <b>
    /// reader.read_resolved_event()?; // <c/>
    ///
    /// // Default bindings at the beginning
    /// assert_eq!(reader.resolver().bindings_of(0).collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Named("xml"), Namespace("http://www.w3.org/XML/1998/namespace")),
    ///     (PrefixDeclaration::Named("xmlns"), Namespace("http://www.w3.org/2000/xmlns/")),
    /// ]);
    ///
    /// // No bindings declared on root
    /// assert_eq!(reader.resolver().bindings_of(1).collect::<Vec<_>>(), vec![]);
    ///
    /// // Two bindings declared on "a"
    /// assert_eq!(reader.resolver().bindings_of(2).collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Default, Namespace("a1")),
    ///     (PrefixDeclaration::Named("a"), Namespace("a2")),
    /// ]);
    ///
    /// // Two bindings declared on "b"
    /// assert_eq!(reader.resolver().bindings_of(3).collect::<Vec<_>>(), vec![
    ///     (PrefixDeclaration::Default, Namespace("b1")),
    ///     (PrefixDeclaration::Named("b"), Namespace("b2")),
    /// ]);
    ///
    /// // No bindings declared on "c"
    /// assert_eq!(reader.resolver().bindings_of(4).collect::<Vec<_>>(), vec![]);
    ///
    /// // No bindings on non-existent level
    /// assert_eq!(reader.resolver().bindings_of(5).collect::<Vec<_>>(), vec![]);
    /// # quick_xml::Result::Ok(())
    /// ```
    ///
    /// [current]: Self::level
    pub const fn bindings_of(&self, level: u16) -> NamespaceBindingsOfLevelIter<'_> {
        NamespaceBindingsOfLevelIter {
            resolver: self,
            cursor: 0,
            level,
        }
    }

    /// Returns the number of [`push`] calls that were not followed by [`pop`] calls.
    ///
    /// Due to use of `u16` for level number the number of nested tags in XML
    /// are limited by [`u16::MAX`], but that is enough for any real application.
    ///
    /// # Example
    ///
    /// ```
    /// # use pretty_assertions::assert_eq;
    /// # use quick_xml::events::BytesStart;
    /// # use quick_xml::name::{Namespace, NamespaceResolver, PrefixDeclaration, QName, ResolveResult};
    /// #
    /// let mut resolver = NamespaceResolver::default();
    ///
    /// assert_eq!(resolver.level(), 0);
    ///
    /// resolver.push(&BytesStart::new("tag"));
    /// assert_eq!(resolver.level(), 1);
    ///
    /// resolver.pop();
    /// assert_eq!(resolver.level(), 0);
    ///
    /// // pop from empty resolver does nothing
    /// resolver.pop();
    /// assert_eq!(resolver.level(), 0);
    /// ```
    ///
    /// [`push`]: Self::push
    /// [`pop`]: Self::pop
    pub const fn level(&self) -> u16 {
        self.nesting_level
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Iterator on the current declared namespace bindings. Returns pairs of the _(prefix, namespace)_.
///
/// See [`NamespaceResolver::bindings`] for documentation.
#[derive(Debug, Clone)]
pub struct NamespaceBindingsIter<'a> {
    resolver: &'a NamespaceResolver,
    cursor: usize,
}

impl<'a> Iterator for NamespaceBindingsIter<'a> {
    type Item = (PrefixDeclaration<'a>, Namespace<'a>);

    fn next(&mut self) -> Option<(PrefixDeclaration<'a>, Namespace<'a>)> {
        while let Some(binding) = self.resolver.bindings.get(self.cursor) {
            self.cursor += 1; // We increment for next read

            // We check if the key has not been overridden by having a look
            // at the namespaces declared after in the array
            let prefix = binding.prefix(&self.resolver.buffer);
            if self.resolver.bindings[self.cursor..]
                .iter()
                .any(|ne| prefix == ne.prefix(&self.resolver.buffer))
            {
                continue; // Overridden
            }
            if let ResolveResult::Bound(namespace) = binding.namespace(&self.resolver.buffer) {
                let prefix = match prefix {
                    Some(Prefix(prefix)) => PrefixDeclaration::Named(prefix),
                    None => PrefixDeclaration::Default,
                };
                return Some((prefix, namespace));
            }
        }
        None // We have exhausted the array
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Real count could be less if some namespaces was overridden
        (0, Some(self.resolver.bindings.len() - self.cursor))
    }
}

impl<'a> FusedIterator for NamespaceBindingsIter<'a> {}

/// Iterator on the declared namespace bindings on specified level. Returns pairs of the _(prefix, namespace)_.
///
/// See [`NamespaceResolver::bindings_of`] for documentation.
#[derive(Debug, Clone)]
pub struct NamespaceBindingsOfLevelIter<'a> {
    resolver: &'a NamespaceResolver,
    cursor: usize,
    level: u16,
}

impl<'a> Iterator for NamespaceBindingsOfLevelIter<'a> {
    type Item = (PrefixDeclaration<'a>, Namespace<'a>);

    fn next(&mut self) -> Option<(PrefixDeclaration<'a>, Namespace<'a>)> {
        while let Some(binding) = self.resolver.bindings.get(self.cursor) {
            self.cursor += 1; // We increment for next read
            if binding.level < self.level {
                continue;
            }
            if binding.level > self.level {
                break;
            }

            if let ResolveResult::Bound(namespace) = binding.namespace(&self.resolver.buffer) {
                let prefix = match binding.prefix(&self.resolver.buffer) {
                    Some(Prefix(prefix)) => PrefixDeclaration::Named(prefix),
                    None => PrefixDeclaration::Default,
                };
                return Some((prefix, namespace));
            }
        }
        None // We have exhausted the array
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Real count could be less
        (0, Some(self.resolver.bindings.len() - self.cursor))
    }
}

impl<'a> FusedIterator for NamespaceBindingsOfLevelIter<'a> {}

////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod namespaces {
    use super::*;
    use ResolveResult::*;
    use pretty_assertions::assert_eq;

    /// Regression test for <https://github.com/tafia/quick-xml/issues/970>: a single element with
    /// many `xmlns:*` declarations must be rejected once the total binding count exceeds the limit.
    #[test]
    fn rejects_too_many_bindings_on_single_element() {
        let limit = DEFAULT_MAX_NAMESPACE_BINDINGS;

        // One more than the limit triggers the error.
        let mut tag = String::from("e");
        for i in 0..=limit {
            tag.push_str(&format!(" xmlns:p{}=''", i));
        }
        let mut resolver = NamespaceResolver::default();
        assert_eq!(
            resolver.push(&BytesStart::from_content(&tag, 1)),
            Err(NamespaceError::TooManyBindings(limit)),
        );

        // Exactly at the limit is accepted.
        let mut tag = String::from("e");
        for i in 0..limit {
            tag.push_str(&format!(" xmlns:p{}=''", i));
        }
        let mut resolver = NamespaceResolver::default();
        assert_eq!(resolver.push(&BytesStart::from_content(&tag, 1)), Ok(()));

        // The limit is configurable, and `usize::MAX` disables it.
        let mut resolver = NamespaceResolver::default();
        resolver.set_max_namespace_bindings(2);
        assert_eq!(
            resolver.push(&BytesStart::from_content(
                "e xmlns:a='' xmlns:b='' xmlns:c=''",
                1,
            )),
            Err(NamespaceError::TooManyBindings(2)),
        );
        let mut resolver = NamespaceResolver::default();
        resolver.set_max_namespace_bindings(usize::MAX);
        assert_eq!(
            resolver.push(&BytesStart::from_content(
                "e xmlns:a='' xmlns:b='' xmlns:c=''",
                1,
            )),
            Ok(()),
        );
    }

    /// Regression test for <https://github.com/tafia/quick-xml/issues/980>:
    /// deeply nested documents where each level declares one `xmlns:*`
    /// binding must be rejected once the total binding count exceeds the
    /// limit, preventing O(depth²) CPU exhaustion in `resolve_prefix`.
    #[test]
    fn rejects_too_many_bindings_across_elements() {
        let limit = 10;
        let mut resolver = NamespaceResolver::default();
        resolver.set_max_namespace_bindings(limit);

        // Push elements, each declaring one new namespace binding.
        for i in 0..limit {
            let tag = format!("e xmlns:p{}='ns{}'", i, i);
            assert_eq!(
                resolver.push(&BytesStart::from_content(&tag, 1)),
                Ok(()),
                "push {} should succeed",
                i,
            );
        }

        // The next binding (on a new element) exceeds the limit.
        assert_eq!(
            resolver.push(&BytesStart::from_content("e xmlns:extra='ns'", 1)),
            Err(NamespaceError::TooManyBindings(limit)),
        );

        // An element without namespace declarations is still fine.
        assert_eq!(resolver.push(&BytesStart::from_content("e", 1)), Ok(()),);
    }

    /// Popping scopes makes room for new bindings under the limit.
    #[test]
    fn popping_frees_room_for_bindings() {
        let limit = 10;
        let mut resolver = NamespaceResolver::default();
        resolver.set_max_namespace_bindings(limit);

        // Fill to the limit.
        for i in 0..limit {
            let tag = format!("e xmlns:p{}='ns{}'", i, i);
            resolver.push(&BytesStart::from_content(&tag, 1)).unwrap();
        }

        // Pop the last element's scope — frees one binding slot.
        resolver.pop();

        // Now a new binding fits.
        assert_eq!(
            resolver.push(&BytesStart::from_content("e xmlns:new='ns'", 1)),
            Ok(()),
        );
    }

    /// Regression test for <https://github.com/tafia/quick-xml/issues/977>:
    /// `push()` previously incremented a `u16` depth counter with an unguarded
    /// `+= 1`, so a document nested past `u16::MAX` panicked under
    /// `overflow-checks` or silently wrapped the counter and corrupted namespace
    /// scoping in release. It now returns `TooDeeplyNested` at the boundary.
    #[test]
    fn push_rejects_pathological_nesting_depth() {
        let mut resolver = NamespaceResolver::default();
        let tag = BytesStart::from_content("a", 1);
        // `u16::MAX` successful pushes, then the next is rejected cleanly.
        for _ in 0..u16::MAX {
            assert_eq!(resolver.push(&tag), Ok(()));
        }
        assert_eq!(
            resolver.push(&tag),
            Err(NamespaceError::TooDeeplyNested(u16::MAX as usize)),
        );
    }

    /// Unprefixed attribute names (resolved with `false` flag) never have a namespace
    /// according to <https://www.w3.org/TR/xml-names11/#defaulting>:
    ///
    /// > A default namespace declaration applies to all unprefixed element names
    /// > within its scope. Default namespace declarations do not apply directly
    /// > to attribute names; the interpretation of unprefixed attributes is
    /// > determined by the element on which they appear.
    mod unprefixed {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Basic tests that checks that basic resolver functionality is working
        #[test]
        fn basic() {
            let name = QName("simple");
            let ns = Namespace("default");

            let mut resolver = NamespaceResolver::default();
            let s = resolver.buffer.len();

            resolver
                .push(&BytesStart::from_content(" xmlns='default'", 0))
                .unwrap();
            assert_eq!(&resolver.buffer[s..], "default");

            // Check that tags without namespaces does not change result
            resolver.push(&BytesStart::from_content("", 0)).unwrap();
            assert_eq!(&resolver.buffer[s..], "default");
            resolver.pop();

            assert_eq!(&resolver.buffer[s..], "default");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(ns), LocalName("simple"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Unbound, LocalName("simple"))
            );
        }

        /// Test adding a second level of namespaces, which replaces the previous binding
        #[test]
        fn override_namespace() {
            let name = QName("simple");
            let old_ns = Namespace("old");
            let new_ns = Namespace("new");

            let mut resolver = NamespaceResolver::default();
            let s = resolver.buffer.len();

            resolver
                .push(&BytesStart::from_content(" xmlns='old'", 0))
                .unwrap();
            resolver
                .push(&BytesStart::from_content(" xmlns='new'", 0))
                .unwrap();

            assert_eq!(&resolver.buffer[s..], "oldnew");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(new_ns), LocalName("simple"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Unbound, LocalName("simple"))
            );

            resolver.pop();
            assert_eq!(&resolver.buffer[s..], "old");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(old_ns), LocalName("simple"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Unbound, LocalName("simple"))
            );
        }

        /// Test adding a second level of namespaces, which reset the previous binding
        /// to not bound state by specifying an empty namespace name.
        ///
        /// See <https://www.w3.org/TR/xml-names11/#scoping>
        #[test]
        fn reset() {
            let name = QName("simple");
            let old_ns = Namespace("old");

            let mut resolver = NamespaceResolver::default();
            let s = resolver.buffer.len();

            resolver
                .push(&BytesStart::from_content(" xmlns='old'", 0))
                .unwrap();
            resolver
                .push(&BytesStart::from_content(" xmlns=''", 0))
                .unwrap();

            assert_eq!(&resolver.buffer[s..], "old");
            assert_eq!(resolver.resolve(name, true), (Unbound, LocalName("simple")));
            assert_eq!(
                resolver.resolve(name, false),
                (Unbound, LocalName("simple"))
            );

            resolver.pop();
            assert_eq!(&resolver.buffer[s..], "old");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(old_ns), LocalName("simple"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Unbound, LocalName("simple"))
            );
        }
    }

    mod declared_prefix {
        use super::*;
        use pretty_assertions::assert_eq;

        /// Basic tests that checks that basic resolver functionality is working
        #[test]
        fn basic() {
            let name = QName("p:with-declared-prefix");
            let ns = Namespace("default");

            let mut resolver = NamespaceResolver::default();
            let s = resolver.buffer.len();

            resolver
                .push(&BytesStart::from_content(" xmlns:p='default'", 0))
                .unwrap();
            assert_eq!(&resolver.buffer[s..], "pdefault");

            // Check that tags without namespaces does not change result
            resolver.push(&BytesStart::from_content("", 0)).unwrap();
            assert_eq!(&resolver.buffer[s..], "pdefault");
            resolver.pop();

            assert_eq!(&resolver.buffer[s..], "pdefault");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(ns), LocalName("with-declared-prefix"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Bound(ns), LocalName("with-declared-prefix"))
            );
        }

        /// Test adding a second level of namespaces, which replaces the previous binding
        #[test]
        fn override_namespace() {
            let name = QName("p:with-declared-prefix");
            let old_ns = Namespace("old");
            let new_ns = Namespace("new");

            let mut resolver = NamespaceResolver::default();
            let s = resolver.buffer.len();

            resolver
                .push(&BytesStart::from_content(" xmlns:p='old'", 0))
                .unwrap();
            resolver
                .push(&BytesStart::from_content(" xmlns:p='new'", 0))
                .unwrap();

            assert_eq!(&resolver.buffer[s..], "poldpnew");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(new_ns), LocalName("with-declared-prefix"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Bound(new_ns), LocalName("with-declared-prefix"))
            );

            resolver.pop();
            assert_eq!(&resolver.buffer[s..], "pold");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(old_ns), LocalName("with-declared-prefix"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Bound(old_ns), LocalName("with-declared-prefix"))
            );
        }

        /// Test adding a second level of namespaces, which reset the previous binding
        /// to not bound state by specifying an empty namespace name.
        ///
        /// See <https://www.w3.org/TR/xml-names11/#scoping>
        #[test]
        fn reset() {
            let name = QName("p:with-declared-prefix");
            let old_ns = Namespace("old");

            let mut resolver = NamespaceResolver::default();
            let s = resolver.buffer.len();

            resolver
                .push(&BytesStart::from_content(" xmlns:p='old'", 0))
                .unwrap();
            resolver
                .push(&BytesStart::from_content(" xmlns:p=''", 0))
                .unwrap();

            assert_eq!(&resolver.buffer[s..], "poldp");
            assert_eq!(
                resolver.resolve(name, true),
                (Unknown("p".to_string()), LocalName("with-declared-prefix"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Unknown("p".to_string()), LocalName("with-declared-prefix"))
            );

            resolver.pop();
            assert_eq!(&resolver.buffer[s..], "pold");
            assert_eq!(
                resolver.resolve(name, true),
                (Bound(old_ns), LocalName("with-declared-prefix"))
            );
            assert_eq!(
                resolver.resolve(name, false),
                (Bound(old_ns), LocalName("with-declared-prefix"))
            );
        }
    }

    /// Tests for `xml` and `xmlns` built-in prefixes.
    ///
    /// See <https://www.w3.org/TR/xml-names11/#xmlReserved>
    mod builtin_prefixes {
        use super::*;

        mod xml {
            use super::*;
            use pretty_assertions::assert_eq;

            /// `xml` prefix are always defined, it is not required to define it explicitly.
            #[test]
            fn undeclared() {
                let name = QName("xml:random");
                let namespace = RESERVED_NAMESPACE_XML.1;

                let resolver = NamespaceResolver::default();

                assert_eq!(
                    resolver.resolve(name, true),
                    (Bound(namespace), LocalName("random"))
                );

                assert_eq!(
                    resolver.resolve(name, false),
                    (Bound(namespace), LocalName("random"))
                );
            }

            /// `xml` prefix can be declared but it must be bound to the value
            /// `http://www.w3.org/XML/1998/namespace`
            #[test]
            fn rebound_to_correct_ns() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                resolver.push(
                    &BytesStart::from_content(
                        " xmlns:xml='http://www.w3.org/XML/1998/namespace'",
                        0,
                    ),
                ).expect("`xml` prefix should be possible to bound to `http://www.w3.org/XML/1998/namespace`");
                assert_eq!(&resolver.buffer[s..], "");
            }

            /// `xml` prefix cannot be re-declared to another namespace
            #[test]
            fn rebound_to_incorrect_ns() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                assert_eq!(
                    resolver.push(&BytesStart::from_content(
                        " xmlns:xml='not_correct_namespace'",
                        0,
                    )),
                    Err(NamespaceError::InvalidXmlPrefixBind(
                        "not_correct_namespace".to_string()
                    )),
                );
                assert_eq!(&resolver.buffer[s..], "");
            }

            /// `xml` prefix cannot be unbound
            #[test]
            fn unbound() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                assert_eq!(
                    resolver.push(&BytesStart::from_content(" xmlns:xml=''", 0)),
                    Err(NamespaceError::InvalidXmlPrefixBind("".to_string())),
                );
                assert_eq!(&resolver.buffer[s..], "");
            }

            /// Other prefix cannot be bound to `xml` namespace
            #[test]
            fn other_prefix_bound_to_xml_namespace() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                assert_eq!(
                    resolver.push(&BytesStart::from_content(
                        " xmlns:not_xml='http://www.w3.org/XML/1998/namespace'",
                        0,
                    )),
                    Err(NamespaceError::InvalidPrefixForXml("not_xml".to_string())),
                );
                assert_eq!(&resolver.buffer[s..], "");
            }
        }

        mod xmlns {
            use super::*;
            use pretty_assertions::assert_eq;

            /// `xmlns` prefix are always defined, it is forbidden to define it explicitly
            #[test]
            fn undeclared() {
                let name = QName("xmlns:random");
                let namespace = RESERVED_NAMESPACE_XMLNS.1;

                let resolver = NamespaceResolver::default();

                assert_eq!(
                    resolver.resolve(name, true),
                    (Bound(namespace), LocalName("random"))
                );

                assert_eq!(
                    resolver.resolve(name, false),
                    (Bound(namespace), LocalName("random"))
                );
            }

            /// `xmlns` prefix cannot be re-declared event to its own namespace
            #[test]
            fn rebound_to_correct_ns() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                assert_eq!(
                    resolver.push(&BytesStart::from_content(
                        " xmlns:xmlns='http://www.w3.org/2000/xmlns/'",
                        0,
                    )),
                    Err(NamespaceError::InvalidXmlnsPrefixBind(
                        "http://www.w3.org/2000/xmlns/".to_string()
                    )),
                );
                assert_eq!(&resolver.buffer[s..], "");
            }

            /// `xmlns` prefix cannot be re-declared
            #[test]
            fn rebound_to_incorrect_ns() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                assert_eq!(
                    resolver.push(&BytesStart::from_content(
                        " xmlns:xmlns='not_correct_namespace'",
                        0,
                    )),
                    Err(NamespaceError::InvalidXmlnsPrefixBind(
                        "not_correct_namespace".to_string()
                    )),
                );
                assert_eq!(&resolver.buffer[s..], "");
            }

            /// `xmlns` prefix cannot be unbound
            #[test]
            fn unbound() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                assert_eq!(
                    resolver.push(&BytesStart::from_content(" xmlns:xmlns=''", 0)),
                    Err(NamespaceError::InvalidXmlnsPrefixBind("".to_string())),
                );
                assert_eq!(&resolver.buffer[s..], "");
            }

            /// Other prefix cannot be bound to `xmlns` namespace
            #[test]
            fn other_prefix_bound_to_xmlns_namespace() {
                let mut resolver = NamespaceResolver::default();
                let s = resolver.buffer.len();
                assert_eq!(
                    resolver.push(&BytesStart::from_content(
                        " xmlns:not_xmlns='http://www.w3.org/2000/xmlns/'",
                        0,
                    )),
                    Err(NamespaceError::InvalidPrefixForXmlns(
                        "not_xmlns".to_string()
                    )),
                );
                assert_eq!(&resolver.buffer[s..], "");
            }
        }
    }

    #[test]
    fn undeclared_prefix() {
        let name = QName("unknown:prefix");

        let resolver = NamespaceResolver::default();

        assert_eq!(
            resolver.buffer,
            "xmlhttp://www.w3.org/XML/1998/namespacexmlnshttp://www.w3.org/2000/xmlns/"
        );
        assert_eq!(
            resolver.resolve(name, true),
            (Unknown("unknown".to_string()), LocalName("prefix"))
        );
        assert_eq!(
            resolver.resolve(name, false),
            (Unknown("unknown".to_string()), LocalName("prefix"))
        );
    }

    /// Checks how the QName is decomposed to a prefix and a local name
    #[test]
    fn prefix_and_local_name() {
        let name = QName("foo:bus");
        assert_eq!(name.prefix(), Some(Prefix("foo")));
        assert_eq!(name.local_name(), LocalName("bus"));
        assert_eq!(name.decompose(), (LocalName("bus"), Some(Prefix("foo"))));

        let name = QName("foo:");
        assert_eq!(name.prefix(), Some(Prefix("foo")));
        assert_eq!(name.local_name(), LocalName(""));
        assert_eq!(name.decompose(), (LocalName(""), Some(Prefix("foo"))));

        let name = QName(":foo");
        assert_eq!(name.prefix(), Some(Prefix("")));
        assert_eq!(name.local_name(), LocalName("foo"));
        assert_eq!(name.decompose(), (LocalName("foo"), Some(Prefix(""))));

        let name = QName("foo:bus:baz");
        assert_eq!(name.prefix(), Some(Prefix("foo")));
        assert_eq!(name.local_name(), LocalName("bus:baz"));
        assert_eq!(
            name.decompose(),
            (LocalName("bus:baz"), Some(Prefix("foo")))
        );
    }
}
