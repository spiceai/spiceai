/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! A key produced run by run, which [`crate::KeyEncoder`] drains into bytes.

/// Most bytes a [`Run`] can carry inline: a nullability marker plus a 128-bit
/// value.
pub(crate) const INLINE_RUN_BYTES: usize = 17;

/// A run of key bytes: either borrowed from the data the key is read from, or
/// a short run produced on the fly (for example a sign-flipped big-endian
/// integer).
#[derive(Clone, Copy, Debug)]
pub struct Run<'a> {
    repr: RunRepr<'a>,
}

#[derive(Clone, Copy, Debug)]
enum RunRepr<'a> {
    Borrowed(&'a [u8]),
    Inline {
        bytes: [u8; INLINE_RUN_BYTES],
        len: u8,
    },
}

impl<'a> Run<'a> {
    /// A run borrowed from the key's source data.
    #[inline]
    #[must_use]
    pub fn borrowed(bytes: &'a [u8]) -> Self {
        Self {
            repr: RunRepr::Borrowed(bytes),
        }
    }

    /// A run of at most 17 bytes produced by the source. Longer input is
    /// truncated in release builds and rejected by a debug assertion; sources
    /// in this crate never produce one.
    #[inline]
    #[must_use]
    pub fn inline(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() <= INLINE_RUN_BYTES);
        let len = bytes.len().min(INLINE_RUN_BYTES);
        let mut buf = [0; INLINE_RUN_BYTES];
        buf[..len].copy_from_slice(&bytes[..len]);
        Self {
            repr: RunRepr::Inline {
                bytes: buf,
                len: u8::try_from(len).unwrap_or(u8::MAX),
            },
        }
    }

    /// The bytes of this run.
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match &self.repr {
            RunRepr::Borrowed(bytes) => bytes,
            RunRepr::Inline { bytes, len } => &bytes[..usize::from(*len)],
        }
    }
}

/// A key read one run at a time. The concatenation of the runs is the key.
pub trait KeySource<'a> {
    /// The next run of the key, or `None` once the key is exhausted. A run may
    /// be empty.
    fn next_run(&mut self) -> Option<Run<'a>>;
}
