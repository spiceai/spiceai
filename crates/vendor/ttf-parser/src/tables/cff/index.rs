use crate::parser::{FromData, NumFrom, Stream, U24};

pub(crate) trait IndexSize: FromData {
    fn to_u32(self) -> u32;
}

impl IndexSize for u16 {
    fn to_u32(self) -> u32 {
        u32::from(self)
    }
}

impl IndexSize for u32 {
    fn to_u32(self) -> u32 {
        self
    }
}

#[inline]
pub(crate) fn parse_index<'a, T: IndexSize>(s: &mut Stream<'a>) -> Option<Index<'a>> {
    let count = s.read::<T>()?;
    parse_index_impl(count.to_u32(), s)
}

#[inline(never)]
fn parse_index_impl<'a>(count: u32, s: &mut Stream<'a>) -> Option<Index<'a>> {
    if count == 0 || count == u32::MAX {
        return Some(Index::default());
    }

    let offset_size = s.read::<OffsetSize>()?;
    let offsets_len = (count + 1).checked_mul(offset_size.to_u32())?;
    let offsets = VarOffsets {
        data: s.read_bytes(usize::num_from(offsets_len))?,
        offset_size,
    };

    // Last offset indicates a Data Index size.
    match offsets.last() {
        Some(last_offset) => {
            let data = s.read_bytes(usize::num_from(last_offset))?;
            Some(Index { data, offsets })
        }
        None => Some(Index::default()),
    }
}

#[inline]
pub(crate) fn skip_index<T: IndexSize>(s: &mut Stream) -> Option<()> {
    let count = s.read::<T>()?;
    skip_index_impl(count.to_u32(), s)
}

#[inline(never)]
fn skip_index_impl(count: u32, s: &mut Stream) -> Option<()> {
    if count == 0 || count == u32::MAX {
        return Some(());
    }

    let offset_size = s.read::<OffsetSize>()?;
    let offsets_len = (count + 1).checked_mul(offset_size.to_u32())?;
    let offsets = VarOffsets {
        data: s.read_bytes(usize::num_from(offsets_len))?,
        offset_size,
    };

    if let Some(last_offset) = offsets.last() {
        s.advance(usize::num_from(last_offset));
    }

    Some(())
}

#[derive(Clone, Copy, Debug)]
pub struct VarOffsets<'a> {
    data: &'a [u8],
    offset_size: OffsetSize,
}

impl<'a> VarOffsets<'a> {
    fn get(&self, index: u32) -> Option<u32> {
        if index >= self.len() {
            return None;
        }

        let start = usize::num_from(index) * self.offset_size.to_usize();
        let mut s = Stream::new_at(self.data, start)?;
        let n: u32 = match self.offset_size {
            OffsetSize::Size1 => u32::from(s.read::<u8>()?),
            OffsetSize::Size2 => u32::from(s.read::<u16>()?),
            OffsetSize::Size3 => s.read::<U24>()?.0,
            OffsetSize::Size4 => s.read::<u32>()?,
        };

        // Offsets are offset by one byte in the font,
        // so we have to shift them back.
        n.checked_sub(1)
    }

    #[inline]
    fn last(&self) -> Option<u32> {
        if !self.is_empty() {
            self.get(self.len() - 1)
        } else {
            None
        }
    }

    #[inline]
    fn len(&self) -> u32 {
        self.data.len() as u32 / self.offset_size as u32
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Index<'a> {
    data: &'a [u8],
    offsets: VarOffsets<'a>,
}

impl<'a> Default for Index<'a> {
    #[inline]
    fn default() -> Self {
        Index {
            data: b"",
            offsets: VarOffsets {
                data: b"",
                offset_size: OffsetSize::Size1,
            },
        }
    }
}

impl<'a> IntoIterator for Index<'a> {
    type Item = &'a [u8];
    type IntoIter = IndexIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IndexIter {
            data: self,
            offset_index: 0,
        }
    }
}

impl<'a> Index<'a> {
    #[inline]
    pub(crate) fn len(&self) -> u32 {
        // Last offset points to the byte after the `Object data`. We should skip it.
        self.offsets.len().saturating_sub(1)
    }

    pub(crate) fn get(&self, index: u32) -> Option<&'a [u8]> {
        let next_index = index.checked_add(1)?; // make sure we do not overflow
        let start = usize::num_from(self.offsets.get(index)?);
        let end = usize::num_from(self.offsets.get(next_index)?);
        self.data.get(start..end)
    }
}

pub struct IndexIter<'a> {
    data: Index<'a>,
    offset_index: u32,
}

impl<'a> Iterator for IndexIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset_index == self.data.len() {
            return None;
        }

        let index = self.offset_index;
        self.offset_index += 1;
        self.data.get(index)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OffsetSize {
    Size1 = 1,
    Size2 = 2,
    Size3 = 3,
    Size4 = 4,
}

impl OffsetSize {
    #[inline]
    fn to_u32(self) -> u32 {
        self as u32
    }
    #[inline]
    fn to_usize(self) -> usize {
        self as usize
    }
}

impl FromData for OffsetSize {
    const SIZE: usize = 1;

    #[inline]
    fn parse(data: &[u8]) -> Option<Self> {
        match data.get(0)? {
            1 => Some(OffsetSize::Size1),
            2 => Some(OffsetSize::Size2),
            3 => Some(OffsetSize::Size3),
            4 => Some(OffsetSize::Size4),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_offset_size() {
        assert_eq!(core::mem::size_of::<OffsetSize>(), 1);

        assert_eq!(Stream::new(&[0x00]).read::<OffsetSize>(), None);
        assert_eq!(
            Stream::new(&[0x01]).read::<OffsetSize>(),
            Some(OffsetSize::Size1)
        );
        assert_eq!(Stream::new(&[0x05]).read::<OffsetSize>(), None);
    }
}
