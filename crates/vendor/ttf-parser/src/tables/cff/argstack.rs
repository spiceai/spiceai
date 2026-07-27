use super::CFFError;

pub(crate) struct ArgumentsStack<'a> {
    pub(crate) data: &'a mut [f32],
    pub(crate) len: usize,
    pub(crate) max_len: usize,
}

impl<'a> ArgumentsStack<'a> {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub(crate) fn push(&mut self, n: f32) -> Result<(), CFFError> {
        if self.len == self.max_len {
            Err(CFFError::ArgumentsStackLimitReached)
        } else {
            self.data[self.len] = n;
            self.len += 1;
            Ok(())
        }
    }

    #[inline]
    pub(crate) fn at(&self, index: usize) -> f32 {
        self.data[index]
    }

    #[inline]
    pub(crate) fn pop(&mut self) -> f32 {
        debug_assert!(!self.is_empty());
        self.len -= 1;
        self.data[self.len]
    }

    #[inline]
    pub(crate) fn reverse(&mut self) {
        if self.is_empty() {
            return;
        }

        // Reverse only the actual data and not the whole stack.
        let (first, _) = self.data.split_at_mut(self.len);
        first.reverse();
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }
}

impl core::fmt::Debug for ArgumentsStack<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_list().entries(&self.data[..self.len]).finish()
    }
}
