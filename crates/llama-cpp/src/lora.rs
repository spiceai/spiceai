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
//! `LoRA` adapters, applied to a model's weights at decode time instead of merged into them.

use std::ffi::CString;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::NonNull;

use snafu::{OptionExt, ResultExt};

use crate::model::Model;
use crate::{LoadLoraSnafu, LoraPathSnafu, Result, sys};

/// A `LoRA` adapter loaded for one model from a GGUF adapter file, such as
/// `convert_lora_to_gguf.py` writes. [`crate::Context::set_lora_adapters`] applies it; one
/// adapter can serve several contexts at once.
#[derive(Debug)]
pub struct LoraAdapter<'m> {
    raw: NonNull<sys::llama_adapter_lora>,
    _model: PhantomData<&'m Model>,
}

// SAFETY: llama.cpp never changes an adapter after it loads; contexts only read its tensors.
unsafe impl Send for LoraAdapter<'_> {}
// SAFETY: as above, so shared references from several threads only read it.
unsafe impl Sync for LoraAdapter<'_> {}

impl<'m> LoraAdapter<'m> {
    /// Loads the adapter at `path` for `model`. llama.cpp refuses an adapter written for
    /// another architecture, or whose tensors do not fit the model's.
    pub fn load(model: &'m Model, path: &Path) -> Result<Self> {
        let display = path.display().to_string();
        let c_path = CString::new(path.as_os_str().as_encoded_bytes()).context(LoraPathSnafu {
            path: display.clone(),
        })?;
        crate::log::clear_last_error();
        // SAFETY: the model is loaded and outlives the adapter (`'m`), and `c_path` is a
        // NUL-terminated path that outlives the call.
        let raw = unsafe { sys::llama_adapter_lora_init(model.as_ptr(), c_path.as_ptr()) };
        let raw = NonNull::new(raw).with_context(|| LoadLoraSnafu {
            path: display,
            reason: crate::log::last_error(),
        })?;
        Ok(Self {
            raw,
            _model: PhantomData,
        })
    }

    pub(crate) fn as_ptr(&self) -> *mut sys::llama_adapter_lora {
        self.raw.as_ptr()
    }
}

impl Drop for LoraAdapter<'_> {
    fn drop(&mut self) {
        // SAFETY: the adapter is freed once, before its model (`'m`); every context that applies
        // it holds it, so none is left to read it.
        unsafe { sys::llama_adapter_lora_free(self.raw.as_ptr()) };
    }
}
