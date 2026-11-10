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

//! The architecture a GGUF model names in its header (`general.architecture`), read from the
//! start of its first shard without loading it: which engine serves a model split across
//! nodes depends on it, and every node has to decide alike.

use std::future::Future;
use std::path::{Path, PathBuf};

use tokio::io::AsyncReadExt;

/// The architecture a draft model names; it drafts for a model, so it never decides.
const DRAFTER: &str = "dflash";

/// The bytes read first; `general.architecture` is the first key a GGUF writer writes.
const FIRST_READ: u64 = 64 * 1024;

/// The most bytes read looking for the key, past a tokenizer's vocabulary written before it.
const MAX_READ: u64 = 64 * 1024 * 1024;

/// Arrays nest no deeper than this in a header that is read.
const MAX_DEPTH: usize = 4;

/// What the start of a GGUF file says about its architecture.
#[derive(Debug, PartialEq, Eq)]
enum Header {
    /// The value of `general.architecture`.
    Architecture(String),
    /// The bytes are not a GGUF header, or it names no architecture.
    Unknown,
    /// The header goes on past the bytes read.
    Truncated,
}

/// Reads GGUF values little-endian from the start of a file.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

/// The header ends before the value does.
struct Short;

impl<'a> Reader<'a> {
    fn take(&mut self, n: u64) -> Result<&'a [u8], Short> {
        let n = usize::try_from(n).map_err(|_| Short)?;
        let end = self.at.checked_add(n).ok_or(Short)?;
        let taken = self.bytes.get(self.at..end).ok_or(Short)?;
        self.at = end;
        Ok(taken)
    }

    fn u32(&mut self) -> Result<u32, Short> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, Short> {
        let bytes = self.take(8)?;
        let mut word = [0; 8];
        word.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(word))
    }

    fn string(&mut self) -> Result<&'a [u8], Short> {
        let len = self.u64()?;
        self.take(len)
    }

    /// Skips a value of GGUF type `kind`. `None` for a type GGUF does not define.
    fn skip(&mut self, kind: u32, depth: usize) -> Result<Option<()>, Short> {
        let width = match kind {
            // uint8, int8, bool
            0 | 1 | 7 => 1,
            // uint16, int16
            2 | 3 => 2,
            // uint32, int32, float32
            4..=6 => 4,
            // uint64, int64, float64
            10..=12 => 8,
            // string
            8 => {
                self.string()?;
                return Ok(Some(()));
            }
            // array
            9 if depth < MAX_DEPTH => {
                let element = self.u32()?;
                let count = self.u64()?;
                for _ in 0..count {
                    if self.skip(element, depth + 1)?.is_none() {
                        return Ok(None);
                    }
                }
                return Ok(Some(()));
            }
            _ => return Ok(None),
        };
        self.take(width)?;
        Ok(Some(()))
    }
}

/// What `bytes`, the start of a GGUF file, say about its architecture.
fn architecture(bytes: &[u8]) -> Header {
    scan(&mut Reader { bytes, at: 0 }).unwrap_or(Header::Truncated)
}

fn scan(reader: &mut Reader<'_>) -> Result<Header, Short> {
    if reader.take(4)? != b"GGUF" {
        return Ok(Header::Unknown);
    }
    // Version 1 counted in 32 bits; every GGUF a current engine loads is version 2 or 3.
    if !matches!(reader.u32()?, 2 | 3) {
        return Ok(Header::Unknown);
    }
    let _tensors = reader.u64()?;
    let keys = reader.u64()?;
    for _ in 0..keys {
        let key = reader.string()?;
        let kind = reader.u32()?;
        if key == b"general.architecture" {
            if kind != 8 {
                return Ok(Header::Unknown);
            }
            let value = reader.string()?;
            return Ok(std::str::from_utf8(value).map_or(Header::Unknown, |arch| {
                Header::Architecture(arch.to_string())
            }));
        }
        if reader.skip(kind, 0)?.is_none() {
            return Ok(Header::Unknown);
        }
    }
    Ok(Header::Unknown)
}

/// The architecture in the header of a GGUF whose first `len` bytes `read` returns, reading
/// more until the key is found or [`MAX_READ`] bytes were read.
async fn read_architecture<F, Fut, E>(read: F) -> Result<Option<String>, E>
where
    F: Fn(u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, E>>,
{
    let mut len = FIRST_READ;
    loop {
        let bytes = read(len).await?;
        match architecture(&bytes) {
            Header::Architecture(arch) => return Ok(Some(arch)),
            Header::Unknown => return Ok(None),
            // a file shorter than asked for has nothing more to give
            Header::Truncated
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) < len || len >= MAX_READ =>
            {
                return Ok(None);
            }
            Header::Truncated => len = (len * 4).min(MAX_READ),
        }
    }
}

/// The files whose headers decide a model's architecture, in the order to read them: the
/// `-00001-of-` shard a sharded model is opened from, else every file but an `mtp-` draft head.
fn candidates<P: AsRef<Path>>(weights: &[P]) -> Vec<&P> {
    let named = |w: &&P, part: &str| {
        w.as_ref()
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(part))
    };
    let first: Vec<&P> = weights.iter().filter(|w| named(w, "-00001-of-")).collect();
    if !first.is_empty() {
        return first;
    }
    weights
        .iter()
        .filter(|w| {
            !w.as_ref()
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("mtp-"))
        })
        .collect()
}

/// The architecture a model of GGUF `weights` names in its first shard's header, read by
/// `read(file, len)`; a drafter's header is passed over. `None` when no header names one.
pub(crate) async fn model_architecture<P, F, Fut, E>(
    weights: &[P],
    read: F,
) -> Result<Option<String>, E>
where
    P: AsRef<Path>,
    F: Fn(&P, u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, E>>,
{
    for file in candidates(weights) {
        match read_architecture(|len| read(file, len)).await? {
            Some(arch) if arch != DRAFTER => return Ok(Some(arch)),
            _ => {}
        }
    }
    Ok(None)
}

/// The architecture a model of local GGUF `weights` names in its first shard's header. `None`
/// when no file can be read or names one, as on a node that does not hold the weights.
pub(crate) async fn local_architecture(weights: &[PathBuf]) -> Option<String> {
    model_architecture(weights, |file: &PathBuf, len| {
        let file = file.clone();
        async move {
            let mut bytes = Vec::new();
            tokio::fs::File::open(&file)
                .await?
                .take(len)
                .read_to_end(&mut bytes)
                .await?;
            Ok::<_, std::io::Error>(bytes)
        }
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::{Header, architecture, local_architecture};

    fn string(bytes: &mut Vec<u8>, text: &str) {
        let len = u64::try_from(text.len()).expect("a short string");
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
    }

    /// A GGUF header with a string array, a `u32` and a string before `general.architecture`,
    /// as a GGUF whose writer put its vocabulary first would have.
    fn header(arch: &str) -> Vec<u8> {
        let mut bytes = b"GGUF".to_vec();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&4u64.to_le_bytes());
        string(&mut bytes, "tokenizer.ggml.tokens");
        bytes.extend_from_slice(&9u32.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&3u64.to_le_bytes());
        for token in ["a", "bc", "def"] {
            string(&mut bytes, token);
        }
        string(&mut bytes, "general.alignment");
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(&32u32.to_le_bytes());
        string(&mut bytes, "general.name");
        bytes.extend_from_slice(&8u32.to_le_bytes());
        string(&mut bytes, "a model");
        string(&mut bytes, "general.architecture");
        bytes.extend_from_slice(&8u32.to_le_bytes());
        string(&mut bytes, arch);
        bytes
    }

    #[test]
    fn the_architecture_is_read_past_the_keys_before_it() {
        assert_eq!(
            architecture(&header("glm-dsa")),
            Header::Architecture("glm-dsa".to_string())
        );
    }

    #[test]
    fn a_header_cut_short_anywhere_asks_for_more() {
        let full = header("qwen3");
        for len in 0..full.len() {
            assert_eq!(architecture(&full[..len]), Header::Truncated, "{len} bytes");
        }
    }

    #[test]
    fn a_file_that_is_not_a_gguf_names_no_architecture() {
        assert_eq!(architecture(b"PK\x03\x04 a zip file"), Header::Unknown);
        let mut v1 = header("qwen3");
        v1[4] = 1;
        assert_eq!(architecture(&v1), Header::Unknown);
    }

    #[tokio::test]
    async fn the_first_shard_decides_and_a_drafter_does_not() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let write = |name: &str, arch: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, header(arch)).expect("the header is written");
            path
        };
        let drafter = write("dspark-model-Q8_0.gguf", "dflash");
        let second = write("model-00002-of-00002.gguf", "deepseek41");
        let first = write("model-00001-of-00002.gguf", "glm-dsa");
        let mtp = write("mtp-model.gguf", "qwen3");
        assert_eq!(
            local_architecture(&[drafter.clone(), second, first, mtp.clone()]).await,
            Some("glm-dsa".to_string())
        );
        let single = write("model.gguf", "qwen3");
        // a drafter listed first is passed over, and so is an MTP head
        assert_eq!(
            local_architecture(&[mtp, drafter, single]).await,
            Some("qwen3".to_string())
        );
        // a node without the weights cannot read them
        assert_eq!(
            local_architecture(&[dir.path().join("absent-00001-of-00002.gguf")]).await,
            None
        );
    }
}
