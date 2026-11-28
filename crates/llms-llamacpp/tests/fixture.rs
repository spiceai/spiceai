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

//! Copies of a downloaded GGUF changed in one place: a metadata key renamed in place, keeping
//! its length, tokens of its vocabulary respelled, or one tensor added after the others. Either
//! way llama.cpp reads every other byte of the copy as it reads the original.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// How much of a GGUF's start to search: enough for the metadata, the tokenizer's vocabulary
/// among it, and the tensor descriptions of the models these tests load.
const HEADER: u64 = 64 << 20;

/// GGUF's type ids for a metadata string and array.
const STRING: u32 = 8;
const ARRAY: u32 = 9;
/// GGUF's tensor type id for 16-bit floats.
const F16: u32 = 1;

/// Copies `source` into `dir`, under the same file name.
pub(crate) fn copy(source: &Path, dir: &Path) -> PathBuf {
    let name = source.file_name().expect("the GGUF has a file name");
    let copy = dir.join(name);
    std::fs::copy(source, &copy).expect("the GGUF copies");
    copy
}

/// The string value of metadata key `key`.
pub(crate) fn string_value(gguf: &Path, key: &str) -> String {
    let header = header(gguf);
    string_value_at(&header, string_at(&header, key), key)
}

/// Renames metadata key `key` to `renamed`, a key of the same length that llama.cpp does not
/// read, and returns the key's string value.
pub(crate) fn rename_string_key(gguf: &Path, key: &str, renamed: &str) -> String {
    assert_eq!(key.len(), renamed.len(), "a rename keeps the key's length");
    let header = header(gguf);
    let at = string_at(&header, key);
    let value = string_value_at(&header, at, key);
    write_at(gguf, at + 8, renamed.as_bytes());
    value
}

/// The string value of the metadata key `key` that starts at `at`.
fn string_value_at(header: &[u8], at: usize, key: &str) -> String {
    let after = at + 8 + key.len();
    assert_eq!(read_u32(header, after), STRING, "'{key}' holds a string");
    let len = usize::try_from(read_u64(header, after + 4)).expect("a string's length fits");
    String::from_utf8(header[after + 12..after + 12 + len].to_vec()).expect("the value is UTF-8")
}

/// Copies `source` into `dir`, under the same file name, with one more tensor: `name`, of
/// 16-bit zeros in the shape `ne`. The new tensor is described after the others and stored
/// after their data, which is copied unchanged, so every other tensor keeps its offset.
pub(crate) fn copy_with_tensor(source: &Path, dir: &Path, name: &str, ne: [u64; 2]) -> PathBuf {
    let header = header(source);
    let layout = layout(&header);
    let copy = dir.join(source.file_name().expect("the GGUF has a file name"));
    let mut out = std::io::BufWriter::new(std::fs::File::create(&copy).expect("the copy opens"));

    let n_tensors = read_u64(&header, 8) + 1;
    out.write_all(&header[..8]).expect("the copy writes");
    out.write_all(&n_tensors.to_le_bytes())
        .expect("the copy writes");
    out.write_all(&header[16..layout.infos_end])
        .expect("the copy writes");

    let mut source = std::fs::File::open(source).expect("the GGUF opens");
    let data_len = source.metadata().expect("the GGUF's size").len() - layout.data_start;
    let offset = data_len.next_multiple_of(layout.alignment);
    out.write_all(&(name.len() as u64).to_le_bytes())
        .expect("the copy writes");
    out.write_all(name.as_bytes()).expect("the copy writes");
    out.write_all(&2u32.to_le_bytes()).expect("the copy writes");
    for dimension in ne {
        out.write_all(&dimension.to_le_bytes())
            .expect("the copy writes");
    }
    out.write_all(&F16.to_le_bytes()).expect("the copy writes");
    out.write_all(&offset.to_le_bytes())
        .expect("the copy writes");

    let infos_end = layout.infos_end as u64 + 8 + name.len() as u64 + 4 + 16 + 4 + 8;
    let padding = infos_end.next_multiple_of(layout.alignment) - infos_end;
    zeros(&mut out, padding);
    source
        .seek(SeekFrom::Start(layout.data_start))
        .expect("the GGUF seeks");
    std::io::copy(&mut source, &mut out).expect("the data copies");
    zeros(&mut out, offset - data_len);
    zeros(&mut out, ne[0] * ne[1] * 2);
    out.flush().expect("the copy writes");
    copy
}

/// Copies `source` into `dir`, under the same file name, with tokens of its vocabulary
/// respelled: each `(from, to)` names a token in `tokenizer.ggml.tokens` by its text and gives
/// the text it takes. The tensors and every other byte of the metadata are copied unchanged;
/// the data moves with the vocabulary's new length, and each tensor keeps its offset in it.
pub(crate) fn copy_with_tokens(source: &Path, dir: &Path, respell: &[(&str, &str)]) -> PathBuf {
    let header = header(source);
    let layout = layout(&header);
    let n_kv = read_u64(&header, 16);
    let (mut at, mut tokens) = (24, None);
    for _ in 0..n_kv {
        let (key, after) = string(&header, at);
        let kind = read_u32(&header, after);
        let next = skip_value(&header, kind, after + 4);
        if key == "tokenizer.ggml.tokens" {
            tokens = Some((after + 4, next));
        }
        at = next;
    }
    let (value, value_end) = tokens.expect("the GGUF has a vocabulary");
    assert_eq!(read_u32(&header, value), STRING, "its tokens are strings");
    // The array's element type and count, then each token, its length then its bytes, which
    // are copied as they are unless the token is respelled.
    let mut array = header[value..value + 12].to_vec();
    let mut respelled = 0;
    let mut item = value + 12;
    for _ in 0..read_u64(&header, value + 4) {
        let len = usize::try_from(read_u64(&header, item)).expect("a token's length fits");
        let bytes = &header[item + 8..item + 8 + len];
        let bytes = match respell.iter().find(|(from, _)| from.as_bytes() == bytes) {
            Some((_, to)) => {
                respelled += 1;
                to.as_bytes()
            }
            None => bytes,
        };
        array.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        array.extend_from_slice(bytes);
        item += 8 + len;
    }
    assert_eq!(
        respelled,
        respell.len(),
        "each token to respell is in the vocabulary once"
    );

    let copy = dir.join(source.file_name().expect("the GGUF has a file name"));
    let mut out = std::io::BufWriter::new(std::fs::File::create(&copy).expect("the copy opens"));
    out.write_all(&header[..value]).expect("the copy writes");
    out.write_all(&array).expect("the copy writes");
    out.write_all(&header[value_end..layout.infos_end])
        .expect("the copy writes");
    let infos_end = (layout.infos_end - (value_end - value) + array.len()) as u64;
    zeros(
        &mut out,
        infos_end.next_multiple_of(layout.alignment) - infos_end,
    );
    let mut source = std::fs::File::open(source).expect("the GGUF opens");
    source
        .seek(SeekFrom::Start(layout.data_start))
        .expect("the GGUF seeks");
    std::io::copy(&mut source, &mut out).expect("the data copies");
    out.flush().expect("the copy writes");
    copy
}

/// Where a GGUF's tensor descriptions end and its data starts, and the data's alignment.
struct Layout {
    infos_end: usize,
    data_start: u64,
    alignment: u64,
}

/// Reads a GGUF's metadata and tensor descriptions, whose sizes place its data.
fn layout(header: &[u8]) -> Layout {
    let n_tensors = read_u64(header, 8);
    let n_kv = read_u64(header, 16);
    let mut at = 24;
    let mut alignment = 32;
    for _ in 0..n_kv {
        let (key, after) = string(header, at);
        let kind = read_u32(header, after);
        let value = after + 4;
        if key == "general.alignment" {
            alignment = u64::from(read_u32(header, value));
        }
        at = skip_value(header, kind, value);
    }
    for _ in 0..n_tensors {
        let (_, after) = string(header, at);
        let n_dims = read_u32(header, after) as usize;
        at = after + 4 + 8 * n_dims + 4 + 8;
    }
    Layout {
        infos_end: at,
        data_start: (at as u64).next_multiple_of(alignment),
        alignment,
    }
}

/// The offset just past a metadata value of GGUF type `kind` that starts at `at`.
fn skip_value(header: &[u8], kind: u32, at: usize) -> usize {
    match kind {
        0 | 1 | 7 => at + 1,
        2 | 3 => at + 2,
        4..=6 => at + 4,
        10..=12 => at + 8,
        STRING => string(header, at).1,
        ARRAY => {
            let element = read_u32(header, at);
            let count = read_u64(header, at + 4);
            (0..count).fold(at + 12, |at, _| skip_value(header, element, at))
        }
        _ => panic!("unknown GGUF metadata type {kind}"),
    }
}

/// The GGUF string at `at`, and the offset just past it.
fn string(header: &[u8], at: usize) -> (&str, usize) {
    let len = usize::try_from(read_u64(header, at)).expect("a string's length fits");
    let text = std::str::from_utf8(&header[at + 8..at + 8 + len]).unwrap_or_default();
    (text, at + 8 + len)
}

fn zeros(out: &mut impl Write, count: u64) {
    std::io::copy(&mut std::io::repeat(0).take(count), out).expect("the copy writes");
}

fn header(gguf: &Path) -> Vec<u8> {
    let mut header = Vec::new();
    std::fs::File::open(gguf)
        .expect("the GGUF opens")
        .take(HEADER)
        .read_to_end(&mut header)
        .expect("the GGUF reads");
    assert_eq!(&header[..4], b"GGUF", "{} is a GGUF", gguf.display());
    header
}

/// The offset of the one GGUF string, its length then its bytes, that spells `text`.
fn string_at(header: &[u8], text: &str) -> usize {
    let mut needle = (text.len() as u64).to_le_bytes().to_vec();
    needle.extend_from_slice(text.as_bytes());
    let found: Vec<usize> = header
        .windows(needle.len())
        .enumerate()
        .filter(|(_, window)| *window == needle.as_slice())
        .map(|(at, _)| at)
        .collect();
    assert_eq!(found.len(), 1, "'{text}' appears once in the header");
    found[0]
}

fn read_u32(header: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(header[at..at + 4].try_into().expect("four bytes"))
}

fn read_u64(header: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(header[at..at + 8].try_into().expect("eight bytes"))
}

fn write_at(gguf: &Path, at: usize, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(gguf)
        .expect("the copy opens for writing");
    file.seek(SeekFrom::Start(at as u64))
        .expect("the copy seeks");
    file.write_all(bytes).expect("the copy writes");
}
