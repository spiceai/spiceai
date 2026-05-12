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

//! TPC-C random data generators (spec §2.1.6 / §4.3.2).

use rand::Rng;

const CHARACTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
const LETTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const NUMBERS: &[u8] = b"1234567890";

/// TPC-C C-Last syllable table (spec §4.3.2.3).
const C_LAST_TOKENS: &[&str] = &[
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

/// Generate a random string of characters in `[min_len, max_len]` (spec §4.3.2.2).
pub fn rand_chars(rng: &mut impl Rng, min_len: usize, max_len: usize) -> String {
    let len = rng.gen_range(min_len..=max_len);
    (0..len)
        .map(|_| CHARACTERS[rng.gen_range(0..CHARACTERS.len())] as char)
        .collect()
}

/// Generate a random string of uppercase letters in `[min_len, max_len]`.
pub fn rand_letters(rng: &mut impl Rng, min_len: usize, max_len: usize) -> String {
    let len = rng.gen_range(min_len..=max_len);
    (0..len)
        .map(|_| LETTERS[rng.gen_range(0..LETTERS.len())] as char)
        .collect()
}

/// Generate a random string of digits in `[min_len, max_len]`.
pub fn rand_numbers(rng: &mut impl Rng, min_len: usize, max_len: usize) -> String {
    let len = rng.gen_range(min_len..=max_len);
    (0..len)
        .map(|_| NUMBERS[rng.gen_range(0..NUMBERS.len())] as char)
        .collect()
}

/// Generate a random state code (2 uppercase letters).
pub fn rand_state(rng: &mut impl Rng) -> String {
    rand_letters(rng, 2, 2)
}

/// Generate a random zip code: 4 random digits + "11111" (spec §4.3.2.7).
pub fn rand_zip(rng: &mut impl Rng) -> String {
    let mut s = rand_numbers(rng, 4, 4);
    s.push_str("11111");
    s
}

/// Generate a random tax rate in `[0.0000, 0.2000]` (spec §2.4.1).
pub fn rand_tax(rng: &mut impl Rng) -> f64 {
    f64::from(rng.gen_range(0..=2000)) / 10_000.0
}

/// Generate a random "original" string (spec §4.3.3.1).
///
/// Returns a random a-string `[26..50]`. For 10% of rows, "ORIGINAL" is placed
/// at a random position within the string.
pub fn rand_original_string(rng: &mut impl Rng) -> String {
    let mut s = rand_chars(rng, 26, 50);
    if rng.gen_range(0..10) == 0 {
        let bytes = unsafe { s.as_bytes_mut() };
        let pos = rng.gen_range(0..bytes.len().saturating_sub(8));
        bytes[pos..pos + 8].copy_from_slice(b"ORIGINAL");
    }
    s
}

/// Generate a C-Last name from syllables (spec §4.3.2.3).
pub fn c_last_syllables(n: usize) -> String {
    let mut s = String::with_capacity(15);
    s.push_str(C_LAST_TOKENS[n / 100]);
    s.push_str(C_LAST_TOKENS[(n / 10) % 10]);
    s.push_str(C_LAST_TOKENS[n % 10]);
    s
}

/// Generate a random C-Last name using NURand (spec §2.1.6).
pub fn rand_c_last(rng: &mut impl Rng, c_load: usize) -> String {
    let a = rng.gen_range(0..256);
    let x = rng.gen_range(0..1000);
    c_last_syllables(((a | x) + c_load) % 1000)
}

/// Generate a random customer ID using NURand(1023, 1, 3000) (spec §2.1.6).
pub fn rand_customer_id(rng: &mut impl Rng) -> i32 {
    let a = rng.gen_range(0..1024);
    let x = rng.gen_range(1..=3000);
    let c = rng.gen_range(0..1024);
    ((a | x) + c) % 3000 + 1
}
