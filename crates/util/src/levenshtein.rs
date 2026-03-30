/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Levenshtein distance algorithm implementation.

/// Computes the Levenshtein distance between two strings.
///
/// The Levenshtein distance is the minimum number of single-character edits
/// (insertions, deletions, or substitutions) required to change one string into the other.
///
/// This implementation uses O(min(m,n)) space complexity by only keeping two rows
/// of the dynamic programming matrix at a time.
///
/// # Examples
///
/// ```
/// use util::levenshtein::distance;
///
/// assert_eq!(distance("kitten", "sitting"), 3);
/// assert_eq!(distance("hello", "hello"), 0);
/// assert_eq!(distance("", "abc"), 3);
/// ```
#[must_use]
pub fn distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let a_len = a_chars.len();
    let b_len = b_chars.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    // Use two rows instead of a full matrix for O(min(m,n)) space complexity
    let mut prev_row: Vec<usize> = (0..=b_len).collect();
    let mut curr_row: Vec<usize> = vec![0; b_len + 1];

    for i in 1..=a_len {
        curr_row[0] = i;
        for j in 1..=b_len {
            let cost = usize::from(a_chars[i - 1] != b_chars[j - 1]);
            curr_row[j] = (prev_row[j] + 1)
                .min(curr_row[j - 1] + 1)
                .min(prev_row[j - 1] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[b_len]
}

/// Computes the normalized Levenshtein similarity between two strings.
///
/// Returns a value between 0.0 and 1.0, where 1.0 means the strings are identical
/// and 0.0 means they are completely different.
///
/// # Examples
///
/// ```
/// use util::levenshtein::similarity;
///
/// assert!((similarity("hello", "hello") - 1.0).abs() < f64::EPSILON);
/// assert!((similarity("", "") - 1.0).abs() < f64::EPSILON);
/// assert!((similarity("abc", "xyz") - 0.0).abs() < f64::EPSILON);
/// ```
#[expect(clippy::cast_precision_loss)]
#[must_use]
pub fn similarity(a: &str, b: &str) -> f64 {
    let max_len = a.chars().count().max(b.chars().count());
    if max_len == 0 {
        return 1.0;
    }
    let dist = distance(a, b);
    1.0 - (dist as f64 / max_len as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== distance() tests ====================

    #[test]
    fn test_distance_identical_strings() {
        assert_eq!(distance("hello", "hello"), 0);
        assert_eq!(distance("", ""), 0);
        assert_eq!(distance("a", "a"), 0);
        assert_eq!(distance("abc", "abc"), 0);
        assert_eq!(distance("The quick brown fox", "The quick brown fox"), 0);
    }

    #[test]
    fn test_distance_empty_strings() {
        assert_eq!(distance("", ""), 0);
        assert_eq!(distance("", "a"), 1);
        assert_eq!(distance("", "ab"), 2);
        assert_eq!(distance("", "abc"), 3);
        assert_eq!(distance("", "hello"), 5);
        assert_eq!(distance("a", ""), 1);
        assert_eq!(distance("ab", ""), 2);
        assert_eq!(distance("abc", ""), 3);
        assert_eq!(distance("hello", ""), 5);
    }

    #[test]
    fn test_distance_single_character_edits() {
        // Single insertion
        assert_eq!(distance("abc", "abcd"), 1);
        assert_eq!(distance("abc", "xabc"), 1);
        assert_eq!(distance("abc", "axbc"), 1);

        // Single deletion
        assert_eq!(distance("abcd", "abc"), 1);
        assert_eq!(distance("xabc", "abc"), 1);
        assert_eq!(distance("axbc", "abc"), 1);

        // Single substitution
        assert_eq!(distance("abc", "xbc"), 1);
        assert_eq!(distance("abc", "axc"), 1);
        assert_eq!(distance("abc", "abx"), 1);
    }

    #[test]
    fn test_distance_classic_examples() {
        // Classic textbook examples
        assert_eq!(distance("kitten", "sitting"), 3);
        assert_eq!(distance("saturday", "sunday"), 3);
        assert_eq!(distance("flaw", "lawn"), 2);
        assert_eq!(distance("gumbo", "gambol"), 2);
    }

    #[test]
    fn test_distance_completely_different() {
        assert_eq!(distance("abc", "xyz"), 3);
        assert_eq!(distance("aaa", "bbb"), 3);
        assert_eq!(distance("hello", "world"), 4);
    }

    #[test]
    fn test_distance_symmetry() {
        // Levenshtein distance should be symmetric
        assert_eq!(distance("abc", "xyz"), distance("xyz", "abc"));
        assert_eq!(distance("kitten", "sitting"), distance("sitting", "kitten"));
        assert_eq!(distance("hello", ""), distance("", "hello"));
        assert_eq!(distance("a", "ab"), distance("ab", "a"));
    }

    #[test]
    fn test_distance_triangle_inequality() {
        // d(a, c) <= d(a, b) + d(b, c)
        let first = "abc";
        let second = "abd";
        let third = "xyz";

        let first_to_third = distance(first, third);
        let first_to_second = distance(first, second);
        let second_to_third = distance(second, third);

        assert!(
            first_to_third <= first_to_second + second_to_third,
            "Triangle inequality violated: d({first}, {third}) = {first_to_third} > d({first}, {second}) + d({second}, {third}) = {} + {} = {}",
            first_to_second,
            second_to_third,
            first_to_second + second_to_third
        );
    }

    #[test]
    fn test_distance_unicode_characters() {
        // Unicode should be handled correctly (character-based, not byte-based)
        assert_eq!(distance("日本語", "日本語"), 0);
        assert_eq!(distance("日本語", "日本"), 1);
        assert_eq!(distance("日本", "日本語"), 1);
        assert_eq!(distance("", "日本語"), 3);
        assert_eq!(distance("日本語", ""), 3);
    }

    #[test]
    fn test_distance_emoji() {
        assert_eq!(distance("🎉", "🎉"), 0);
        assert_eq!(distance("🎉", "🎊"), 1);
        assert_eq!(distance("hello 🎉", "hello 🎊"), 1);
        assert_eq!(distance("🎉🎊🎋", "🎉🎊🎋"), 0);
        assert_eq!(distance("🎉🎊🎋", "🎉🎊"), 1);
    }

    #[test]
    fn test_distance_mixed_unicode_and_ascii() {
        assert_eq!(distance("hello世界", "hello世界"), 0);
        assert_eq!(distance("hello世界", "hello"), 2);
        assert_eq!(distance("hello", "hello世界"), 2);
        assert_eq!(distance("a日b本c語", "a日b本c語"), 0);
    }

    #[test]
    fn test_distance_case_sensitivity() {
        // Distance is case-sensitive
        assert_eq!(distance("hello", "Hello"), 1);
        assert_eq!(distance("HELLO", "hello"), 5);
        assert_eq!(distance("HeLLo", "hello"), 3);
    }

    #[test]
    fn test_distance_whitespace() {
        assert_eq!(distance("hello world", "helloworld"), 1);
        assert_eq!(distance("hello  world", "hello world"), 1);
        assert_eq!(distance("hello", "hello "), 1);
        assert_eq!(distance(" hello", "hello"), 1);
        assert_eq!(distance("hello\tworld", "hello world"), 1);
        assert_eq!(distance("hello\nworld", "hello world"), 1);
    }

    #[test]
    fn test_distance_repeated_characters() {
        assert_eq!(distance("aaa", "aaaa"), 1);
        assert_eq!(distance("aaaa", "aaa"), 1);
        assert_eq!(distance("aaa", "aaaaaa"), 3);
        assert_eq!(distance("aaaa", "bbbb"), 4);
    }

    #[test]
    fn test_distance_prefix_suffix() {
        // Common prefix
        assert_eq!(distance("prefix_abc", "prefix_xyz"), 3);
        // Common suffix
        assert_eq!(distance("abc_suffix", "xyz_suffix"), 3);
        // Both
        assert_eq!(distance("pre_abc_suf", "pre_xyz_suf"), 3);
    }

    #[test]
    fn test_distance_long_strings() {
        let long_a = "a".repeat(100);
        let long_b = "b".repeat(100);
        assert_eq!(distance(&long_a, &long_a), 0);
        assert_eq!(distance(&long_a, &long_b), 100);

        let almost_same = format!("{}x{}", "a".repeat(50), "a".repeat(49));
        let same = "a".repeat(100);
        assert_eq!(distance(&almost_same, &same), 1);
    }

    #[test]
    fn test_distance_reversal() {
        assert_eq!(distance("abc", "cba"), 2);
        assert_eq!(distance("abcd", "dcba"), 4);
        assert_eq!(distance("hello", "olleh"), 4);
    }

    // ==================== similarity() tests ====================

    #[test]
    fn test_similarity_identical_strings() {
        assert!((similarity("hello", "hello") - 1.0).abs() < f64::EPSILON);
        assert!((similarity("", "") - 1.0).abs() < f64::EPSILON);
        assert!((similarity("a", "a") - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_similarity_completely_different() {
        assert!((similarity("abc", "xyz") - 0.0).abs() < f64::EPSILON);
        assert!((similarity("aaa", "bbb") - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_similarity_empty_vs_non_empty() {
        assert!((similarity("", "hello") - 0.0).abs() < f64::EPSILON);
        assert!((similarity("hello", "") - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_similarity_partial_match() {
        // kitten vs sitting: distance 3, max_len 7
        // similarity = 1 - 3/7 ≈ 0.571
        let sim = similarity("kitten", "sitting");
        let expected = 1.0 - (3.0 / 7.0);
        assert!(
            (sim - expected).abs() < 1e-10,
            "Expected {expected}, got {sim}"
        );
    }

    #[test]
    fn test_similarity_symmetry() {
        assert!((similarity("abc", "xyz") - similarity("xyz", "abc")).abs() < f64::EPSILON);
        assert!(
            (similarity("kitten", "sitting") - similarity("sitting", "kitten")).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn test_similarity_range() {
        // Similarity should always be between 0 and 1
        let test_cases = [
            ("", ""),
            ("", "hello"),
            ("hello", ""),
            ("hello", "hello"),
            ("abc", "xyz"),
            ("kitten", "sitting"),
            ("日本語", "中国語"),
        ];

        for (a, b) in test_cases {
            let sim = similarity(a, b);
            assert!(
                (0.0..=1.0).contains(&sim),
                "Similarity of ({a}, {b}) = {sim} is out of range [0, 1]"
            );
        }
    }

    #[test]
    fn test_similarity_unicode() {
        assert!((similarity("日本語", "日本語") - 1.0).abs() < f64::EPSILON);
        // 日本語 vs 日本: distance 1, max_len 3, similarity = 1 - 1/3 ≈ 0.667
        let sim = similarity("日本語", "日本");
        let expected = 1.0 - (1.0 / 3.0);
        assert!(
            (sim - expected).abs() < 1e-10,
            "Expected {expected}, got {sim}"
        );
    }

    #[test]
    fn test_similarity_with_single_edit() {
        // Single edit in a 5-character string: similarity = 1 - 1/5 = 0.8
        let sim = similarity("hello", "hallo");
        let expected = 0.8;
        assert!(
            (sim - expected).abs() < f64::EPSILON,
            "Expected {expected}, got {sim}"
        );
    }
}
