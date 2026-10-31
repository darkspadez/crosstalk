//! Levenshtein similarity and the length-scaled threshold.
//!
//! Port of `matching_core.py::calculate_similarity` and `_length_scaled_threshold`.
//!
//! The ratio is `1 - distance / max(len)`, counted in Unicode scalar values, which is
//! what Python's `len()` measures. The definition matters: the older
//! `(len1 + len2 - distance) / (len1 + len2)` formula scores everything higher for the
//! same edit distance and lets numbered siblings (`Fox Sports 1` vs `2`) through a 95
//! threshold.
//!
//! `min_ratio` reproduces the Python gate exactly, including its two deliberate quirks:
//!   * the comparison is `>=`, so a score landing exactly on the cutoff is KEPT;
//!   * below the cutoff the result is `0.0`, not the raw ratio.

/// Levenshtein similarity in `0.0..=1.0`, defined as `1 - distance / max_len`.
///
/// Returns `0.0` when either input is empty. When `min_ratio > 0`, returns `0.0` as
/// soon as the pair provably cannot reach the cutoff, so callers get the same
/// below-threshold answer the pure-Python fallback gives.
pub fn calculate_similarity(a: &str, b: &str) -> f64 {
    calculate_similarity_cutoff(a, b, 0.0)
}

/// `calculate_similarity` with an early-exit cutoff, matching the Python signature
/// `calculate_similarity(str1, str2, min_ratio=0.0)`.
pub fn calculate_similarity_cutoff(a: &str, b: &str, min_ratio: f64) -> f64 {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    calculate_similarity_chars(&a, &b, min_ratio)
}

/// Same as [`calculate_similarity_cutoff`] for pre-collected scalar values.
pub fn calculate_similarity_chars(a: &[char], b: &[char], min_ratio: f64) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    // Python swaps so the longer string is `str1`; `max_len` is then `len1`.
    let (long, short) = if a.len() >= b.len() { (a, b) } else { (b, a) };
    let len1 = long.len();
    let len2 = short.len();
    let max_len = len1 as f64;

    if min_ratio > 0.0 {
        // The minimum possible distance is the length difference, so the best ratio
        // reachable is `(max_len - (len1 - len2)) / max_len`. Statement for statement
        // with the Python pre-check, including its shape: dropping the `max_len -`
        // term here silently zeroes every equal-length pair.
        let max_possible = (len1 - (len1 - len2)) as f64 / max_len;
        if max_possible < min_ratio {
            return 0.0;
        }
    }

    // Two-row Levenshtein. `previous_row[j]` mirrors the Python list exactly.
    let mut previous_row: Vec<usize> = (0..=len2).collect();
    let mut current_row: Vec<usize> = vec![0; len2 + 1];

    for (i, c1) in long.iter().enumerate() {
        current_row[0] = i + 1;
        for (j, c2) in short.iter().enumerate() {
            let insertions = previous_row[j + 1] + 1;
            let deletions = current_row[j] + 1;
            let substitutions = previous_row[j] + usize::from(c1 != c2);
            current_row[j + 1] = insertions.min(deletions).min(substitutions);
        }

        if min_ratio > 0.0 {
            // Lower bound on the final distance is the current row minimum minus the
            // characters of `long` still unprocessed.
            let min_distance_so_far = *current_row.iter().min().expect("row is non-empty");
            let remaining = len1 - i - 1;
            let best_possible_distance = min_distance_so_far.saturating_sub(remaining);
            let best_possible_ratio = (len1 - best_possible_distance) as f64 / max_len;
            if best_possible_ratio < min_ratio {
                return 0.0;
            }
        }

        std::mem::swap(&mut previous_row, &mut current_row);
    }

    let distance = previous_row[len2];
    let sim = (len1 - distance) as f64 / max_len;
    if min_ratio > 0.0 && sim < min_ratio {
        0.0
    } else {
        sim
    }
}

/// Raise the required similarity for short strings, where a single edit is a large
/// share of the name and passes for a different channel all too easily.
///
/// Python: `<= 4` chars -> at least 95, `<= 8` -> at least 90, else the base threshold.
pub fn length_scaled_threshold(base_threshold: u8, shorter_len: usize) -> u8 {
    if shorter_len <= 4 {
        base_threshold.max(95)
    } else if shorter_len <= 8 {
        base_threshold.max(90)
    } else {
        base_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_and_empty() {
        assert_eq!(calculate_similarity("espn", "espn"), 1.0);
        assert_eq!(calculate_similarity("", "espn"), 0.0);
        assert_eq!(calculate_similarity("espn", ""), 0.0);
        assert_eq!(calculate_similarity("", ""), 0.0);
    }

    #[test]
    fn numbered_siblings_fall_below_95() {
        let ratio = calculate_similarity("fox sports 1", "fox sports 2");
        assert!((ratio - 11.0 / 12.0).abs() < 1e-9, "got {ratio}");
        assert!(ratio < 0.95);
    }

    #[test]
    fn cutoff_is_inclusive_at_the_boundary() {
        // "abcde" vs "abcdf" is exactly 4/5.
        assert!((calculate_similarity_cutoff("abcde", "abcdf", 0.8) - 0.8).abs() < 1e-9);
        assert_eq!(calculate_similarity_cutoff("abcde", "abcdf", 0.81), 0.0);
    }

    #[test]
    fn counts_code_points_not_bytes() {
        // Two 3-scalar Cyrillic strings, one substitution: 2/3.
        let ratio = calculate_similarity("Россия", "России");
        assert!((ratio - 5.0 / 6.0).abs() < 1e-9, "got {ratio}");
        // Multi-byte but equal length: similarity stays symmetric.
        assert_eq!(
            calculate_similarity("日本語", "日本語"),
            calculate_similarity("日本語", "日本語")
        );
    }

    #[test]
    fn symmetric() {
        assert_eq!(
            calculate_similarity("hbo", "hbo2"),
            calculate_similarity("hbo2", "hbo")
        );
    }

    #[test]
    fn length_scaled_threshold_ladder() {
        assert_eq!(length_scaled_threshold(80, 4), 95);
        assert_eq!(length_scaled_threshold(80, 8), 90);
        assert_eq!(length_scaled_threshold(80, 20), 80);
        // An already-stricter base is never lowered.
        assert_eq!(length_scaled_threshold(97, 4), 97);
        assert_eq!(length_scaled_threshold(95, 6), 95);
    }
}
