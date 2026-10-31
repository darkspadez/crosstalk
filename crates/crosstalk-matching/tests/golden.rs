//! Golden-baseline parity with the Python implementation.
//!
//! `fixtures/matcher_golden_baseline.json` is copied unchanged from
//! `reference/Dispatcharr-EPG-Janitor-Plugin/tests/matcher_golden_baseline.json`, where
//! it locks the Python matcher's output. Any divergence here is a port bug, not a
//! design choice.

use crosstalk_matching::similarity::calculate_similarity;
use crosstalk_matching::tokens::{has_token_overlap, process_string_for_matching};
use serde_json::Value;

fn baseline() -> Value {
    let raw = include_str!("fixtures/matcher_golden_baseline.json");
    serde_json::from_str(raw).expect("golden baseline parses")
}

#[test]
fn calculate_similarity_matches_baseline() {
    let data = baseline();
    let cases = data["calculate_similarity"].as_object().expect("object");

    let mut checked = 0;
    for (pair, expected) in cases {
        let expected = expected.as_f64().expect("number");
        // The baseline keys are `a|b`; both sides may themselves be empty.
        let (a, b) = pair
            .split_once('|')
            .unwrap_or_else(|| panic!("malformed baseline key {pair:?}"));
        let got = calculate_similarity(a, b);
        assert!(
            (got - expected).abs() < 1e-9,
            "similarity({a:?}, {b:?}) = {got}, baseline says {expected}"
        );
        // Similarity is symmetric; the Python is too, so assert it rather than assume.
        let reversed = calculate_similarity(b, a);
        assert!(
            (reversed - expected).abs() < 1e-9,
            "similarity({b:?}, {a:?}) = {reversed}, baseline says {expected}"
        );
        checked += 1;
    }
    assert!(
        checked >= 14,
        "expected at least 14 baseline pairs, saw {checked}"
    );
}

#[test]
fn process_string_matches_baseline() {
    let data = baseline();
    let cases = data["process_string"].as_object().expect("object");

    let mut checked = 0;
    for (input, expected) in cases {
        let expected = expected.as_str().expect("string");
        let got = process_string_for_matching(input);
        assert_eq!(got, expected, "process_string({input:?})");
        checked += 1;
    }
    assert!(checked >= 50, "expected at least 50 process_string cases");
}

/// The token guards have no entries in the JSON baseline, so the cases verified by the
/// Python test suite (`test_fuzzy_matcher.py`, `test_matcher_parity_port.py`) are
/// transcribed here instead.
#[test]
fn token_guard_cases_from_python_suite() {
    // Basic mode: one shared four-character token is enough.
    assert!(has_token_overlap("america racing", "america bbc", 4, false));
    assert!(!has_token_overlap("the one show", "the big bang", 4, false));

    // Majority mode.
    assert!(has_token_overlap(
        "america racing",
        "america racing hd",
        4,
        true
    ));
    assert!(!has_token_overlap("america racing", "america bbc", 4, true));
    assert!(!has_token_overlap(
        "america racing sports",
        "america racing news",
        4,
        true
    ));

    // Numeric and ordinal siblings.
    assert!(!has_token_overlap("bbc one", "bbc two", 4, true));
    assert!(!has_token_overlap("espn 1", "espn 2", 4, true));

    // Divergent brands and strict subsets.
    assert!(!has_token_overlap(
        "sky cinema disney",
        "sky cinema decades",
        4,
        true
    ));
    assert!(!has_token_overlap(
        "in country television",
        "country music television",
        4,
        true
    ));

    // Legitimate extension through a non-distinctive token.
    assert!(has_token_overlap("abc news", "abc news live", 4, true));
    assert!(has_token_overlap("abc news", "abc live news", 4, true));
}
