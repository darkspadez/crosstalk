//! Target 1 of docs/audit/D-inputs.md: the webhook body and event readers.
//!
//! Property tests only: `hooks::read_fields` and `hooks::read_event` are exercised as the
//! `/hooks/dispatcharr` route calls them, over generated bodies. `PROPTEST_CASES` sets the
//! case count (256 by default; 10_000 for a local soak).

use crosstalk_core::hooks::{read_event, read_fields, EventSource};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|cases| cases.parse().ok())
            .unwrap_or(256),
        ..ProptestConfig::default()
    }
}

/// The longest name a delivery may carry (`hooks::MAX_EVENT_NAME_LEN`).
const MAX_EVENT_NAME_LEN: usize = 128;

/// Form-encode a map the way `requests.post(data=dict)` does, with every byte outside the
/// unreserved set escaped.
fn form_encode(fields: &BTreeMap<String, String>) -> String {
    let encode = |text: &str| {
        let mut out = String::new();
        for byte in text.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                out.push(byte as char);
            } else if byte == b' ' {
                out.push('+');
            } else {
                out.push_str(&format!("%{byte:02X}"));
            }
        }
        out
    };
    fields
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// A field map with non-empty keys (an empty key is not something either encoding names).
fn field_map() -> impl Strategy<Value = BTreeMap<String, String>> {
    proptest::collection::btree_map(".{1,12}", ".{0,24}", 0..8)
}

/// Bodies biased toward what breaks a hand-written parser: escapes, NUL, CRLF, braces.
fn hostile_body() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        prop_oneof![
            Just(b'%'),
            Just(b'&'),
            Just(b'='),
            Just(b'+'),
            Just(b'{'),
            Just(b'}'),
            Just(b'"'),
            Just(0u8),
            Just(b'\r'),
            Just(b'\n'),
            Just(0xFFu8),
            Just(0xC3u8),
            any::<u8>(),
        ],
        0..4096,
    )
}

fn valid(name: &str) -> Option<String> {
    let name = name.trim();
    (!name.is_empty() && name.len() <= MAX_EVENT_NAME_LEN).then(|| name.to_string())
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn any_body_reads_without_panicking(body in hostile_body()) {
        let _ = read_fields(&body);
    }

    #[test]
    fn a_form_body_round_trips(fields in field_map()) {
        let body = form_encode(&fields);
        prop_assert_eq!(read_fields(body.as_bytes()), fields);
    }

    #[test]
    fn a_json_object_of_strings_reads_like_the_same_form(fields in field_map()) {
        let json = serde_json::to_string(&fields).expect("serializes");
        prop_assert_eq!(read_fields(json.as_bytes()), read_fields(form_encode(&fields).as_bytes()));
        prop_assert_eq!(read_fields(json.as_bytes()), fields);
    }

    #[test]
    fn a_malformed_escape_is_kept_literally(value in "[g-zG-Z%]{0,40}") {
        // No hex digit anywhere, so no `%` can start a valid escape.
        let fields = read_fields(format!("k={value}").as_bytes());
        prop_assert_eq!(fields.get("k").map(String::as_str), Some(value.as_str()));
    }

    #[test]
    fn a_body_that_is_not_utf8_reads_as_nothing(prefix in "[a-z=&]{0,20}", suffix in "[a-z=&]{0,20}") {
        let mut body = prefix.into_bytes();
        body.extend_from_slice(&[0xFF, 0xFE]);
        body.extend_from_slice(suffix.as_bytes());
        prop_assert!(read_fields(&body).is_empty());
    }

    #[test]
    fn an_event_name_is_trimmed_bounded_and_found_in_precedence_order(
        query in proptest::option::of(prop_oneof![
            Just(String::new()),
            Just("  ".to_string()),
            Just(" m3u_refresh ".to_string()),
            "\\PC{0,140}",
            "[a-z]{129,140}",
        ]),
        // No control or format characters: whether those are names is N2's call, and
        // the test below pins it once that lands.
        event in proptest::option::of("\\PC{0,20}"),
        kind in proptest::option::of("\\PC{0,20}"),
        markers in proptest::collection::vec(proptest::sample::select(&[
            "total_processed", "streams_created", "elapsed_time", "programs", "channels",
            "account_name", "source_name", "message",
        ][..]), 0..4),
        configured in proptest::collection::vec(proptest::sample::select(&[
            "m3u_refresh", "epg_refresh", "custom",
        ][..]), 0..4),
    ) {
        let configured: BTreeSet<String> = configured.iter().map(|name| name.to_string()).collect();
        let mut fields: BTreeMap<String, String> =
            markers.iter().map(|key| (key.to_string(), "1".to_string())).collect();
        if let Some(event) = &event {
            fields.insert("event".to_string(), event.clone());
        }
        if let Some(kind) = &kind {
            fields.insert("type".to_string(), kind.clone());
        }
        let resolved = read_event(query.as_deref(), &fields, &configured);
        if let Some(resolved) = &resolved {
            prop_assert_eq!(resolved.name.trim(), resolved.name.as_str());
            prop_assert!(!resolved.name.is_empty());
            prop_assert!(resolved.name.len() <= MAX_EVENT_NAME_LEN);
        }

        // Precedence: query > event > type > shape.
        let expected = query
            .as_deref()
            .and_then(valid)
            .map(|name| (name, EventSource::Query))
            .or_else(|| event.as_deref().and_then(valid).map(|name| (name, EventSource::Field)))
            .or_else(|| kind.as_deref().and_then(valid).map(|name| (name, EventSource::Type)));
        match expected {
            Some((name, source)) => {
                let resolved = resolved.expect("an explicit name resolves");
                prop_assert_eq!(resolved.name, name);
                prop_assert_eq!(resolved.source, source);
            }
            None => {
                if let Some(resolved) = resolved {
                    // The shape only ever answers with a configured refresh event.
                    prop_assert_eq!(resolved.source, EventSource::PayloadShape);
                    prop_assert!(configured.contains(&resolved.name));
                    prop_assert!(["m3u_refresh", "epg_refresh"].contains(&resolved.name.as_str()));
                }
            }
        }
    }

    #[test]
    fn an_error_shaped_body_never_resolves_to_a_refresh(
        keys in proptest::collection::vec(proptest::sample::select(&[
            "account_name", "source_name", "message",
        ][..]), 0..4),
        value in ".{0,20}",
    ) {
        let fields: BTreeMap<String, String> =
            keys.iter().map(|key| (key.to_string(), value.clone())).collect();
        let configured: BTreeSet<String> =
            ["m3u_refresh", "epg_refresh"].iter().map(|name| name.to_string()).collect();
        prop_assert_eq!(read_event(None, &fields, &configured), None);
    }
}

proptest! {
    #![proptest_config(config())]

    /// Needs fix 2 (N2: `valid_name` refuses control characters); un-ignore once it lands.
    #[test]
    #[ignore = "needs N2 (fix 2): valid_name must refuse control characters"]
    fn an_event_name_carries_no_control_character(query in ".{0,40}", event in ".{0,40}") {
        let fields: BTreeMap<String, String> =
            [("event".to_string(), event)].into_iter().collect();
        for resolved in [
            read_event(Some(&query), &fields, &BTreeSet::new()),
            read_event(None, &fields, &BTreeSet::new()),
        ]
        .into_iter()
        .flatten()
        {
            prop_assert!(!resolved.name.chars().any(char::is_control), "{:?}", resolved.name);
        }
    }
}

#[test]
fn a_two_mebibyte_body_reads_in_linear_time() {
    // Every shape the parser branches on, repeated: escapes valid and not, `+`, empty pairs.
    let unit = "account_name=Caf%C3%A9+One&bad=%zz%4&&x=%41%42%43&";
    let body = unit.repeat((2 * 1024 * 1024) / unit.len());
    let started = Instant::now();
    let fields = read_fields(body.as_bytes());
    let elapsed = started.elapsed();
    assert_eq!(
        fields.get("account_name").map(String::as_str),
        Some("Café One")
    );
    // ~50 ms in release; an unoptimised test build gets a looser bound. Quadratic
    // behaviour on 2 MiB would take minutes either way.
    let budget = if cfg!(debug_assertions) {
        Duration::from_secs(2)
    } else {
        Duration::from_millis(250)
    };
    assert!(elapsed < budget, "{elapsed:?}");

    let json = format!("{{\"k\": \"{}\"}}", "a".repeat(2 * 1024 * 1024));
    let started = Instant::now();
    assert_eq!(read_fields(json.as_bytes()).len(), 1);
    assert!(started.elapsed() < budget, "{:?}", started.elapsed());
}
