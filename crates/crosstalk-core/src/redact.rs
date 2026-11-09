//! Scrubbing URLs out of text that is stored or shown.
//!
//! A provider stream URL is a credential: Xtream-style providers put the account's user name
//! and password in the path (`/live/<user>/<pass>/<id>.ts`), others in the query
//! (`?token=...`) or the userinfo (`user:pass@`). Anything crosstalk keeps (run rows, CSV
//! reports, `stream_health`, run summaries, the AI audit trail) or logs may therefore carry a
//! URL only as `scheme://host[:port]`: enough to say which provider answered, nothing an
//! operator could leak by pasting a report into an issue.
//!
//! The exact-match masks that know the probed URL (`[Stream ID: n]`) run first; this pass
//! catches every URL they could not know about: an HLS segment, a redirect target, a variant
//! playlist, a URL echoed back in a Dispatcharr error body.

/// Where a URL run ends: whitespace, a quote, or the angle brackets and backtick that wrap
/// URLs in logs and markup.
fn ends_url(character: char) -> bool {
    character.is_whitespace() || matches!(character, '"' | '\'' | '<' | '>' | '`')
}

/// Punctuation that closes the sentence a URL sits in rather than belonging to the URL
/// (`http://h/x: Server returned 404`). It is kept in the text after the shortened URL.
fn trailing_punctuation(character: char) -> bool {
    matches!(
        character,
        ':' | ',' | ';' | '.' | ')' | ']' | '}' | '!' | '\\'
    )
}

/// Whether `authority` is a plain `host[:port]` (or `[v6]:port`): anything else is not
/// trusted to be free of a credential and is replaced whole.
fn plain_host_port(authority: &str) -> bool {
    if authority.is_empty() {
        return false;
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let Some((inside, after)) = rest.split_once(']') else {
            return false;
        };
        if !inside
            .chars()
            .all(|character| character.is_ascii_hexdigit() || matches!(character, ':' | '.' | '%'))
        {
            return false;
        }
        match after {
            "" => ("v6", None),
            _ => match after.strip_prefix(':') {
                Some(port) => ("v6", Some(port)),
                None => return false,
            },
        }
    } else {
        match authority.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        }
    };
    let host_ok = !host.is_empty()
        && host.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '.' | '_')
        });
    let port_ok = port.is_none_or(|port| {
        !port.is_empty() && port.len() <= 5 && port.chars().all(|digit| digit.is_ascii_digit())
    });
    host_ok && port_ok
}

/// The `host[:port]` of the URL body after `://`, or `None` when it cannot be told apart from
/// a credential.
///
/// The userinfo ends at the LAST `@` before the query: a password may itself contain `/` or
/// `?`, and cutting at the first `/` would keep half of it as a "host".
fn host_of(body: &str) -> Option<&str> {
    let before_query = body.split(['?', '#']).next().unwrap_or_default();
    let after_userinfo = match before_query.rfind('@') {
        Some(at) => &body[at + 1..],
        None => body,
    };
    let authority = after_userinfo
        .split(['/', '?', '#', '\\'])
        .next()
        .unwrap_or_default();
    plain_host_port(authority).then_some(authority)
}

/// The scheme in front of a `://` that ends at byte `end` of `text` (RFC 3986:
/// `ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`), as a byte offset where it starts.
fn scheme_start(text: &str, end: usize) -> usize {
    let mut start = end;
    for (index, character) in text[..end].char_indices().rev() {
        if character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.') {
            start = index;
        } else {
            break;
        }
    }
    // A scheme starts with a letter: `1http://` keeps the digit as text.
    while start < end && !text.as_bytes()[start].is_ascii_alphabetic() {
        start += 1;
    }
    start
}

/// Every `scheme://...` in `text` shortened to `scheme://host[:port]`.
///
/// The userinfo, path, query and fragment are dropped; a URL whose authority does not read
/// as a plain host and port is replaced with `scheme://[redacted]`. Text without `://` is
/// returned unchanged. The URL-encoded spelling (`%3A%2F%2F`, as a tool quoting a whole
/// request line prints it) is replaced whole, since its host cannot be told apart from its
/// credentials without decoding.
pub fn urls_to_hosts(text: &str) -> String {
    rewrite_urls(text, |body| match host_of(body) {
        Some(host) => host.to_string(),
        None if body.is_empty() => String::new(),
        None => "[redacted]".to_string(),
    })
}

/// Every `scheme://...` in `text` with its userinfo, query and fragment dropped and its path
/// kept: the text-wide form of [`url_without_secrets`], for a reason or report cell that
/// quotes a logo URL whose path is the useful part.
pub fn urls_without_secrets(text: &str) -> String {
    rewrite_urls(text, |body| path_without_secrets(body).to_string())
}

/// The body after `://` with the userinfo, query and fragment cut off.
fn path_without_secrets(body: &str) -> &str {
    let before_query = body.split(['?', '#']).next().unwrap_or_default();
    match before_query.rfind('@') {
        Some(at) => &before_query[at + 1..],
        None => before_query,
    }
}

/// Every URL run in `text` with its body (what follows `://`, minus trailing punctuation)
/// replaced by `rewrite(body)`.
fn rewrite_urls(text: &str, rewrite: impl Fn(&str) -> String) -> String {
    let text = encoded_urls_redacted(text);
    if !text.contains("://") {
        return text;
    }
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(found) = text[cursor..].find("://") {
        let separator = cursor + found;
        let start = scheme_start(&text, separator).max(cursor);
        out.push_str(&text[cursor..start]);
        let scheme = &text[start..separator];
        let body_start = separator + 3;
        let body_end = text[body_start..]
            .char_indices()
            .find(|(_, character)| ends_url(*character))
            .map(|(index, _)| body_start + index)
            .unwrap_or(text.len());
        let body = &text[body_start..body_end];
        let trimmed = body.trim_end_matches(trailing_punctuation);
        let tail = &body[trimmed.len()..];
        out.push_str(scheme);
        out.push_str("://");
        out.push_str(&rewrite(trimmed));
        out.push_str(tail);
        cursor = body_end;
    }
    out.push_str(&text[cursor..]);
    out
}

/// `scheme%3A%2F%2F...` (any case) replaced with `scheme%3A%2F%2F[redacted]`, up to the
/// same delimiters a plain URL ends at.
fn encoded_urls_redacted(text: &str) -> String {
    const ENCODED: &str = "%3a%2f%2f";
    let lower = text.to_ascii_lowercase();
    if !lower.contains(ENCODED) {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(found) = lower[cursor..].find(ENCODED) {
        let separator = cursor + found;
        let body_start = separator + ENCODED.len();
        let body_end = text[body_start..]
            .char_indices()
            .find(|(_, character)| ends_url(*character))
            .map(|(index, _)| body_start + index)
            .unwrap_or(text.len());
        out.push_str(&text[cursor..body_start]);
        if body_end > body_start {
            out.push_str("[redacted]");
        }
        cursor = body_end;
    }
    out.push_str(&text[cursor..]);
    out
}

/// One URL with its userinfo, query and fragment dropped and its path kept.
///
/// For a value whose path is the useful part and is not a credential (a logo image URL in a
/// report cell): the path stays readable, the parts a token travels in do not. Text that is
/// not a URL is returned unchanged.
pub fn url_without_secrets(url: &str) -> String {
    let Some((scheme, body)) = url.split_once("://") else {
        return url.to_string();
    };
    format!("{scheme}://{}", path_without_secrets(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_segment_url_with_credentials_in_the_path_keeps_only_its_host() {
        assert_eq!(
            urls_to_hosts(
                "Failed to open segment http://edge.provider.test:8080/live/USER/PASS/1.ts: 403"
            ),
            "Failed to open segment http://edge.provider.test:8080: 403"
        );
    }

    #[test]
    fn a_query_token_is_dropped() {
        let clean = urls_to_hosts("redirected to https://cdn.test/hls/1.m3u8?token=SECRET&x=1 ok");
        assert_eq!(clean, "redirected to https://cdn.test ok");
    }

    #[test]
    fn userinfo_is_dropped_even_when_the_password_has_a_slash() {
        assert_eq!(
            urls_to_hosts("GET http://user:pass@host.test/live/1.ts"),
            "GET http://host.test"
        );
        let clean = urls_to_hosts("GET http://user:pa/ss@host.test/live/1.ts");
        assert!(!clean.contains("user") && !clean.contains("pa"), "{clean}");
        assert_eq!(clean, "GET http://host.test");
    }

    #[test]
    fn several_urls_and_any_scheme_in_one_message() {
        let clean = urls_to_hosts(
            "\"rtmp://a.test/app/KEY\" then <udp://239.0.0.1:1234?pkt=1> and hls+http://b.test/x/y",
        );
        assert_eq!(
            clean,
            "\"rtmp://a.test\" then <udp://239.0.0.1:1234> and hls+http://b.test"
        );
    }

    #[test]
    fn a_json_body_and_an_ipv6_host() {
        assert_eq!(
            urls_to_hosts(r#"{"url":"http://p/live/u/p/1.ts","id":"x"}"#),
            r#"{"url":"http://p","id":"x"}"#
        );
        assert_eq!(
            urls_to_hosts("http://[2001:db8::1]:8080/live/u/p/1.ts"),
            "http://[2001:db8::1]:8080"
        );
    }

    #[test]
    fn an_authority_that_is_not_a_plain_host_is_redacted_whole() {
        // No `@`, but a non-numeric "port": a `user:pass` with the host cut off.
        assert_eq!(urls_to_hosts("http://user:secret"), "http://[redacted]");
        assert_eq!(urls_to_hosts("see http://"), "see http://");
    }

    #[test]
    fn the_encoded_spelling_is_redacted_whole() {
        let clean = urls_to_hosts("request http%3A%2F%2Fu%3Ap%40h%2Flive%2FU%2FP%2F1.ts failed");
        assert_eq!(clean, "request http%3A%2F%2F[redacted] failed");
    }

    #[test]
    fn text_without_a_url_is_unchanged() {
        for text in [
            "",
            "boom",
            "Connection timeout after 10 seconds",
            "a: b / c",
        ] {
            assert_eq!(urls_to_hosts(text), text);
        }
    }

    #[test]
    fn a_logo_url_keeps_its_path_and_loses_its_token() {
        assert_eq!(
            url_without_secrets("https://u:p@logos.test/a/cnn.png?token=T#f"),
            "https://logos.test/a/cnn.png"
        );
        assert_eq!(
            url_without_secrets("https://logos.test/a/cnn.png"),
            "https://logos.test/a/cnn.png"
        );
        assert_eq!(url_without_secrets("not a url"), "not a url");
        assert_eq!(
            urls_without_secrets(
                "logo \"https://u:p@logos.test/a/cnn.png?token=T\" could not be created: \
                 https://cdn.test/b.png."
            ),
            "logo \"https://logos.test/a/cnn.png\" could not be created: https://cdn.test/b.png."
        );
    }
}
