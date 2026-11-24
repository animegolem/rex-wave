//! The loopback HTTP guards every rex-wave listener shares (the HTTP/SSE
//! server, the MCP HTTP endpoint and the wake-bell sink): DNS-rebinding
//! `Host` checks, `Origin` checks against cross-site requests, JSON
//! content-type checks against form CSRF, and bearer parsing.

use std::net::{Ipv4Addr, Ipv6Addr};

fn port_ok(raw: &str) -> bool {
    raw.parse::<u16>().is_ok_and(|n| n > 0)
}

/// True when `authority` (`host[:port]`) names a loopback address:
/// `localhost`, `127.0.0.0/8`, or `[::1]`, with an optional nonzero port.
pub fn is_loopback_host(authority: &str) -> bool {
    let trimmed = authority.trim();
    if trimmed.is_empty() {
        return false;
    }
    if let Some(rest) = trimmed.strip_prefix('[') {
        let Some((ipv6, tail)) = rest.split_once(']') else {
            return false;
        };
        let port = tail.is_empty() || tail.strip_prefix(':').is_some_and(port_ok);
        return port && ipv6.parse::<Ipv6Addr>().is_ok_and(|ip| ip.is_loopback());
    }
    let (host, port) = match trimmed.split_once(':') {
        Some((h, p)) => (h, port_ok(p)),
        None => (trimmed, true),
    };
    port && (host.eq_ignore_ascii_case("localhost")
        || host.parse::<Ipv4Addr>().is_ok_and(|ip| ip.is_loopback()))
}

/// The authority of an `http://` or `https://` origin with no path, query,
/// fragment or userinfo.
fn origin_authority(origin: &str) -> Option<&str> {
    let trimmed = origin.trim();
    let bytes = trimmed.as_bytes();
    let authority = if bytes.len() > 7 && bytes[..7].eq_ignore_ascii_case(b"http://") {
        &trimmed[7..]
    } else if bytes.len() > 8 && bytes[..8].eq_ignore_ascii_case(b"https://") {
        &trimmed[8..]
    } else {
        return None;
    };
    (!authority.is_empty() && !authority.contains(['/', '?', '#', '@'])).then_some(authority)
}

/// True when `origin` is any loopback `http(s)://` origin.
pub fn is_loopback_origin(origin: &str) -> bool {
    origin_authority(origin).is_some_and(is_loopback_host)
}

/// True when `origin` is a loopback origin naming exactly `host`.
pub fn is_matching_loopback_origin(origin: &str, host: &str) -> bool {
    origin_authority(origin)
        .is_some_and(|a| is_loopback_host(a) && a.eq_ignore_ascii_case(host.trim()))
}

/// True when the media type of a `Content-Type` value is `application/json`.
pub fn is_json_content_type(content_type: &str) -> bool {
    let media = content_type.split(';').next().unwrap_or_default().trim();
    media.eq_ignore_ascii_case("application/json")
}

/// The token of an `Authorization: Bearer <token>` value; `None` for any
/// other scheme, an empty token, or a token containing whitespace.
pub fn strip_bearer(header: &str) -> Option<&str> {
    let (scheme, rest) = header.trim().split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim();
    (!token.is_empty() && !token.contains(char::is_whitespace)).then_some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_hosts_and_origins() {
        for ok in [
            "127.0.0.1:7782",
            "127.0.0.1",
            "127.1.2.3",
            "[::1]:7782",
            "[::1]",
            "localhost:7782",
            "LOCALHOST",
        ] {
            assert!(is_loopback_host(ok), "{ok}");
        }
        for bad in [
            "",
            "0.0.0.0:7782",
            "evil.example:7782",
            "127.0.0.1:0",
            "127.0.0.1:x",
            "192.168.1.5:7782",
            "[::2]:1",
            "[::1",
            "localhost.evil.example",
        ] {
            assert!(!is_loopback_host(bad), "{bad}");
        }
        assert!(is_matching_loopback_origin(
            "http://127.0.0.1:7782",
            "127.0.0.1:7782"
        ));
        assert!(is_matching_loopback_origin(
            "https://localhost:7782",
            "localhost:7782"
        ));
        assert!(!is_matching_loopback_origin(
            "http://127.0.0.1:9999",
            "127.0.0.1:7782"
        ));
        assert!(!is_matching_loopback_origin(
            "http://evil.example",
            "127.0.0.1:7782"
        ));
        assert!(!is_matching_loopback_origin("null", "127.0.0.1:7782"));
        assert!(is_loopback_origin("http://[::1]:9"));
        assert!(!is_loopback_origin("http://127.0.0.1/path"));
        assert!(!is_loopback_origin("http://user@127.0.0.1"));
        for multibyte in [
            "abcdef\u{e9}",
            "abcdefg\u{e9}",
            "123456\u{1f600}",
            "http://\u{e9}",
            "https://\u{e9}:7782",
        ] {
            assert!(!is_loopback_origin(multibyte), "{multibyte:?}");
            assert!(
                !is_matching_loopback_origin(multibyte, "127.0.0.1:7782"),
                "{multibyte:?}"
            );
        }
    }

    #[test]
    fn content_type_and_bearer() {
        assert!(is_json_content_type("application/json; charset=utf-8"));
        assert!(is_json_content_type("Application/JSON"));
        assert!(!is_json_content_type("text/plain"));
        assert_eq!(strip_bearer("Bearer abc"), Some("abc"));
        assert_eq!(strip_bearer("bearer\tabc "), Some("abc"));
        assert_eq!(strip_bearer("Basic abc"), None);
        assert_eq!(strip_bearer("Bearer "), None);
        assert_eq!(strip_bearer("Bearer a b"), None);
    }
}
