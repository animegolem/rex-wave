//! Pure-Tokio loopback HTTP/1.1 + SSE server for `rex-wave daemon --http`.
//!
//! Enforces loopback-only bind, loopback `Host`, matching loopback `Origin`,
//! `Sec-Fetch-Site: same-origin|none`, CSRF header/content-type checks on
//! mutating methods, unauthenticated `Principal::Operator` pinning, Bearer
//! token authentication, and `404 Not Found` visibility masking.

use rex_wave_api::board::{Priority, Title};
use rex_wave_api::http_guard::{is_json_content_type, strip_bearer};
pub use rex_wave_api::http_guard::{is_loopback_host, is_matching_loopback_origin};
use rex_wave_api::notify::{InboxAck, InboxList};
use rex_wave_api::presence::{PresenceList, PresenceResolve};
use rex_wave_api::read::{Page, Read, Sidebar, Wait};
use rex_wave_api::room::{
    Body, RoomId, RoomLabel, RoomPost, RoomRef, RoomTasks, TaskClaim, TaskFilter, TaskHeartbeat,
    TaskSpec, TaskStatus, TaskTransition, TaskUpdate,
};
use rex_wave_api::*;
use rex_wave_runtime::{LocalClient, Router, can_read_visible, mask_unauthorized_as_not_found};
use rex_wave_transport::{Authenticator, Credential};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const MAX_HEADER_LINES: usize = 64;
const MAX_HEADER_LINE_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(15);
pub const DEFAULT_SSE_KEEPALIVE: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug)]
pub struct HttpConfig {
    pub keepalive_interval: Duration,
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            keepalive_interval: DEFAULT_SSE_KEEPALIVE,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HttpMethod {
    Get,
    Post,
    Patch,
    Delete,
}

impl HttpMethod {
    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "GET" => Some(HttpMethod::Get),
            "POST" => Some(HttpMethod::Post),
            "PATCH" => Some(HttpMethod::Patch),
            "DELETE" => Some(HttpMethod::Delete),
            _ => None,
        }
    }

    fn is_mutating(self) -> bool {
        matches!(
            self,
            HttpMethod::Post | HttpMethod::Patch | HttpMethod::Delete
        )
    }
}

#[derive(Clone, Debug, Default)]
struct RequestHeaders {
    host: Option<String>,
    origin: Option<String>,
    sec_fetch_site: Option<String>,
    authorization: Option<String>,
    content_type: Option<String>,
    x_rex_wave: Option<String>,
    last_event_id: Option<String>,
    content_length: Option<usize>,
    has_transfer_encoding: bool,
    duplicate_security_header: Option<&'static str>,
}

struct ParsedRequest {
    method: HttpMethod,
    path: String,
    query: Vec<(String, String)>,
    headers: RequestHeaders,
    body: Vec<u8>,
}

impl ParsedRequest {
    fn query_param(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn query_params<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.query
            .iter()
            .filter(move |(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

struct HttpError {
    status: &'static str,
    body: Value,
}

impl HttpError {
    fn new(status: &'static str, code: &str, detail: impl Into<String>) -> Self {
        HttpError {
            status,
            body: json!({
                "error": code,
                "detail": detail.into(),
            }),
        }
    }

    fn from_api(err: ApiError) -> Self {
        let status = match &err {
            ApiError::Invalid(_) => "400 Bad Request",
            ApiError::Denied { .. } => "403 Forbidden",
            ApiError::NotFound { .. } => "404 Not Found",
            ApiError::Conflict { .. } => "409 Conflict",
            ApiError::Unavailable { .. } => "503 Service Unavailable",
            ApiError::Timeout => "504 Gateway Timeout",
            ApiError::Transport(_) => "502 Bad Gateway",
            ApiError::ReplyMismatch { .. } | ApiError::Internal(_) => "500 Internal Server Error",
        };
        let body = serde_json::to_value(&err).unwrap_or_else(|_| {
            json!({
                "error": "internal",
                "detail": err.to_string(),
            })
        });
        HttpError { status, body }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Route {
    Health,
    Sidebar,
    Inbox,
    InboxAck,
    Presence,
    Events,
    RoomMessagesGet { room: String },
    RoomMessagesPost { room: String },
    RoomMessageLabelsPatch { room: String, seq: u64 },
    RoomTasksGet { room: String },
    RoomTaskClaim { room: String, seq: u64 },
    RoomTaskHeartbeat { room: String, seq: u64 },
    RoomTaskTransition { room: String, seq: u64 },
    RoomTaskUpdate { room: String, seq: u64 },
    CallTool { tool: String },
}

pub async fn bind_http(addr: SocketAddr) -> io::Result<TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("HTTP server refuses non-loopback bind address {addr}; use 127.0.0.1 or ::1"),
        ));
    }
    TcpListener::bind(addr).await
}

pub async fn serve_http(
    addr: SocketAddr,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
    operator: SeatName,
) -> io::Result<()> {
    let listener = bind_http(addr).await?;
    serve_http_listener(listener, router, auth, operator, HttpConfig::default()).await
}

pub async fn serve_http_listener(
    listener: TcpListener,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
    operator: SeatName,
    config: HttpConfig,
) -> io::Result<()> {
    let local = listener.local_addr()?;
    if !local.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("HTTP listener must be loopback, got {local}"),
        ));
    }
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("rex-wave http: accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let router = router.clone();
        let auth = auth.clone();
        let operator = operator.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, router, auth, operator, config).await;
        });
    }
}

async fn wait_for_client_eof<R: AsyncRead + Unpin>(reader: &mut BufReader<R>) {
    let mut sink = [0u8; 64];
    loop {
        match reader.read(&mut sink).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    router: Arc<Router>,
    auth: Arc<dyn Authenticator>,
    operator: SeatName,
    config: HttpConfig,
) -> io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let req = match tokio::time::timeout(HEADER_READ_TIMEOUT, read_http_request(&mut reader)).await
    {
        Ok(Ok(Some(req))) => req,
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(err)) => return write_error_response(&mut write_half, &err).await,
        Err(_) => {
            return write_error_response(
                &mut write_half,
                &HttpError::new(
                    "408 Request Timeout",
                    "timeout",
                    "request headers timed out",
                ),
            )
            .await;
        }
    };

    if let Err(err) = validate_security_headers(req.method, &req.headers) {
        return write_error_response(&mut write_half, &err).await;
    }

    let principal = match authenticate_request(&req.headers, auth.as_ref(), &operator) {
        Ok(p) => p,
        Err(err) => return write_error_response(&mut write_half, &err).await,
    };

    let route = match match_route(req.method, &req.path) {
        Ok(r) => r,
        Err(err) => return write_error_response(&mut write_half, &err).await,
    };

    if matches!(route, Route::Events) {
        return handle_sse_events(
            &mut reader,
            &mut write_half,
            &req,
            router,
            principal,
            config,
        )
        .await;
    }

    let is_blocking_wait = matches!(&route, Route::CallTool { tool } if tool == "wait");
    let result = if is_blocking_wait {
        tokio::select! {
            biased;
            res = dispatch_json_route(route, &req, router, principal) => res,
            () = wait_for_client_eof(&mut reader) => return Ok(()),
        }
    } else {
        dispatch_json_route(route, &req, router, principal).await
    };

    match result {
        Ok((status, value)) => write_json_response(&mut write_half, status, &value).await,
        Err(err) => write_error_response(&mut write_half, &err).await,
    }
}

async fn read_http_request<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> Result<Option<ParsedRequest>, HttpError> {
    let mut req_line = Vec::new();
    let n = read_bounded_line(reader, &mut req_line).await?;
    if n == 0 {
        return Ok(None);
    }
    let line_str = std::str::from_utf8(&req_line)
        .map_err(|_| HttpError::new("400 Bad Request", "invalid", "non-UTF-8 request line"))?
        .trim_end_matches(['\r', '\n']);

    let mut parts = line_str.split_whitespace();
    let method_raw = parts
        .next()
        .ok_or_else(|| HttpError::new("400 Bad Request", "invalid", "missing HTTP method"))?;
    let target = parts
        .next()
        .ok_or_else(|| HttpError::new("400 Bad Request", "invalid", "missing request target"))?;
    let version = parts
        .next()
        .ok_or_else(|| HttpError::new("400 Bad Request", "invalid", "missing HTTP version"))?;
    if parts.next().is_some() || !version.starts_with("HTTP/1.") {
        return Err(HttpError::new(
            "400 Bad Request",
            "invalid",
            "malformed HTTP/1.x request line",
        ));
    }

    let (raw_path, raw_query) = match target.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (target, None),
    };
    if !raw_path.starts_with('/') {
        return Err(HttpError::new(
            "400 Bad Request",
            "invalid",
            "request path must start with '/'",
        ));
    }
    let query = match raw_query {
        Some(q) => parse_query_string(q)?,
        None => Vec::new(),
    };

    let mut headers = RequestHeaders::default();
    let mut saw_blank = false;
    for _ in 0..MAX_HEADER_LINES {
        let mut hdr_buf = Vec::new();
        let hn = read_bounded_line(reader, &mut hdr_buf).await?;
        if hn == 0 {
            return Err(HttpError::new(
                "400 Bad Request",
                "invalid",
                "truncated HTTP headers",
            ));
        }
        let line = std::str::from_utf8(&hdr_buf)
            .map_err(|_| HttpError::new("400 Bad Request", "invalid", "non-UTF-8 header"))?
            .trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            saw_blank = true;
            break;
        }
        let Some((name, val)) = line.split_once(':') else {
            return Err(HttpError::new(
                "400 Bad Request",
                "invalid",
                "malformed HTTP header",
            ));
        };
        let key = name.trim();
        let value = val.trim();
        if key.eq_ignore_ascii_case("host") {
            set_single_header(
                &mut headers.host,
                value,
                "Host",
                &mut headers.duplicate_security_header,
            );
        } else if key.eq_ignore_ascii_case("origin") {
            set_single_header(
                &mut headers.origin,
                value,
                "Origin",
                &mut headers.duplicate_security_header,
            );
        } else if key.eq_ignore_ascii_case("sec-fetch-site") {
            set_single_header(
                &mut headers.sec_fetch_site,
                value,
                "Sec-Fetch-Site",
                &mut headers.duplicate_security_header,
            );
        } else if key.eq_ignore_ascii_case("authorization") {
            set_single_header(
                &mut headers.authorization,
                value,
                "Authorization",
                &mut headers.duplicate_security_header,
            );
        } else if key.eq_ignore_ascii_case("content-type") {
            headers.content_type = Some(value.to_owned());
        } else if key.eq_ignore_ascii_case("x-rex-wave") {
            headers.x_rex_wave = Some(value.to_owned());
        } else if key.eq_ignore_ascii_case("last-event-id") {
            headers.last_event_id = Some(value.to_owned());
        } else if key.eq_ignore_ascii_case("content-length") {
            let parsed = value.parse::<usize>().map_err(|_| {
                HttpError::new("400 Bad Request", "invalid", "invalid Content-Length")
            })?;
            if let Some(prev) = headers.content_length
                && prev != parsed
            {
                return Err(HttpError::new(
                    "400 Bad Request",
                    "invalid",
                    "conflicting Content-Length headers",
                ));
            }
            if parsed > MAX_BODY_BYTES {
                return Err(HttpError::new(
                    "413 Payload Too Large",
                    "payload_too_large",
                    "request body exceeds 4 MiB limit",
                ));
            }
            headers.content_length = Some(parsed);
        } else if key.eq_ignore_ascii_case("transfer-encoding") {
            headers.has_transfer_encoding = true;
        }
    }

    if !saw_blank {
        return Err(HttpError::new(
            "431 Request Header Fields Too Large",
            "headers_too_large",
            "too many HTTP headers",
        ));
    }
    if headers.has_transfer_encoding {
        return Err(HttpError::new(
            "501 Not Implemented",
            "unsupported_transfer_encoding",
            "Transfer-Encoding is not supported; send Content-Length",
        ));
    }

    let method = HttpMethod::parse(method_raw).ok_or_else(|| {
        HttpError::new(
            "405 Method Not Allowed",
            "method_not_allowed",
            format!("unsupported HTTP method {method_raw}"),
        )
    })?;

    let body_len = headers.content_length.unwrap_or(0);
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        reader
            .read_exact(&mut body)
            .await
            .map_err(|_| HttpError::new("400 Bad Request", "invalid", "truncated request body"))?;
    }

    Ok(Some(ParsedRequest {
        method,
        path: raw_path.to_owned(),
        query,
        headers,
        body,
    }))
}

fn set_single_header(
    slot: &mut Option<String>,
    value: &str,
    name: &'static str,
    dup: &mut Option<&'static str>,
) {
    if slot.is_some() {
        *dup = Some(name);
    } else {
        *slot = Some(value.to_owned());
    }
}

async fn read_bounded_line<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    out: &mut Vec<u8>,
) -> Result<usize, HttpError> {
    let mut total = 0usize;
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|e| HttpError::new("400 Bad Request", "io", e.to_string()))?;
        if available.is_empty() {
            return Ok(total);
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let take = pos + 1;
            if total + take > MAX_HEADER_LINE_BYTES {
                return Err(HttpError::new(
                    "431 Request Header Fields Too Large",
                    "headers_too_large",
                    "HTTP header line exceeds 16 KiB",
                ));
            }
            out.extend_from_slice(&available[..take]);
            reader.consume(take);
            return Ok(total + take);
        }
        let len = available.len();
        if total + len > MAX_HEADER_LINE_BYTES {
            return Err(HttpError::new(
                "431 Request Header Fields Too Large",
                "headers_too_large",
                "HTTP header line exceeds 16 KiB",
            ));
        }
        out.extend_from_slice(available);
        reader.consume(len);
        total += len;
    }
}

fn validate_security_headers(
    method: HttpMethod,
    headers: &RequestHeaders,
) -> Result<(), HttpError> {
    if let Some(dup) = headers.duplicate_security_header {
        return Err(HttpError::new(
            "400 Bad Request",
            "invalid",
            format!("duplicate {dup} header"),
        ));
    }

    let host = headers
        .host
        .as_deref()
        .ok_or_else(|| HttpError::new("400 Bad Request", "invalid", "missing Host header"))?;
    if !is_loopback_host(host) {
        return Err(HttpError::new(
            "403 Forbidden",
            "forbidden_host",
            "Host header must target loopback",
        ));
    }

    if let Some(origin) = headers.origin.as_deref()
        && !is_matching_loopback_origin(origin, host)
    {
        return Err(HttpError::new(
            "403 Forbidden",
            "forbidden_origin",
            "Origin header must match loopback Host",
        ));
    }

    if let Some(site) = headers.sec_fetch_site.as_deref() {
        let trimmed = site.trim();
        if !trimmed.eq_ignore_ascii_case("same-origin") && !trimmed.eq_ignore_ascii_case("none") {
            return Err(HttpError::new(
                "403 Forbidden",
                "forbidden_fetch_site",
                "Sec-Fetch-Site must be same-origin or none",
            ));
        }
    }

    if method.is_mutating() {
        let json_ct = headers
            .content_type
            .as_deref()
            .is_some_and(is_json_content_type);
        let custom_hdr = headers
            .x_rex_wave
            .as_deref()
            .is_some_and(|v| v.trim() == "1");
        if !json_ct && !custom_hdr {
            return Err(HttpError::new(
                "415 Unsupported Media Type",
                "unsupported_media_type",
                "mutating requests require Content-Type: application/json or X-Rex-Wave: 1",
            ));
        }
    }

    Ok(())
}

fn authenticate_request(
    headers: &RequestHeaders,
    auth: &dyn Authenticator,
    operator: &SeatName,
) -> Result<Principal, HttpError> {
    let Some(raw_auth) = headers.authorization.as_deref() else {
        return Ok(Principal::Operator {
            seat: operator.clone(),
        });
    };
    let Some(token) = strip_bearer(raw_auth) else {
        return Err(HttpError::new(
            "401 Unauthorized",
            "unauthorized",
            "Authorization header must use Bearer <token>",
        ));
    };
    auth.authenticate(
        None,
        &Credential::Token {
            token: token.to_owned(),
        },
    )
    .map_err(|_| {
        HttpError::new(
            "401 Unauthorized",
            "unauthorized",
            "invalid or revoked bearer token",
        )
    })
}

fn match_route(method: HttpMethod, raw_path: &str) -> Result<Route, HttpError> {
    let trimmed = raw_path.strip_prefix('/').unwrap_or(raw_path);
    let segments: Vec<&str> = if trimmed.is_empty() {
        Vec::new()
    } else {
        trimmed.split('/').collect()
    };

    match (method, segments.as_slice()) {
        (HttpMethod::Get, ["v1", "health"]) => Ok(Route::Health),
        (HttpMethod::Get, ["v1", "sidebar"]) => Ok(Route::Sidebar),
        (HttpMethod::Get, ["v1", "inbox"]) => Ok(Route::Inbox),
        (HttpMethod::Post, ["v1", "inbox", "ack"]) => Ok(Route::InboxAck),
        (HttpMethod::Get, ["v1", "presence"]) => Ok(Route::Presence),
        (HttpMethod::Get, ["v1", "events"]) => Ok(Route::Events),
        (HttpMethod::Get, ["v1", "rooms", room, "messages"]) => Ok(Route::RoomMessagesGet {
            room: percent_decode_path(room)?,
        }),
        (HttpMethod::Post, ["v1", "rooms", room, "messages"]) => Ok(Route::RoomMessagesPost {
            room: percent_decode_path(room)?,
        }),
        (HttpMethod::Patch, ["v1", "rooms", room, "messages", seq, "labels"]) => {
            Ok(Route::RoomMessageLabelsPatch {
                room: percent_decode_path(room)?,
                seq: parse_seq_segment(seq)?,
            })
        }
        (HttpMethod::Get, ["v1", "rooms", room, "tasks"]) => Ok(Route::RoomTasksGet {
            room: percent_decode_path(room)?,
        }),
        (HttpMethod::Post, ["v1", "rooms", room, "tasks", seq, "claim"]) => {
            Ok(Route::RoomTaskClaim {
                room: percent_decode_path(room)?,
                seq: parse_seq_segment(seq)?,
            })
        }
        (HttpMethod::Post, ["v1", "rooms", room, "tasks", seq, "heartbeat"]) => {
            Ok(Route::RoomTaskHeartbeat {
                room: percent_decode_path(room)?,
                seq: parse_seq_segment(seq)?,
            })
        }
        (HttpMethod::Post, ["v1", "rooms", room, "tasks", seq, "transition"]) => {
            Ok(Route::RoomTaskTransition {
                room: percent_decode_path(room)?,
                seq: parse_seq_segment(seq)?,
            })
        }
        (HttpMethod::Patch, ["v1", "rooms", room, "tasks", seq]) => Ok(Route::RoomTaskUpdate {
            room: percent_decode_path(room)?,
            seq: parse_seq_segment(seq)?,
        }),
        (HttpMethod::Post, ["v1", "call", tool]) => Ok(Route::CallTool {
            tool: percent_decode_path(tool)?,
        }),
        _ => Err(HttpError::new(
            "404 Not Found",
            "not_found",
            format!("unknown route {raw_path}"),
        )),
    }
}

fn parse_seq_segment(raw: &str) -> Result<u64, HttpError> {
    raw.parse::<u64>()
        .map_err(|_| HttpError::new("400 Bad Request", "invalid", format!("invalid seq {raw}")))
}

fn percent_decode_bytes(raw: &str, plus_as_space: bool) -> Result<String, HttpError> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(HttpError::new(
                        "400 Bad Request",
                        "invalid",
                        "truncated percent-encoding",
                    ));
                }
                let hi = hex_nibble(bytes[i + 1]).ok_or_else(|| {
                    HttpError::new("400 Bad Request", "invalid", "invalid percent-encoding")
                })?;
                let lo = hex_nibble(bytes[i + 2]).ok_or_else(|| {
                    HttpError::new("400 Bad Request", "invalid", "invalid percent-encoding")
                })?;
                let decoded = (hi << 4) | lo;
                if decoded == 0 {
                    return Err(HttpError::new(
                        "400 Bad Request",
                        "invalid",
                        "NUL byte in URL",
                    ));
                }
                out.push(decoded);
                i += 3;
            }
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|_| HttpError::new("400 Bad Request", "invalid", "non-UTF-8 URL component"))
}

fn percent_decode_path(raw: &str) -> Result<String, HttpError> {
    percent_decode_bytes(raw, false)
}

fn parse_query_string(raw: &str) -> Result<Vec<(String, String)>, HttpError> {
    let mut out = Vec::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (
                percent_decode_bytes(k, true)?,
                percent_decode_bytes(v, true)?,
            ),
            None => (percent_decode_bytes(pair, true)?, String::new()),
        };
        out.push((k, v));
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn parse_bool_query(raw: &str) -> Result<bool, HttpError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        other => Err(HttpError::new(
            "400 Bad Request",
            "invalid",
            format!("invalid boolean query value '{other}'"),
        )),
    }
}

pub fn parse_http_principal(raw: &str) -> Result<Principal, ApiError> {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("operator:") {
        return Ok(Principal::Operator {
            seat: SeatName::parse(rest.strip_prefix('@').unwrap_or(rest))?,
        });
    }
    if let Some(rest) = trimmed.strip_prefix("seat:") {
        return Ok(Principal::Seat {
            seat: SeatName::parse(rest.strip_prefix('@').unwrap_or(rest))?,
        });
    }
    if let Some(rest) = trimmed.strip_prefix("worker:") {
        let (w, r) = rest.split_once('/').ok_or_else(|| {
            ApiError::Invalid(InvalidId {
                what: "principal".into(),
                reason: "expected worker:<wave>/<role>".into(),
            })
        })?;
        return Ok(Principal::Worker {
            wave: WaveSlug::parse(w)?,
            role: RoleName::parse(r)?,
        });
    }
    crate::parse_principal_ref(trimmed)
}

/// Parses an HTTP `{room}` segment or `?room=` parameter into a [`RoomRef`].
///
/// Accepts `#channel`, `#project/channel`, `@seat`, `@wave/role`, `proj:<project>/<channel>`,
/// `dm:<principal_a>,<principal_b>`, bare `<channel>`, or JSON [`RoomId`].
pub fn parse_http_room_ref(raw: &str) -> Result<RoomRef, ApiError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ApiError::Invalid(InvalidId {
            what: "room".into(),
            reason: "room identifier must not be empty".into(),
        }));
    }
    if trimmed.starts_with('{') {
        let id: RoomId = serde_json::from_str(trimmed).map_err(|e| {
            ApiError::Invalid(InvalidId {
                what: "room".into(),
                reason: format!("invalid RoomId JSON: {e}"),
            })
        })?;
        return Ok(RoomRef::Id { id });
    }
    if let Some(pair_raw) = trimmed.strip_prefix("dm:") {
        let (a_raw, b_raw) = pair_raw.split_once(',').ok_or_else(|| {
            ApiError::Invalid(InvalidId {
                what: "room".into(),
                reason: "expected dm:<principal_a>,<principal_b>".into(),
            })
        })?;
        let a = parse_http_principal(a_raw)?;
        let b = parse_http_principal(b_raw)?;
        let pair = rex_wave_api::room::PrincipalPair::new(a, b)?;
        return Ok(RoomRef::Id {
            id: RoomId::Direct { pair },
        });
    }
    if trimmed.starts_with('#') || trimmed.starts_with('@') {
        return crate::parse_room_ref(trimmed);
    }
    crate::parse_room_ref(&format!("#{trimmed}"))
}

fn parse_task_status_str(raw: &str) -> Result<TaskStatus, ApiError> {
    rex_wave_api::parse_enum("status", raw)
}

fn parse_json_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, HttpError> {
    let slice = if body.is_empty() {
        b"{}".as_slice()
    } else {
        body
    };
    serde_json::from_slice(slice).map_err(|e| {
        HttpError::new(
            "400 Bad Request",
            "invalid_json",
            format!("invalid JSON body: {e}"),
        )
    })
}

fn mask_err(err: ApiError, resource: &str) -> HttpError {
    HttpError::from_api(mask_unauthorized_as_not_found(err, resource))
}

fn check_room_visible(
    router: &Router,
    principal: &Principal,
    room_ref: &RoomRef,
    raw_label: &str,
) -> Result<RoomId, HttpError> {
    let room_id = router
        .read
        .resolve_room_ref(room_ref, principal)
        .map_err(HttpError::from_api)?;
    can_read_visible(
        principal,
        &StreamId::Room {
            room: room_id.clone(),
        },
        format!("room {raw_label}"),
    )
    .map_err(HttpError::from_api)?;
    Ok(room_id)
}

#[derive(Deserialize)]
struct PostMessagePayload {
    body: Body,
    #[serde(default, alias = "reply_to")]
    re: Option<Seq>,
    #[serde(default)]
    after: Option<Seq>,
    #[serde(default)]
    labels: Vec<Label>,
    #[serde(default)]
    task: Option<TaskSpec>,
    #[serde(default, alias = "id")]
    idem: Option<IdemKey>,
}

#[derive(Deserialize)]
struct PatchLabelsPayload {
    #[serde(default)]
    add: Vec<Label>,
    #[serde(default)]
    remove: Vec<Label>,
    #[serde(default, alias = "id")]
    idem: Option<IdemKey>,
}

#[derive(Deserialize, Default)]
struct ClaimTaskPayload {
    #[serde(default)]
    ttl_ms: Option<u64>,
    #[serde(default)]
    expected_rev: Option<u64>,
    #[serde(default)]
    force: bool,
    #[serde(default, alias = "id")]
    idem: Option<IdemKey>,
}

#[derive(Deserialize)]
struct HeartbeatTaskPayload {
    epoch: LeaseEpoch,
    #[serde(default)]
    ttl_ms: Option<u64>,
}

#[derive(Deserialize)]
struct TransitionTaskPayload {
    status: TaskStatus,
    #[serde(default)]
    epoch: Option<LeaseEpoch>,
    #[serde(default)]
    expected_rev: Option<u64>,
    #[serde(default)]
    note: Option<Body>,
    #[serde(default, alias = "id")]
    idem: Option<IdemKey>,
}

#[derive(Deserialize, Default)]
struct UpdateTaskPayload {
    #[serde(default)]
    expected_rev: Option<u64>,
    #[serde(default)]
    title: Option<Title>,
    #[serde(default)]
    needs_add: Vec<Seq>,
    #[serde(default)]
    needs_remove: Vec<Seq>,
    #[serde(default)]
    priority: Option<Priority>,
    #[serde(default)]
    due_ms: Option<UnixMs>,
    #[serde(default)]
    clear_due: bool,
    #[serde(default, alias = "id")]
    idem: Option<IdemKey>,
}

async fn dispatch_json_route(
    route: Route,
    req: &ParsedRequest,
    router: Arc<Router>,
    principal: Principal,
) -> Result<(&'static str, Value), HttpError> {
    let client = LocalClient::new(router.clone(), principal.clone());

    match route {
        Route::Health => Ok((
            "200 OK",
            json!({
                "ok": true,
                "version": env!("CARGO_PKG_VERSION"),
                "principal": principal,
            }),
        )),
        Route::Sidebar => {
            let snap = client
                .call(Sidebar {})
                .await
                .map_err(|e| mask_err(e, "sidebar"))?;
            Ok(("200 OK", to_json_value(&snap)?))
        }
        Route::Inbox => {
            if let Some(owner_raw) = req.query_param("owner") {
                let owner = parse_http_principal(owner_raw).map_err(HttpError::from_api)?;
                can_read_visible(
                    &principal,
                    &StreamId::Inbox {
                        owner: owner.clone(),
                    },
                    "inbox",
                )
                .map_err(HttpError::from_api)?;
                if owner != principal {
                    let limit = match req.query_param("limit") {
                        Some(raw) => Some(raw.parse::<u32>().map_err(|_| {
                            HttpError::new("400 Bad Request", "invalid", "invalid limit")
                        })?),
                        None => None,
                    };
                    let after = match req.query_param("after") {
                        Some(raw) => Seq(raw.parse::<u64>().map_err(|_| {
                            HttpError::new("400 Bad Request", "invalid", "invalid after")
                        })?),
                        None => Seq::ORIGIN,
                    };
                    let page = client
                        .call(Read::stream(StreamId::Inbox { owner }, after, limit))
                        .await
                        .map_err(|e| mask_err(e, "inbox"))?;
                    return Ok(("200 OK", to_json_value(&page)?));
                }
            }
            let include_done = match req
                .query_param("include_done")
                .or_else(|| req.query_param("all"))
            {
                Some(v) => parse_bool_query(v)?,
                None => false,
            };
            let limit =
                match req.query_param("limit") {
                    Some(raw) => Some(raw.parse::<u32>().map_err(|_| {
                        HttpError::new("400 Bad Request", "invalid", "invalid limit")
                    })?),
                    None => None,
                };
            let page = client
                .call(InboxList {
                    include_done,
                    limit,
                })
                .await
                .map_err(|e| mask_err(e, "inbox"))?;
            Ok(("200 OK", to_json_value(&page)?))
        }
        Route::InboxAck => {
            let ack: InboxAck = parse_json_body(&req.body)?;
            client.call(ack).await.map_err(|e| mask_err(e, "inbox"))?;
            Ok(("200 OK", json!({ "ok": true })))
        }
        Route::Presence => {
            if let Some(query) = req.query_param("resolve") {
                let res = client
                    .call(PresenceResolve {
                        query: query.to_owned(),
                        lane: req.query_param("lane").map(str::to_owned),
                    })
                    .await
                    .map_err(|e| mask_err(e, "presence"))?;
                return Ok(("200 OK", to_json_value(&res)?));
            }
            let live_only = match req.query_param("live_only") {
                Some(v) => parse_bool_query(v)?,
                None => false,
            };
            let include_retired = match req.query_param("include_retired") {
                Some(v) => parse_bool_query(v)?,
                None => false,
            };
            let snap = client
                .call(PresenceList {
                    project: req.query_param("project").map(str::to_owned),
                    live_only,
                    include_retired,
                })
                .await
                .map_err(|e| mask_err(e, "presence"))?;
            Ok(("200 OK", to_json_value(&snap)?))
        }
        Route::RoomMessagesGet { room } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let after = match req.query_param("after") {
                Some(raw) => Seq(raw.parse::<u64>().map_err(|_| {
                    HttpError::new("400 Bad Request", "invalid", "invalid after seq")
                })?),
                None => Seq::ORIGIN,
            };
            let limit =
                match req.query_param("limit") {
                    Some(raw) => Some(raw.parse::<u32>().map_err(|_| {
                        HttpError::new("400 Bad Request", "invalid", "invalid limit")
                    })?),
                    None => None,
                };
            let re = match req.query_param("re") {
                Some(raw) => Some(Seq(raw.parse::<u64>().map_err(|_| {
                    HttpError::new("400 Bad Request", "invalid", "invalid re seq")
                })?)),
                None => None,
            };
            let labels: Vec<Label> = req
                .query_params("label")
                .map(Label::parse)
                .collect::<Result<_, _>>()
                .map_err(|e| HttpError::from_api(e.into()))?;

            let mut page: Page = if re.is_some() || !labels.is_empty() {
                let mut wait = Wait::room_ref_filtered(room_ref, &principal, after, re, labels)
                    .map_err(|e| HttpError::from_api(e.into()))?;
                wait.timeout_ms = Some(0);
                wait.limit = Some(limit.unwrap_or(200).clamp(1, 1000));
                let mut p = client
                    .call(wait)
                    .await
                    .map_err(|e| mask_err(e, &format!("room {room}")))?;
                p.timed_out = false;
                p
            } else {
                let read_cmd = Read::room_ref(room_ref, &principal, after, None, limit)
                    .map_err(|e| HttpError::from_api(e.into()))?;
                client
                    .call(read_cmd)
                    .await
                    .map_err(|e| mask_err(e, &format!("room {room}")))?
            };
            page.timed_out = false;
            Ok(("200 OK", to_json_value(&page)?))
        }
        Route::RoomMessagesPost { room } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let payload: PostMessagePayload = parse_json_body(&req.body)?;
            let posted = client
                .call(RoomPost {
                    room: room_ref,
                    body: payload.body,
                    re: payload.re,
                    after: payload.after,
                    labels: payload.labels,
                    task: payload.task,
                    idem: payload.idem,
                })
                .await
                .map_err(|e| mask_err(e, &format!("room {room}")))?;
            Ok(("201 Created", to_json_value(&posted)?))
        }
        Route::RoomMessageLabelsPatch { room, seq } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let payload: PatchLabelsPayload = parse_json_body(&req.body)?;
            let labeled = client
                .call(RoomLabel {
                    room: room_ref,
                    seq: Seq(seq),
                    add: payload.add,
                    remove: payload.remove,
                    idem: payload.idem,
                })
                .await
                .map_err(|e| mask_err(e, &format!("room {room} message {seq}")))?;
            Ok(("200 OK", to_json_value(&labeled)?))
        }
        Route::RoomTasksGet { room } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let status = match req.query_param("status") {
                Some(raw) => Some(parse_task_status_str(raw).map_err(HttpError::from_api)?),
                None => None,
            };
            let ready_only = match req
                .query_param("ready_only")
                .or_else(|| req.query_param("ready"))
            {
                Some(raw) => parse_bool_query(raw)?,
                None => false,
            };
            let mine = match req.query_param("mine") {
                Some(raw) => parse_bool_query(raw)?,
                None => false,
            };
            let holder = if mine {
                Some(principal.clone())
            } else if let Some(raw) = req.query_param("holder") {
                Some(parse_http_principal(raw).map_err(HttpError::from_api)?)
            } else {
                None
            };
            let label = match req.query_param("label") {
                Some(raw) => Some(Label::parse(raw).map_err(|e| HttpError::from_api(e.into()))?),
                None => None,
            };
            let filter = if status.is_some() || ready_only || holder.is_some() || label.is_some() {
                Some(TaskFilter {
                    status,
                    ready_only,
                    holder,
                    label,
                })
            } else {
                None
            };
            let limit =
                match req.query_param("limit") {
                    Some(raw) => Some(raw.parse::<u32>().map_err(|_| {
                        HttpError::new("400 Bad Request", "invalid", "invalid limit")
                    })?),
                    None => None,
                };
            let tasks = client
                .call(RoomTasks {
                    room: room_ref,
                    filter,
                    limit,
                })
                .await
                .map_err(|e| mask_err(e, &format!("room {room}")))?;
            Ok(("200 OK", to_json_value(&tasks)?))
        }
        Route::RoomTaskClaim { room, seq } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let payload: ClaimTaskPayload = parse_json_body(&req.body)?;
            let receipt = client
                .call(TaskClaim {
                    room: room_ref,
                    task: Seq(seq),
                    ttl_ms: payload.ttl_ms,
                    expected_rev: payload.expected_rev,
                    force: payload.force,
                    idem: payload.idem,
                })
                .await
                .map_err(|e| mask_err(e, &format!("room {room} task {seq}")))?;
            Ok(("200 OK", to_json_value(&receipt)?))
        }
        Route::RoomTaskHeartbeat { room, seq } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let payload: HeartbeatTaskPayload = parse_json_body(&req.body)?;
            let receipt = client
                .call(TaskHeartbeat {
                    room: room_ref,
                    task: Seq(seq),
                    epoch: payload.epoch,
                    ttl_ms: payload.ttl_ms,
                })
                .await
                .map_err(|e| mask_err(e, &format!("room {room} task {seq}")))?;
            Ok(("200 OK", to_json_value(&receipt)?))
        }
        Route::RoomTaskTransition { room, seq } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let payload: TransitionTaskPayload = parse_json_body(&req.body)?;
            let view = client
                .call(TaskTransition {
                    room: room_ref,
                    task: Seq(seq),
                    status: payload.status,
                    epoch: payload.epoch,
                    expected_rev: payload.expected_rev,
                    note: payload.note,
                    idem: payload.idem,
                })
                .await
                .map_err(|e| mask_err(e, &format!("room {room} task {seq}")))?;
            Ok(("200 OK", to_json_value(&view)?))
        }
        Route::RoomTaskUpdate { room, seq } => {
            let room_ref = parse_http_room_ref(&room).map_err(HttpError::from_api)?;
            let _ = check_room_visible(&router, &principal, &room_ref, &room)?;
            let payload: UpdateTaskPayload = parse_json_body(&req.body)?;
            let view = client
                .call(TaskUpdate {
                    room: room_ref,
                    task: Seq(seq),
                    expected_rev: payload.expected_rev,
                    title: payload.title,
                    needs_add: payload.needs_add,
                    needs_remove: payload.needs_remove,
                    priority: payload.priority,
                    due_ms: payload.due_ms,
                    clear_due: payload.clear_due,
                    idem: payload.idem,
                })
                .await
                .map_err(|e| mask_err(e, &format!("room {room} task {seq}")))?;
            Ok(("200 OK", to_json_value(&view)?))
        }
        Route::CallTool { tool } => {
            let args: Value = parse_json_body(&req.body)?;
            let envelope = json!({
                "cmd": tool,
                "args": args,
            });
            let cmd: WireCommand = serde_json::from_value(envelope).map_err(|e| {
                HttpError::new(
                    "400 Bad Request",
                    "invalid",
                    format!("invalid command payload for '{tool}': {e}"),
                )
            })?;
            if let WireCommand::Read(ref r) = cmd {
                let target_stream = match &r.room {
                    Some(room_ref) => StreamId::Room {
                        room: router
                            .read
                            .resolve_room_ref(room_ref, &principal)
                            .map_err(HttpError::from_api)?,
                    },
                    None => r.stream.clone(),
                };
                can_read_visible(&principal, &target_stream, &tool).map_err(HttpError::from_api)?;
            }
            if let WireCommand::PresenceBind(ref b) = cmd
                && matches!(
                    b.endpoint,
                    rex_wave_api::presence::TransportEndpoint::UdsSession { .. }
                )
            {
                return Err(HttpError::new(
                    "400 Bad Request",
                    "invalid",
                    "UdsSession presence bindings are only allowed over a Unix socket connection",
                ));
            }
            let reply = client
                .call_wire(cmd)
                .await
                .map_err(|e| mask_err(e, &tool))?;
            Ok(("200 OK", to_json_value(&reply)?))
        }
        Route::Events => unreachable!("SSE events handled before JSON dispatch"),
    }
}

fn parse_sse_interest_and_cursor(
    router: &Router,
    req: &ParsedRequest,
    principal: &Principal,
) -> Result<(Interest, Cursor), HttpError> {
    let mut single_stream: Option<StreamId> = None;
    let interest = if let Some(room_raw) = req.query_param("room") {
        let room_ref = parse_http_room_ref(room_raw).map_err(HttpError::from_api)?;
        let room_id = check_room_visible(router, principal, &room_ref, room_raw)?;
        single_stream = Some(StreamId::Room { room: room_id });
        let re = match req.query_param("re") {
            Some(raw) => Some(Seq(raw.parse::<u64>().map_err(|_| {
                HttpError::new("400 Bad Request", "invalid", "invalid re seq")
            })?)),
            None => None,
        };
        let labels: Vec<Label> = req
            .query_params("label")
            .map(Label::parse)
            .collect::<Result<_, _>>()
            .map_err(|e| HttpError::from_api(e.into()))?;
        if re.is_some() || !labels.is_empty() {
            Interest::RoomFiltered {
                room: room_ref,
                re,
                labels,
            }
        } else {
            Interest::Room { room: room_ref }
        }
    } else if let Some(stream_raw) = req.query_param("stream") {
        let stream = parse_sse_stream_param(stream_raw).map_err(HttpError::from_api)?;
        can_read_visible(principal, &stream, "stream").map_err(HttpError::from_api)?;
        single_stream = Some(stream.clone());
        Interest::Streams {
            streams: vec![stream],
        }
    } else if let Some(project_raw) = req.query_param("project") {
        Interest::Project {
            project: BbProjectId::parse(project_raw).map_err(|e| HttpError::from_api(e.into()))?,
        }
    } else if let Some(agent_raw) = req.query_param("agent") {
        let (w, r) = agent_raw.split_once('/').ok_or_else(|| {
            HttpError::new("400 Bad Request", "invalid", "agent must be <wave>/<role>")
        })?;
        Interest::Agent {
            wave: WaveSlug::parse(w).map_err(|e| HttpError::from_api(e.into()))?,
            role: RoleName::parse(r).map_err(|e| HttpError::from_api(e.into()))?,
        }
    } else if let Some(kind) = req.query_param("interest") {
        match kind.trim().to_ascii_lowercase().as_str() {
            "everything" => Interest::Everything,
            "mentions" => Interest::Mentions,
            "inbox" | "my_inbox" => Interest::MyInbox,
            "sidebar" => Interest::Sidebar,
            other => {
                return Err(HttpError::new(
                    "400 Bad Request",
                    "invalid",
                    format!("unknown interest '{other}'"),
                ));
            }
        }
    } else if principal.is_operator() {
        Interest::Everything
    } else {
        Interest::MyInbox
    };

    let resume_raw = req
        .headers
        .last_event_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            req.query_param("after")
                .map(str::trim)
                .filter(|s| !s.is_empty())
        });

    let cursor = match resume_raw {
        Some(raw) => parse_resume_cursor(raw, single_stream.as_ref())?,
        None => match req.query_param("live") {
            Some(v) if parse_bool_query(v)? => Cursor::live(),
            _ if single_stream.is_some() => Cursor::default(),
            _ => Cursor::live(),
        },
    };

    Ok((interest, cursor))
}

fn parse_sse_stream_param(raw: &str) -> Result<StreamId, ApiError> {
    let trimmed = raw.trim();
    if trimmed.starts_with('{') {
        return serde_json::from_str(trimmed).map_err(|e| {
            ApiError::Invalid(InvalidId {
                what: "stream".into(),
                reason: format!("invalid StreamId JSON: {e}"),
            })
        });
    }
    if let Some(owner_raw) = trimmed.strip_prefix("inbox:") {
        let owner = parse_http_principal(owner_raw)?;
        return Ok(StreamId::Inbox { owner });
    }
    if trimmed == "presence" {
        return Ok(StreamId::Presence);
    }
    if trimmed == "mail" {
        return Ok(StreamId::Mail);
    }
    Err(ApiError::Invalid(InvalidId {
        what: "stream".into(),
        reason: "expected StreamId JSON, inbox:<principal>, presence, or mail".into(),
    }))
}

fn parse_resume_cursor(raw: &str, single_stream: Option<&StreamId>) -> Result<Cursor, HttpError> {
    if raw.starts_with('{') {
        return serde_json::from_str::<Cursor>(raw).map_err(|e| {
            HttpError::new(
                "400 Bad Request",
                "invalid",
                format!("invalid cursor JSON: {e}"),
            )
        });
    }
    if let Ok(seq) = raw.parse::<u64>() {
        if let Some(stream) = single_stream {
            return Ok(Cursor::default().with(stream.clone(), Seq(seq)));
        }
        return Err(HttpError::new(
            "400 Bad Request",
            "invalid",
            "numeric after cursor requires ?room= or ?stream=",
        ));
    }
    Err(HttpError::new(
        "400 Bad Request",
        "invalid",
        "invalid Last-Event-ID / after cursor",
    ))
}

async fn handle_sse_events<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut BufReader<R>,
    writer: &mut W,
    req: &ParsedRequest,
    router: Arc<Router>,
    principal: Principal,
    config: HttpConfig,
) -> io::Result<()> {
    let (interest, initial_cursor) = match parse_sse_interest_and_cursor(&router, req, &principal) {
        Ok(pair) => pair,
        Err(err) => return write_error_response(writer, &err).await,
    };

    let client = LocalClient::new(router, principal);
    let mut stream = match client.watch(interest, initial_cursor.clone()).await {
        Ok(s) => s,
        Err(err) => return write_error_response(writer, &mask_err(err, "events")).await,
    };

    writer
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        )
        .await?;
    writer.flush().await?;

    let mut running_cursor = initial_cursor;
    let mut keepalive = tokio::time::interval(config.keepalive_interval);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    keepalive.tick().await;

    loop {
        tokio::select! {
            biased;
            () = wait_for_client_eof(reader) => break,
            item = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)) => {
                match item {
                    Some(Ok(frame)) => {
                        match &frame {
                            Frame::Committed(env) => {
                                running_cursor.advance(&env.stream, env.seq);
                            }
                            Frame::CaughtUp { cursor } => {
                                running_cursor = cursor.clone();
                            }
                            Frame::Sidebar(snap) => {
                                running_cursor = snap.cursor.clone();
                            }
                            _ => {}
                        }
                        let id_json = serde_json::to_string(&running_cursor)
                            .map_err(io::Error::other)?;
                        let data_json = serde_json::to_string(&frame)
                            .map_err(io::Error::other)?;
                        let chunk = format!("id: {id_json}\nevent: frame\ndata: {data_json}\n\n");
                        writer.write_all(chunk.as_bytes()).await?;
                        writer.flush().await?;
                    }
                    Some(Err(err)) => {
                        let masked = mask_unauthorized_as_not_found(err, "events");
                        let data_json = serde_json::to_string(&masked)
                            .map_err(io::Error::other)?;
                        let chunk = format!("event: error\ndata: {data_json}\n\n");
                        let _ = writer.write_all(chunk.as_bytes()).await;
                        let _ = writer.flush().await;
                        break;
                    }
                    None => break,
                }
            }
            _ = keepalive.tick() => {
                writer.write_all(b": keepalive\n\n").await?;
                writer.flush().await?;
            }
        }
    }

    Ok(())
}

fn to_json_value<T: serde::Serialize>(val: &T) -> Result<Value, HttpError> {
    serde_json::to_value(val).map_err(|e| {
        HttpError::new(
            "500 Internal Server Error",
            "internal",
            format!("serialize response: {e}"),
        )
    })
}

async fn write_json_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    status: &str,
    value: &Value,
) -> io::Result<()> {
    let body = serde_json::to_vec(value).map_err(io::Error::other)?;
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        body.len()
    );
    writer.write_all(header.as_bytes()).await?;
    if !body.is_empty() {
        writer.write_all(&body).await?;
    }
    writer.flush().await
}

async fn write_error_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    err: &HttpError,
) -> io::Result<()> {
    write_json_response(writer, err.status, &err.body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_loopback_host_origin_sec_fetch_site_and_csrf_guards() {
        let mut hdrs = RequestHeaders {
            host: Some("127.0.0.1:7782".into()),
            ..RequestHeaders::default()
        };
        assert!(validate_security_headers(HttpMethod::Get, &hdrs).is_ok());

        assert_eq!(
            validate_security_headers(HttpMethod::Post, &hdrs)
                .unwrap_err()
                .status,
            "415 Unsupported Media Type"
        );

        hdrs.content_type = Some("application/json; charset=utf-8".into());
        assert!(validate_security_headers(HttpMethod::Post, &hdrs).is_ok());

        hdrs.content_type = None;
        hdrs.x_rex_wave = Some("1".into());
        assert!(validate_security_headers(HttpMethod::Patch, &hdrs).is_ok());

        hdrs.sec_fetch_site = Some("cross-site".into());
        assert_eq!(
            validate_security_headers(HttpMethod::Get, &hdrs)
                .unwrap_err()
                .status,
            "403 Forbidden"
        );

        hdrs.sec_fetch_site = Some("same-origin".into());
        assert!(validate_security_headers(HttpMethod::Get, &hdrs).is_ok());

        hdrs.sec_fetch_site = Some("none".into());
        assert!(validate_security_headers(HttpMethod::Get, &hdrs).is_ok());
    }
}
