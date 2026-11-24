//! Pure-Tokio HTTP/1.1 client and RFC 6455 WebSocket framing over loopback TCP.

use crate::discovery::{ORCHESTRATION_PROTOCOL_VERSION, T3Origin};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rex_wave_waves::HostError;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// RFC 6455 requires SHA-1 solely for the `Sec-WebSocket-Accept` handshake digest.
pub(crate) fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x6745_2301;
    let mut h1: u32 = 0xEFCD_AB89;
    let mut h2: u32 = 0x98BA_DCFE;
    let mut h3: u32 = 0x1032_5476;
    let mut h4: u32 = 0xC3D2_E1F0;

    let bit_len = (data.len() as u64) * 8;
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

pub fn ws_accept(client_key: &str) -> String {
    let mut combined = Vec::with_capacity(client_key.len() + WS_GUID.len());
    combined.extend_from_slice(client_key.as_bytes());
    combined.extend_from_slice(WS_GUID.as_bytes());
    BASE64.encode(sha1(&combined))
}

fn next_entropy_block() -> [u8; 32] {
    let seq = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = {
        use std::hash::{BuildHasher as _, Hasher as _};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(seq);
        h.finish()
    };
    let mut hasher = Sha256::new();
    hasher.update(seq.to_le_bytes());
    hasher.update(nanos.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.finalize().into()
}

fn next_mask_bytes() -> [u8; 4] {
    let block = next_entropy_block();
    [block[0], block[1], block[2], block[3]]
}

fn next_ws_key() -> String {
    let block = next_entropy_block();
    BASE64.encode(&block[..16])
}

#[derive(Debug)]
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

pub(crate) async fn http_request(
    origin: &T3Origin,
    method: &str,
    path: &str,
    bearer_token: Option<&str>,
    json_body: Option<&[u8]>,
    timeout: Duration,
) -> Result<HttpResponse, HostError> {
    tokio::time::timeout(
        timeout,
        http_request_inner(origin, method, path, bearer_token, json_body),
    )
    .await
    .map_err(|_| HostError::Unreachable(format!("T3 HTTP {method} {path} timed out")))?
}

async fn http_request_inner(
    origin: &T3Origin,
    method: &str,
    path: &str,
    bearer_token: Option<&str>,
    json_body: Option<&[u8]>,
) -> Result<HttpResponse, HostError> {
    if path.contains(['\r', '\n']) {
        return Err(HostError::Rejected("HTTP path contains CR/LF".into()));
    }
    if bearer_token.is_some_and(|t| t.contains(['\r', '\n'])) {
        return Err(HostError::Rejected("T3 bearer token contains CR/LF".into()));
    }
    let authority = origin.authority();
    let mut stream = TcpStream::connect(&authority)
        .await
        .map_err(|e| HostError::Unreachable(format!("connect T3 HTTP {authority}: {e}")))?;
    let _ = stream.set_nodelay(true);

    let mut req = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {authority}\r\n\
         Connection: close\r\n\
         Accept: application/json\r\n\
         x-t3-orchestration-protocol: {ORCHESTRATION_PROTOCOL_VERSION}\r\n"
    );
    if let Some(token) = bearer_token {
        req.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    if let Some(body) = json_body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    } else if method == "POST" {
        req.push_str("Content-Length: 0\r\n");
    }
    req.push_str("\r\n");

    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| HostError::Unreachable(format!("T3 HTTP write headers: {e}")))?;
    if let Some(body) = json_body {
        stream
            .write_all(body)
            .await
            .map_err(|e| HostError::Unreachable(format!("T3 HTTP write body: {e}")))?;
    }
    stream
        .flush()
        .await
        .map_err(|e| HostError::Unreachable(format!("T3 HTTP flush: {e}")))?;

    let (header_bytes, leftover) = read_http_headers(&mut stream).await?;
    let header_str = std::str::from_utf8(&header_bytes)
        .map_err(|_| HostError::Unreachable("non-UTF8 T3 HTTP response headers".into()))?;
    let mut lines = header_str.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            HostError::Unreachable(format!("invalid T3 HTTP status line: {status_line}"))
        })?;

    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    for line in lines {
        if let Some((name, val)) = line.split_once(':') {
            let name = name.trim();
            let val = val.trim();
            if name.eq_ignore_ascii_case("content-length") {
                if let Ok(len) = val.parse::<usize>() {
                    content_length = Some(len);
                }
            } else if name.eq_ignore_ascii_case("transfer-encoding")
                && val
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("chunked"))
            {
                chunked = true;
            }
        }
    }

    let body = if chunked {
        read_chunked_body(&mut stream, leftover).await?
    } else if let Some(len) = content_length {
        if len > MAX_BODY_BYTES {
            return Err(HostError::Unreachable(format!(
                "T3 HTTP body exceeds {MAX_BODY_BYTES} bytes"
            )));
        }
        read_exact_body(&mut stream, leftover, len).await?
    } else {
        read_eof_body(&mut stream, leftover).await?
    };

    Ok(HttpResponse { status, body })
}

async fn read_exact_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    mut buf: Vec<u8>,
    len: usize,
) -> Result<Vec<u8>, HostError> {
    if buf.len() >= len {
        buf.truncate(len);
        return Ok(buf);
    }
    let start = buf.len();
    buf.resize(len, 0);
    stream
        .read_exact(&mut buf[start..])
        .await
        .map_err(|e| HostError::Unreachable(format!("T3 HTTP body read: {e}")))?;
    Ok(buf)
}

async fn read_eof_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    mut buf: Vec<u8>,
) -> Result<Vec<u8>, HostError> {
    let mut scratch = [0u8; 4096];
    loop {
        let n = stream
            .read(&mut scratch)
            .await
            .map_err(|e| HostError::Unreachable(format!("T3 HTTP body read: {e}")))?;
        if n == 0 {
            return Ok(buf);
        }
        if buf.len().saturating_add(n) > MAX_BODY_BYTES {
            return Err(HostError::Unreachable(format!(
                "T3 HTTP body exceeds {MAX_BODY_BYTES} bytes"
            )));
        }
        buf.extend_from_slice(&scratch[..n]);
    }
}

async fn read_chunked_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    mut raw: Vec<u8>,
) -> Result<Vec<u8>, HostError> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    let mut scratch = [0u8; 4096];

    loop {
        let line_end = loop {
            if let Some(rel) = raw[pos..].windows(2).position(|w| w == b"\r\n") {
                break pos + rel;
            }
            let n = stream
                .read(&mut scratch)
                .await
                .map_err(|e| HostError::Unreachable(format!("T3 HTTP chunk header read: {e}")))?;
            if n == 0 {
                return Err(HostError::Unreachable(
                    "unexpected EOF in T3 HTTP chunked body".into(),
                ));
            }
            raw.extend_from_slice(&scratch[..n]);
            if raw.len() - pos > MAX_HEADER_BYTES {
                return Err(HostError::Unreachable(
                    "T3 HTTP chunk header too large".into(),
                ));
            }
        };

        let size_line = std::str::from_utf8(&raw[pos..line_end])
            .map_err(|_| HostError::Unreachable("invalid UTF-8 in chunk size".into()))?;
        let hex_part = size_line.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(hex_part, 16)
            .map_err(|_| HostError::Unreachable(format!("invalid chunk size: {hex_part}")))?;
        pos = line_end + 2;

        if chunk_len == 0 {
            return Ok(out);
        }
        if out.len().saturating_add(chunk_len) > MAX_BODY_BYTES {
            return Err(HostError::Unreachable(format!(
                "T3 HTTP chunked body exceeds {MAX_BODY_BYTES} bytes"
            )));
        }

        let needed = chunk_len + 2;
        while raw.len() - pos < needed {
            let n = stream
                .read(&mut scratch)
                .await
                .map_err(|e| HostError::Unreachable(format!("T3 HTTP chunk body read: {e}")))?;
            if n == 0 {
                return Err(HostError::Unreachable(
                    "unexpected EOF in T3 HTTP chunk payload".into(),
                ));
            }
            raw.extend_from_slice(&scratch[..n]);
        }

        out.extend_from_slice(&raw[pos..pos + chunk_len]);
        if &raw[pos + chunk_len..pos + chunk_len + 2] != b"\r\n" {
            return Err(HostError::Unreachable(
                "missing CRLF after HTTP chunk payload".into(),
            ));
        }
        pos += needed;

        if pos > 64 * 1024 {
            raw.drain(..pos);
            pos = 0;
        }
    }
}

#[derive(Debug)]
pub(crate) enum WsInbound {
    Text(String),
    Ping(Vec<u8>),
    Close(Vec<u8>),
}

pub(crate) struct WsConn<S> {
    stream: S,
    leftover: Vec<u8>,
    mask_outbound: bool,
}

pub(crate) struct WsReader<R> {
    reader: R,
    read_buf: Vec<u8>,
    read_pos: usize,
    frag_buf: Vec<u8>,
    frag_active: bool,
}

pub(crate) struct WsWriter<W> {
    writer: W,
    mask_outbound: bool,
    closed: bool,
}

impl WsConn<TcpStream> {
    pub(crate) async fn connect(
        origin: &T3Origin,
        ws_ticket: &str,
        bearer_token: &str,
        timeout: Duration,
    ) -> Result<Self, HostError> {
        tokio::time::timeout(
            timeout,
            Self::connect_inner(origin, ws_ticket, bearer_token),
        )
        .await
        .map_err(|_| HostError::Unreachable("T3 WebSocket handshake timed out".into()))?
    }

    async fn connect_inner(
        origin: &T3Origin,
        ws_ticket: &str,
        bearer_token: &str,
    ) -> Result<Self, HostError> {
        if bearer_token.contains(['\r', '\n']) {
            return Err(HostError::Rejected("T3 bearer token contains CR/LF".into()));
        }
        let authority = origin.authority();
        let encoded_ticket = crate::discovery::percent_encode_query(ws_ticket);
        let path = format!(
            "/ws?orchestrationProtocol={ORCHESTRATION_PROTOCOL_VERSION}&wsTicket={encoded_ticket}"
        );
        let mut stream = TcpStream::connect(&authority)
            .await
            .map_err(|e| HostError::Unreachable(format!("connect T3 WS {authority}: {e}")))?;
        let _ = stream.set_nodelay(true);

        let key = next_ws_key();
        let req = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: {authority}\r\n\
             Origin: {}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Authorization: Bearer {bearer_token}\r\n\
             x-t3-orchestration-protocol: {ORCHESTRATION_PROTOCOL_VERSION}\r\n\
             \r\n",
            origin.origin
        );
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| HostError::Unreachable(format!("T3 WS handshake write: {e}")))?;
        stream
            .flush()
            .await
            .map_err(|e| HostError::Unreachable(format!("T3 WS handshake flush: {e}")))?;

        let (header_bytes, leftover) = read_http_headers(&mut stream).await?;
        let header_str = std::str::from_utf8(&header_bytes)
            .map_err(|_| HostError::Unreachable("invalid UTF-8 in WS upgrade response".into()))?;
        let mut lines = header_str.split("\r\n");
        let status = lines.next().unwrap_or("");
        if status.split_whitespace().nth(1) != Some("101") {
            return Err(HostError::Unreachable(format!(
                "unexpected T3 WS upgrade status: {status}"
            )));
        }

        let expected_accept = ws_accept(&key);
        let mut accept_ok = false;
        for line in lines {
            if let Some((name, val)) = line.split_once(':')
                && name.trim().eq_ignore_ascii_case("sec-websocket-accept")
                && val.trim() == expected_accept
            {
                accept_ok = true;
            }
        }
        if !accept_ok {
            return Err(HostError::Unreachable(
                "missing or invalid Sec-WebSocket-Accept from T3".into(),
            ));
        }

        Ok(WsConn {
            stream,
            leftover,
            mask_outbound: true,
        })
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsConn<S> {
    pub(crate) fn into_split(self) -> (WsReader<ReadHalf<S>>, WsWriter<WriteHalf<S>>) {
        let (reader, writer) = tokio::io::split(self.stream);
        (
            WsReader {
                reader,
                read_buf: self.leftover,
                read_pos: 0,
                frag_buf: Vec::new(),
                frag_active: false,
            },
            WsWriter {
                writer,
                mask_outbound: self.mask_outbound,
                closed: false,
            },
        )
    }
}

impl<R: AsyncRead + Unpin> WsReader<R> {
    pub(crate) async fn recv(&mut self) -> Result<Option<WsInbound>, HostError> {
        loop {
            let mut hdr = [0u8; 2];
            if !self.read_exact_or_eof(&mut hdr).await? {
                if self.frag_active {
                    return Err(HostError::Unreachable(
                        "unexpected EOF during fragmented websocket message".into(),
                    ));
                }
                return Ok(None);
            }
            let fin = (hdr[0] & 0x80) != 0;
            let opcode = hdr[0] & 0x0F;
            let masked = (hdr[1] & 0x80) != 0;
            let len7 = (hdr[1] & 0x7F) as usize;

            let payload_len = match len7 {
                126 => {
                    let mut ext = [0u8; 2];
                    self.read_exact_bytes(&mut ext).await?;
                    u16::from_be_bytes(ext) as usize
                }
                127 => {
                    let mut ext = [0u8; 8];
                    self.read_exact_bytes(&mut ext).await?;
                    let n = u64::from_be_bytes(ext);
                    usize::try_from(n)
                        .map_err(|_| HostError::Unreachable("frame too large".into()))?
                }
                n => n,
            };
            if payload_len > MAX_FRAME_BYTES {
                return Err(HostError::Unreachable(format!(
                    "frame exceeds {MAX_FRAME_BYTES} bytes"
                )));
            }

            let mask_key = if masked {
                let mut key = [0u8; 4];
                self.read_exact_bytes(&mut key).await?;
                Some(key)
            } else {
                None
            };

            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                self.read_exact_bytes(&mut payload).await?;
                if let Some(key) = mask_key {
                    for (i, b) in payload.iter_mut().enumerate() {
                        *b ^= key[i & 3];
                    }
                }
            }

            match opcode {
                0x8 => {
                    let reply = if payload.len() >= 2 {
                        payload[..2].to_vec()
                    } else {
                        Vec::new()
                    };
                    return Ok(Some(WsInbound::Close(reply)));
                }
                0x9 => return Ok(Some(WsInbound::Ping(payload))),
                0xA => {}
                0x1 | 0x2 if !self.frag_active => {
                    if fin {
                        return String::from_utf8(payload)
                            .map(|s| Some(WsInbound::Text(s)))
                            .map_err(|_| {
                                HostError::Unreachable("non-UTF8 websocket frame".into())
                            });
                    }
                    self.frag_active = true;
                    self.frag_buf.clear();
                    self.frag_buf.extend_from_slice(&payload);
                }
                0x0 if self.frag_active => {
                    if self.frag_buf.len().saturating_add(payload.len()) > MAX_FRAME_BYTES {
                        return Err(HostError::Unreachable(format!(
                            "fragmented message exceeds {MAX_FRAME_BYTES} bytes"
                        )));
                    }
                    self.frag_buf.extend_from_slice(&payload);
                    if fin {
                        self.frag_active = false;
                        let bytes = std::mem::take(&mut self.frag_buf);
                        return String::from_utf8(bytes)
                            .map(|s| Some(WsInbound::Text(s)))
                            .map_err(|_| {
                                HostError::Unreachable(
                                    "non-UTF8 fragmented websocket message".into(),
                                )
                            });
                    }
                }
                other => {
                    return Err(HostError::Unreachable(format!(
                        "unsupported websocket opcode {other}"
                    )));
                }
            }
        }
    }

    async fn read_exact_or_eof(&mut self, dst: &mut [u8]) -> Result<bool, HostError> {
        let mut filled = 0;
        while filled < dst.len() {
            let buffered = self.read_buf.len() - self.read_pos;
            if buffered > 0 {
                let take = buffered.min(dst.len() - filled);
                dst[filled..filled + take]
                    .copy_from_slice(&self.read_buf[self.read_pos..self.read_pos + take]);
                self.read_pos += take;
                if self.read_pos == self.read_buf.len() {
                    self.read_buf.clear();
                    self.read_pos = 0;
                }
                filled += take;
            } else {
                let n = self
                    .reader
                    .read(&mut dst[filled..])
                    .await
                    .map_err(|e| HostError::Unreachable(format!("websocket read: {e}")))?;
                if n == 0 {
                    if filled == 0 {
                        return Ok(false);
                    }
                    return Err(HostError::Unreachable("unexpected EOF mid-frame".into()));
                }
                filled += n;
            }
        }
        Ok(true)
    }

    async fn read_exact_bytes(&mut self, dst: &mut [u8]) -> Result<(), HostError> {
        if !self.read_exact_or_eof(dst).await? {
            return Err(HostError::Unreachable("unexpected EOF".into()));
        }
        Ok(())
    }
}

impl<W: AsyncWrite + Unpin> WsWriter<W> {
    pub(crate) async fn send_text(&mut self, text: &str) -> Result<(), HostError> {
        self.write_frame(0x1, text.as_bytes()).await
    }

    pub(crate) async fn send_pong(&mut self, payload: &[u8]) -> Result<(), HostError> {
        self.write_frame(0xA, payload).await
    }

    pub(crate) async fn send_close_reply(&mut self, reply: &[u8]) -> Result<(), HostError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.write_frame(0x8, reply).await
    }

    async fn write_frame(&mut self, opcode: u8, payload: &[u8]) -> Result<(), HostError> {
        let mask_bit = if self.mask_outbound { 0x80u8 } else { 0u8 };
        let mut frame = Vec::with_capacity(14 + payload.len());
        frame.push(0x80 | (opcode & 0x0F));

        let len = payload.len();
        if len < 126 {
            frame.push(mask_bit | (len as u8));
        } else if len <= u16::MAX as usize {
            frame.push(mask_bit | 126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            frame.push(mask_bit | 127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }

        if self.mask_outbound {
            let key = next_mask_bytes();
            frame.extend_from_slice(&key);
            for (i, &b) in payload.iter().enumerate() {
                frame.push(b ^ key[i & 3]);
            }
        } else {
            frame.extend_from_slice(payload);
        }

        self.writer
            .write_all(&frame)
            .await
            .map_err(|e| HostError::Unreachable(format!("websocket write: {e}")))?;
        self.writer
            .flush()
            .await
            .map_err(|e| HostError::Unreachable(format!("websocket flush: {e}")))
    }
}

async fn read_http_headers<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<(Vec<u8>, Vec<u8>), HostError> {
    let mut buf = Vec::with_capacity(512);
    let mut scratch = [0u8; 1024];
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let leftover = buf[pos + 4..].to_vec();
            buf.truncate(pos);
            return Ok((buf, leftover));
        }
        if buf.len() >= MAX_HEADER_BYTES {
            return Err(HostError::Unreachable(
                "HTTP headers exceeded 32 KiB".into(),
            ));
        }
        let n = stream
            .read(&mut scratch)
            .await
            .map_err(|e| HostError::Unreachable(format!("HTTP header read: {e}")))?;
        if n == 0 {
            return Err(HostError::Unreachable("EOF during HTTP headers".into()));
        }
        buf.extend_from_slice(&scratch[..n]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc6455_accept_vector() {
        assert_eq!(
            ws_accept("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[tokio::test]
    async fn decodes_chunked_http_response() {
        let wire = b"4\r\n{\"ok\r\n7\r\n\":true}\r\n0\r\n\r\n";
        let mut cursor = std::io::Cursor::new(&wire[..]);
        let body = read_chunked_body(&mut cursor, Vec::new()).await.unwrap();
        assert_eq!(body, br#"{"ok":true}"#);
    }
}
