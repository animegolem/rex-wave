use crate::HostError;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

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
            let temp = a.rotate_left(5).wrapping_add(f).wrapping_add(e).wrapping_add(k).wrapping_add(wi);
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

pub(crate) fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub(crate) fn ws_accept(client_key: &str) -> String {
    let mut combined = Vec::with_capacity(client_key.len() + WS_GUID.len());
    combined.extend_from_slice(client_key.as_bytes());
    combined.extend_from_slice(WS_GUID.as_bytes());
    base64_encode(&sha1(&combined))
}

pub(crate) fn parse_ws_url(url: &str) -> Result<(String, String), HostError> {
    let rest = url
        .strip_prefix("ws://")
        .ok_or_else(|| HostError::Rejected(format!("unsupported bridge URL scheme (expected ws://): {url}")))?;
    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return Err(HostError::Rejected(format!("missing host:port in bridge URL: {url}")));
    }
    Ok((authority.to_owned(), path.to_owned()))
}

fn next_mask_bytes() -> [u8; 4] {
    let seq = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mixed = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ nanos.rotate_left(17) ^ 0xA5A5_5A5A_C3C3_3C3C;
    (mixed as u32).to_be_bytes()
}

fn next_ws_key() -> String {
    let mut raw = [0u8; 16];
    for chunk in raw.chunks_exact_mut(4) {
        chunk.copy_from_slice(&next_mask_bytes());
    }
    base64_encode(&raw)
}

pub(crate) struct WsConn<S> {
    stream: S,
    read_buf: Vec<u8>,
    read_pos: usize,
    mask_outbound: bool,
    closed: bool,
}

impl WsConn<TcpStream> {
    pub(crate) async fn connect(url: &str, token: &str) -> Result<Self, HostError> {
        let (authority, path) = parse_ws_url(url)?;
        let mut stream = TcpStream::connect(&authority)
            .await
            .map_err(|e| HostError::Unreachable(format!("connect {authority}: {e}")))?;
        let _ = stream.set_nodelay(true);

        let key = next_ws_key();
        let req = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: {authority}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             Authorization: Bearer {token}\r\n\
             \r\n"
        );
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| HostError::Unreachable(format!("handshake write: {e}")))?;

        let (header_bytes, leftover) = read_http_headers(&mut stream).await?;
        let header_str = std::str::from_utf8(&header_bytes)
            .map_err(|_| HostError::Unreachable("invalid UTF-8 in HTTP upgrade response".into()))?;
        let mut lines = header_str.split("\r\n");
        let status = lines.next().unwrap_or("");
        if !status.starts_with("HTTP/1.1 101") {
            return Err(HostError::Unreachable(format!("unexpected upgrade status: {status}")));
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
            return Err(HostError::Unreachable("missing or invalid Sec-WebSocket-Accept".into()));
        }

        Ok(WsConn {
            stream,
            read_buf: leftover,
            read_pos: 0,
            mask_outbound: true,
            closed: false,
        })
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> WsConn<S> {
    #[cfg(test)]
    pub(crate) async fn accept(mut stream: S, expected_token: &str) -> Result<Self, HostError> {
        let (header_bytes, leftover) = read_http_headers(&mut stream).await?;
        let header_str = std::str::from_utf8(&header_bytes)
            .map_err(|_| HostError::Rejected("invalid UTF-8 in upgrade request".into()))?;
        let mut ws_key: Option<String> = None;
        let mut auth_ok = expected_token.is_empty();

        for line in header_str.split("\r\n").skip(1) {
            if let Some((name, val)) = line.split_once(':') {
                let name = name.trim();
                let val = val.trim();
                if name.eq_ignore_ascii_case("sec-websocket-key") {
                    ws_key = Some(val.to_owned());
                } else if name.eq_ignore_ascii_case("authorization")
                    && val.strip_prefix("Bearer ").map(str::trim) == Some(expected_token)
                {
                    auth_ok = true;
                }
            }
        }
        if !auth_ok {
            let _ = stream.write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n").await;
            return Err(HostError::Rejected("unauthorized token".into()));
        }
        let key = ws_key.ok_or_else(|| HostError::Rejected("missing Sec-WebSocket-Key".into()))?;
        let accept = ws_accept(&key);
        let resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n\
             \r\n"
        );
        stream
            .write_all(resp.as_bytes())
            .await
            .map_err(|e| HostError::Unreachable(format!("accept write: {e}")))?;

        Ok(WsConn {
            stream,
            read_buf: leftover,
            read_pos: 0,
            mask_outbound: false,
            closed: false,
        })
    }

    pub(crate) async fn send_text(&mut self, text: &str) -> Result<(), HostError> {
        self.write_frame(0x1, text.as_bytes()).await
    }

    #[cfg(test)]
    pub(crate) async fn send_close(&mut self, code: u16, reason: &str) -> Result<(), HostError> {
        let mut payload = Vec::with_capacity(2 + reason.len());
        payload.extend_from_slice(&code.to_be_bytes());
        payload.extend_from_slice(reason.as_bytes());
        self.closed = true;
        self.write_frame(0x8, &payload).await
    }

    pub(crate) async fn recv_text(&mut self) -> Result<Option<String>, HostError> {
        let mut message = Vec::new();
        let mut fragmenting = false;

        loop {
            let mut hdr = [0u8; 2];
            if !self.read_exact_or_eof(&mut hdr).await? {
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
                    usize::try_from(n).map_err(|_| HostError::Unreachable("frame too large".into()))?
                }
                n => n,
            };
            if payload_len > MAX_FRAME_BYTES {
                return Err(HostError::Unreachable(format!("frame exceeds {MAX_FRAME_BYTES} bytes")));
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
                    if !self.closed {
                        self.closed = true;
                        let reply = if payload.len() >= 2 { &payload[..2] } else { &[] };
                        let _ = self.write_frame(0x8, reply).await;
                    }
                    return Ok(None);
                }
                0x9 => {
                    self.write_frame(0xA, &payload).await?;
                }
                0xA => {}
                0x1 | 0x2 => {
                    if fin && !fragmenting {
                        return String::from_utf8(payload)
                            .map(Some)
                            .map_err(|_| HostError::Unreachable("non-UTF8 websocket frame".into()));
                    }
                    fragmenting = true;
                    message.extend_from_slice(&payload);
                    if fin {
                        return String::from_utf8(message)
                            .map(Some)
                            .map_err(|_| HostError::Unreachable("non-UTF8 fragmented websocket message".into()));
                    }
                }
                0x0 if fragmenting => {
                    message.extend_from_slice(&payload);
                    if fin {
                        return String::from_utf8(message)
                            .map(Some)
                            .map_err(|_| HostError::Unreachable("non-UTF8 fragmented websocket message".into()));
                    }
                }
                other => return Err(HostError::Unreachable(format!("unsupported websocket opcode {other}"))),
            }
        }
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

        self.stream
            .write_all(&frame)
            .await
            .map_err(|e| HostError::Unreachable(format!("websocket write: {e}")))?;
        self.stream
            .flush()
            .await
            .map_err(|e| HostError::Unreachable(format!("websocket flush: {e}")))
    }

    async fn read_exact_or_eof(&mut self, dst: &mut [u8]) -> Result<bool, HostError> {
        let mut filled = 0;
        while filled < dst.len() {
            let buffered = self.read_buf.len() - self.read_pos;
            if buffered > 0 {
                let take = buffered.min(dst.len() - filled);
                dst[filled..filled + take].copy_from_slice(&self.read_buf[self.read_pos..self.read_pos + take]);
                self.read_pos += take;
                if self.read_pos == self.read_buf.len() {
                    self.read_buf.clear();
                    self.read_pos = 0;
                }
                filled += take;
            } else {
                let n = self
                    .stream
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

async fn read_http_headers<S: AsyncRead + Unpin>(stream: &mut S) -> Result<(Vec<u8>, Vec<u8>), HostError> {
    let mut buf = Vec::with_capacity(512);
    let mut scratch = [0u8; 1024];
    loop {
        if let Some(pos) = find_header_end(&buf) {
            let leftover = buf[pos + 4..].to_vec();
            buf.truncate(pos);
            return Ok((buf, leftover));
        }
        if buf.len() >= MAX_HEADER_BYTES {
            return Err(HostError::Unreachable("HTTP upgrade headers exceeded 16 KiB".into()));
        }
        let n = stream
            .read(&mut scratch)
            .await
            .map_err(|e| HostError::Unreachable(format!("upgrade read: {e}")))?;
        if n == 0 {
            return Err(HostError::Unreachable("EOF during HTTP upgrade".into()));
        }
        buf.extend_from_slice(&scratch[..n]);
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}
