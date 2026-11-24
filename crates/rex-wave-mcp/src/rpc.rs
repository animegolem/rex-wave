use crate::resources::{self, ResourceError};
use crate::{McpServer, PROTOCOL_VERSION};
use rex_wave_api::http_guard::{
    is_json_content_type, is_loopback_host, is_loopback_origin, strip_bearer,
};
use rex_wave_api::{ApiError, Client};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MAX_HEADER_LINES: usize = 64;

const FORMAT_LINE: u8 = 0;
const FORMAT_CONTENT_LENGTH: u8 = 1;

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const RESOURCE_NOT_FOUND: i64 = -32002;
const SERVER_ERROR: i64 = -32000;

enum FrameInput {
    Json(Value),
    ParseError(String),
}

enum ParsedRpc {
    Notification {
        method: String,
        params: Option<Value>,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Invalid(Value),
}

pub(crate) async fn serve_stream<C, R, W>(
    server: Arc<McpServer<C>>,
    reader: R,
    mut writer: W,
) -> io::Result<()>
where
    C: Client + Clone + 'static,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let frame_mode = Arc::new(AtomicU8::new(FORMAT_LINE));
    let writer_mode = frame_mode.clone();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Value>();
    let (in_tx, mut in_rx) = mpsc::channel::<io::Result<(FrameInput, u8)>>(32);

    let reader_task = tokio::spawn(async move {
        let mut buf_reader = BufReader::new(reader);
        loop {
            match read_next_frame(&mut buf_reader).await {
                Ok(Some(item)) => {
                    if in_tx.send(Ok(item)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    let _ = in_tx.send(Err(err)).await;
                    break;
                }
            }
        }
    });

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            write_framed(&mut writer, &msg, writer_mode.load(Ordering::Relaxed)).await?;
        }
        Ok::<(), io::Error>(())
    });

    let mut subscriptions: HashMap<String, AbortHandle> = HashMap::new();
    let mut in_flight_ids: HashMap<String, AbortHandle> = HashMap::new();
    let mut rpc_tasks: JoinSet<(Option<String>, tokio::task::Id)> = JoinSet::new();

    let read_res = loop {
        tokio::select! {
            biased;
            Some(done) = rpc_tasks.join_next(), if !rpc_tasks.is_empty() => {
                if let Ok((Some(req_key), task_id)) = done
                    && in_flight_ids.get(&req_key).is_some_and(|h| h.id() == task_id)
                {
                    in_flight_ids.remove(&req_key);
                }
            }
            incoming = in_rx.recv() => {
                match incoming {
                    Some(Ok((frame, mode))) => {
                        frame_mode.store(mode, Ordering::Relaxed);
                        match frame {
                            FrameInput::ParseError(detail) => {
                                let _ = out_tx.send(rpc_error(Value::Null, PARSE_ERROR, format!("parse error: {detail}")));
                            }
                            FrameInput::Json(payload) => {
                                dispatch_single_stream_msg(
                                    &server,
                                    payload,
                                    &out_tx,
                                    &mut subscriptions,
                                    &mut in_flight_ids,
                                    &mut rpc_tasks,
                                )
                                .await;
                            }
                        }
                    }
                    Some(Err(err)) => break Err(err),
                    None => break Ok(()),
                }
            }
        }
    };

    reader_task.abort();
    while let Some(done) = rpc_tasks.join_next().await {
        if let Ok((Some(req_key), task_id)) = done
            && in_flight_ids
                .get(&req_key)
                .is_some_and(|h| h.id() == task_id)
        {
            in_flight_ids.remove(&req_key);
        }
    }
    for (_, handle) in subscriptions {
        handle.abort();
    }
    drop(out_tx);
    let writer_res = writer_task
        .await
        .unwrap_or_else(|e| Err(io::Error::other(e)));

    read_res.and(writer_res)
}

pub(crate) async fn serve_http_listener<C>(
    server: Arc<McpServer<C>>,
    listener: TcpListener,
) -> io::Result<()>
where
    C: Client + Clone + 'static,
{
    loop {
        let (stream, _) = listener.accept().await?;
        let srv = server.clone();
        tokio::spawn(async move {
            let _ = handle_http_conn(srv, stream).await;
        });
    }
}

async fn handle_http_conn<C>(server: Arc<McpServer<C>>, stream: TcpStream) -> io::Result<()>
where
    C: Client + Clone + 'static,
{
    let (reader_half, mut writer_half) = stream.into_split();
    let mut reader = BufReader::new(reader_half);

    let mut req_line = Vec::new();
    let n = read_bounded_line(&mut reader, &mut req_line).await?;
    if n == 0 {
        return Ok(());
    }
    let req_str = std::str::from_utf8(&req_line)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .trim_end_matches(['\r', '\n']);
    let mut parts = req_str.split_whitespace();
    let method = parts.next().unwrap_or_default();

    let mut content_length: usize = 0;
    let mut content_type: Option<String> = None;
    let mut origin: Option<String> = None;
    let mut host: Option<String> = None;
    let mut authorization: Option<String> = None;
    let mut saw_blank = false;

    for _ in 0..MAX_HEADER_LINES {
        let mut hdr = Vec::new();
        let hn = read_bounded_line(&mut reader, &mut hdr).await?;
        if hn == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated HTTP headers",
            ));
        }
        let line = std::str::from_utf8(&hdr)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            saw_blank = true;
            break;
        }
        if let Some((name, val)) = line.split_once(':') {
            let key = name.trim();
            let value = val.trim();
            if key.eq_ignore_ascii_case("content-length") {
                content_length = value
                    .parse::<usize>()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if content_length > MAX_FRAME_BYTES {
                    return write_http_response(
                        &mut writer_half,
                        "413 Payload Too Large",
                        "text/plain",
                        b"payload exceeds 16 MiB",
                    )
                    .await;
                }
            } else if key.eq_ignore_ascii_case("content-type") {
                content_type = Some(value.to_owned());
            } else if key.eq_ignore_ascii_case("origin") {
                origin = Some(value.to_owned());
            } else if key.eq_ignore_ascii_case("host") {
                host = Some(value.to_owned());
            } else if key.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.to_owned());
            }
        }
    }
    if !saw_blank {
        return write_http_response(
            &mut writer_half,
            "431 Request Header Fields Too Large",
            "text/plain",
            b"too many headers",
        )
        .await;
    }

    if let Some(ref orig) = origin
        && !is_loopback_origin(orig)
    {
        return write_http_response(
            &mut writer_half,
            "403 Forbidden",
            "text/plain",
            b"forbidden Origin",
        )
        .await;
    }
    if let Some(ref h) = host
        && !is_loopback_host(h)
    {
        return write_http_response(
            &mut writer_half,
            "403 Forbidden",
            "text/plain",
            b"forbidden Host",
        )
        .await;
    }
    if let Some(ref expected) = server.http_token {
        let ok = authorization
            .as_deref()
            .and_then(strip_bearer)
            .is_some_and(|got| constant_time_eq(got.as_bytes(), expected.as_bytes()));
        if !ok {
            return write_http_response(
                &mut writer_half,
                "401 Unauthorized",
                "text/plain",
                b"bearer token required",
            )
            .await;
        }
    }

    if method != "POST" {
        return write_http_response(
            &mut writer_half,
            "405 Method Not Allowed",
            "text/plain",
            b"POST required",
        )
        .await;
    }
    if !content_type.as_deref().is_some_and(is_json_content_type) {
        return write_http_response(
            &mut writer_half,
            "415 Unsupported Media Type",
            "text/plain",
            b"application/json required",
        )
        .await;
    }

    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).await?;

    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            let err_body = serde_json::to_vec(&rpc_error(
                Value::Null,
                PARSE_ERROR,
                format!("parse error: {e}"),
            ))
            .map_err(io::Error::other)?;
            return write_http_response(&mut writer_half, "200 OK", "application/json", &err_body)
                .await;
        }
    };

    match handle_stateless_payload(&server, payload).await {
        Some(resp) => {
            let resp_bytes = serde_json::to_vec(&resp).map_err(io::Error::other)?;
            write_http_response(&mut writer_half, "200 OK", "application/json", &resp_bytes).await
        }
        None => {
            write_http_response(&mut writer_half, "202 Accepted", "application/json", b"").await
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn write_http_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    writer.write_all(header.as_bytes()).await?;
    if !body.is_empty() {
        writer.write_all(body).await?;
    }
    writer.flush().await
}

fn parse_rpc_message(msg: Value) -> ParsedRpc {
    let Value::Object(mut obj) = msg else {
        return ParsedRpc::Invalid(rpc_error(
            Value::Null,
            INVALID_REQUEST,
            "JSON-RPC message must be a single object",
        ));
    };
    let id_opt = obj.remove("id");
    let has_id = id_opt.is_some();
    let id = id_opt.unwrap_or(Value::Null);
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return ParsedRpc::Invalid(rpc_error(id, INVALID_REQUEST, "jsonrpc must equal \"2.0\""));
    }
    let Some(Value::String(method)) = obj.remove("method") else {
        return ParsedRpc::Invalid(rpc_error(id, INVALID_REQUEST, "missing string method"));
    };
    let raw_params = obj.remove("params");
    if !has_id {
        return ParsedRpc::Notification {
            method,
            params: raw_params,
        };
    }
    match raw_params {
        None | Some(Value::Null) => ParsedRpc::Request {
            id,
            method,
            params: json!({}),
        },
        Some(v @ Value::Object(_)) => ParsedRpc::Request {
            id,
            method,
            params: v,
        },
        Some(_) => ParsedRpc::Invalid(rpc_error(
            id,
            INVALID_PARAMS,
            "MCP request params must be a JSON object",
        )),
    }
}

async fn dispatch_single_stream_msg<C>(
    server: &Arc<McpServer<C>>,
    msg: Value,
    out_tx: &mpsc::UnboundedSender<Value>,
    subscriptions: &mut HashMap<String, AbortHandle>,
    in_flight_ids: &mut HashMap<String, AbortHandle>,
    rpc_tasks: &mut JoinSet<(Option<String>, tokio::task::Id)>,
) where
    C: Client + Clone + 'static,
{
    let (id, method, params) = match parse_rpc_message(msg) {
        ParsedRpc::Invalid(err) => {
            let _ = out_tx.send(err);
            return;
        }
        ParsedRpc::Notification { method, params } => {
            if let Some(p) = &params {
                handle_notification(&method, p, in_flight_ids);
            }
            return;
        }
        ParsedRpc::Request { id, method, params } => (id, method, params),
    };

    if matches!(method.as_str(), "tools/call" | "resources/read") {
        let req_key = request_key(&id);
        let reg_key = req_key.clone();
        let srv = server.clone();
        let tx = out_tx.clone();
        let abort = rpc_tasks.spawn(async move {
            let resp = execute_request_inline(&srv, id, &method, params, None).await;
            let _ = tx.send(resp);
            (req_key, tokio::task::id())
        });
        if let Some(key) = reg_key
            && let Some(prev) = in_flight_ids.insert(key, abort)
        {
            prev.abort();
        }
        return;
    }

    let resp =
        execute_request_inline(server, id, &method, params, Some((out_tx, subscriptions))).await;
    let _ = out_tx.send(resp);
}

async fn handle_stateless_payload<C>(server: &Arc<McpServer<C>>, payload: Value) -> Option<Value>
where
    C: Client + Clone + 'static,
{
    match parse_rpc_message(payload) {
        ParsedRpc::Invalid(err) => Some(err),
        ParsedRpc::Notification { .. } => None,
        ParsedRpc::Request { id, method, params } => {
            Some(execute_request_inline(server, id, &method, params, None).await)
        }
    }
}

fn handle_notification(
    method: &str,
    params: &Value,
    in_flight_ids: &mut HashMap<String, AbortHandle>,
) {
    if method == "notifications/cancelled"
        && let Some(key) = request_key_ref(params.get("requestId"))
        && let Some(handle) = in_flight_ids.remove(&key)
    {
        handle.abort();
    }
}

async fn execute_request_inline<C>(
    server: &McpServer<C>,
    id: Value,
    method: &str,
    params: Value,
    mut sub_ctx: Option<(
        &mpsc::UnboundedSender<Value>,
        &mut HashMap<String, AbortHandle>,
    )>,
) -> Value
where
    C: Client,
{
    match method {
        "initialize" => rpc_ok(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": { "listChanged": false },
                    "resources": {
                        "subscribe": sub_ctx.is_some(),
                        "listChanged": false,
                    }
                },
                "serverInfo": {
                    "name": "rex-wave-mcp",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }),
        ),
        "ping" => rpc_ok(id, json!({})),
        "tools/list" => match serde_json::to_value(server.tools()) {
            Ok(tools) => rpc_ok(id, json!({ "tools": tools })),
            Err(e) => rpc_error(id, SERVER_ERROR, e.to_string()),
        },
        "tools/call" => {
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return rpc_error(id, INVALID_PARAMS, "tools/call requires string params.name");
            };
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let res = server.call_tool(name, arguments).await;
            match serde_json::to_value(res) {
                Ok(v) => rpc_ok(id, v),
                Err(e) => rpc_error(id, SERVER_ERROR, e.to_string()),
            }
        }
        "resources/list" => match resources::list_resources(&server.client).await {
            Ok(val) => rpc_ok(id, val),
            Err(err) => rpc_api_error(id, err),
        },
        "resources/templates/list" => rpc_ok(id, resources::resource_templates()),
        "resources/read" => {
            let Some(uri) = params.get("uri").and_then(Value::as_str) else {
                return rpc_error(
                    id,
                    INVALID_PARAMS,
                    "resources/read requires string params.uri",
                );
            };
            match resources::read_resource(&server.client, uri).await {
                Ok(val) => rpc_ok(id, val),
                Err(err) => rpc_resource_error(id, err),
            }
        }
        "resources/subscribe" => {
            let Some((out_tx, subscriptions)) = sub_ctx.as_mut() else {
                return rpc_error(
                    id,
                    METHOD_NOT_FOUND,
                    "resources/subscribe requires a streaming MCP session",
                );
            };
            let Some(uri) = params.get("uri").and_then(Value::as_str) else {
                return rpc_error(
                    id,
                    INVALID_PARAMS,
                    "resources/subscribe requires string params.uri",
                );
            };
            if subscriptions.get(uri).is_some_and(|h| !h.is_finished()) {
                return rpc_ok(id, json!({}));
            }
            match resources::subscribe_resource(&server.client, uri.to_owned(), (*out_tx).clone())
                .await
            {
                Ok(handle) => {
                    subscriptions.insert(uri.to_owned(), handle);
                    rpc_ok(id, json!({}))
                }
                Err(err) => rpc_resource_error(id, err),
            }
        }
        "resources/unsubscribe" => {
            let Some((_, subscriptions)) = sub_ctx.as_mut() else {
                return rpc_error(
                    id,
                    METHOD_NOT_FOUND,
                    "resources/unsubscribe requires a streaming MCP session",
                );
            };
            let Some(uri) = params.get("uri").and_then(Value::as_str) else {
                return rpc_error(
                    id,
                    INVALID_PARAMS,
                    "resources/unsubscribe requires string params.uri",
                );
            };
            if let Some(handle) = subscriptions.remove(uri) {
                handle.abort();
            }
            rpc_ok(id, json!({}))
        }
        other => rpc_error(id, METHOD_NOT_FOUND, format!("method not found: {other}")),
    }
}

fn rpc_ok(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message.into(),
        }
    })
}

fn rpc_resource_error(id: Value, err: ResourceError) -> Value {
    match err {
        ResourceError::InvalidUri(msg) => rpc_error(id, INVALID_PARAMS, msg),
        ResourceError::Api(api) => rpc_api_error(id, api),
    }
}

fn rpc_api_error(id: Value, err: ApiError) -> Value {
    let code = match &err {
        ApiError::Invalid(_) => INVALID_PARAMS,
        ApiError::NotFound { .. } => RESOURCE_NOT_FOUND,
        _ => SERVER_ERROR,
    };
    rpc_error(id, code, err.to_string())
}

fn request_key(id: &Value) -> Option<String> {
    match id {
        Value::String(s) => Some(format!("s:{s}")),
        Value::Number(n) => Some(format!("n:{n}")),
        _ => None,
    }
}

fn request_key_ref(id: Option<&Value>) -> Option<String> {
    id.and_then(request_key)
}

async fn read_next_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<(FrameInput, u8)>> {
    let mut line_buf = Vec::new();
    loop {
        line_buf.clear();
        let n = read_bounded_line(reader, &mut line_buf).await?;
        if n == 0 {
            return Ok(None);
        }
        let line = std::str::from_utf8(&line_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .trim_matches([' ', '\t', '\r', '\n']);
        if line.is_empty() {
            continue;
        }
        if let Some((head, tail)) = line.split_once(':')
            && head.trim().eq_ignore_ascii_case("content-length")
        {
            let len = tail
                .trim()
                .parse::<usize>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            if len > MAX_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "MCP frame exceeds 16 MiB",
                ));
            }
            let mut saw_blank = false;
            for _ in 0..MAX_HEADER_LINES {
                let mut hdr = Vec::new();
                let hn = read_bounded_line(reader, &mut hdr).await?;
                if hn == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "EOF inside Content-Length frame headers",
                    ));
                }
                let trimmed = std::str::from_utf8(&hdr)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .trim_end_matches(['\r', '\n']);
                if trimmed.is_empty() {
                    saw_blank = true;
                    break;
                }
            }
            if !saw_blank {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "too many Content-Length frame headers",
                ));
            }
            let mut body = vec![0u8; len];
            reader.read_exact(&mut body).await?;
            let parsed = match serde_json::from_slice::<Value>(&body) {
                Ok(v) => FrameInput::Json(v),
                Err(e) => FrameInput::ParseError(e.to_string()),
            };
            return Ok(Some((parsed, FORMAT_CONTENT_LENGTH)));
        }

        let parsed = match serde_json::from_str::<Value>(line) {
            Ok(v) => FrameInput::Json(v),
            Err(e) => FrameInput::ParseError(e.to_string()),
        };
        return Ok(Some((parsed, FORMAT_LINE)));
    }
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    out: &mut Vec<u8>,
) -> io::Result<usize> {
    let mut total = 0usize;
    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            return Ok(total);
        }
        if let Some(nl_pos) = buf.iter().position(|&b| b == b'\n') {
            let take = nl_pos + 1;
            if total + take > MAX_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "MCP line exceeds 16 MiB",
                ));
            }
            out.extend_from_slice(&buf[..take]);
            reader.consume(take);
            return Ok(total + take);
        }
        let len = buf.len();
        if total + len > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP line exceeds 16 MiB",
            ));
        }
        out.extend_from_slice(buf);
        reader.consume(len);
        total += len;
    }
}

async fn write_framed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &Value,
    mode: u8,
) -> io::Result<()> {
    let bytes =
        serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if mode == FORMAT_CONTENT_LENGTH {
        let header = format!("Content-Length: {}\r\n\r\n", bytes.len());
        writer.write_all(header.as_bytes()).await?;
        writer.write_all(&bytes).await?;
    } else {
        writer.write_all(&bytes).await?;
        writer.write_all(b"\n").await?;
    }
    writer.flush().await
}
