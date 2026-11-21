//! Effect RPC JSON WebSocket + REST client for T3 Code 0.1.11.
//!
//! Speaks T3's Effect RPC framing (`Request`, `Interrupt`, `Ack`, `Ping`/`Pong`,
//! `Exit`, `Chunk`) over `/ws?orchestrationProtocol=2&wsTicket=<ticket>`,
//! minting a fresh WebSocket ticket on every connect/reconnect.

use crate::discovery::{T3Config, T3Origin, TokenCache};
use crate::transport::{HttpResponse, WsConn, WsInbound, http_request};
use rex_wave_waves::HostError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// Client-to-server Effect RPC frame.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "_tag")]
pub enum RpcClientFrame {
    Request {
        id: String,
        tag: String,
        payload: Value,
        headers: Vec<(String, String)>,
    },
    Interrupt {
        #[serde(rename = "requestId")]
        request_id: String,
    },
    Ack {
        #[serde(rename = "requestId")]
        request_id: String,
    },
}

/// Server-to-client Effect RPC frame.
#[derive(Clone, Debug, PartialEq)]
pub enum RpcServerFrame {
    Exit {
        request_id: String,
        exit: RpcExit,
    },
    Chunk {
        request_id: String,
        values: Vec<Value>,
    },
    Pong,
    Defect {
        defect: Value,
    },
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RpcExit {
    Success(Value),
    Failure(Value),
}

pub fn decode_server_frames(raw: &str) -> Result<Vec<RpcServerFrame>, serde_json::Error> {
    let val: Value = serde_json::from_str(raw)?;
    match val {
        Value::Array(items) => Ok(items.into_iter().map(parse_server_frame_value).collect()),
        single => Ok(vec![parse_server_frame_value(single)]),
    }
}

fn parse_server_frame_value(v: Value) -> RpcServerFrame {
    let Some(tag) = v.get("_tag").and_then(|t| t.as_str()) else {
        return RpcServerFrame::Unknown;
    };
    match tag {
        "Exit" => {
            let req_id = v
                .get("requestId")
                .and_then(value_as_req_id)
                .unwrap_or_default();
            let exit_obj = v.get("exit").cloned().unwrap_or(Value::Null);
            let exit = match exit_obj.get("_tag").and_then(|t| t.as_str()) {
                Some("Success") => {
                    RpcExit::Success(exit_obj.get("value").cloned().unwrap_or(Value::Null))
                }
                _ => RpcExit::Failure(exit_obj.get("cause").cloned().unwrap_or(exit_obj)),
            };
            RpcServerFrame::Exit {
                request_id: req_id,
                exit,
            }
        }
        "Chunk" => {
            let req_id = v
                .get("requestId")
                .and_then(value_as_req_id)
                .unwrap_or_default();
            let values = v
                .get("values")
                .and_then(|arr| arr.as_array())
                .cloned()
                .unwrap_or_default();
            RpcServerFrame::Chunk {
                request_id: req_id,
                values,
            }
        }
        "Pong" => RpcServerFrame::Pong,
        "Defect" => RpcServerFrame::Defect {
            defect: v.get("defect").cloned().unwrap_or(Value::Null),
        },
        _ => RpcServerFrame::Unknown,
    }
}

fn value_as_req_id(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        Some(s.to_owned())
    } else {
        v.as_u64().map(|n| n.to_string())
    }
}

pub fn format_effect_cause(cause: &Value) -> String {
    if let Some(s) = cause.as_str() {
        return s.to_owned();
    }
    if let Some(arr) = cause.as_array() {
        for item in arr {
            if let Some(msg) = extract_cause_error_message(item) {
                return msg;
            }
        }
    }
    if let Some(msg) = extract_cause_error_message(cause) {
        return msg;
    }
    cause.to_string()
}

fn extract_cause_error_message(v: &Value) -> Option<String> {
    if let Some(msg) = v.get("message").and_then(|m| m.as_str())
        && !msg.is_empty()
    {
        return Some(msg.to_owned());
    }
    if let Some(err) = v.get("error") {
        if let Some(msg) = extract_cause_error_message(err) {
            return Some(msg);
        }
        if let Some(s) = err.as_str() {
            return Some(s.to_owned());
        }
    }
    if let Some(defect) = v.get("defect") {
        if let Some(msg) = extract_cause_error_message(defect) {
            return Some(msg);
        }
        if let Some(s) = defect.as_str() {
            return Some(s.to_owned());
        }
    }
    if let Some(failures) = v.get("failures").and_then(|f| f.as_array()) {
        for f in failures {
            if let Some(msg) = extract_cause_error_message(f) {
                return Some(msg);
            }
        }
    }
    if let Some(errors) = v.get("errors").and_then(|e| e.as_array()) {
        for e in errors {
            if let Some(msg) = extract_cause_error_message(e) {
                return Some(msg);
            }
        }
    }
    if let Some(tag) = v.get("_tag").and_then(|t| t.as_str())
        && !matches!(tag, "Fail" | "Die" | "Cause" | "Sequential" | "Parallel")
    {
        return Some(tag.to_owned());
    }
    None
}

#[derive(Debug)]
pub enum StreamEvent {
    Values(Vec<Value>),
    Reconnected,
    Closed(Option<HostError>),
}

pub struct RpcStreamLease {
    lease_id: u64,
    owner: mpsc::UnboundedSender<RpcOwnerMsg>,
    armed: bool,
}

impl Drop for RpcStreamLease {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.owner.send(RpcOwnerMsg::CloseStream {
                lease_id: self.lease_id,
            });
        }
    }
}

pub struct RpcStream {
    pub rx: mpsc::UnboundedReceiver<StreamEvent>,
    lease: RpcStreamLease,
}

impl RpcStream {
    pub fn into_parts(self) -> (mpsc::UnboundedReceiver<StreamEvent>, RpcStreamLease) {
        (self.rx, self.lease)
    }

    pub fn update_resume_payload(&self, payload: Value) {
        let _ = self.lease.owner.send(RpcOwnerMsg::UpdateStreamPayload {
            lease_id: self.lease.lease_id,
            payload,
        });
    }
}

impl RpcStreamLease {
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    pub fn update_resume_payload(&self, payload: Value) {
        let _ = self.owner.send(RpcOwnerMsg::UpdateStreamPayload {
            lease_id: self.lease_id,
            payload,
        });
    }
}

enum RpcOwnerMsg {
    Connected {
        tx: mpsc::UnboundedSender<RpcClientFrame>,
        reply: oneshot::Sender<u64>,
    },
    Disconnected {
        conn_id: u64,
    },
    WaitConnected {
        reply: oneshot::Sender<()>,
    },
    Call {
        tag: String,
        payload: Value,
        started: oneshot::Sender<
            Result<(String, oneshot::Receiver<Result<Value, HostError>>), HostError>,
        >,
    },
    CancelCall {
        request_id: String,
    },
    OpenStream {
        tag: String,
        payload: Value,
        reply: oneshot::Sender<(u64, mpsc::UnboundedReceiver<StreamEvent>)>,
    },
    UpdateStreamPayload {
        lease_id: u64,
        payload: Value,
    },
    CloseStream {
        lease_id: u64,
    },
    Inbound {
        conn_id: u64,
        frame: RpcServerFrame,
    },
}

struct ActiveStream {
    tag: String,
    payload: Value,
    current_req_id: Option<String>,
    tx: mpsc::UnboundedSender<StreamEvent>,
}

struct RpcOwner {
    conn_id: u64,
    tx_out: Option<mpsc::UnboundedSender<RpcClientFrame>>,
    conn_waiters: Vec<oneshot::Sender<()>>,
    next_req_id: u64,
    next_lease_id: u64,
    pending_calls: HashMap<String, oneshot::Sender<Result<Value, HostError>>>,
    streams: HashMap<u64, ActiveStream>,
    lease_by_req: HashMap<String, u64>,
}

impl RpcOwner {
    fn spawn() -> mpsc::UnboundedSender<RpcOwnerMsg> {
        let (tx, rx) = mpsc::unbounded_channel();
        let owner = RpcOwner {
            conn_id: 0,
            tx_out: None,
            conn_waiters: Vec::new(),
            next_req_id: 0,
            next_lease_id: 0,
            pending_calls: HashMap::new(),
            streams: HashMap::new(),
            lease_by_req: HashMap::new(),
        };
        tokio::spawn(owner.run(rx));
        tx
    }

    fn alloc_req_id(&mut self) -> String {
        self.next_req_id += 1;
        self.next_req_id.to_string()
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<RpcOwnerMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                RpcOwnerMsg::Connected { tx, reply } => {
                    self.conn_id += 1;
                    let id = self.conn_id;
                    self.tx_out = Some(tx);
                    self.lease_by_req.clear();

                    let mut dead = Vec::new();
                    let lease_ids: Vec<u64> = self.streams.keys().copied().collect();
                    for lease_id in lease_ids {
                        let pending_req = match self.streams.get(&lease_id) {
                            Some(entry) if entry.tx.is_closed() => {
                                dead.push(lease_id);
                                None
                            }
                            Some(entry) if entry.current_req_id.is_none() => {
                                Some((entry.tag.clone(), entry.payload.clone()))
                            }
                            _ => None,
                        };
                        if let Some((tag, payload)) = pending_req {
                            let req_id = self.alloc_req_id();
                            let frame = RpcClientFrame::Request {
                                id: req_id.clone(),
                                tag,
                                payload,
                                headers: Vec::new(),
                            };
                            if let Some(out) = &self.tx_out
                                && out.send(frame).is_ok()
                                && let Some(entry) = self.streams.get_mut(&lease_id)
                            {
                                entry.current_req_id = Some(req_id.clone());
                                self.lease_by_req.insert(req_id, lease_id);
                                let _ = entry.tx.send(StreamEvent::Reconnected);
                            }
                        }
                    }
                    for lease_id in dead {
                        self.streams.remove(&lease_id);
                    }

                    let _ = reply.send(id);
                    for waiter in self.conn_waiters.drain(..) {
                        let _ = waiter.send(());
                    }
                }
                RpcOwnerMsg::Disconnected { conn_id } => {
                    if self.conn_id != conn_id {
                        continue;
                    }
                    self.tx_out = None;
                    self.lease_by_req.clear();
                    for (_, waiter) in self.pending_calls.drain() {
                        let _ = waiter.send(Err(HostError::Unreachable(
                            "T3 WebSocket disconnected".into(),
                        )));
                    }
                    for entry in self.streams.values_mut() {
                        entry.current_req_id = None;
                    }
                }
                RpcOwnerMsg::WaitConnected { reply } => {
                    if self.tx_out.is_some() {
                        let _ = reply.send(());
                    } else {
                        self.conn_waiters.push(reply);
                    }
                }
                RpcOwnerMsg::Call {
                    tag,
                    payload,
                    started,
                } => {
                    let req_id = self.alloc_req_id();
                    let Some(out) = &self.tx_out else {
                        let _ = started.send(Err(HostError::Unreachable(
                            "T3 WebSocket is not connected".into(),
                        )));
                        continue;
                    };
                    let frame = RpcClientFrame::Request {
                        id: req_id.clone(),
                        tag,
                        payload,
                        headers: Vec::new(),
                    };
                    if out.send(frame).is_err() {
                        self.tx_out = None;
                        let _ = started.send(Err(HostError::Unreachable(
                            "T3 WebSocket write channel closed".into(),
                        )));
                    } else {
                        let (res_tx, res_rx) = oneshot::channel();
                        self.pending_calls.insert(req_id.clone(), res_tx);
                        let _ = started.send(Ok((req_id, res_rx)));
                    }
                }
                RpcOwnerMsg::CancelCall { request_id } => {
                    if self.pending_calls.remove(&request_id).is_some()
                        && let Some(out) = &self.tx_out
                    {
                        let _ = out.send(RpcClientFrame::Interrupt { request_id });
                    }
                }
                RpcOwnerMsg::OpenStream {
                    tag,
                    payload,
                    reply,
                } => {
                    self.next_lease_id += 1;
                    let lease_id = self.next_lease_id;
                    let (ev_tx, ev_rx) = mpsc::unbounded_channel();
                    let mut current_req_id = None;
                    if self.tx_out.is_some() {
                        let req_id = self.alloc_req_id();
                        let frame = RpcClientFrame::Request {
                            id: req_id.clone(),
                            tag: tag.clone(),
                            payload: payload.clone(),
                            headers: Vec::new(),
                        };
                        if let Some(out) = &self.tx_out
                            && out.send(frame).is_ok()
                        {
                            current_req_id = Some(req_id.clone());
                            self.lease_by_req.insert(req_id, lease_id);
                        }
                    }
                    self.streams.insert(
                        lease_id,
                        ActiveStream {
                            tag,
                            payload,
                            current_req_id,
                            tx: ev_tx,
                        },
                    );
                    let _ = reply.send((lease_id, ev_rx));
                }
                RpcOwnerMsg::UpdateStreamPayload { lease_id, payload } => {
                    if let Some(entry) = self.streams.get_mut(&lease_id) {
                        entry.payload = payload;
                    }
                }
                RpcOwnerMsg::CloseStream { lease_id } => {
                    if let Some(entry) = self.streams.remove(&lease_id)
                        && let Some(req_id) = entry.current_req_id
                    {
                        self.lease_by_req.remove(&req_id);
                        if let Some(out) = &self.tx_out {
                            let _ = out.send(RpcClientFrame::Interrupt { request_id: req_id });
                        }
                    }
                }
                RpcOwnerMsg::Inbound { conn_id, frame } => {
                    if self.conn_id != conn_id {
                        continue;
                    }
                    self.handle_inbound(frame);
                }
            }
        }
    }

    fn handle_inbound(&mut self, frame: RpcServerFrame) {
        match frame {
            RpcServerFrame::Pong | RpcServerFrame::Unknown => {}
            RpcServerFrame::Chunk { request_id, values } => {
                if let Some(&lease_id) = self.lease_by_req.get(&request_id)
                    && let Some(entry) = self.streams.get(&lease_id)
                {
                    if entry.tx.send(StreamEvent::Values(values)).is_ok() {
                        if let Some(out) = &self.tx_out {
                            let _ = out.send(RpcClientFrame::Ack { request_id });
                        }
                    } else {
                        self.lease_by_req.remove(&request_id);
                        self.streams.remove(&lease_id);
                        if let Some(out) = &self.tx_out {
                            let _ = out.send(RpcClientFrame::Interrupt { request_id });
                        }
                    }
                }
            }
            RpcServerFrame::Exit { request_id, exit } => {
                if let Some(waiter) = self.pending_calls.remove(&request_id) {
                    let res = match exit {
                        RpcExit::Success(val) => Ok(val),
                        RpcExit::Failure(cause) => {
                            Err(HostError::Rejected(format_effect_cause(&cause)))
                        }
                    };
                    let _ = waiter.send(res);
                    return;
                }
                if let Some(lease_id) = self.lease_by_req.remove(&request_id)
                    && let Some(entry) = self.streams.remove(&lease_id)
                {
                    let err = match exit {
                        RpcExit::Success(_) => None,
                        RpcExit::Failure(cause) => {
                            Some(HostError::Rejected(format_effect_cause(&cause)))
                        }
                    };
                    let _ = entry.tx.send(StreamEvent::Closed(err));
                }
            }
            RpcServerFrame::Defect { defect } => {
                let msg = format!("T3 RPC defect: {}", format_effect_cause(&defect));
                for (_, waiter) in self.pending_calls.drain() {
                    let _ = waiter.send(Err(HostError::Unreachable(msg.clone())));
                }
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsTicketReply {
    ticket: String,
}

/// Shared T3 Code HTTP + Effect RPC WebSocket client.
#[derive(Clone)]
pub struct T3RpcClient {
    cfg: Arc<T3Config>,
    tokens: TokenCache,
    owner: mpsc::UnboundedSender<RpcOwnerMsg>,
    connected_tx: Arc<watch::Sender<bool>>,
    connected_rx: watch::Receiver<bool>,
}

impl T3RpcClient {
    pub fn new(cfg: T3Config) -> Self {
        let tokens = TokenCache::new(cfg.auth.clone());
        let (connected_tx, connected_rx) = watch::channel(false);
        T3RpcClient {
            cfg: Arc::new(cfg),
            tokens,
            owner: RpcOwner::spawn(),
            connected_tx: Arc::new(connected_tx),
            connected_rx,
        }
    }

    pub fn config(&self) -> &T3Config {
        &self.cfg
    }

    pub fn connected(&self) -> bool {
        *self.connected_rx.borrow()
    }

    pub fn watch_connected(&self) -> watch::Receiver<bool> {
        self.connected_rx.clone()
    }

    pub fn resolve_origin(&self) -> Result<T3Origin, HostError> {
        self.cfg.endpoint.resolve()
    }

    pub async fn run_until(&self, mut shutdown: watch::Receiver<bool>) {
        let mut backoff = self.cfg.reconnect_min;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let dial = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                    continue;
                }
                res = self.dial_ws() => res,
            };
            match dial {
                Ok(ws_conn) => {
                    backoff = self.cfg.reconnect_min;
                    self.serve_connection(ws_conn, &mut shutdown).await;
                }
                Err(_) => {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() { break; }
                        }
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(self.cfg.reconnect_max);
                }
            }
        }
    }

    async fn dial_ws(&self) -> Result<WsConn<tokio::net::TcpStream>, HostError> {
        let origin = self.resolve_origin()?;
        let (token, ticket) = self.mint_ws_ticket(&origin).await?;
        WsConn::connect(&origin, &ticket, &token, self.cfg.connect_timeout).await
    }

    async fn mint_ws_ticket(&self, origin: &T3Origin) -> Result<(String, String), HostError> {
        let mut attempts = 0u8;
        loop {
            attempts += 1;
            let token = self.tokens.bearer_token(self.cfg.rpc_timeout).await?;
            let resp = http_request(
                origin,
                "POST",
                "/api/auth/websocket-ticket",
                Some(&token),
                None,
                self.cfg.rpc_timeout,
            )
            .await?;
            if resp.status == 401 && attempts == 1 && self.tokens.can_refresh() {
                self.tokens.invalidate().await;
                continue;
            }
            if !(200..300).contains(&resp.status) {
                return Err(http_status_error("POST /api/auth/websocket-ticket", &resp));
            }
            let parsed: WsTicketReply = serde_json::from_slice(&resp.body).map_err(|e| {
                HostError::Unreachable(format!("decode websocket-ticket reply: {e}"))
            })?;
            return Ok((token, parsed.ticket));
        }
    }

    async fn serve_connection(
        &self,
        ws_conn: WsConn<tokio::net::TcpStream>,
        shutdown: &mut watch::Receiver<bool>,
    ) {
        let (mut ws_reader, mut ws_writer) = ws_conn.into_split();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<RpcClientFrame>();
        let (conn_tx, conn_rx) = oneshot::channel();
        if self
            .owner
            .send(RpcOwnerMsg::Connected {
                tx: out_tx,
                reply: conn_tx,
            })
            .is_err()
        {
            return;
        }
        let Ok(conn_id) = conn_rx.await else { return };
        let _ = self.connected_tx.send(true);

        let (in_tx, mut in_rx) = mpsc::channel::<WsInbound>(128);
        let reader_task = tokio::spawn(async move {
            while let Ok(Some(item)) = ws_reader.recv().await {
                if in_tx.send(item).await.is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = ws_writer.send_close_reply(&[]).await;
                        break;
                    }
                }
                Some(out_frame) = out_rx.recv() => {
                    let Ok(text) = serde_json::to_string(&out_frame) else { continue };
                    if ws_writer.send_text(&text).await.is_err() {
                        break;
                    }
                }
                inbound = in_rx.recv() => match inbound {
                    Some(WsInbound::Text(raw)) => {
                        if let Ok(frames) = decode_server_frames(&raw) {
                            let mut fatal_defect = false;
                            for frame in frames {
                                if matches!(frame, RpcServerFrame::Defect { .. }) {
                                    fatal_defect = true;
                                }
                                let _ = self.owner.send(RpcOwnerMsg::Inbound { conn_id, frame });
                            }
                            if fatal_defect {
                                break;
                            }
                        }
                    }
                    Some(WsInbound::Ping(payload)) => {
                        if ws_writer.send_pong(&payload).await.is_err() {
                            break;
                        }
                    }
                    Some(WsInbound::Close(reply)) => {
                        let _ = ws_writer.send_close_reply(&reply).await;
                        break;
                    }
                    None => break,
                }
            }
        }

        reader_task.abort();
        let _ = self.connected_tx.send(false);
        let _ = self.owner.send(RpcOwnerMsg::Disconnected { conn_id });
    }

    pub async fn ensure_connected(&self) -> Result<(), HostError> {
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(RpcOwnerMsg::WaitConnected { reply: tx })
            .map_err(|_| HostError::Unreachable("T3 RPC client stopped".into()))?;
        tokio::time::timeout(self.cfg.connect_timeout, rx)
            .await
            .map_err(|_| HostError::Unreachable("timed out waiting for T3 WebSocket".into()))?
            .map_err(|_| HostError::Unreachable("T3 RPC client stopped".into()))
    }

    pub async fn call(&self, tag: &str, payload: Value) -> Result<Value, HostError> {
        self.call_with_timeout(tag, payload, self.cfg.rpc_timeout)
            .await
    }

    pub async fn call_with_timeout(
        &self,
        tag: &str,
        payload: Value,
        timeout: Duration,
    ) -> Result<Value, HostError> {
        self.ensure_connected().await?;
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(RpcOwnerMsg::Call {
                tag: tag.to_owned(),
                payload,
                started: tx,
            })
            .map_err(|_| HostError::Unreachable("T3 RPC client stopped".into()))?;
        let (req_id, res_rx) = rx
            .await
            .map_err(|_| HostError::Unreachable("T3 RPC client dropped call".into()))??;
        match tokio::time::timeout(timeout, res_rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(HostError::Unreachable("T3 RPC call dropped".into())),
            Err(_) => {
                let _ = self
                    .owner
                    .send(RpcOwnerMsg::CancelCall { request_id: req_id });
                Err(HostError::Unreachable(format!(
                    "T3 RPC {tag} timed out after {}ms",
                    timeout.as_millis()
                )))
            }
        }
    }

    pub async fn open_stream(&self, tag: &str, payload: Value) -> Result<RpcStream, HostError> {
        let (tx, rx) = oneshot::channel();
        self.owner
            .send(RpcOwnerMsg::OpenStream {
                tag: tag.to_owned(),
                payload,
                reply: tx,
            })
            .map_err(|_| HostError::Unreachable("T3 RPC client stopped".into()))?;
        let (lease_id, ev_rx) = rx
            .await
            .map_err(|_| HostError::Unreachable("T3 RPC client dropped stream".into()))?;
        Ok(RpcStream {
            rx: ev_rx,
            lease: RpcStreamLease {
                lease_id,
                owner: self.owner.clone(),
                armed: true,
            },
        })
    }

    pub async fn http_get_json(&self, path: &str) -> Result<Value, HostError> {
        self.http_json("GET", path, None).await
    }

    pub async fn http_post_json(&self, path: &str, body: &Value) -> Result<Value, HostError> {
        let encoded = serde_json::to_vec(body)
            .map_err(|e| HostError::Rejected(format!("encode JSON body for {path}: {e}")))?;
        self.http_json("POST", path, Some(&encoded)).await
    }

    async fn http_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<Value, HostError> {
        let origin = self.resolve_origin()?;
        let mut attempts = 0u8;
        loop {
            attempts += 1;
            let token = self.tokens.bearer_token(self.cfg.rpc_timeout).await?;
            let resp = http_request(
                &origin,
                method,
                path,
                Some(&token),
                body,
                self.cfg.rpc_timeout,
            )
            .await?;
            if resp.status == 401 && attempts == 1 && self.tokens.can_refresh() {
                self.tokens.invalidate().await;
                continue;
            }
            if !(200..300).contains(&resp.status) {
                return Err(http_status_error(&format!("{method} {path}"), &resp));
            }
            if resp.body.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_slice(&resp.body)
                .map_err(|e| HostError::Unreachable(format!("decode {method} {path} JSON: {e}")));
        }
    }
}

fn http_status_error(label: &str, resp: &HttpResponse) -> HostError {
    let snippet = String::from_utf8_lossy(&resp.body);
    let trimmed = snippet.trim();
    let msg = if trimmed.is_empty() {
        format!("{label} returned HTTP {}", resp.status)
    } else {
        format!("{label} returned HTTP {}: {trimmed}", resp.status)
    };
    if (400..500).contains(&resp.status) && resp.status != 408 && resp.status != 429 {
        HostError::Rejected(msg)
    } else {
        HostError::Unreachable(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_and_decodes_effect_rpc_frames() {
        let req = RpcClientFrame::Request {
            id: "7".into(),
            tag: "orchestration.launchThread".into(),
            payload: serde_json::json!({"threadId": "t3:w:r:a1"}),
            headers: vec![],
        };
        let s = serde_json::to_string(&req).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["_tag"], "Request");
        assert_eq!(v["id"], "7");
        assert_eq!(v["tag"], "orchestration.launchThread");
        assert_eq!(v["headers"], serde_json::json!([]));

        let int = RpcClientFrame::Interrupt {
            request_id: "7".into(),
        };
        assert_eq!(
            serde_json::to_value(&int).unwrap(),
            serde_json::json!({"_tag": "Interrupt", "requestId": "7"})
        );

        let frames = decode_server_frames(
            r#"[{"_tag":"Chunk","requestId":"3","values":[{"kind":"synchronized"}]},{"_tag":"Exit","requestId":"4","exit":{"_tag":"Success","value":{"ok":true}}}]"#,
        )
        .unwrap();
        assert_eq!(frames.len(), 2);
        assert!(
            matches!(&frames[0], RpcServerFrame::Chunk { request_id, values } if request_id == "3" && values.len() == 1)
        );
        assert!(
            matches!(&frames[1], RpcServerFrame::Exit { request_id, exit: RpcExit::Success(_) } if request_id == "4")
        );
    }
}
