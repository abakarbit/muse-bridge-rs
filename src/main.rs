//! muse-bridge-rs — OpenAI-compatible bridge with a worker-pull job queue.
//!
//! Rust rewrite of `bridge.py` v5.1. A client (PicoClaw, 9Router, …) POSTs a
//! standard `/v1/chat/completions` request; the bridge stores it as a job in
//! SQLite and holds the HTTP connection (long-poll). A worker polls
//! `GET /muse/pending`, answers via `POST /muse/answer`, and the waiting
//! client receives an OpenAI-style response. The worker only ever makes
//! *outbound* calls, so the Muse side needs no inbound port/API.

// Handlers return `Response` as the `Err` variant on purpose (the idiomatic
// axum pattern for early HTTP error returns); boxing it would only obscure
// the control flow, so the size lint is allowed crate-wide.
#![allow(clippy::result_large_err)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command as ProcCommand,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use clap::{Parser, Subcommand};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::info;

// ---------------------------------------------------------------- config ---

#[derive(Clone)]
struct Config {
    db_path: PathBuf,
    port: u16,
    lease_secs: i64,
    wait_secs: u64,
    max_pending: i64,
    pending_ttl_secs: i64,
    failed_ttl_secs: i64,
    tls_cert: Option<String>,
    tls_key: Option<String>,
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    fn from_env() -> Self {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let default_db = format!("{}/workspace/muse-bridge-rs/bridge.db", home);
        Self {
            db_path: PathBuf::from(std::env::var("BRIDGE_DB").unwrap_or(default_db)),
            port: env_parse("BRIDGE_PORT", 8765u16),
            lease_secs: env_parse("BRIDGE_LEASE_SECS", 180i64),
            wait_secs: env_parse("BRIDGE_WAIT_SECS", 240u64),
            max_pending: env_parse("BRIDGE_MAX_PENDING", 5i64),
            pending_ttl_secs: env_parse("BRIDGE_PENDING_TTL_SECS", 1800i64),
            failed_ttl_secs: env_parse("BRIDGE_FAILED_TTL_SECS", 86400i64),
            tls_cert: std::env::var("BRIDGE_TLS_CERT")
                .ok()
                .filter(|s| !s.is_empty()),
            tls_key: std::env::var("BRIDGE_TLS_KEY")
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }
}

// ------------------------------------------------------------------ state ---

/// Events a waiting /v1/chat/completions call can receive from the worker.
#[derive(Debug)]
enum JobEvent {
    /// Incremental text; the job stays `processing`.
    Chunk(String),
    /// Final authoritative answer; the job is `done`.
    Answer(String),
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    /// job id -> channel that wakes the long-polling /v1/chat/completions call
    waiters: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<JobEvent>>>>,
    /// job id -> answered_at epoch (duplicate-answer detection after pickup)
    recent_done: Arc<Mutex<HashMap<String, i64>>>,
    /// Prometheus-style counters (Fase 2).
    lease_expiries: Arc<AtomicU64>,
    answers: Arc<AtomicU64>,
    timeouts: Arc<AtomicU64>,
    chunks: Arc<AtomicU64>,
    cfg: Config,
}

// ------------------------------------------------------------------ utils ---

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().map(|b| format!("{:02x}", b)).collect()
}

/// ISO-8601 UTC timestamp, e.g. 2026-10-11T20:55:01Z (Howard Hinnant's algorithm).
fn iso8601_now() -> String {
    let secs = now_epoch();
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m,
        d,
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

fn err_json(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": {"message": msg}}))).into_response()
}

fn completion_json(answer: &str) -> Value {
    let cid = format!(
        "chatcmpl-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    json!({
        "id": cid,
        "object": "chat.completion",
        "created": now_epoch(),
        "model": "muse",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": answer},
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    })
}

/// Detect the 9Router dashboard "Test Connection" probe (kept for compatibility).
fn is_dashboard_probe(v: &Value) -> bool {
    if v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
        return false;
    }
    if v.get("max_tokens").and_then(|m| m.as_u64()) != Some(1024) {
        return false;
    }
    let msgs = match v.get("messages").and_then(|m| m.as_array()) {
        Some(a) => a,
        None => return false,
    };
    let last = match msgs.last() {
        Some(l) => l,
        None => return false,
    };
    last.get("role").and_then(|r| r.as_str()) == Some("user")
        && last
            .get("content")
            .and_then(|c| c.as_str())
            .map(|s| s.trim().to_lowercase())
            == Some("hi".to_string())
}

fn probe_completion() -> Value {
    completion_json("Halo! Muse online — bridge 9Router aktif dan siap.")
}

// --------------------------------------------------------------------- db ---

fn init_db(path: &Path) -> rusqlite::Result<Connection> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).ok();
        }
    }
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS jobs (
            id          TEXT PRIMARY KEY,
            state       TEXT NOT NULL,
            request     TEXT NOT NULL,
            created_at  INTEGER NOT NULL,
            lease_by    TEXT,
            lease_until INTEGER,
            answer      TEXT,
            answered_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state, created_at);
        CREATE TABLE IF NOT EXISTS keys (
            key_hash TEXT PRIMARY KEY,
            role     TEXT NOT NULL,
            label    TEXT NOT NULL,
            created  TEXT NOT NULL
        );",
    )?;
    // Restrictive perms: the DB holds key hashes (best effort, ignore errors).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).ok();
    }
    // Crash recovery: anything left in processing/ goes back to pending.
    conn.execute(
        "UPDATE jobs SET state='pending', lease_by=NULL, lease_until=NULL WHERE state='processing'",
        [],
    )?;
    Ok(conn)
}

// ------------------------------------------------------------------- auth ---

/// Authenticated caller identity: role + label from the `keys` table.
#[derive(Clone)]
struct Auth {
    role: String,
    label: String,
}

/// Middleware: `Authorization: Bearer <token>` -> sha256 hex -> keys table.
/// Inserts `Auth` into request extensions; rejects with 401 otherwise.
async fn auth_middleware(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let unauthorized = || {
        err_json(
            StatusCode::UNAUTHORIZED,
            "unauthorized: need Bearer <redacted> with role=user|worker",
        )
    };
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|a| a.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("");
    if token.is_empty() {
        return unauthorized();
    }
    let hash = sha256_hex(token);
    let row: Option<(String, String)> = state
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT role, label FROM keys WHERE key_hash = ?1",
            params![hash],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()
        .unwrap_or(None);
    match row {
        Some((role, label)) => {
            req.extensions_mut().insert(Auth { role, label });
            next.run(req).await
        }
        None => unauthorized(),
    }
}

fn require_role(auth: &Auth, role: &str) -> Result<(), Response> {
    if auth.role == role {
        Ok(())
    } else {
        Err(err_json(
            StatusCode::UNAUTHORIZED,
            &format!("unauthorized: need Bearer <redacted> with role={}", role),
        ))
    }
}

// ---------------------------------------------------------------- handlers ---

async fn health() -> impl IntoResponse {
    Json(json!({"ok": true}))
}

/// Prometheus text exposition (Fase 2). No auth, loopback-only like /health.
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let (pending, processing, done, failed) = {
        let conn = state.db.lock().unwrap();
        let count = |s: &str| -> i64 {
            conn.query_row(
                &format!("SELECT COUNT(*) FROM jobs WHERE state='{s}'"),
                [],
                |r| r.get(0),
            )
            .unwrap_or(0)
        };
        (
            count("pending"),
            count("processing"),
            count("done"),
            count("failed"),
        )
    };
    let body = format!(
        "# HELP bridge_jobs_pending Jobs waiting for a worker claim\n\
         # TYPE bridge_jobs_pending gauge\n\
         bridge_jobs_pending {pending}\n\
         # HELP bridge_jobs_processing Jobs currently leased by a worker\n\
         # TYPE bridge_jobs_processing gauge\n\
         bridge_jobs_processing {processing}\n\
         # HELP bridge_jobs_done Jobs answered but not yet picked up\n\
         # TYPE bridge_jobs_done gauge\n\
         bridge_jobs_done {done}\n\
         # HELP bridge_jobs_failed Jobs that exceeded the pending TTL\n\
         # TYPE bridge_jobs_failed gauge\n\
         bridge_jobs_failed {failed}\n\
         # HELP bridge_lease_expiries_total Leases returned to pending by the sweeper\n\
         # TYPE bridge_lease_expiries_total counter\n\
         bridge_lease_expiries_total {lease_expiries}\n\
         # HELP bridge_answers_total Jobs answered by workers\n\
         # TYPE bridge_answers_total counter\n\
         bridge_answers_total {answers}\n\
         # HELP bridge_timeouts_total Client long-polls that timed out\n\
         # TYPE bridge_timeouts_total counter\n\
         bridge_timeouts_total {timeouts}\n\
         # HELP bridge_chunks_total Stream chunks received from workers\n\
         # TYPE bridge_chunks_total counter\n\
         bridge_chunks_total {chunks}\n",
        pending = pending,
        processing = processing,
        done = done,
        failed = failed,
        lease_expiries = state.lease_expiries.load(Ordering::Relaxed),
        answers = state.answers.load(Ordering::Relaxed),
        timeouts = state.timeouts.load(Ordering::Relaxed),
        chunks = state.chunks.load(Ordering::Relaxed),
    );
    ([("content-type", "text/plain; version=0.0.4")], body)
}

async fn models(State(_state): State<AppState>, Extension(auth): Extension<Auth>) -> Response {
    if let Err(r) = require_role(&auth, "user") {
        return r;
    }
    Json(json!({
        "object": "list",
        "data": [{"id": "muse", "object": "model", "created": 0, "owned_by": "muse"}]
    }))
    .into_response()
}

/// Insert a pending job; returns the job id or an error response (429/500).
fn enqueue_job(state: &AppState, body: &Value) -> Result<String, Response> {
    let now = now_epoch();
    let job_id = uuid::Uuid::new_v4().simple().to_string();

    // Backpressure + insert (single lock, no await while held).
    let conn = state.db.lock().unwrap();
    let npend: i64 = conn
        .query_row("SELECT COUNT(*) FROM jobs WHERE state='pending'", [], |r| {
            r.get(0)
        })
        .unwrap_or(0);
    if npend >= state.cfg.max_pending {
        return Err(err_json(
            StatusCode::TOO_MANY_REQUESTS,
            "Muse bridge busy, try again in a bit",
        ));
    }
    if let Err(e) = conn.execute(
        "INSERT INTO jobs (id, state, request, created_at) VALUES (?1, 'pending', ?2, ?3)",
        params![job_id, body.to_string(), now],
    ) {
        let m = e.to_string();
        return Err(err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("db error: {}", &m[..m.len().min(120)]),
        ));
    }
    info!(job_id = %job_id, "job queued");
    Ok(job_id)
}

/// Remove the waiter and delete the job row (orphan cleanup, e.g. the client
/// disconnected or the wait timed out).
fn drop_job(state: &AppState, job_id: &str) {
    state.waiters.lock().unwrap().remove(job_id);
    state
        .db
        .lock()
        .unwrap()
        .execute("DELETE FROM jobs WHERE id = ?1", params![job_id])
        .ok();
}

/// Grace check: the worker may have answered between enqueue and waiter
/// registration (microsecond window, no await in between).
fn grace_answer(state: &AppState, job_id: &str) -> Option<String> {
    state
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT answer FROM jobs WHERE id = ?1 AND state = 'done'",
            params![job_id],
            |r| r.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
}

/// Non-streaming path: accumulate until the authoritative Answer arrives.
async fn chat_completions_plain(state: AppState, job_id: String) -> Response {
    let (tx, mut rx) = mpsc::unbounded_channel::<JobEvent>();
    state.waiters.lock().unwrap().insert(job_id.clone(), tx);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(state.cfg.wait_secs);
    let mut answer: Option<String> = None;
    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Some(JobEvent::Answer(t)) => { answer = Some(t); break; }
                // Chunks alone never complete a non-stream call; the
                // authoritative Answer (or the DB grace check) decides.
                Some(JobEvent::Chunk(_)) => {}
                None => break,
            },
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    if answer.is_none() {
        answer = grace_answer(&state, &job_id);
    }
    drop_job(&state, &job_id);

    match answer {
        Some(a) => {
            info!(job_id = %job_id, "job answered");
            Json(completion_json(&a)).into_response()
        }
        None => {
            info!(job_id = %job_id, "job timed out");
            state.timeouts.fetch_add(1, Ordering::Relaxed);
            err_json(StatusCode::GATEWAY_TIMEOUT, "Muse did not answer in time")
        }
    }
}

fn sse_chunk_frame(cid: &str, created: i64, delta_json: &str) -> String {
    format!(
        "data: {{\"id\":\"{cid}\",\"object\":\"chat.completion.chunk\",\
         \"created\":{created},\"model\":\"muse\",\
         \"choices\":[{{\"index\":0,\"delta\":{delta_json},\"finish_reason\":null}}]}}\n\n"
    )
}

/// Streaming path (request `"stream": true`): forward worker chunks as SSE
/// frames in real time. Runs the event loop on a spawned task so a client
/// disconnect reliably triggers orphan cleanup.
async fn chat_completions_stream(state: AppState, job_id: String) -> Response {
    let (etx, mut erx) = mpsc::unbounded_channel::<JobEvent>();
    state.waiters.lock().unwrap().insert(job_id.clone(), etx);
    let (btx, brx) = mpsc::unbounded_channel::<Result<Bytes, std::io::Error>>();
    let wait_secs = state.cfg.wait_secs;
    let st = state.clone();
    let jid = job_id.clone();

    tokio::spawn(async move {
        let send = |s: String| -> bool { btx.send(Ok(Bytes::from(s))).is_ok() };
        let cleanup = || drop_job(&st, &jid);
        let cid = format!(
            "chatcmpl-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let created = now_epoch();

        // First frame: role delta (OpenAI convention).
        if !send(sse_chunk_frame(&cid, created, r#"{"role":"assistant"}"#)) {
            cleanup();
            return;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
        let mut keepalive = tokio::time::interval(Duration::from_secs(15));
        keepalive.tick().await; // consume the immediate first tick
        let mut streamed = String::new();
        let mut got_first = false;

        loop {
            tokio::select! {
                ev = erx.recv() => {
                    match ev {
                        Some(JobEvent::Chunk(d)) => {
                            got_first = true;
                            streamed.push_str(&d);
                            let dj = serde_json::to_string(&json!({"content": d}))
                                .unwrap_or_else(|_| "{\"content\":\"\"}".to_string());
                            if !send(sse_chunk_frame(&cid, created, &dj)) {
                                cleanup();
                                return;
                            }
                        }
                        Some(JobEvent::Answer(final_text)) => {
                            // Emit whatever the worker's final text adds beyond
                            // what was already streamed (covers full-answer
                            // workers and authoritative overwrites).
                            let rem = if final_text.starts_with(&streamed) {
                                &final_text[streamed.len()..]
                            } else {
                                final_text.as_str()
                            };
                            if !rem.is_empty() {
                                let dj = serde_json::to_string(&json!({"content": rem}))
                                    .unwrap_or_else(|_| "{\"content\":\"\"}".to_string());
                                if !send(sse_chunk_frame(&cid, created, &dj)) {
                                    cleanup();
                                    return;
                                }
                            }
                            let stop = format!(
                                "data: {{\"id\":\"{cid}\",\"object\":\"chat.completion.chunk\",\
                                 \"created\":{created},\"model\":\"muse\",\
                                 \"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\n"
                            );
                            let _ = send(stop);
                            let _ = send("data: [DONE]\n\n".to_string());
                            st.waiters.lock().unwrap().remove(&jid);
                            st.db.lock().unwrap()
                                .execute("DELETE FROM jobs WHERE id = ?1", params![&jid])
                                .ok();
                            return;
                        }
                        None => { cleanup(); return; }
                    }
                }
                _ = keepalive.tick(), if !got_first => {
                    if !send(": ping\n\n".to_string()) { cleanup(); return; }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    let _ = send(
                        "data: {\"error\":{\"message\":\"Muse did not answer in time\",\
                         \"type\":\"timeout\"}}\n\ndata: [DONE]\n\n"
                            .to_string(),
                    );
                    st.timeouts.fetch_add(1, Ordering::Relaxed);
                    cleanup();
                    return;
                }
            }
        }
    });

    Response::builder()
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("X-Accel-Buffering", "no")
        .body(axum::body::Body::from_stream(UnboundedReceiverStream::new(
            brx,
        )))
        .unwrap()
}

async fn chat_completions(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(r) = require_role(&auth, "user") {
        return r;
    }
    if !body.is_object() {
        return err_json(StatusCode::BAD_REQUEST, "bad json");
    }
    if is_dashboard_probe(&body) {
        return Json(probe_completion()).into_response();
    }
    let stream = body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let job_id = match enqueue_job(&state, &body) {
        Ok(id) => id,
        Err(r) => return r,
    };
    if stream {
        chat_completions_stream(state, job_id).await
    } else {
        chat_completions_plain(state, job_id).await
    }
}

/// Atomically move up to `limit` pending jobs to processing with a lease,
/// in a single UPDATE...RETURNING statement.
fn claim_jobs(
    conn: &Connection,
    label: &str,
    until: i64,
    limit: i64,
) -> rusqlite::Result<Vec<(String, i64, String)>> {
    let mut stmt = conn.prepare(
        "UPDATE jobs SET state='processing', lease_by=?1, lease_until=?2
         WHERE id IN (SELECT id FROM jobs WHERE state='pending' ORDER BY created_at LIMIT ?3)
         RETURNING id, created_at, request",
    )?;
    let rows = stmt.query_map(params![label, until, limit], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    rows.collect()
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<i64>,
}

async fn muse_pending(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Query(q): Query<LimitQuery>,
) -> Response {
    if let Err(r) = require_role(&auth, "worker") {
        return r;
    }
    let limit = q.limit.unwrap_or(3).clamp(1, 10);
    let until = now_epoch() + state.cfg.lease_secs;

    // Atomic claim in a single statement.
    let claimed = {
        let conn = state.db.lock().unwrap();
        match claim_jobs(&conn, &auth.label, until, limit) {
            Ok(v) => v,
            Err(e) => return err_json(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    };

    let pending_n: i64 = state
        .db
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM jobs WHERE state='pending'", [], |r| {
            r.get(0)
        })
        .unwrap_or(0);

    let jobs: Vec<Value> = claimed
        .into_iter()
        .map(|(id, created_at, request)| {
            let req: Value = serde_json::from_str(&request).unwrap_or(Value::Null);
            json!({"id": id, "received_at": created_at, "request": req})
        })
        .collect();
    let count = jobs.len();
    info!(worker = %auth.label, count, "jobs claimed");
    Json(json!({"jobs": jobs, "count": count, "pending": pending_n})).into_response()
}

#[derive(Deserialize)]
struct AnswerBody {
    id: String,
    content: String,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 64 && id.chars().all(|c| c.is_alphanumeric())
}

async fn muse_answer(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(body): Json<AnswerBody>,
) -> Response {
    if let Err(r) = require_role(&auth, "worker") {
        return r;
    }
    if !valid_id(&body.id) {
        return err_json(StatusCode::BAD_REQUEST, "invalid job id");
    }
    let now = now_epoch();

    enum Outcome {
        Ok(bool), // duplicate?
        Unknown,
    }
    let outcome = {
        let conn = state.db.lock().unwrap();
        let cur: Option<String> = conn
            .query_row(
                "SELECT state FROM jobs WHERE id = ?1",
                params![body.id],
                |r| r.get(0),
            )
            .optional()
            .unwrap_or(None);
        match cur.as_deref() {
            Some("processing") => {
                conn.execute(
                    "UPDATE jobs SET state='done', answer=?1, answered_at=?2 WHERE id=?3",
                    params![body.content, now, body.id],
                )
                .ok();
                Outcome::Ok(false)
            }
            Some("done") => Outcome::Ok(true),
            _ => {
                if state.recent_done.lock().unwrap().contains_key(&body.id) {
                    Outcome::Ok(true)
                } else {
                    Outcome::Unknown
                }
            }
        }
    };

    match outcome {
        Outcome::Unknown => err_json(StatusCode::NOT_FOUND, "unknown job id (not leased?)"),
        Outcome::Ok(duplicate) => {
            if !duplicate {
                state.answers.fetch_add(1, Ordering::Relaxed);
            }
            state
                .recent_done
                .lock()
                .unwrap()
                .insert(body.id.clone(), now);
            // Wake the waiting /v1/chat/completions call, if any. If the
            // waiter is already gone (client disconnected), drop the orphaned
            // job row right away so it never blocks the queue.
            let tx = state.waiters.lock().unwrap().remove(&body.id);
            if let Some(tx) = tx {
                if tx.send(JobEvent::Answer(body.content)).is_err() {
                    state
                        .db
                        .lock()
                        .unwrap()
                        .execute("DELETE FROM jobs WHERE id = ?1", params![body.id])
                        .ok();
                }
            }
            info!(job_id = %body.id, duplicate, "job answered");
            Json(json!({"ok": true, "id": body.id, "duplicate": duplicate})).into_response()
        }
    }
}

/// Worker chunk protocol (Fase 2).
///
/// Contract: a worker EITHER streams `{"id","delta"}` chunks and finishes with
/// `{"id","done":true}`, OR sends one full `/muse/answer`. If `/muse/answer`
/// arrives after chunks, its `content` is the authoritative final answer
/// (overwrite, not append).
#[derive(Deserialize)]
struct ChunkBody {
    id: String,
    delta: Option<String>,
    done: Option<bool>,
}

async fn muse_answer_chunk(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(body): Json<ChunkBody>,
) -> Response {
    if let Err(r) = require_role(&auth, "worker") {
        return r;
    }
    if !valid_id(&body.id) {
        return err_json(StatusCode::BAD_REQUEST, "invalid job id");
    }
    if body.delta.is_none() && !body.done.unwrap_or(false) {
        return err_json(
            StatusCode::BAD_REQUEST,
            "need {\"delta\": \"...\"} and/or {\"done\": true}",
        );
    }
    let now = now_epoch();

    enum COutcome {
        Chunked,
        Done(String),
        Duplicate,
        Unknown,
    }
    let outcome = {
        let conn = state.db.lock().unwrap();
        let row: Option<(String,)> = conn
            .query_row(
                "SELECT state FROM jobs WHERE id = ?1",
                params![body.id],
                |r| Ok((r.get::<_, String>(0)?,)),
            )
            .optional()
            .unwrap_or(None);
        match row.map(|(s,)| s).as_deref() {
            Some("processing") => {
                if let Some(d) = body.delta.as_deref() {
                    conn.execute(
                        "UPDATE jobs SET answer = COALESCE(answer,'') || ?1 WHERE id = ?2",
                        params![d, body.id],
                    )
                    .ok();
                    state.chunks.fetch_add(1, Ordering::Relaxed);
                }
                if body.done.unwrap_or(false) {
                    conn.execute(
                        "UPDATE jobs SET state='done', answered_at=?1 WHERE id=?2",
                        params![now, body.id],
                    )
                    .ok();
                    state.answers.fetch_add(1, Ordering::Relaxed);
                    state
                        .recent_done
                        .lock()
                        .unwrap()
                        .insert(body.id.clone(), now);
                    let final_text: Option<String> = conn
                        .query_row(
                            "SELECT answer FROM jobs WHERE id=?1",
                            params![body.id],
                            |r| r.get::<_, Option<String>>(0),
                        )
                        .unwrap_or(None);
                    COutcome::Done(final_text.unwrap_or_default())
                } else {
                    COutcome::Chunked
                }
            }
            Some("done") => COutcome::Duplicate,
            _ => {
                if state.recent_done.lock().unwrap().contains_key(&body.id) {
                    COutcome::Duplicate
                } else {
                    COutcome::Unknown
                }
            }
        }
    };

    match outcome {
        COutcome::Unknown => err_json(StatusCode::NOT_FOUND, "unknown job id (not leased?)"),
        COutcome::Duplicate => {
            Json(json!({"ok": true, "id": body.id, "duplicate": true})).into_response()
        }
        COutcome::Chunked => {
            // Forward to the waiter if one is registered; the delta is also
            // persisted in jobs.answer, so nothing is lost if it isn't.
            if let Some(tx) = state.waiters.lock().unwrap().get(&body.id) {
                let _ = tx.send(JobEvent::Chunk(body.delta.clone().unwrap_or_default()));
            }
            Json(json!({"ok": true, "id": body.id})).into_response()
        }
        COutcome::Done(final_text) => {
            let tx = state.waiters.lock().unwrap().remove(&body.id);
            if let Some(tx) = tx {
                if tx.send(JobEvent::Answer(final_text)).is_err() {
                    state
                        .db
                        .lock()
                        .unwrap()
                        .execute("DELETE FROM jobs WHERE id = ?1", params![body.id])
                        .ok();
                }
            }
            info!(job_id = %body.id, "job answered (streamed)");
            Json(json!({"ok": true, "id": body.id, "duplicate": false})).into_response()
        }
    }
}

#[derive(Deserialize)]
struct ReleaseBody {
    id: String,
}

async fn muse_release(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(body): Json<ReleaseBody>,
) -> Response {
    if let Err(r) = require_role(&auth, "worker") {
        return r;
    }
    if !valid_id(&body.id) {
        return err_json(StatusCode::BAD_REQUEST, "invalid job id");
    }
    let n = state
        .db
        .lock()
        .unwrap()
        .execute(
            "UPDATE jobs SET state='pending', lease_by=NULL, lease_until=NULL
             WHERE id = ?1 AND state = 'processing'",
            params![body.id],
        )
        .unwrap_or(0);
    if n > 0 {
        info!(job_id = %body.id, "lease released");
        Json(json!({"ok": true})).into_response()
    } else {
        err_json(StatusCode::NOT_FOUND, "unknown job id (not leased?)")
    }
}

// ----------------------------------------------------------------- sweeper ---

async fn sweeper(state: AppState) {
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    loop {
        tick.tick().await;
        let now = now_epoch();
        let (expired, failed, cleaned) = {
            let conn = state.db.lock().unwrap();
            let expired = conn
                .execute(
                    "UPDATE jobs SET state='pending', lease_by=NULL, lease_until=NULL
                     WHERE state='processing' AND lease_until IS NOT NULL AND lease_until < ?1",
                    params![now],
                )
                .unwrap_or(0);
            let failed = conn
                .execute(
                    "UPDATE jobs SET state='failed'
                     WHERE state='pending' AND created_at < ?1",
                    params![now - state.cfg.pending_ttl_secs],
                )
                .unwrap_or(0);
            // Fase 2: prune old failed rows so the table doesn't grow forever.
            let cleaned = conn
                .execute(
                    "DELETE FROM jobs WHERE state='failed' AND created_at < ?1",
                    params![now - state.cfg.failed_ttl_secs],
                )
                .unwrap_or(0);
            (expired, failed, cleaned)
        };
        if expired > 0 {
            state
                .lease_expiries
                .fetch_add(expired as u64, Ordering::Relaxed);
        }
        // Prune duplicate-answer memory older than 1h.
        {
            let mut rd = state.recent_done.lock().unwrap();
            rd.retain(|_, ts| *ts > now - 3600);
        }
        if expired > 0 || failed > 0 || cleaned > 0 {
            info!(expired, failed, cleaned, "sweeper pass");
        }
    }
}

// --------------------------------------------------------------------- cli ---

#[derive(Parser)]
#[command(
    name = "muse-bridge",
    about = "Muse bridge: OpenAI-compatible API + worker-pull queue"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the HTTP bridge (default)
    Serve,
    /// Create an API key (printed once)
    Keygen {
        #[arg(long)]
        role: String,
        #[arg(long)]
        label: String,
    },
    /// List keys (hash prefixes only)
    Keylist,
    /// Revoke a key by hash prefix or label
    Keydel { ident: String },
}

fn new_token() -> String {
    format!("mbr_{}", uuid::Uuid::new_v4().simple())
}

fn cmd_keygen(cfg: &Config, role: &str, label: &str) -> i32 {
    if role != "user" && role != "worker" {
        eprintln!("role must be 'user' or 'worker'");
        return 2;
    }
    let conn = match init_db(&cfg.db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("db error: {}", e);
            return 1;
        }
    };
    let token = new_token();
    let hash = sha256_hex(&token);
    let created = iso8601_now();
    if let Err(e) = conn.execute(
        "INSERT INTO keys (key_hash, role, label, created) VALUES (?1, ?2, ?3, ?4)",
        params![hash, role, label, created],
    ) {
        eprintln!("db error: {}", e);
        return 1;
    }
    println!("{}", token); // full key shown ONLY here
    eprintln!(
        "role={} label={} saved to {} (full key is shown only this once)",
        role,
        label,
        cfg.db_path.display()
    );
    0
}

fn cmd_keylist(cfg: &Config) -> i32 {
    let conn = match init_db(&cfg.db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("db error: {}", e);
            return 1;
        }
    };
    let mut stmt =
        match conn.prepare("SELECT label, role, created, key_hash FROM keys ORDER BY created") {
            Ok(s) => s,
            Err(e) => {
                eprintln!("db error: {}", e);
                return 1;
            }
        };
    let rows: Vec<(String, String, String, String)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map(|it| it.filter_map(|r| r.ok()).collect())
        .unwrap_or_default();
    if rows.is_empty() {
        println!("(no keys yet — run: muse-bridge keygen --role worker --label muse-vm)");
        return 0;
    }
    println!("{:<20}{:<8}{:<33}PREFIX", "LABEL", "ROLE", "CREATED");
    for (label, role, created, hash) in rows {
        let prefix: String = hash.chars().take(14).collect();
        println!("{:<20}{:<8}{:<33}{}...", label, role, created, prefix);
    }
    0
}

fn cmd_keydel(cfg: &Config, ident: &str) -> i32 {
    let conn = match init_db(&cfg.db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("db error: {}", e);
            return 1;
        }
    };
    // Match by hash prefix OR exact label. LIKE pattern is built from a
    // caller-supplied prefix; escape LIKE wildcards to avoid surprises.
    let esc: String = ident
        .chars()
        .flat_map(|c| match c {
            '%' | '_' | '\\' => vec!['\\', c],
            _ => vec![c],
        })
        .collect();
    let pattern = format!("{}%", esc);
    let removed = conn
        .execute(
            "DELETE FROM keys WHERE (key_hash LIKE ?1 ESCAPE '\\') OR label = ?2",
            params![pattern, ident],
        )
        .unwrap_or(0);
    println!("removed {} key(s)", removed);
    0
}

// ------------------------------------------------------------------- serve ---

fn tailscale_ips() -> Vec<String> {
    let out = ProcCommand::new("tailscale")
        .args(["ip", "-4"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();
    out.split_whitespace()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

async fn cmd_serve(cfg: Config) -> i32 {
    let conn = match init_db(&cfg.db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("FATAL: db error: {}", e);
            return 2;
        }
    };
    let state = AppState {
        db: Arc::new(Mutex::new(conn)),
        waiters: Arc::new(Mutex::new(HashMap::new())),
        recent_done: Arc::new(Mutex::new(HashMap::new())),
        lease_expiries: Arc::new(AtomicU64::new(0)),
        answers: Arc::new(AtomicU64::new(0)),
        timeouts: Arc::new(AtomicU64::new(0)),
        chunks: Arc::new(AtomicU64::new(0)),
        cfg: cfg.clone(),
    };
    tokio::spawn(sweeper(state.clone()));

    let authed = Router::new()
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/muse/pending", get(muse_pending))
        .route("/muse/answer", post(muse_answer))
        .route("/muse/answer-chunk", post(muse_answer_chunk))
        .route("/muse/release", post(muse_release))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .merge(authed)
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .with_state(state);

    // Optional TLS (Fase 2): enabled only when BOTH cert and key are set.
    let tls: Option<axum_server::tls_rustls::RustlsConfig> =
        match (cfg.tls_cert.clone(), cfg.tls_key.clone()) {
            (Some(cert), Some(key)) => {
                match axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await {
                    Ok(t) => {
                        info!("TLS enabled (rustls)");
                        Some(t)
                    }
                    Err(e) => {
                        eprintln!("FATAL: TLS config error: {}", e);
                        return 2;
                    }
                }
            }
            _ => None,
        };

    let mut addrs = vec![format!("127.0.0.1:{}", cfg.port)];
    for ip in tailscale_ips() {
        let a = format!("{}:{}", ip, cfg.port);
        if !addrs.contains(&a) {
            addrs.push(a);
        }
    }

    let mut ok = 0;
    for a in &addrs {
        let sock: SocketAddr = match a.parse() {
            Ok(s) => s,
            Err(e) => {
                info!("skip invalid listen addr {}: {}", a, e);
                continue;
            }
        };
        if let Some(tls_cfg) = tls.clone() {
            let app = app.clone();
            info!("muse-bridge-rs listening (https) on {}", a);
            tokio::spawn(async move {
                if let Err(e) = axum_server::bind_rustls(sock, tls_cfg)
                    .serve(app.into_make_service())
                    .await
                {
                    info!("tls server on {} stopped: {}", sock, e);
                }
            });
            ok += 1;
            continue;
        }
        match tokio::net::TcpListener::bind(sock).await {
            Ok(listener) => {
                info!("muse-bridge-rs listening on {}", a);
                let app = app.clone();
                tokio::spawn(async move {
                    if let Err(e) = axum::serve(listener, app).await {
                        info!("server on {} stopped: {}", sock, e);
                    }
                });
                ok += 1;
            }
            Err(e) => info!("cannot listen on {}: {}", a, e),
        }
    }
    if ok == 0 {
        eprintln!("FATAL: tidak ada listen address yang berhasil");
        return 2;
    }
    info!(
        "keys ready | lease={}s wait={}s max_pending={} pending_ttl={}s failed_ttl={}s tls={}",
        cfg.lease_secs,
        cfg.wait_secs,
        cfg.max_pending,
        cfg.pending_ttl_secs,
        cfg.failed_ttl_secs,
        if tls.is_some() { "on" } else { "off" }
    );
    std::future::pending::<()>().await;
    0
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = Config::from_env();
    let code = match cli.cmd {
        None | Some(Cmd::Serve) => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            rt.block_on(cmd_serve(cfg))
        }
        Some(Cmd::Keygen { role, label }) => cmd_keygen(&cfg, &role, &label),
        Some(Cmd::Keylist) => cmd_keylist(&cfg),
        Some(Cmd::Keydel { ident }) => cmd_keydel(&cfg, &ident),
    };
    std::process::exit(code);
}
