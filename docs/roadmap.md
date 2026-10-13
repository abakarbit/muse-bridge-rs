# Roadmap & verification log — muse-bridge-rs

Delivery checklist, referencing `docs/architecture.md`. An item is only
checked when it is **done and verified** — a claim without evidence does not
count.

---

## Phase 0 — Preparation (no code) ✅

- [x] Verified PicoClaw custom-provider field names against the official
      `config.example.json`: `model_list[]` with `model_name`,
      `model: "openai/muse"`, `api_keys: [...]` (array),
      `api_base: "http://127.0.0.1:8765/v1"` — see architecture §6
- [x] Locked decision: worker poll interval = **5 seconds** (responsiveness
      of chat prioritized)
- [x] Locked decision: deployment host = **single VM, everything loopback**
      (no systemd-user/cron daemon available → supervision via `watchdog.sh`
      + the agent runtime's scheduler)

## Phase 1 — Core (parity with the Python bridge v5) ✅ 2026-10-11

- [x] Cargo scaffold: tokio, axum, rusqlite (bundled), serde, tracing, clap, sha2
- [x] DB schema per architecture §3 + initial migration
- [x] Auth middleware: Bearer → SHA-256 → match `keys.role`
- [x] CLI: `keygen` (print once) / `keylist` (hash prefixes only) / `keydel` / `serve`
- [x] `GET /v1/models`, `GET /health`
- [x] `POST /v1/chat/completions`: write pending job, event-driven long-poll,
      429 when the queue is full, 504 on timeout
- [x] `GET /muse/pending` (atomic claim + lease), `POST /muse/answer`,
      `POST /muse/release`
- [x] 60 s sweeper: expired lease → pending; pending older than TTL → failed
- [x] Crash recovery at startup: processing → pending
- [x] Small worker script (poll → answer) for local testing
- [x] Local end-to-end test: keygen → chat/completions → worker answers → response

**Verification (e2e suite re-run, 2026-10-11): ALL PASS in 6.46 s** — keygen
0.31 s, server start 0.18 s, health/models/401 ≈ 0.01–0.02 s, chat→worker→
answer roundtrip 0.72 s, duplicate-answer protection 0.59 s, 429 backpressure
3.03 s, release 0.57 s, keylist/keydel + revoke → 401 OK. Extra tests: 504
timeout (3.06 s), lease expiry → reclaim, `kill -9` → crash recovery, 413 on
an 11 MB body. Release build with 0 warnings.

## Phase 2 — Streaming, TLS, observability ✅ 2026-10-11

- [x] `POST /muse/answer-chunk {"id","delta","done"}` on the bridge
- [x] `POST /v1/chat/completions` with `stream: true` → real-time SSE
- [x] Worker can send chunks (full answer remains a supported fallback)
- [x] Optional TLS via rustls (enabled via env)
- [x] `GET /metrics`: queue depth, wait time, lease expiries
- [x] Streaming end-to-end test: chunks reach the client < 1 s

**Verification (streaming suite re-run, 2026-10-11): ALL PASS in 5.23 s** —
byte-level: exactly 8 frames (role delta, 5 ordered content chunks, stop,
`[DONE]`), all JSON valid, consistent `chatcmpl-*` id; worst per-chunk
worker→client latency 32.5 ms; non-stream request + chunking worker →
correct accumulation; legacy full answer → 1 chunk + stop + DONE (backward
compatible); `/metrics` exposes 8 series with incrementing counters;
self-signed TLS → 200, plain HTTP on the TLS port rejected, no env → plain
HTTP unchanged. Phase 1 regression suite against the final binary:
**ALL PASS in 5.97 s**.

## Phase 3 — Production deployment + Telegram ✅ 2026-10-11

- [x] Release build deployed; PicoClaw v0.3.1 official binary (SHA-256 verified)
- [x] `keygen` user + worker keys stored in files with mode 600; DB mode 600
- [x] Idempotent `bin/watchdog.sh` (incl. the `pgrep -f` → `pgrep -x` fix)
- [x] Supervision: watchdog every 5 minutes via the agent runtime scheduler
- [x] PicoClaw config (mode 600): model `muse` → bridge; Telegram channel
- [x] Worker scheduled task every 5 s — live end-to-end verified
- [x] Telegram wiring: bot token + owner chat ID in the 600-mode config,
      channel enabled, gateway on :18790
- [x] End-to-end test over Telegram ✅ 2026-10-11 21:50 — a message sent to
      the bot travelled Telegram → PicoClaw → bridge → worker → answer and
      came back through the full chain
- [x] Resilience test: `kill -9` → watchdog revived the bridge in 1 s, no job lost

**Phase 3 done:** a Telegram chat with the agent works end-to-end on a single
host; all secrets live in 600-mode files; the watchdog demonstrably restarts
dead components.

---

## Later hardening backlog (not yet scheduled)

- [ ] Split `src/main.rs` (~1,300 lines) into modules (`db`, `auth`, `api`, `sse`)
- [ ] Rust integration tests mirroring `scripts/e2e_*.sh`
- [ ] Tag-based GitHub Releases with prebuilt binaries
- [ ] Optional systemd unit for hosts that have a working user bus
