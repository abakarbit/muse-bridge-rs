# muse-bridge-rs

[![CI](https://github.com/abakarbit/muse-bridge-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/abakarbit/muse-bridge-rs/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021%20edition-orange.svg)](https://www.rust-lang.org)

An **OpenAI-compatible bridge** that connects chat gateways (e.g. Telegram via
[PicoClaw](https://github.com/sipeed/picoclaw)) to an LLM agent worker —
**without opening any inbound port on the agent's host**.

The bridge is the only server. Gateways and workers are both outbound-only
clients: a chat request becomes a job in a SQLite queue, a worker *pulls* the
job, answers it, and the bridge wakes the waiting HTTP request — as JSON or as
real-time SSE chunks.

```
Telegram ──message──▶ PicoClaw gateway ──POST /v1/chat/completions──▶ muse-bridge-rs
                                                                          │ SQLite (WAL) job queue
                                              GET /muse/pending ◀───────┤ (atomic claim + lease)
                                              POST /muse/answer ────────▶│ wakes the long-poll waiter
                                              agent worker (scheduled)   ▼
                                                              response (JSON / SSE)
```

## Why

- **No inbound surface on the agent host** — the worker pulls work; nothing
  needs to reach into the machine that runs your agent.
- **Works with any OpenAI-compatible client** — point `api_base` at the
  bridge and send `model: "muse"`.
- **Real streaming** — workers can answer in chunks that are forwarded as
  OpenAI-style SSE frames with sub-second latency (measured worst chunk
  latency: 32.5 ms on loopback).
- **Keys are never stored in plaintext** — only SHA-256 hashes, with
  `user` / `worker` roles enforced per endpoint.
- **Self-healing queue** — leases expire back to `pending`, stale jobs are
  swept to `failed`, and a crash mid-job loses nothing (`kill -9` tested).
- **Single static binary** — tokio/axum, bundled SQLite, optional rustls TLS,
  Prometheus metrics. Supervised by a small idempotent watchdog script for
  hosts without systemd/cron.

## Quick start

```bash
# Build
cargo build --release
cp target/release/muse-bridge ./muse-bridge

# Create keys (each key is printed exactly once — store it safely)
./muse-bridge keygen --role user   --label gateway
./muse-bridge keygen --role worker --label worker-1

# Run (defaults: 127.0.0.1:8765, DB at ~/workspace/muse-bridge-rs/bridge.db)
./muse-bridge serve

# Ask something (as the "user" role)
curl http://127.0.0.1:8765/v1/chat/completions \
  -H "Authorization: Bearer <user-key>" -H 'Content-Type: application/json' \
  -d '{"model":"muse","messages":[{"role":"user","content":"Hello"}]}'
```

A minimal worker loop ships in [`scripts/worker_poll.py`](scripts/worker_poll.py):
it polls `/muse/pending`, and posts answers to `/muse/answer`. The worker key
is read from a file by the wrapper and never printed — your agent never needs
to see the raw credential.

End-to-end test suites live in [`scripts/`](scripts/) (`e2e_test.sh`,
`e2e_stream.sh`); both were run green against the release binary
(11-step functional suite in 6.46 s, streaming suite in 5.23 s).

## API

**User role** (OpenAI-compatible)

| Endpoint | Description |
|---|---|
| `GET /v1/models` | Lists the single model `muse` |
| `POST /v1/chat/completions` | Enqueues a job and long-polls for the answer (JSON, or SSE with `"stream": true`). `429` when the queue is full, `504` on timeout |

**Worker role**

| Endpoint | Description |
|---|---|
| `GET /muse/pending?limit=N` | Atomically claims up to N jobs under a lease |
| `POST /muse/answer` | `{"id","content"}` — full answer; wakes the waiter, marks `done` |
| `POST /muse/answer-chunk` | `{"id","delta"}` streams a chunk; `{"id","done":true}` finishes |
| `POST /muse/release` | `{"id"}` — returns a claimed job to `pending` early |

**Operational** (no auth, loopback-oriented)

| Endpoint | Description |
|---|---|
| `GET /health` | `{"ok":true}` |
| `GET /metrics` | Prometheus text: queue gauges + answer/timeout/chunk counters |

**CLI**: `serve` (default), `keygen --role --label`, `keylist` (hash prefixes
only), `keydel <hash-prefix|label>`.

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `BRIDGE_DB` | `~/workspace/muse-bridge-rs/bridge.db` | SQLite database path |
| `BRIDGE_PORT` | `8765` | Listen port (binds loopback; also a detected Tailscale IP when present) |
| `BRIDGE_LEASE_SECS` | `180` | Worker lease duration |
| `BRIDGE_WAIT_SECS` | `240` | Max long-poll wait for an answer |
| `BRIDGE_MAX_PENDING` | `5` | Queue bound — beyond this, `429` |
| `BRIDGE_PENDING_TTL_SECS` | `1800` | Max age of a pending job before it is swept to `failed` |
| `BRIDGE_FAILED_TTL_SECS` | `86400` | Retention of failed jobs |
| `BRIDGE_TLS_CERT` | *(unset)* | PEM certificate; set together with the key to serve HTTPS via rustls |
| `BRIDGE_TLS_KEY` | *(unset)* | PEM private key |

See [`examples/bridge.env.example`](examples/bridge.env.example) and
[`examples/muse-bridge.service`](examples/muse-bridge.service).

## Deployment shape

The reference deployment runs everything on one host, loopback-only:

- `muse-bridge` on `127.0.0.1:8765`
- PicoClaw gateway on `127.0.0.1:18790` (Telegram connects *outbound*)
- the worker as a scheduled task every 5 s
- [`scripts/watchdog.sh`](scripts/watchdog.sh) every 5 min keeps bridge +
  gateway alive and can optionally maintain a reverse-SSH tunnel for
  dashboard access over a private overlay network

A PicoClaw wiring example is in
[`examples/picoclaw.config.example.json`](examples/picoclaw.config.example.json)
— note the locked-down profile: all gateway tools disabled, channel restricted
to the owner's chat ID, so one message = one job and the agent worker does all
the thinking.

Full design rationale, DB schema, and risk analysis:
[`docs/architecture.md`](docs/architecture.md) (Indonesian, with an English
abstract). Delivery history and test evidence:
[`docs/roadmap.md`](docs/roadmap.md).

## Security

- Keys are stored **only as SHA-256 hashes**; a leaked database does not leak
  usable keys. Revoke with `keydel`, rotate with `keygen`.
- Keep key files and the PicoClaw config at mode `600`. Never commit them —
  the provided `.gitignore` excludes `keys/`, `*.db*`, `logs/`, `backups/`,
  and `.env` files.
- The default topology exposes nothing publicly. If you bind beyond
  loopback, put the bridge behind TLS (`BRIDGE_TLS_*`) and a firewall, and
  read [SECURITY.md](SECURITY.md) first — including the note on why the
  gateway's own tool/exec surface should stay disabled or hardened when a
  chat channel can reach it.

## Development

```bash
cargo build --release   # binary: target/release/muse-bridge
cargo clippy --all-targets
cargo fmt --check
./scripts/e2e_test.sh   # needs a scratch DB; spins up its own server
```

The implementation is currently a single `src/main.rs` (~1,300 lines) — a
deliberate minimum-viable choice; a module split is on the roadmap.
Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
