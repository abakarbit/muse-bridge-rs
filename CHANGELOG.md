# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] — 2026-10-11

First public release. Phases 1–3 were completed and verified end-to-end on
2026-10-11 (see `docs/roadmap.md` for the full evidence log).

### Added

- OpenAI-compatible API: `GET /v1/models`, `POST /v1/chat/completions`
  (long-poll, event-driven wake-up, `429` backpressure, `504` timeout).
- Worker-pull job queue on SQLite (WAL): atomic claim with leases,
  `GET /muse/pending`, `POST /muse/answer`, `POST /muse/release`.
- Real SSE streaming: `POST /muse/answer-chunk` forwards worker chunks as
  OpenAI-style frames (role delta → content chunks → stop → `[DONE]`),
  with 15 s keepalive pings.
- API keys stored as SHA-256 hashes with `user`/`worker` roles; CLI
  `keygen` (printed once), `keylist` (hash prefixes only), `keydel`.
- Background sweeper (60 s): expired leases return to `pending`, stale
  pending jobs become `failed`, failed jobs are purged after their TTL.
- Crash recovery at startup: `processing` jobs return to `pending`.
- Optional HTTPS via rustls (`BRIDGE_TLS_CERT` / `BRIDGE_TLS_KEY`).
- `GET /health` and Prometheus-format `GET /metrics`.
- Operations tooling: idempotent `scripts/watchdog.sh` supervisor
  (bridge + PicoClaw gateway + optional reverse-SSH tunnel keeper),
  `scripts/worker_poll.py` credential-isolating worker wrapper.
- End-to-end suites `scripts/e2e_test.sh` and `scripts/e2e_stream.sh`
  (both green against the release binary; worst streaming chunk latency
  32.5 ms on loopback).
- Production wiring with PicoClaw as a Telegram gateway, verified
  end-to-end over Telegram on 2026-10-11.

[0.1.0]: https://github.com/abakarbit/muse-bridge-rs/releases/tag/v0.1.0
