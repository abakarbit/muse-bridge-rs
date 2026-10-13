# Architecture — muse-bridge-rs

> **Abstract (EN):** Single-host, loopback-only topology. A Rust bridge
> (tokio/axum) holds an OpenAI-compatible front door and a SQLite (WAL)
> worker-pull queue; a chat gateway (PicoClaw) and an agent worker are both
> outbound-only clients, so the agent host needs no inbound ports. API keys
> are stored as SHA-256 hashes with `user`/`worker` roles; workers claim jobs
> atomically under a lease and answer (optionally as SSE chunks) over
> long-poll. Operations rely on an idempotent watchdog instead of
> systemd/cron daemons. Full document in Indonesian below.

Bridge OpenAI-compatible untuk Muse, ditulis ulang dalam Rust dari sebuah
bridge Python (v5.1). Dokumen ini adalah acuan tunggal arsitektur; semua
keputusan implementasi merujuk ke sini.

Status: Fase 1–3 SELESAI & terverifikasi end-to-end (2026-10-11).

## 1. Gambaran arsitektur

```
Telegram ──pesan──▶ PicoClaw (gateway, tools dimatikan pada profil rujukan)
                         │  POST /v1/chat/completions  {model: "muse"}
                         │  Authorization: Bearer <user-key>
                         ▼
              ┌─────────────────────┐
              │  muse-bridge-rs     │  Rust · tokio/axum · :8765
              │  SQLite (WAL) queue │  bind 127.0.0.1 (+ IP Tailscale)
              └────────┬────────────┘
                       │  GET /muse/pending?limit=N   (lease 180 dtk)
                       │  Authorization: Bearer <worker-key>
                       ▼
              Muse VM — worker (scheduled task, skrip kecil)
                       │  POST /muse/answer {id, content}
                       │  (Fase 2: POST /muse/answer-chunk untuk streaming)
                       ▼
              ┌─────────────────────┐
              │  muse-bridge-rs     │  bangunkan request yang long-poll
              └────────┬────────────┘  via event (bukan cek file)
                       │  response OpenAI (JSON / SSE)
                       ▼
              PicoClaw ──balasan──▶ Telegram
```

Prinsip: bridge satu-satunya server. PicoClaw dan worker Muse keduanya
hanyalah client outbound. Muse VM tidak membuka port/API apa pun.

## 2. Keputusan yang sudah dikunci

| # | Keputusan | Alasan |
|---|-----------|--------|
| 1 | Rewrite Rust (tokio + axum), binary statis | Konkurensi long-poll, latency event-driven, deploy tanpa runtime |
| 2 | Antrean SQLite WAL, bukan file | Claim atomik via transaksi, tanpa global lock, query mudah |
| 3 | PicoClaw mode gateway tipis (tools mati) | 1 pesan = 1 job; otak tetap 100% worker Muse |
| 4 | Tanpa router/agent perantara tambahan | PicoClaw bicara OpenAI-native langsung ke bridge |
| 5 | Worker tetap skrip kecil terjadwal | YAGNI — tidak di-rewrite |
| 6 | Bind default 127.0.0.1 (+ deteksi IP Tailscale) | Tidak ekspos publik; pola dari bridge.py dipertahankan |
| 7 | Kunci API disimpan sebagai hash SHA-256 | Perbaikan dari bridge.py yang menyimpan plaintext di keys.json |

## 3. Skema database (SQLite)

```sql
CREATE TABLE jobs (
  id          TEXT PRIMARY KEY,          -- uuid hex
  state       TEXT NOT NULL,             -- pending | processing | done | failed
  request     TEXT NOT NULL,             -- JSON body /v1/chat/completions
  created_at  INTEGER NOT NULL,          -- unix epoch
  lease_by    TEXT,                      -- label worker
  lease_until INTEGER,                   -- unix epoch
  answer      TEXT,                      -- jawaban worker
  answered_at INTEGER
);
CREATE INDEX idx_jobs_state ON jobs(state, created_at);

CREATE TABLE keys (
  key_hash TEXT PRIMARY KEY,             -- sha256 hex dari bearer token
  role     TEXT NOT NULL,                -- user | worker
  label    TEXT NOT NULL,
  created  TEXT NOT NULL                 -- ISO8601
);
```

Sweeper (interval 60 dtk): kembalikan `processing` yang lease-nya kedaluwarsa
ke `pending`; tandai `failed` untuk `pending` yang lebih tua dari
PENDING_TTL (default 30 menit) — menutup lubang "job diam selamanya".

## 4. API

### 4.1 OpenAI-compatible (butuh Bearer role=user)

- `GET /v1/models` → `{"object":"list","data":[{"id":"muse",...}]}`
- `POST /v1/chat/completions` → tulis job `pending`, tahan koneksi (long-poll,
  maks WAIT_SECS=240). Fase 1: balas JSON penuh. Fase 2: balas SSE chunk
  real-time dari worker.
- Backpressure: 429 jika job `pending` >= MAX_PENDING (default 5).

### 4.2 Worker API (butuh Bearer role=worker)

- `GET /muse/pending?limit=N` → claim atomik N job (transaksi), set lease.
- `POST /muse/answer {"id","content"}` → simpan jawaban, bangunkan request
  yang menunggu via event channel, tandai `done`.
- `POST /muse/release {"id"}` → kembalikan lease lebih awal ke `pending`.
- `POST /muse/answer-chunk {"id","delta"}` → append delta ke jawaban
  (akumulasi di DB), teruskan sebagai event Chunk ke waiter; job tetap
  `processing`. `{"id","done":true}` → tandai `done`, bangunkan waiter.
  Kontrak: worker EITHER chunk-lalu-done ATAU satu `/muse/answer` penuh;
  `content` dari `/muse/answer` selalu authoritative (overwrite).

### 4.3 Operasional

- `GET /health` → `{"ok":true}` (tanpa auth, untuk probe tunnel/monitoring).
- `GET /metrics` → format teks Prometheus (tanpa auth, loopback-only):
  `bridge_jobs_pending|processing|done|failed` (gauge),
  `bridge_lease_expiries_total`, `bridge_answers_total`,
  `bridge_timeouts_total`, `bridge_chunks_total` (counter).
- CLI: `serve` (default), `keygen --role --label` (cetak sekali),
  `keylist` (prefix hash saja), `keydel <ident>`.

### 4.4 Perilaku yang dipertahankan dari bridge.py

- Lease 180 dtk (env BRIDGE_LEASE_SECS), crash recovery saat start
  (processing → pending), deteksi jawaban duplikat, proteksi traversal ID,
  batasi body 10 MB, probe dashboard legacy (dipertahankan untuk kompatibilitas).

## 5. Konfigurasi (env)

| Var | Default | Keterangan |
|-----|---------|------------|
| BRIDGE_DB | ./bridge.db | Path SQLite |
| BRIDGE_PORT | 8765 | Port listen |
| BRIDGE_LEASE_SECS | 180 | Lama lease worker |
| BRIDGE_WAIT_SECS | 240 | Maks long-poll |
| BRIDGE_MAX_PENDING | 5 | Batas antrean (429) |
| BRIDGE_PENDING_TTL_SECS | 1800 | Umur maks job pending |
| BRIDGE_FAILED_TTL_SECS | 86400 | Umur maks job failed sebelum dibersihkan sweeper |
| BRIDGE_TLS_CERT | (kosong) | Path PEM cert; jika diset bersama KEY → HTTPS via rustls |
| BRIDGE_TLS_KEY | (kosong) | Path PEM key |

`POST /v1/chat/completions` dengan `"stream": true` → response SSE:
frame role-delta → N frame content chunk → frame `finish_reason:"stop"` →
`data: [DONE]`; keepalive `: ping` tiap 15 dtk; timeout → frame error + `[DONE]`.

## 6. Wiring PicoClaw

Field terverifikasi dari `config.example.json` resmi (2026-10-11):

```json
{
  "agents": { "defaults": { "model_name": "muse" } },
  "model_list": [
    {
      "model_name": "muse",
      "model": "openai/muse",
      "api_keys": ["<bridge-user-key>"],
      "api_base": "http://127.0.0.1:8765/v1"
    }
  ],
  "channel_list": {
    "telegram": {
      "enabled": true,
      "allow_from": ["<your-chat-id>"],
      "settings": { "token": "<bot-token>", "streaming": { "enabled": true } }
    }
  },
  "tools": {
    "exec": { "enabled": false },
    "web_fetch": { "enabled": false },
    "read_file": { "enabled": false },
    "write_file": { "enabled": false },
    "edit_file": { "enabled": false },
    "append_file": { "enabled": false },
    "cron": { "enabled": false },
    "mcp": { "enabled": false },
    "skills": { "enabled": false },
    "spawn": { "enabled": false },
    "subagent": { "enabled": false }
  },
  "gateway": { "host": "127.0.0.1", "port": 18790 }
}
```

Catatan: `api_keys` berbentuk array; `agents.defaults.model_name` memilih
model default; `allow_from` array berisi chat ID. Semua tools dimatikan
(mode gateway tipis) — system prompt PicoClaw dibuat netral pass-through
agar tidak berebut persona dengan worker Muse.

## 7. Topologi deploy — SEMUA DI MUSE VM (revisi 2026-10-11)

Satu host: Muse VM (x86_64). Semua komponen loopback-only, **tanpa inbound
ke mana pun** — pola worker-pull memang tidak butuh itu.

- `muse-bridge-rs` → 127.0.0.1:8765
- PicoClaw gateway → 127.0.0.1:18790 (Telegram via **outbound** long-poll
  ke api.telegram.org — tidak butuh port inbound)
- SQLite DB, config, dan binary di `~/workspace/muse-bridge-rs/` (persisten
  antar VM replacement)
- Worker = **scheduled task pada runtime agen** (bukan cron VM — VM tidak
  punya cron daemon): poll `GET /muse/pending` → jawab sebagai Muse →
  `POST /muse/answer`

### Supervisi proses (keterbatasan VM)

Hasil cek 2026-10-11: VM punya `systemd` tapi **tanpa user bus**
(`Failed to connect to bus`), tanpa cron daemon, dan `/etc` ephemeral.
Maka:

- TIDAK pakai systemd unit / crontab VM.
- Pakai `watchdog.sh` idempoten di `~/workspace/muse-bridge-rs/bin/`:
  cek port 8765 & 18790, start yang mati. Dijalankan oleh **scheduler runtime
  agen tiap 5 menit** + sekali saat deploy.
- Downtime maksimum setelah VM replacement ≈ interval watchdog (5 menit).
- Scheduler runtime agen survive VM replacement (dikelola runtime, bukan VM).

### Build

- Toolchain Rust/Go awalnya **belum ada** di VM → dipasang rustup, atau
  build di luar lalu copy binary.
- PicoClaw: unduh binary release x86_64 resmi dari GitHub (tanpa build).

## 8. Risiko & mitigasi

- VM replacement mematikan proses (bisa tiap jam) → watchdog cron 5 menit
  adalah satu-satunya jaring pengaman; pastikan runtime cron selalu aktif.
- PicoClaw pre-v1.0 → isolasi dulu, jangan satu-satunya pintu sebelum terbukti.
- Setiap pesan Telegram = 1 LLM turn penuh → batasi allow_from, matikan tools.
- Worker key bocor = jawaban palsu "Muse" → rotasi via keydel/keygen, hash di DB.
- Bridge tanpa TLS di loopback aman; tidak ada ekspos publik dalam topologi ini.
