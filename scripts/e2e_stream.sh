#!/usr/bin/env bash
# SSE streaming e2e for muse-bridge-rs (Phase 2). Run from the repo root: ./scripts/e2e_stream.sh
# Usage: ./e2e_stream.sh
# Verifies SSE frames byte-per-byte, per-chunk latency < 1s, non-stream
# accumulation, full-answer fallback, /metrics, and optional TLS.
set -u
cd "$(dirname "$0")/.."

BIN=./target/release/muse-bridge
export BRIDGE_DB="$PWD/stream-test.db"
export BRIDGE_PORT=18766
BASE="http://127.0.0.1:18766"
export BRIDGE_URL="$BASE"
rm -f "$BRIDGE_DB" "$BRIDGE_DB-wal" "$BRIDGE_DB-shm" sse_ts.txt worker_ts.txt

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; echo "$2"; exit 1; }
t0=$(date +%s.%N)
elapsed() { awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t0}"; }

echo "=== setup: keygen + server ==="
USER_KEY=$($BIN keygen --role user --label stream-e2e 2>/dev/null) || fail "keygen" ""
WORKER_KEY=$($BIN keygen --role worker --label stream-worker 2>/dev/null) || fail "keygen" ""
$BIN serve > stream-server.log 2>&1 &
SRV=$!
for i in $(seq 1 50); do curl -sf "$BASE/health" >/dev/null 2>&1 && break; sleep 0.1; done
curl -sf "$BASE/health" >/dev/null 2>&1 || { kill $SRV; fail "server start" "$(cat stream-server.log)"; }
pass "server up (pid $SRV)"
cleanup() { kill $SRV 2>/dev/null; wait $SRV 2>/dev/null; }

echo ""
echo "=== (b) SSE stream:true + chunking worker ==="
t=$(date +%s.%N)
curl -sN -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
  -d '{"model":"muse","stream":true,"messages":[{"role":"user","content":"stream me"}]}' \
  "$BASE/v1/chat/completions" \
  | while IFS= read -r line; do printf '%s %s\n' "$(date +%s.%N)" "$line"; done > sse_ts.txt &
CURLPID=$!
sleep 0.5  # let the job land in pending
BRIDGE_WORKER_KEY="$WORKER_KEY" python3 tests/test_worker_stream.py > worker_ts.txt 2>&1 \
  || { kill $CURLPID; cleanup; fail "stream worker" "$(cat worker_ts.txt)"; }
wait $CURLPID
echo "stream roundtrip: $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"
python3 <<'PYEOF'
import json, sys

def fail(m):
    print("FAIL:", m); sys.exit(1)

# --- parse client capture: "<sec>.<nanosec> <raw line>"
def parse_ns(s):
    sec, _, frac = s.partition(".")
    return int(sec) * 1_000_000_000 + int((frac + "000000000")[:9])

events, pings = [], 0
for line in open("sse_ts.txt"):
    line = line.rstrip("\n")
    ts_s, _, raw = line.partition(" ")
    if not raw.strip():
        continue  # blank SSE frame separator
    ts = parse_ns(ts_s)
    if raw.startswith(":"):
        pings += 1
        continue
    if not raw.startswith("data: "):
        fail("non-data line: %r" % raw[:80])
    events.append((ts, raw[6:]))

# --- byte-level frame assertions
if len(events) != 8:
    fail("expected 8 data frames, got %d" % len(events))
frames = []
for ts, payload in events:
    if payload == "[DONE]":
        frames.append((ts, "DONE")); continue
    try:
        frames.append((ts, json.loads(payload)))
    except Exception as e:
        fail("frame not valid JSON: %r (%s)" % (payload[:80], e))

f0 = frames[0][1]
assert f0["object"] == "chat.completion.chunk", f0
assert f0["model"] == "muse", f0
assert f0["choices"][0]["delta"] == {"role": "assistant"}, f0
assert f0["choices"][0]["finish_reason"] is None, f0
cid = f0["id"]
assert cid.startswith("chatcmpl-"), cid

for i in range(1, 6):
    f = frames[i][1]
    assert f["id"] == cid, (i, f["id"])
    assert f["object"] == "chat.completion.chunk", f
    assert f["choices"][0]["delta"] == {"content": "chunk-%d " % i}, f["choices"][0]["delta"]
    assert f["choices"][0]["finish_reason"] is None, f

f6 = frames[6][1]
assert f6["id"] == cid, f6["id"]
assert f6["choices"][0]["delta"] == {}, f6["choices"][0]["delta"]
assert f6["choices"][0]["finish_reason"] == "stop", f6
assert frames[7][1] == "DONE", frames[7]
print("PASS: byte-level SSE frames (role, 5 chunks in order, stop, [DONE])")

# --- per-chunk latency worker -> client (< 1s each)
sends = {}
for line in open("worker_ts.txt"):
    p = line.split()
    if len(p) >= 4 and p[0] == "SEND" and p[1].isdigit():
        sends[int(p[1])] = int(p[2])
assert len(sends) >= 5, "worker send timestamps missing: %r" % sends
worst = 0
for i in range(1, 6):
    lat_ms = (frames[i][0] - sends[i]) / 1e6
    worst = max(worst, lat_ms)
    print("  chunk-%d latency: %.1f ms" % (i, lat_ms))
    assert lat_ms < 1000, "chunk-%d too slow: %.1f ms" % (i, lat_ms)
print("PASS: per-chunk latency < 1s (worst %.1f ms), pings=%d" % (worst, pings))
PYEOF
[ $? -eq 0 ] || { cleanup; exit 1; }
pass "SSE byte-verification + latency"

echo ""
echo "=== (c) non-stream request + chunking worker (accumulation) ==="
t=$(date +%s.%N)
curl -s -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
  -d '{"model":"muse","messages":[{"role":"user","content":"accumulate"}]}' \
  "$BASE/v1/chat/completions" > accum.json &
CP=$!
sleep 0.5
BRIDGE_WORKER_KEY="$WORKER_KEY" python3 tests/test_worker_stream.py > /dev/null 2>&1 \
  || { kill $CP; cleanup; fail "stream worker (accum)"; }
wait $CP
CONTENT=$(python3 -c "import json;print(json.load(open('accum.json'))['choices'][0]['message']['content'])")
[ "$CONTENT" = "chunk-1 chunk-2 chunk-3 chunk-4 chunk-5 " ] \
  || { cleanup; fail "accumulation" "$CONTENT"; }
echo "non-stream accumulation in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"
pass "non-stream accumulation -> full JSON"

echo ""
echo "=== (d) full-answer worker -> stream:true endpoint ==="
t=$(date +%s.%N)
curl -sN -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
  -d '{"model":"muse","stream":true,"messages":[{"role":"user","content":"full"}]}' \
  "$BASE/v1/chat/completions" > sse_full.txt &
CP=$!
sleep 0.5
BRIDGE_WORKER_KEY="$WORKER_KEY" python3 tests/test_worker.py > /dev/null 2>&1 \
  || { kill $CP; cleanup; fail "full worker"; }
wait $CP
python3 - <<'PYEOF'
import json, sys
frames = []
for line in open("sse_full.txt"):
    line = line.rstrip("\n")
    if line.startswith("data: "):
        p = line[6:]
        frames.append("DONE" if p == "[DONE]" else json.loads(p))
def fail(m): print("FAIL:", m); sys.exit(1)
if len(frames) != 4: fail("expected 4 frames, got %d" % len(frames))
assert frames[0]["choices"][0]["delta"] == {"role": "assistant"}, frames[0]
c = frames[1]["choices"][0]["delta"].get("content", "")
assert c.startswith("Halo dari worker uji (job "), repr(c)
assert frames[2]["choices"][0]["finish_reason"] == "stop", frames[2]
assert frames[3] == "DONE", frames[3]
print("PASS: full-answer worker -> 1 content chunk + stop + [DONE]")
PYEOF
[ $? -eq 0 ] || { cleanup; exit 1; }
echo "full-answer over SSE in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"

echo ""
echo "=== (e) /metrics ==="
t=$(date +%s.%N)
M=$(curl -s "$BASE/metrics")
for name in bridge_jobs_pending bridge_jobs_processing bridge_jobs_done bridge_jobs_failed \
            bridge_lease_expiries_total bridge_answers_total bridge_timeouts_total bridge_chunks_total; do
  echo "$M" | grep -q "^${name} " || { cleanup; fail "metrics" "missing $name"; }
done
echo "$M" | grep -E "^bridge_(answers|chunks)_total [1-9]" >/dev/null \
  || { cleanup; fail "metrics" "counters not incremented"; }
echo "metrics in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"
pass "metrics (8 series, counters incremented)"
echo "--- sample ---"
echo "$M" | grep -v "^#" | head -8

echo ""
echo "=== (f) TLS ==="
t=$(date +%s.%N)
openssl req -x509 -newkey rsa:2048 -keyout tls-key.pem -out tls-cert.pem \
  -days 1 -nodes -subj "/CN=127.0.0.1" 2>/dev/null \
  || { cleanup; fail "openssl" "cert generation failed"; }
export BRIDGE_DB="$PWD/tls-test.db"
export BRIDGE_PORT=18767
export BRIDGE_TLS_CERT="$PWD/tls-cert.pem"
export BRIDGE_TLS_KEY="$PWD/tls-key.pem"
rm -f "$BRIDGE_DB" "$BRIDGE_DB-wal" "$BRIDGE_DB-shm"
$BIN serve > tls-server.log 2>&1 &
TLSRV=$!
for i in $(seq 1 50); do curl -sk "https://127.0.0.1:18767/health" >/dev/null 2>&1 && break; sleep 0.1; done
OUT=$(curl -sk "https://127.0.0.1:18767/health")
[ "$OUT" = '{"ok":true}' ] || { kill $TLSRV; cleanup; fail "tls health" "$OUT"; }
grep -q "TLS enabled" tls-server.log || { kill $TLSRV; cleanup; fail "tls log" "$(cat tls-server.log)"; }
# plain HTTP on the TLS port must NOT speak HTTP
if curl -s --max-time 3 "http://127.0.0.1:18767/health" 2>/dev/null | grep -q '"ok"'; then
  kill $TLSRV; cleanup; fail "tls" "plain HTTP unexpectedly worked on TLS port"
fi
kill $TLSRV 2>/dev/null; wait $TLSRV 2>/dev/null
echo "TLS handshake + health in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"
pass "TLS (self-signed, -k)"

cleanup
rm -f "$BRIDGE_DB" "$BRIDGE_DB-wal" "$BRIDGE_DB-shm" tls-cert.pem tls-key.pem \
  "$PWD/stream-test.db" "$PWD/stream-test.db-wal" "$PWD/stream-test.db-shm" \
  sse_ts.txt worker_ts.txt sse_full.txt accum.json
unset BRIDGE_TLS_CERT BRIDGE_TLS_KEY
echo ""
echo "ALL STREAM TESTS PASSED in $(elapsed)s"
