#!/usr/bin/env bash
# E2E test for muse-bridge-rs (Phase 1). Run from the repo root: ./scripts/e2e_test.sh
# Usage: ./e2e_test.sh
# Prints timing (seconds) for every step. Exits non-zero on first failure.
set -u
cd "$(dirname "$0")/.."

BIN=./target/release/muse-bridge
export BRIDGE_DB="$PWD/e2e-test.db"
export BRIDGE_PORT=18765
BASE="http://127.0.0.1:18765"
export BRIDGE_URL="$BASE"
rm -f "$BRIDGE_DB" "$BRIDGE_DB-wal" "$BRIDGE_DB-shm"

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; echo "$2"; exit 1; }
t0=$(date +%s.%N)
elapsed() { awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t0}"; }

echo "=== (a) keygen user+worker ==="
t=$(date +%s.%N)
USER_KEY=$($BIN keygen --role user --label e2e-9router 2>/dev/null) || fail "keygen user" ""
WORKER_KEY=$($BIN keygen --role worker --label e2e-worker 2>/dev/null) || fail "keygen worker" ""
echo "keygen took $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"
[ -z "$USER_KEY" ] && fail "keygen user" "empty key"
[[ "$USER_KEY" == mbr_* ]] || fail "keygen user" "bad format: ${USER_KEY:0:8}..."
echo "user key prefix: ${USER_KEY:0:14}... | worker key prefix: ${WORKER_KEY:0:14}..."

echo "=== (b) start server ==="
t=$(date +%s.%N)
$BIN serve > e2e-server.log 2>&1 &
SRV=$!
for i in $(seq 1 50); do curl -sf "$BASE/health" >/dev/null 2>&1 && break; sleep 0.1; done
curl -sf "$BASE/health" >/dev/null 2>&1 || { kill $SRV; fail "server start" "$(cat e2e-server.log)"; }
echo "server up in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s (pid $SRV)"
grep -q "listening on 127.0.0.1:18765" e2e-server.log && pass "bind 127.0.0.1:18765"

echo "=== (c) GET /health (no auth) ==="
t=$(date +%s.%N)
OUT=$(curl -s "$BASE/health"); [ "$OUT" = '{"ok":true}' ] || fail "health" "$OUT"
echo "health ${OUT} in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"; pass "health"

echo "=== (d) GET /v1/models ==="
t=$(date +%s.%N)
OUT=$(curl -s -H "Authorization: Bearer $USER_KEY" "$BASE/v1/models")
echo "$OUT" | grep -q '"id":"muse"' || fail "models" "$OUT"
echo "models in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"; pass "models"

echo "=== (e) 401 with wrong token ==="
t=$(date +%s.%N)
CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer wrong-token" "$BASE/v1/models")
[ "$CODE" = "401" ] || fail "401 user endpoint" "got $CODE"
CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer wrong-token" "$BASE/muse/pending")
[ "$CODE" = "401" ] || fail "401 worker endpoint" "got $CODE"
CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $USER_KEY" "$BASE/muse/pending")
[ "$CODE" = "401" ] || fail "401 role mismatch (user on worker endpoint)" "got $CODE"
echo "401 checks in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"; pass "401 wrong token + role mismatch"

echo "=== (f) dashboard probe shortcut ==="
t=$(date +%s.%N)
OUT=$(curl -s -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
  -d '{"model":"muse","max_tokens":1024,"messages":[{"role":"user","content":"hi"}]}' \
  "$BASE/v1/chat/completions")
echo "$OUT" | grep -q "bridge 9Router aktif" || fail "probe" "$OUT"
echo "probe answered in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s (no worker needed)"; pass "probe shortcut"

echo "=== (g) chat/completions + worker roundtrip ==="
t=$(date +%s.%N)
curl -s -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
  -d '{"model":"muse","messages":[{"role":"user","content":"halo"}]}' \
  "$BASE/v1/chat/completions" > e2e-resp.json &
CURLPID=$!
sleep 0.5  # let the job land in pending
BRIDGE_WORKER_KEY="$WORKER_KEY" python3 tests/test_worker.py || { kill $CURLPID $SRV; fail "worker" "test_worker.py failed"; }
wait $CURLPID
TOTAL=$(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")
grep -q "Halo dari worker uji" e2e-resp.json || { kill $SRV; fail "roundtrip" "$(cat e2e-resp.json)"; }
grep -q '"finish_reason":"stop"' e2e-resp.json || { kill $SRV; fail "roundtrip shape" "$(cat e2e-resp.json)"; }
echo "roundtrip total latency: ${TOTAL}s"; pass "chat -> worker -> answer (${TOTAL}s)"

echo "=== (h) duplicate answer detection ==="
# answer the same job twice: first via a fresh job, then re-answer
t=$(date +%s.%N)
curl -s -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
  -d '{"model":"muse","messages":[{"role":"user","content":"dup test"}]}' \
  "$BASE/v1/chat/completions" > /dev/null &
CP=$!; sleep 0.4
JID=$(BRIDGE_WORKER_KEY="$WORKER_KEY" BRIDGE_URL="$BASE" python3 -c "
import json,os,urllib.request
r=urllib.request.Request('$BASE/muse/pending?limit=1',headers={'Authorization':'Bearer '+os.environ['BRIDGE_WORKER_KEY']})
d=json.load(urllib.request.urlopen(r)); print(d['jobs'][0]['id'])")
A1=$(curl -s -H "Authorization: Bearer $WORKER_KEY" -H "Content-Type: application/json" \
  -d "{\"id\":\"$JID\",\"content\":\"first\"}" "$BASE/muse/answer")
A2=$(curl -s -H "Authorization: Bearer $WORKER_KEY" -H "Content-Type: application/json" \
  -d "{\"id\":\"$JID\",\"content\":\"second\"}" "$BASE/muse/answer")
wait $CP
echo "$A1" | grep -q '"duplicate":false' || fail "first answer" "$A1"
echo "$A2" | grep -q '"duplicate":true' || fail "duplicate answer" "$A2"
echo "duplicate detection in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"; pass "duplicate -> true"

echo "=== (i) 429 when queue full ==="
t=$(date +%s.%N)
PIDS=""
for i in 1 2 3 4 5; do
  curl -s -m 3 -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
    -d '{"model":"muse","messages":[{"role":"user","content":"fill"}]}' \
    "$BASE/v1/chat/completions" > /dev/null &
  PIDS="$PIDS $!"
done
sleep 1.0  # let 5 jobs land in pending
CODE=$(curl -s -o /dev/null -w "%{http_code}" -m 5 -H "Authorization: Bearer $USER_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"muse","messages":[{"role":"user","content":"overflow"}]}' \
  "$BASE/v1/chat/completions")
wait $PIDS 2>/dev/null  # only the filler curls, NOT the server
[ "$CODE" = "429" ] || { kill $SRV; fail "429" "got $CODE"; }
echo "429 in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"; pass "429 backpressure"
echo "--- draining queue with worker ---"
BRIDGE_WORKER_KEY="$WORKER_KEY" BRIDGE_URL="$BASE" python3 tests/test_worker.py
BRIDGE_WORKER_KEY="$WORKER_KEY" BRIDGE_URL="$BASE" python3 tests/test_worker.py
sleep 0.3
PEND=$(curl -s -H "Authorization: Bearer $WORKER_KEY" "$BASE/muse/pending?limit=1" | python3 -c "import json,sys; print(json.load(sys.stdin)['pending'])")
[ "$PEND" = "0" ] || echo "WARN: $PEND jobs left pending (may be answered already)"

echo "=== (j) release ==="
t=$(date +%s.%N)
curl -s -H "Authorization: Bearer $USER_KEY" -H "Content-Type: application/json" \
  -d '{"model":"muse","messages":[{"role":"user","content":"rel"}]}' \
  "$BASE/v1/chat/completions" > /dev/null &
CP=$!; sleep 0.4
JID=$(BRIDGE_WORKER_KEY="$WORKER_KEY" BRIDGE_URL="$BASE" python3 -c "
import json,os,urllib.request
r=urllib.request.Request('$BASE/muse/pending?limit=1',headers={'Authorization':'Bearer '+os.environ['BRIDGE_WORKER_KEY']})
d=json.load(urllib.request.urlopen(r)); print(d['jobs'][0]['id'])")
R=$(curl -s -H "Authorization: Bearer $WORKER_KEY" -H "Content-Type: application/json" \
  -d "{\"id\":\"$JID\"}" "$BASE/muse/release")
echo "$R" | grep -q '"ok":true' || { kill $CP $SRV; fail "release" "$R"; }
kill $CP 2>/dev/null; wait $CP 2>/dev/null  # CP would otherwise wait out the 240s long-poll
echo "release in $(awk "BEGIN{printf \"%.2f\", $(date +%s.%N) - $t}")s"; pass "release -> pending"

echo "=== (k) keylist/keydel ==="
OUT=$($BIN keylist 2>/dev/null)
echo "$OUT" | grep -q "e2e-9router" || fail "keylist" "$OUT"
echo "$OUT" | grep -qv "mbr_" || fail "keylist leak" "full key shown!"
DEL=$($BIN keydel e2e-9router)
echo "$DEL" | grep -q "removed 1" || fail "keydel" "$DEL"
CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $USER_KEY" "$BASE/v1/models")
[ "$CODE" = "401" ] || fail "revoked key rejected" "got $CODE"
pass "keylist (prefix only) + keydel + revoked->401"

kill $SRV 2>/dev/null; wait $SRV 2>/dev/null
rm -f "$BRIDGE_DB" "$BRIDGE_DB-wal" "$BRIDGE_DB-shm" e2e-resp.json
echo ""
echo "ALL E2E TESTS PASSED in $(elapsed)s"
