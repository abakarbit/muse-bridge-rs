#!/usr/bin/env python3
"""Streaming test worker for muse-bridge-rs (Fase 2). Stdlib only.

Single pass: claim 1 job, send 5 chunks ("chunk-1 " .. "chunk-5 ") 0.2s apart
via POST /muse/answer-chunk, then finish with {"done": true}.
Prints SEND <n> <epoch_ns> lines for latency measurement.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

BASE = os.environ.get("BRIDGE_URL", "http://127.0.0.1:8765")
KEY = os.environ.get("BRIDGE_WORKER_KEY", "")
if not KEY:
    print("BRIDGE_WORKER_KEY not set", file=sys.stderr)
    sys.exit(2)


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Bearer " + KEY,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode() or "{}")
        except Exception:
            payload = {}
        return e.code, payload


def main():
    st, pending = req("GET", "/muse/pending?limit=1")
    if st != 200:
        print("pending -> %s %s" % (st, pending), file=sys.stderr)
        return 1
    jobs = pending.get("jobs", [])
    if not jobs:
        print("no jobs to stream", file=sys.stderr)
        return 1
    jid = jobs[0]["id"]
    print("streaming job %s" % jid[:8], flush=True)
    for i in range(1, 6):
        delta = "chunk-%d " % i
        t = time.time_ns()
        st, res = req("POST", "/muse/answer-chunk", {"id": jid, "delta": delta})
        print("SEND %d %d status=%s" % (i, t, st), flush=True)
        if st != 200:
            print("chunk %d failed: %s %s" % (i, st, res), file=sys.stderr)
            return 1
        time.sleep(0.2)
    t = time.time_ns()
    st, res = req("POST", "/muse/answer-chunk", {"id": jid, "done": True})
    print("SEND done %d status=%s duplicate=%s" % (t, st, res.get("duplicate")), flush=True)
    return 0 if st == 200 else 1


if __name__ == "__main__":
    sys.exit(main())
