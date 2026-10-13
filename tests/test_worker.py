#!/usr/bin/env python3
"""Test worker for muse-bridge-rs (Fase 1). Stdlib only.

Single pass: GET /muse/pending?limit=3 with the worker key, answer each job
with canned text via POST /muse/answer. Quiet unless something fails.
"""
import json
import os
import sys
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
    st, pending = req("GET", "/muse/pending?limit=3")
    if st != 200:
        print("pending -> %s %s" % (st, pending), file=sys.stderr)
        return 1
    jobs = pending.get("jobs", [])
    print("claimed %d job(s), %s still pending" % (len(jobs), pending.get("pending")))
    for j in jobs:
        jid = j["id"]
        content = "Halo dari worker uji (job %s)" % jid[:8]
        st, res = req("POST", "/muse/answer", {"id": jid, "content": content})
        print("answer %s -> %s duplicate=%s" % (jid[:8], st, res.get("duplicate")))
        if st != 200:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
