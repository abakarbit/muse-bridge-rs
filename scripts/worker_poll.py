#!/usr/bin/env python3
"""Worker poll step 1: fetch pending jobs from the bridge.

Usage:
  worker_poll.py poll            -> prints jobs JSON to stdout, exit 0 (no jobs prints '[]')
  worker_poll.py answer '<id>'   -> reads reply content from stdin, POSTs /muse/answer
Errors:
  connection refused/timeout -> appends timestamped line to logs/worker-cron.log, exits 3
  401 -> exits 4 (caller handles the marker file)
The worker key is read from keys/worker.key (under the deployment root) and
never printed. Deployment root = parent of this script's directory, or
$MUSE_BRIDGE_HOME when set. Bridge URL = $BRIDGE_URL (default loopback).
"""
import json, os, sys, urllib.request, urllib.error, datetime

BASE = os.environ.get("MUSE_BRIDGE_HOME") or os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)
KEY_PATH = os.path.join(BASE, "keys", "worker.key")
LOG_PATH = os.path.join(BASE, "logs", "worker-cron.log")
BRIDGE = os.environ.get("BRIDGE_URL", "http://127.0.0.1:8765")


def _key():
    with open(KEY_PATH) as f:
        return f.read().strip()


def _req(url, data=None, method=None):
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", "Bearer " + _key())
    if data:
        r.add_header("Content-Type", "application/json")
    return r


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "poll"
    try:
        if cmd == "poll":
            with urllib.request.urlopen(_req(BRIDGE + "/muse/pending?limit=5"), timeout=10) as resp:
                body = json.loads(resp.read().decode())
            jobs = body.get("jobs", [])
            print(json.dumps(jobs, ensure_ascii=False))
            return 0
        elif cmd == "answer":
            job_id = sys.argv[2]
            content = sys.stdin.read()
            payload = json.dumps({"id": job_id, "content": content}).encode()
            with urllib.request.urlopen(_req(BRIDGE + "/muse/answer", data=payload, method="POST"), timeout=10) as resp:
                print(json.dumps(json.loads(resp.read().decode()), ensure_ascii=False))
            return 0
        else:
            print("unknown command", file=sys.stderr)
            return 2
    except urllib.error.HTTPError as e:
        if e.code == 401:
            return 4
        print(f"HTTP {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
        return 5
    except Exception as e:
        ts = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a") as f:
            f.write(f"{ts} worker-cron: bridge unreachable ({type(e).__name__}: {e})\n")
        return 3


sys.exit(main())
