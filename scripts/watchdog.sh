#!/usr/bin/env bash
# watchdog.sh — idempotent supervisor for a muse-bridge-rs deployment.
# Checks the bridge (:8765) and the PicoClaw gateway (:18790), starting
# whatever is down. Safe to run repeatedly (e.g. every 5 min via cron).
# PicoClaw is SKIPPED while no channel is enabled in ~/.picoclaw/config.json
# (a gateway with no channels serves nothing — don't restart-loop it).
#
# Layout assumed (all overridable by env):
#   MUSE_BRIDGE_HOME   deployment root (default: $HOME/workspace/muse-bridge-rs)
#     bin/muse-bridge  bridge binary
#     bin/picoclaw     PicoClaw binary (optional companion)
#     logs/            watchdog/bridge/picoclaw logs
#     bridge.db        SQLite queue (BRIDGE_DB)
#
# Optional reverse-SSH tunnel keeper (e.g. to expose the gateway dashboard
# on a private overlay network such as Tailscale): set TUNNEL_ENABLED=1 plus
# TUNNEL_TARGET (user@host), TUNNEL_REMOTE_BIND (e.g. 100.x.x.x:18790).
# TUNNEL_IDENTITY / TUNNEL_KNOWN_HOSTS / TUNNEL_PROXY_COMMAND are optional.
set -u

BASE="${MUSE_BRIDGE_HOME:-$HOME/workspace/muse-bridge-rs}"
BIN="$BASE/bin"
LOGS="$BASE/logs"
BRIDGE_PORT="${BRIDGE_PORT:-8765}"
GATEWAY_PORT="${GATEWAY_PORT:-18790}"
BRIDGE_DB="${BRIDGE_DB:-$BASE/bridge.db}"
export BRIDGE_DB BRIDGE_PORT

mkdir -p "$LOGS"
WLOG="$LOGS/watchdog.log"

log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$WLOG"; }

port_open() { curl -sf -m 5 "http://127.0.0.1:$1/health" >/dev/null 2>&1; }

# NOTE: pgrep -x matches the process *name* (comm), never the full command
# line. This is deliberate: `pgrep -f "muse-bridge serve"` can false-positive
# on a parent shell whose own command line merely *mentions* that string,
# which would wrongly skip a restart. `-x` is immune to that class of bug.
pgrep_running() { pgrep -x "$1" >/dev/null 2>&1; }

start_bridge() {
    log "bridge :$BRIDGE_PORT DOWN -> starting bin/muse-bridge serve"
    setsid nohup "$BIN/muse-bridge" serve >> "$LOGS/bridge.log" 2>&1 < /dev/null &
    disown 2>/dev/null || true
    for _ in $(seq 1 30); do
        port_open "$BRIDGE_PORT" && { log "bridge :$BRIDGE_PORT UP"; return 0; }
        sleep 1
    done
    log "bridge :$BRIDGE_PORT FAILED to come up (see $LOGS/bridge.log)"
    return 1
}

channels_enabled() {
    python3 -c "
import json, os, sys
try:
    cfg = json.load(open(os.path.expanduser('~/.picoclaw/config.json')))
    ch = cfg.get('channel_list') or cfg.get('channels') or {}
    sys.exit(0 if any(isinstance(v, dict) and v.get('enabled') for v in ch.values()) else 1)
except Exception:
    sys.exit(1)
" 2>/dev/null
}

start_picoclaw() {
    log "picoclaw :$GATEWAY_PORT DOWN (channels enabled) -> starting bin/picoclaw gateway"
    setsid nohup "$BIN/picoclaw" gateway >> "$LOGS/picoclaw.log" 2>&1 < /dev/null &
    disown 2>/dev/null || true
    for _ in $(seq 1 30); do
        port_open "$GATEWAY_PORT" && { log "picoclaw :$GATEWAY_PORT UP"; return 0; }
        sleep 1
    done
    log "picoclaw :$GATEWAY_PORT FAILED to come up (see $LOGS/picoclaw.log)"
    return 1
}

# --- bridge (always supervised) ---
if port_open "$BRIDGE_PORT"; then
    : # healthy, silent
elif pgrep_running "muse-bridge"; then
    log "bridge process exists but :$BRIDGE_PORT not responding (still starting?)"
else
    start_bridge || true
fi

# --- picoclaw (only when a channel is enabled) ---
if channels_enabled; then
    if port_open "$GATEWAY_PORT"; then
        : # healthy, silent
    elif pgrep_running "picoclaw"; then
        log "picoclaw process exists but :$GATEWAY_PORT not responding (still starting?)"
    else
        start_picoclaw || true
    fi
else
    if pgrep_running "picoclaw"; then
        log "INFO: no channels enabled in picoclaw config; gateway process found left running (not managed)"
    fi
fi

# --- optional reverse SSH tunnel keeper ---
# Example: expose the gateway dashboard on a tailnet IP of a remote host:
#   TUNNEL_ENABLED=1 TUNNEL_TARGET=user@100.x.x.x \
#   TUNNEL_REMOTE_BIND=100.x.x.x:18790 ./watchdog.sh
# Requires GatewayPorts clientspecified (or yes) on the remote sshd when
# binding to a non-loopback remote address.
if [ "${TUNNEL_ENABLED:-0}" = "1" ] && [ -n "${TUNNEL_TARGET:-}" ] && [ -n "${TUNNEL_REMOTE_BIND:-}" ]; then
    TUNNEL_PIDFILE="$BASE/tunnel.pid"
    TUNNEL_LOCAL_PORT="${TUNNEL_LOCAL_PORT:-$GATEWAY_PORT}"

    tunnel_alive() {
        [ -f "$TUNNEL_PIDFILE" ] && kill -0 "$(cat "$TUNNEL_PIDFILE" 2>/dev/null)" 2>/dev/null
    }

    start_tunnel() {
        local ssh_opts=(
            -o BatchMode=yes -o ConnectTimeout=40
            -o StrictHostKeyChecking=accept-new
            -o ServerAliveInterval=30 -o ServerAliveCountMax=3
            -o ExitOnForwardFailure=yes
        )
        [ -n "${TUNNEL_IDENTITY:-}" ] && ssh_opts+=(-i "$TUNNEL_IDENTITY")
        [ -n "${TUNNEL_KNOWN_HOSTS:-}" ] && ssh_opts+=(-o "UserKnownHostsFile=$TUNNEL_KNOWN_HOSTS")
        [ -n "${TUNNEL_PROXY_COMMAND:-}" ] && ssh_opts+=(-o "ProxyCommand=$TUNNEL_PROXY_COMMAND")
        # Retry loop: a fresh -R bind can fail if the remote sshd still holds
        # the port from a just-died session ("remote port forwarding failed").
        # The stale listener clears shortly after the old session ends.
        for attempt in 1 2 3; do
            log "tunnel :$TUNNEL_LOCAL_PORT -> $TUNNEL_TARGET DOWN -> starting ssh -R (attempt $attempt)"
            setsid nohup ssh "${ssh_opts[@]}" \
                -N -R "$TUNNEL_REMOTE_BIND:127.0.0.1:$TUNNEL_LOCAL_PORT" \
                "$TUNNEL_TARGET" >> "$LOGS/tunnel.log" 2>&1 < /dev/null &
            # NOTE: $! is the setsid wrapper PID, NOT the ssh PID (setsid
            # forks). Find the real ssh child by its -R argument instead.
            sleep 3
            TPID="$(pgrep -f "ssh.*-R $TUNNEL_REMOTE_BIND" 2>/dev/null | head -1)"
            [ -n "$TPID" ] && echo "$TPID" > "$TUNNEL_PIDFILE"
            sleep 7
            if tunnel_alive; then
                log "tunnel :$TUNNEL_LOCAL_PORT -> $TUNNEL_TARGET UP"
                return 0
            fi
            log "tunnel attempt $attempt failed, retrying in 10s"
            sleep 10
        done
        log "tunnel FAILED to come up after 3 attempts (see $LOGS/tunnel.log)"
        return 1
    }

    if tunnel_alive; then
        : # healthy, silent
    else
        start_tunnel || true
    fi
fi
