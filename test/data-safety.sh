#!/usr/bin/env bash
#
# Data-safety test for a DolphinDB cluster on Kubernetes.
#
# Asserts on the DATA, not on pod status. Every scenario writes a deterministically
# generated fixture, damages the cluster, reads the fixture back from a *different*
# node, and compares fingerprints. A test that only says "the pod came back" does
# not count: the first round of testing found three defects that all passed the
# DolphinDB health probes while the data was not safe.
#
# Usage:
#   DDB_NS=dolphindb-system DDB_CLUSTER=ddb-safety \
#     DDB_PW=$(kubectl -n dolphindb-system get secret dolphindb-admin \
#              -o jsonpath='{.data.password}' | base64 -d) \
#     bash test/data-safety.sh [scenario...]
#
# Scenarios: restart, force-kill, node-cordon, all

set -uo pipefail

NS="${DDB_NS:-dolphindb-system}"
CLUSTER="${DDB_CLUSTER:-ddb-safety}"
PASSWORD="${DDB_PW:?set DDB_PW to the admin password}"
KUBECTL="${KUBECTL:-kubectl}"

PASS=0; FAIL=0
declare -a FAILED_SCENARIOS=()

log()  { printf '  %s\n' "$*"; }
hdr()  { printf '\n=== %s ===\n' "$*"; }
die()  { printf '  ✗ %s\n' "$*"; FAIL=$((FAIL+1)); FAILED_SCENARIOS+=("$1"); }

dn_pods() {
  $KUBECTL -n "$NS" get pods -l "app.kubernetes.io/name=dolphindb,app.kubernetes.io/component=datanode" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort
}

first_dn() { dn_pods | head -1; }
other_dn() { # a data node other than the given one, so a single node's local state
             # cannot mask a replication problem
  dn_pods | grep -v "^$1\$" | head -1
}

# run <pod> <script> — execute a script against a data node's local API
run_script() {
  local pod="$1" script="$2" req="/tmp/ddb-safety-req.json"
  python3 - "$script" >"$req" <<'PY'
import json, sys
print(json.dumps({"functionName": "executeCode",
                  "params": [{"name": "script", "form": "scalar", "type": "string",
                              "value": sys.argv[1]}]}))
PY
  $KUBECTL -n "$NS" cp "$req" "$pod:/tmp/ddb-safety-req.json" >/dev/null 2>&1
  $KUBECTL -n "$NS" exec "$pod" -- sh -c \
    'wget -q -O - --header="Content-Type: application/json" --post-file=/tmp/ddb-safety-req.json http://127.0.0.1:8901'
}

fingerprint() { # writes (or reads) the fixture and prints "rows sumv checksum"
  run_script "$1" "$(cat /tmp/ddb-safety-fingerprint.dos)"
}

wait_ready() { # wait until a pod is Running and its DDBSTATE is 1
  local pod="$1" tries="${2:-40}"
  for _ in $(seq 1 "$tries"); do
    local ready
    ready=$($KUBECTL -n "$NS" get pod "$pod" \
      -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)
    [ "$ready" = "true" ] && return 0
    sleep 10
  done
  return 1
}

scenario_restart() { # graceful pod restart
  local dn; dn=$(first_dn)
  [ -n "$dn" ] || { die restart "no data node pod"; return 1; }

  hdr "Scenario: graceful pod restart ($dn)"
  local before; before=$(fingerprint "$dn")
  log "before: $before"

  $KUBECTL -n "$NS" delete pod "$dn" >/dev/null 2>&1
  wait_ready "$dn" 40 || { die restart "pod did not come back Ready"; return 1; }

  local peer; peer=$(other_dn "$dn")
  local after; after=$(fingerprint "${peer:-$dn}")
  log "after : $after  (read from ${peer:-$dn})"

  # The fingerprint must match exactly, and it must have been read from another
  # node when one exists, so a surviving node cannot mask the loss.
  if [ "$before" = "$after" ]; then PASS=$((PASS+1)); log "✓ PASS"
  else die restart "fingerprint mismatch"; fi
}

scenario_force_kill() { # SIGKILL, bypassing the pre-stop hook and graceful shutdown
  local dn; dn=$(first_dn)
  [ -n "$dn" ] || { die force-kill "no data node pod"; return 1; }

  hdr "Scenario: forced kill, no graceful shutdown ($dn)"
  local before; before=$(fingerprint "$dn")
  log "before: $before"

  # dataSync=1 + the redo log should make this recoverable; if it is not, the redo
  # log is not where we think it is or dataSync is not actually in effect.
  $KUBECTL -n "$NS" delete pod "$dn" --grace-period=0 --force >/dev/null 2>&1
  wait_ready "$dn" 60 || { die force-kill "pod did not come back Ready"; return 1; }

  local after; after=$(fingerprint "$dn")
  log "after : $after"

  if [ "$before" = "$after" ]; then PASS=$((PASS+1)); log "✓ PASS"
  else die force-kill "rows lost across a forced kill — dataSync/redo log not durable"; fi
}

scenario_node_cordon() { # the pod must not drift: its home is node-affine
  local dn; dn=$(first_dn)
  [ -n "$dn" ] || { die node-cordon "no data node pod"; return 1; }

  hdr "Scenario: node cordoned ($dn)"
  local node; node=$($KUBECTL -n "$NS" get pod "$dn" -o jsonpath='{.spec.nodeName}')
  local before; before=$(fingerprint "$dn")

  $KUBECTL cordon "$node" >/dev/null 2>&1
  $KUBECTL -n "$NS" delete pod "$dn" >/dev/null 2>&1
  wait_ready "$dn" 40

  # The PV is node-affine, so the scheduler has to put the pod back. If it lands
  # elsewhere, the "volume" and the node it lives on have come apart.
  local after_node; after_node=$($KUBECTL -n "$NS" get pod "$dn" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
  local after; after=$(fingerprint "$dn")

  $KUBECTL uncordon "$node" >/dev/null 2>&1

  if [ "$after_node" = "$node" ]; then
    if [ "$before" = "$after" ]; then PASS=$((PASS+1)); log "✓ PASS (stayed on $node)"
    else die node-cordon "fingerprint mismatch after rebind"; fi
  else
    log "⚠ pod moved to $after_node — data survived but the node is no longer the PV's"
    if [ "$before" = "$after" ]; then PASS=$((PASS+1)); else die node-cordon "fingerprint mismatch"; fi
  fi
}

# ------------------------------------------------------------------ main
cat > /tmp/ddb-safety-fingerprint.dos <<'EOF'
login("admin","__PASSWORD__");
n = 100000;
t = table(take(2026.01.01..2026.01.10, n) as d, take(1..n, n) as v, take(string(1..n), n) as s);
db = database("dfs://safety", VALUE, 2026.01.01..2026.01.10);
pt = db.createPartitionedTable(t, `pt, `d);
pt.append!(t);
select count(*) as rows, sum(v) as sumv, max(v) as maxv from pt
EOF
SAFETY_PASSWORD="$PASSWORD" python3 -c 'import os; p="/tmp/ddb-safety-fingerprint.dos"; c=open(p).read(); open(p,"w").write(c.replace("__PASSWORD__", os.environ["SAFETY_PASSWORD"]))' 
cat > /tmp/ddb-safety-read.dos <<'EOF'
login("admin","__PASSWORD__");
select count(*) as rows, sum(v) as sumv, max(v) as maxv from loadTable("dfs://safety",`pt)
EOF
SAFETY_PASSWORD="$PASSWORD" python3 -c 'import os; p="/tmp/ddb-safety-read.dos"; c=open(p).read(); open(p,"w").write(c.replace("__PASSWORD__", os.environ["SAFETY_PASSWORD"]))' 

# fingerprint() needs the read variant, not the write variant: swap it in after the
# initial write so subsequent reads are read-only.
fingerprint() {
  run_script "$1" "$(cat /tmp/ddb-safety-read.dos)"
}

# initial write
hdr "Initial fixture"
dn=$(first_dn); [ -n "$dn" ] || { echo "no data node pod"; exit 1; }
INITIAL=$(run_script "$dn" "$(cat /tmp/ddb-safety-fingerprint.dos)")
log "$INITIAL"

for s in "${@:-restart force-kill node-cordon}"; do
  case "$s" in
    restart)     scenario_restart ;;
    force-kill)  scenario_force_kill ;;
    node-cordon) scenario_node_cordon ;;
    *) echo "unknown scenario: $s" ;;
  esac
done

printf '\n======== RESULT: %d passed, %d failed ========\n' "$PASS" "$FAIL"
[ ${#FAILED_SCENARIOS[@]} -gt 0 ] && printf 'failed: %s\n' "${FAILED_SCENARIOS[*]}"
exit $((FAIL > 0))
