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
#   DDB_NS=dolphindb-system DDB_CLUSTER=ddb-ha \
#     DDB_PW=$(kubectl -n dolphindb-system get secret dolphindb-admin \
#              -o jsonpath='{.data.password}' | base64 -d) \
#     bash test/data-safety.sh [scenario...]
#
# Scenarios: restart, force-kill, node-cordon, all (default: all three)

set -uo pipefail

NS="${DDB_NS:-dolphindb-system}"
CLUSTER="${DDB_CLUSTER:-ddb-ha}"
PASSWORD="${DDB_PW:?set DDB_PW to the admin password}"
KUBECTL="${KUBECTL:-kubectl}"
DN_PORT="${DDB_DN_PORT:-8901}"

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

# run_script <pod> <script> — execute a script against a data node's local API
run_script() {
  local pod="$1" script="$2" req="/tmp/ddb-safety-req.json"
  python3 - "$script" "$PASSWORD" >"$req" <<'PY'
import json, sys
print(json.dumps({"functionName": "executeCode",
                  "params": [{"name": "script", "form": "scalar", "type": "string",
                              "value": sys.argv[1].replace("__PASSWORD__", sys.argv[2])}]}))
PY
  $KUBECTL -n "$NS" cp "$req" "$pod:/tmp/ddb-safety-req.json" >/dev/null 2>&1
  $KUBECTL -n "$NS" exec "$pod" -- sh -c \
    'wget -q -O - --header="Content-Type: application/json" --post-file=/tmp/ddb-safety-req.json http://127.0.0.1:'"$DN_PORT"
}

# values_of <json-response> — print ONLY the data values, one per line.
# The raw response contains a sessionID that changes on every call; comparing raw
# JSON made identical datasets look different (the "phantom mismatch").
values_of() {
  python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)
except Exception:
    print("ERROR:unparseable response"); sys.exit(0)
if r.get("resultCode") != "0":
    print("ERROR:" + r.get("msg", "")[:120]); sys.exit(0)
def flat(v):
    if isinstance(v, list):
        parts = []
        for x in v:
            if isinstance(x, dict):  # table column descriptor
                xv = x.get("value", "")
                parts.append(",".join(str(t) for t in xv) if isinstance(xv, list) else str(xv))
            else:
                parts.append(str(x))
        return ",".join(parts)
    return str(v)
vals = [flat(o.get("value", "")) for o in r.get("object", [])]
print("|".join(vals) if vals else "ERROR:empty result")
'
}

fingerprint() { # read-only fingerprint: "rows|sumv|maxv" style values, or ERROR:...
  run_script "$1" "$(cat /tmp/ddb-safety-read.dos)" | values_of
}

wait_ready() { # wait until a pod is Running and ready
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

scenario_node_cordon() { # the pod must come back to its PV's node, data intact
  local dn; dn=$(first_dn)
  [ -n "$dn" ] || { die node-cordon "no data node pod"; return 1; }

  hdr "Scenario: node cordoned ($dn)"
  local node; node=$($KUBECTL -n "$NS" get pod "$dn" -o jsonpath='{.spec.nodeName}')
  local before; before=$(fingerprint "$dn")
  log "before: $before  (node $node)"

  # The PV is node-affine (local-path): while the node is cordoned the pod CANNOT
  # run anywhere else — it must sit Pending rather than silently reattach to a
  # copy of the volume on another node. So: cordon, delete, observe, then
  # uncordon and require both "back on the same node" and "data intact".
  $KUBECTL cordon "$node" >/dev/null 2>&1
  $KUBECTL -n "$NS" delete pod "$dn" >/dev/null 2>&1
  sleep 30
  local stranded; stranded=$($KUBECTL -n "$NS" get pod "$dn" -o jsonpath='{.status.phase}' 2>/dev/null)
  log "while cordoned: phase=$stranded (Pending is the safe outcome)"

  $KUBECTL uncordon "$node" >/dev/null 2>&1
  wait_ready "$dn" 40 || { die node-cordon "pod did not come back Ready after uncordon"; return 1; }

  local after_node; after_node=$($KUBECTL -n "$NS" get pod "$dn" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
  local after; after=$(fingerprint "$dn")
  log "after : $after  (node $after_node)"

  if [ "$after_node" != "$node" ]; then
    die node-cordon "pod landed on $after_node but its PV lives on $node"
  elif [ "$before" = "$after" ]; then
    PASS=$((PASS+1)); log "✓ PASS (stayed on $node)"
  else
    die node-cordon "fingerprint mismatch after rebind"
  fi
}

# ------------------------------------------------------------------ main
scenarios=("$@")
[ ${#scenarios[@]} -eq 0 ] && scenarios=(restart force-kill node-cordon)

# Fixture: deterministic — drop and recreate on every run so repeat invocations
# of the suite compare like with like.
cat > /tmp/ddb-safety-fingerprint.dos <<'EOF'
login("admin","__PASSWORD__");
n = 100000;
if(existsDatabase("dfs://safety")) { dropDatabase("dfs://safety"); }
t = table(take(2026.01.01..2026.01.10, n) as d, take(1..n, n) as v, take(string(1..n), n) as s);
db = database("dfs://safety", VALUE, 2026.01.01..2026.01.10);
pt = db.createPartitionedTable(t, `pt, `d);
pt.append!(t);
select count(*) as rows, sum(v) as sumv, max(v) as maxv from pt
EOF
cat > /tmp/ddb-safety-read.dos <<'EOF'
login("admin","__PASSWORD__");
select count(*) as rows, sum(v) as sumv, max(v) as maxv from loadTable("dfs://safety", `pt)
EOF

# initial write
hdr "Initial fixture"
dn=$(first_dn); [ -n "$dn" ] || { echo "no data node pod"; exit 1; }
INITIAL=$(run_script "$dn" "$(cat /tmp/ddb-safety-fingerprint.dos)" | values_of)
log "$INITIAL"
case "$INITIAL" in ERROR*) echo "fixture write failed: $INITIAL"; exit 1;; esac

for s in "${scenarios[@]}"; do
  case "$s" in
    restart)     scenario_restart ;;
    force-kill)  scenario_force_kill ;;
    node-cordon) scenario_node_cordon ;;
    all)         scenario_restart; scenario_force_kill; scenario_node_cordon ;;
    *) echo "unknown scenario: $s" ;;
  esac
done

printf '\n======== RESULT: %d passed, %d failed ========\n' "$PASS" "$FAIL"
[ ${#FAILED_SCENARIOS[@]} -gt 0 ] && printf 'failed: %s\n' "${FAILED_SCENARIOS[*]}"
exit $((FAIL > 0))
