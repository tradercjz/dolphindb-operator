# Data Safety Test Plan

A concrete, reproducible plan for validating that a DolphinDB cluster on Kubernetes
does not lose or corrupt data. Everything here is written to be run against a live
cluster and to produce a definite pass/fail, because the failures found so far were
all silent.

---

## 0. Why this exists

Three defects were found during the first round of testing, all of which passed the
DolphinDB health probes while the data was not actually safe:

| Defect | Symptom during normal operation | Reality |
| :--- | :--- | :--- |
| Data node home on the container's ephemeral layer | `DDBSTATE=1`, probes green | the redo log was discarded on every pod restart; **6 rows written → 2 rows readable** |
| Admin password reverted to the factory default | cluster reported `Ready` | every authenticated operator call failed, so the ACL key sync and backups silently stopped working |
| ACL keys mounted at the wrong path, and only the public half | none | data nodes generated their own pair and never came up: `Failed to decrypt the given message by RSA private key` |

The lesson: **health probes are not a data-safety test.** Every scenario below must
assert on the data itself, not on pod status.

All three are fixed in `aad97f2`. The plan below is what should be run to confirm
they stay fixed and to find the ones that have not been found yet.

---

## 1. Test harness

Use one cluster, one writer, and assertions that read data back. Do not reuse a
single table across tests — a test that passes because a previous test wrote the
data is worse than no test.

```
cluster:  ddb-safety
mode:     cluster
controller: replicas 3, antiAffinity Required   # 3 for Raft; 1 if the node count is short
datanode:   replicas 3, antiAffinity Required
config:   dfsReplicationFactor "2", dfsReplicaReliabilityLevel "1", dataSync "1"
datanode.config: chunkCacheEngineMemSize "1", TSDBCacheEngineSize "1"
storage:  the class under test (see §7)
```

Helper used throughout (writes then reads a known checksum):

```bash
# write a deterministically generated table and remember its checksum
cat > /tmp/write.dos <<'EOF'
login("admin","<password>");
n = 100000;
t = table(take(2026.01.01..2026.01.10, n) as d,
          take(1..n, n) as v,
          take(string(1..n), n) as s);
db = database("dfs://safety", VALUE, 2026.01.01..2026.01.10);
pt = db.createPartitionedTable(t, `pt, `d);
pt.append!(t);
// expected fingerprint, computed by the database itself
select count(*) as rows, sum(v) as sumv, sum(size(s)) as sizes from pt
EOF

# read it back and print the same fingerprint
cat > /tmp/read.dos <<'EOF'
login("admin","<password>");
select count(*) as rows, sum(v) as sumv, sum(size(s)) as sizes from loadTable("dfs://safety",`pt)
EOF
```

A test passes only when the read fingerprint equals the write fingerprint. Also
verify from a **different** data node than the one that wrote, so a single node's
local state cannot mask a replication problem.

---

## 2. Durability against pod restarts

The scenario that exposed the first defect.

| Step | Command | Assert |
| :--- | :--- | :--- |
| 1 | write the fixture | fingerprint F |
| 2 | confirm the home is on a PVC: `df <home>` must not report `overlay` | not `overlay` |
| 3 | `kubectl delete pod <dn-0>` | pod returns Ready |
| 4 | read back | fingerprint == F |
| 5 | check the log for `[startup::redoLogReplay]` | replay ran, and `dfsRedoLogDir` is under the home, not empty |
| 6 | repeat for **every** role: controller, data node, compute node | data intact after each |

**Also test the un-graceful path**, which is the case that actually loses data:

* `kubectl delete pod --grace-period=0 --force` (SIGKILL, no pre-stop hook)
* `kubectl delete pod` with a short `terminationGracePeriodSeconds`
* kill PID 1 inside the container with `kill -9`

With `dataSync=1` the redo log should make all of these recoverable. If a forced
kill loses rows while a graceful stop does not, `dataSync` is not actually in effect
or the redo log is not where we think it is.

---

## 3. Durability against node loss

This is where storage class choice decides the answer.

| Step | Assert |
| :--- | :--- |
| write the fixture, note which node hosts each data node | fingerprint F |
| cordon + drain the node, then terminate the EC2 instance | the data node pod cannot reschedule to another node if its PV is node-affine |
| for `local-path`: expect the pod to stay `Pending` until a replacement node is added, and expect the *PV's* data to be gone if the node's disk is gone | the cluster must still serve reads from the surviving replicas (`dfsReplicationFactor`) |
| for EBS CSI: expect the pod to reschedule within the same AZ and find its volume | fingerprint == F without any recovery step |
| after the node is replaced, `getClusterChunksStatus` / re-balance | the replication factor is restored automatically |

**The headline assertion for `local-path`:** data survives a node loss **only if
`dfsReplicationFactor >= 2`**. Test it by killing one data node's node with
replication set to 1 as a control — that run *should* lose data, and if it does not,
the test is not measuring what it claims to.

---

## 4. Controller failover (Raft)

| Step | Assert |
| :--- | :--- |
| 3 controllers, write the fixture | fingerprint F |
| kill the Raft leader pod | a new leader is elected, `getMasterController` names it |
| read + write through the new leader | fingerprint == F, and the new write is durable |
| kill a second controller (2 of 3 down) | writes are refused rather than silently accepted |
| bring both back | the cluster converges, no split brain, fingerprint == F |

Watch for: the operator's `getLeaderControllerAlias` and `status.masterController`
must follow the real leader, not a cached one.

---

## 5. Backup and restore

| Step | Assert |
| :--- | :--- |
| create `DDBBackup` with a cron schedule | `phase: Completed`, chunk count > 0 |
| write more rows after the backup | fingerprint changes |
| `DDBRestore` into `dfs://safety_restored` | restored fingerprint == the fingerprint **at backup time**, not the current one |
| restore into the **original** database | the old state is back, and the restore is recorded as such |
| delete the backup target and restore | the failure is reported, not silently swallowed |

Also verify the operator can actually authenticate for backups — with a rotated
admin password, a broken `spec.security.adminSecretName` must fail loudly rather
than produce an empty backup.

---

## 6. Data integrity

Checksums catch corruption that reads do not:

* after each destructive scenario, run a table scan with a checksum
  (`select sum(v), count(*), sum(size(s))`) and compare, rather than `select count(*)`
  alone — a truncated chunk keeps the row count plausible;
* compare the **same** query executed on two different data nodes;
* check `getRecoverTaskList()` / `getRecoveryTaskStatus()` for outstanding recovery;
* look for `ERROR`/`WARNING` in every node's log after the test, not just the one
  that was killed.

---

## 7. Storage class matrix

Run §2 and §3 against each class available in the cluster.

| Class | Provisioner | Expected |
| :--- | :--- | :--- |
| `local-path` | `rancher.io/local-path` | survives pod restart; survives node loss **only** with replication >= 2 |
| EBS `gp3` via CSI | `ebs.csi.aws.com` | survives node loss within the AZ; needs the CSI driver installed |
| `gp2` | `kubernetes.io/aws-ebs` | **expected to fail** — CSI migration forwards it to the EBS CSI driver |

The dashboard's create wizard already reports which of these the cluster can
actually use (`GET /api/platform`); this matrix is the data-level confirmation.

---

## 8. Operational edges worth adding

* **Disk full** — fill the data volume, write more, and confirm DolphinDB refuses
  the write instead of corrupting a chunk.
* **PVC expansion** — grow `dataVolumeSize` and confirm the PV expands without a
  restart and without data loss.
* **Clock skew** — restart a node with a shifted clock and confirm redo replay still
  behaves.
* **Rolling upgrade** — change the image and confirm the ordered restart (controller
  first, then staggered data nodes) keeps the cluster readable throughout.
* **Concurrent writer during failover** — keep a writer running while a node is
  killed; assert either a clean error or a durable write, never a silent partial.

---

## 9. Reporting

For each scenario record: the fingerprint before and after, the exact command, the
node/pod that was destroyed, and the log lines that prove what happened. A test that
only says "the pod came back" does not count.

Target for the next session: **every row in §2 and §3 passing with the fixture
fingerprint unchanged, on at least two storage classes.**
