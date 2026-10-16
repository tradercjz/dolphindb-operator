# DolphinDB Operator for Kubernetes

[![CI](https://github.com/tradercjz/dolphindb-operator/actions/workflows/ci.yaml/badge.svg)](https://github.com/tradercjz/dolphindb-operator/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go)](https://golang.org)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.26+-326CE5?logo=kubernetes)](https://kubernetes.io)

**DolphinDB Operator** is a production-ready Kubernetes operator designed to automate the deployment, scaling, Day-2 operations, disaster recovery, and observability of [DolphinDB](https://www.dolphindb.com) high-performance time-series database clusters.

---

## 🚀 Key Features

* **Cloud-Native Topology & Compute-Storage Separation**:
  * Supports standalone (`Single`) and multi-node clustered deployments (`Controller`, `DataNode`, `ComputeNode`).
  * Pure stateless ComputeNodes for elastic query computing without persistent storage overhead.
  * Multi-disk DFS storage support (`volumes` mapping to separate physical NVMe/SATA volumes with automatic `storageConfig.csv` configuration).
* **Deterministic DAG Orchestration (`DDBOperation`)**:
  * Finite-state DAG engine managing rolling restarts, rolling upgrades, and configuration reloads.
  * Guaranteed order: Controller cycles first, followed by staggered DataNodes to eliminate cluster split-brain or metadata sync deadlocks.
* **Safe Elastic Scaling**:
  * Dynamic scale-up and scale-down of DataNodes and ComputeNodes.
  * Safety guards prevent scale-down below `dfsReplicationFactor` to avoid DFS data loss or chunk corruption.
* **Production Health Probes & Graceful Shutdown**:
  * Deep application readiness checks via DolphinDB's native `isDataNodeInitialized()` API.
  * Safe pod termination using standard SIGTERM (`kill -15`) handling, allowing background flush of redo logs and metadata.
* **Automated Cron Backups & Disaster Recovery (`DDBBackup` & `DDBRestore`)**:
  * Standard 5-field cron scheduling (e.g. `0 2 * * *` for daily 2:00 AM DFS backup).
  * Support for partitioned DFS databases and tables (`backup(..., parallel=true, snapshot=true)`).
  * One-click restore to original or cloned databases (`restore(...)`).
* **Self-Contained Web Console (`ddb-dashboard`)**:
  * Lightweight web UI and REST API embedded via Go standard library `embed.FS`.
  * Zero external CDN dependencies; operates completely in air-gapped on-premise environments.
  * Visualizes cluster health, node readiness, DFS replication factors, scheduled backups, and 1-click rolling restarts.
* **Cloud-Aware Service Exposure**:
  * Provider detection from `node.spec.providerID` (AWS / GCP / Azure / bare metal).
  * One click provisions a real cloud load balancer (NLB on AWS) with the security
    groups wired automatically — no AWS Load Balancer Controller and **no IRSA** required.
  * Returns an actually reachable address, and reports progress while the cloud
    finishes provisioning instead of handing out a dead link.
  * See [docs/cloud-exposure.md](docs/cloud-exposure.md) and the
    [v0.3.0 release notes](docs/release-notes-0.3.0.md).
* **HTTPS Without a Domain**:
  * One flag (`spec.security.tls.enabled`) turns the exposed endpoint into HTTPS by
    letting DolphinDB terminate TLS itself.
  * The operator issues and rotates a self-signed certificate and derives the
    domain DolphinDB requires — no ACM certificate, no registered domain, no load
    balancer controller, no IRSA.
  * Bring your own certificate by pointing `spec.security.tls.secretName` at any
    `kubernetes.io/tls` Secret.
  * See [docs/tls.md](docs/tls.md).
* **Create Wizard That Asks the Cluster**:
  * Namespaces, storage classes and the container registry come from the
    installation (`GET /api/platform`), not from hardcoded browser defaults.
  * Only workable choices are offered: namespaces the operator may not write to are
    disabled with a reason, and a CSI-backed storage class whose driver is missing
    is flagged instead of silently leaving PVCs `Pending`.
  * On a cloud the wizard defaults to **nothing exposed** and states the load
    balancer cost per component; on bare metal the traditional NodePort behaviour
    is preserved.
  * See [docs/cluster-provisioning.md](docs/cluster-provisioning.md).
* **Enterprise Observability**:
  * Prometheus metrics exported on port `8989` (`dolphindb_cluster_nodes`, `dolphindb_backup_total_chunks`, etc.).
  * Native `ServiceMonitor` integration for Prometheus Operator.

---

## 🏛️ Architecture Overview

```
                      +-----------------------------+
                      |   Kubernetes API Server     |
                      +--------------+--------------+
                                     |
               +---------------------+---------------------+
               |                                           |
               v                                           v
    +----------------------+                   +----------------------+
    | dolphindb-operator   |                   | ddb-dashboard        |
    | - Cluster Controller |                   | - Web UI (:9527)     |
    | - Node Controller    |                   | - REST API           |
    | - Backup Controller  |                   | - Embedded Assets    |
    | - Restore Controller |                   +----------------------+
    +----------+-----------+
               |
    +----------+---------------------------------------------+
    |                                                        |
    v                                                        v
+------------------------+                        +------------------------+
| DolphinDB Controller   |                        | DolphinDB Nodes        |
| - Leader Election / Raft|                       | - DataNodes (DFS Data) |
| - Metadata Management  | <=== heartbeat ======> | - ComputeNodes (Stateless)
+------------------------+                        +------------------------+
```

---

## 📦 Custom Resource Definitions (CRDs)

| CRD | Kind | Description |
| :--- | :--- | :--- |
| `dolphindbclusters.dolphindb.io` | `DolphinDBCluster` | Declares DolphinDB cluster spec, topology (Controller, DataNode, ComputeNode), storage, and license. |
| `ddbnodes.dolphindb.io` | `DDBNode` | Low-level node instance lifecycle, pod volume mounts, ports, and configuration. |
| `ddboperations.dolphindb.io` | `DDBOperation` | Declarative DAG workflow engine for rolling restarts, rolling upgrades, and maintenance tasks. |
| `ddbbackups.dolphindb.io` | `DDBBackup` | Defines automated cron or one-shot backups for DFS databases and tables. |
| `ddbrestores.dolphindb.io` | `DDBRestore` | Defines DFS restore operations into production or sandbox targets. |

---

## 🛠️ Quick Start

### 1. Installation via Helm (Recommended)

```bash
# Clone the repository
git clone https://github.com/tradercjz/dolphindb-operator.git
cd dolphindb-operator

# Install CRDs and Operator with Web Dashboard
helm install dolphindb-operator deploy/helm/dolphindb-operator \
  --namespace dolphindb-system \
  --create-namespace \
  --set dashboard.enabled=true \
  --set dashboard.service.type=NodePort
```

Verify the deployment:
```bash
kubectl get pods -n dolphindb-system
```

Access the Web Console:
```bash
# Default NodePort is 30527 (or port-forward to local machine)
kubectl port-forward -n dolphindb-system svc/dolphindb-operator-dashboard 9527:9527
# Open browser at http://localhost:9527
```

---

### 2. Deploy a Multi-Node Clustered DolphinDB

Create `cluster.yaml`:
```yaml
apiVersion: dolphindb.io/v1alpha1
kind: DolphinDBCluster
metadata:
  name: ddb-cluster
  namespace: dolphindb-system
spec:
  mode: "cluster"
  version: "3.00.5"
  image: "192.168.100.250/dolphindb/dolphindb:v3.00.5"
  controller:
    replicas: 1
    resources:
      requests:
        cpu: "1000m"
        memory: "2Gi"
    volume:
      storageClassName: "local-path"
      size: "10Gi"
  dataNode:
    replicas: 2
    resources:
      requests:
        cpu: "2000m"
        memory: "4Gi"
    volumes:
      - name: "dfs-data"
        storageClassName: "local-path"
        size: "50Gi"
        mountPath: "/data/ddb/server/data/dfs"
  computeNode:
    replicas: 1
    resources:
      requests:
        cpu: "2000m"
        memory: "4Gi"
```

Apply to cluster:
```bash
kubectl apply -f cluster.yaml
kubectl get dolphindbclusters -n dolphindb-system
```

---

### 3. Scheduled Automated Backups (`DDBBackup`)

Schedule an automated backup every night at 02:00 AM:
```yaml
apiVersion: dolphindb.io/v1alpha1
kind: DDBBackup
metadata:
  name: daily-dfs-backup
  namespace: dolphindb-system
spec:
  cluster: ddb-cluster
  schedule: "0 2 * * *"
  backupDir: "/tmp/backup/daily"
  database: "dfs://prodDB"
  parallel: true
  snapshot: true
```

Check backup status:
```bash
kubectl get ddbbackup -n dolphindb-system
# NAME               CLUSTER       PHASE       CHUNKS   NEXT
# daily-dfs-backup   ddb-cluster   Completed   15       2026-10-15 02:00:00
```

---

### 4. Restore DFS Database (`DDBRestore`)

Restore from a previously completed backup:
```yaml
apiVersion: dolphindb.io/v1alpha1
kind: DDBRestore
metadata:
  name: restore-prod-db
  namespace: dolphindb-system
spec:
  cluster: ddb-cluster
  backupDir: "/tmp/backup/daily"
  database: "dfs://prodDB"
  restoreDatabase: "dfs://prodDB_restored"
```

---

## ☁️ Deploying on AWS EKS

Verified on EKS 1.34. Four things bite you on a fresh AWS project; each is
explained in detail in [docs/cloud-exposure.md](docs/cloud-exposure.md).

**1. Expose with a LoadBalancer, not a NodePort.**
`Service.spec.type = LoadBalancer` is reconciled by the *cloud control plane*, so
no AWS Load Balancer Controller and **no IRSA** are needed, and the load balancer
and node security groups are wired for you. This is the default when the
dashboard detects a cloud. A NodePort is unreachable until the node security
group is opened by hand.

**2. Do not assume IRSA is available.**
Projects governed by a restrictive SCP can deny `iam:*OpenIDConnectProvider*`,
which makes IRSA (and therefore the EBS CSI driver's documented install path)
impossible. Any AWS integration in this operator is deliberately built to work
without it. If EBS is unavailable, a `local-path` provisioner is a workable
substitute for testing.

**3. Free Tier projects restrict instance types.**
Only instances with `free-tier-eligible=true` can be launched. Query them with:

```bash
aws ec2 describe-instance-types --region <region> \
  --filters Name=free-tier-eligible,Values=true \
  --query "sort_by(InstanceTypes,&MemoryInfo.SizeInMiB)[].{Type:InstanceType,vCPU:VCpuInfo.DefaultVCpus,MemGiB:MemoryInfo.SizeInMiB}" \
  --output table
```

`t3.large` is **not** eligible; `m7i-flex.large` (2 vCPU / 8 GiB) is the largest
commonly available option. Requesting an ineligible type fails with
`InvalidParameterCombination - The specified instance type is not eligible for Free Tier`.

**4. `spec.hostname` on EKS.**
Kubernetes rejects a pod `hostname` containing dots, and EKS node names are
private DNS names (`ip-10-0-1-23.<region>.compute.internal`). The operator now
sanitises any node-derived hostname, so no workaround is needed. For a stable,
readable hostname you can still set one explicitly:

```yaml
spec:
  mode: standalone
  standalone:
    hostname: "ddb-standalone"   # optional; defaults to the sanitised node name
```

**5. Want HTTPS?** One flag, no domain required:

```yaml
spec:
  security:
    tls:
      enabled: true
```

The operator issues a self-signed certificate and enables DolphinDB native HTTPS,
so the endpoint becomes `https://…`. See [docs/tls.md](docs/tls.md) for importing
the certificate to remove the browser warning, bringing your own certificate, and
using ACM + load balancer TLS instead once you own a domain.

### Other managed-Kubernetes fixes in this release

See [docs/release-notes-0.3.0.md](docs/release-notes-0.3.0.md) for the full list
with evidence, and [docs/cloud-exposure.md](docs/cloud-exposure.md) for how
endpoints are published.

Highlights:

* **Licence defaults** — `license.mode` defaults to `secret`; host-bound
  licensing is opt-in and no longer mounts a path from the author's machine.
* **Cache engine** — the renderer emits `chunkCacheEngineMemSize` /
  `TSDBCacheEngineSize` whenever `dataSync` is on, so a minimal cluster CR starts.
* **Credentials** — the hardcoded `admin`/`123456` is replaced by a lookup of
  `spec.security.adminSecretName`.
* **Deployment selectors** — the operator and dashboard no longer share a pod
  selector. This changes an immutable field: **delete the operator Deployment
  before upgrading**.

---

## 🔧 Operational Best Practices

1. **Keep-Alive Connection Management**:
   All internal API clients communicate over HTTP with `DisableKeepAlives: true` and `req.Close = true` to prevent accumulating lingering TCP connections against DolphinDB's concurrent connection ceiling (`maxConnections`).
2. **Readiness Probes**:
   The operator uses `isDataNodeInitialized()` to evaluate node availability. Do not use simple TCP port checks as DolphinDB listens on the port before DFS redo-log replay finishes.
3. **Graceful Termination**:
   Containers are terminated via `kill -15` (SIGTERM). DolphinDB intercepts this signal to flush memory-mapped state, flush metadata caches, and deregister from the Controller.

---

## 🧪 Development & Testing

```bash
# Run unit tests
go test -v -count=1 ./...

# Run code format and vet
make fmt
make vet

# Build local binaries
make build

# Cross-compile Linux amd64 binaries
make build-linux
```

---

## 📄 License

Apache License 2.0. See [LICENSE](LICENSE) for details.
