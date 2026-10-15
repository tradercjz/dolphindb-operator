# Release Notes — v0.3.0

**Theme:** make the operator actually work on managed Kubernetes (verified on
Amazon EKS 1.34), and turn "expose this database" into a one-click operation that
provisions a real cloud load balancer.

Everything below was found and verified against a live EKS cluster in
`ap-southeast-2` (2 × `m7i-flex.large`), not by reading the code alone.

---

## TL;DR

| Area | Before | After |
| :--- | :--- | :--- |
| Expose a node | `type=NodePort`, URL built from a **private** IP → unreachable | Cloud-aware: `LoadBalancer` (NLB) on a cloud, real reachable URL, `pending` + progress while provisioning |
| Standalone on EKS | Pod creation failed: `spec.hostname ... must not contain dots` | Node-derived hostnames are sanitised automatically |
| Licence | HostPath mount of `/home/jzchen/dolphindb.lic` → container never started | Opt-in only; `license.mode` defaults to `secret` |
| Cluster mode | DataNodes crash-looped: `dataSync and cache engine must be enable at same time` | Cache engine defaults emitted automatically |
| Credentials | `admin` / `123456` hardcoded in 8 places | Read from `spec.security.adminSecretName` |
| Helm chart | `nodes` RBAC missing, container command hardcoded, colliding pod selectors | All fixed (one breaking change, see *Upgrade notes*) |

---

## 1. Cloud-aware service exposure (new)

### Problem

`exposeNodeService()` created a `type=NodePort` Service and reported:

```go
externalURL = fmt.Sprintf("http://%s:%d", node.Status.HostIP, nodePort)
```

`DDBNode.Status.HostIP` is the **pod-subnet address** — `172.31.x.x` on EKS. The
Service was healthy but the URL could never be opened, the node security group
was never opened, and `spec.networking.annotations` was dead configuration that
no code ever read.

### Design

New package [`internal/exposure`](../internal/exposure/exposure.go).

The key observation is that **`Service.spec.type = LoadBalancer` is reconciled by
the cloud control plane, not by an in-cluster controller**. On EKS that means:

* no **AWS Load Balancer Controller** is required;
* no **IRSA** is required — which is what makes this viable on projects whose SCP
  denies `iam:*OpenIDConnectProvider*`;
* the cloud provider also **creates the load balancer security group and wires the
  node security group for you**, so the operator needs no cloud credentials and no
  cloud SDK dependency at all.

Behaviour:

| Concern | Implementation |
| :--- | :--- |
| Provider detection | `DetectProvider` reads `node.spec.providerID` (`aws://`, `gce://`, `azure://`), majority vote |
| Default mode | `LoadBalancer` on a cloud, `NodePort` on bare metal |
| AWS annotations | `aws-load-balancer-type: nlb`, cross-zone enabled, `aws-load-balancer-internal: "true"` for internal |
| Address resolution | LB hostname/IP → node `ExternalIP` → `ExternalDNS` → `InternalIP`; **never** a fabricated address |
| Waiting | `pending=true` + human-readable reason; the UI polls instead of showing a dead link |
| Upgrade path | An existing NodePort `-ext` Service is converted **in place** — no address churn, no leaked load balancer |

### API

```
POST   /api/nodes/{ns}/{node}/expose   {"mode":"loadBalancer","scheme":"internet-facing"}
GET    /api/nodes/{ns}/{node}/expose   # poll without mutating
DELETE /api/nodes/{ns}/{node}/expose   # releases the cloud load balancer
```

See [cloud-exposure.md](cloud-exposure.md) for the full guide and troubleshooting.

### The bug found while shipping this: two writers, one Service

The first cut of this feature also patched the cluster-level Access Services
directly from the dashboard. The `DolphinDBCluster` controller owns those
Services, so this created a race: the controller could read the Service from its
cache mid-write, decide the type was still `ClusterIP`, and set it back while the
cloud provider was creating the load balancer:

```
Warning SyncLoadBalancerFailed  Service "ddb-standalone" is invalid:
    status.loadBalancer.ingress: Forbidden: may only be used when `spec.type` is 'LoadBalancer'
Normal  DeletingLoadBalancer
```

The user-visible symptom was a load balancer URL that never loaded, because the
load balancer kept being deleted and recreated.

**Fixed** by making the CR the only write path: `toggleClusterExpose` updates
`spec.networking` and lets the single owner reconcile. Verified afterwards: zero
`DeletingLoadBalancer` and zero `SyncLoadBalancerFailed` events, one load
balancer for the whole lifecycle.

### Cluster-level entry point, and a toggle that turned exposure off

The cloud-aware address resolution was first applied only to the **per-node**
endpoint. The **cluster-level** `accessURL` still ran the old code path, which
handled `NodePort` alone and derived the host from the request `Host` header or a
hardcoded fallback IP. A cluster exposed through a load balancer therefore
reported `serviceType: LoadBalancer` with an **empty** `accessURL`.

The dashboard inferred "not exposed" from that empty URL, so it showed
*"ClusterIP (仅集群内私有)"* with a **一键开启** button. The API implemented that
button as a blind toggle, so clicking it on an already-exposed cluster set
`serviceType` back to `ClusterIP` - which deletes the cloud load balancer. The
next click recreated it, with a different DNS name. The user could never open the
link because it kept changing.

**Fixed** in three places:

1. `clusterAccessURL` is resolved through `exposure.ResolveEndpoint`, the same
   cloud-aware path as per-node exposure, and `ClusterDetail` now carries
   `exposureMode` / `exposureScheme` / `exposurePending` / `exposureMessage`.
2. `POST /api/clusters/{ns}/{name}/expose` accepts an explicit desired state
   (`{"enabled": true|false, "scheme": "..."}`). The old toggle remains for
   backwards compatibility but the UI never relies on it.
3. The banner and buttons are rendered from the reported state: a real address
   with its mode, a **pending** state with automatic polling while the cloud
   provisions, or an explicit *not exposed* state.

Verified on EKS: two consecutive `{"enabled": true}` calls left the load
balancer's `CreatedTime` unchanged, and the public URL served `HTTP 200` with a
live DolphinDB query.

---

## 2. Managed-Kubernetes compatibility fixes

### 2.1 Node names are not valid pod hostnames

`internal/controller/dolphindbcluster_controller.go`

```go
if cluster.Spec.Mode == ModeStandalone && nodeSpec.Hostname == "" {
    nodeSpec.Hostname = defaultNodeName   // the Kubernetes node name
}
```

Kubernetes rejects a pod `hostname` containing dots, and cloud node names are
private DNS names, so every standalone Pod failed with:

```
spec.hostname: Invalid value: "ip-172-31-2-254.ap-southeast-2.compute.internal":
must not contain dots
```

Self-managed clusters hid this because their node names are short (`aftersale2`).

**Fixed** by `SanitizeHostname()` (`internal/controller/naming.go`), applied at
both the cluster controller (node pinning / standalone default) and the DDBNode
controller (pod construction). It keeps the leading DNS label, lowercases,
replaces illegal characters, trims separators and enforces the 63-character
limit.

### 2.2 Hardcoded licence path removed

The operator hardcoded `/home/jzchen/dolphindb.lic` in four places, and the CRD
defaulted `license.mode` to `machineFingerprint`, so a plain CR automatically got
privileged hostPath mounts (`/etc/machine-id`, `/etc/hostname`, `/etc`) plus a
licence mount pointing at a directory in the original author's home. On any other
machine the kubelet failed with:

```
error mounting "/home/jzchen/dolphindb.lic" ... not a directory
```

**Fixed**:

* `license.mode` now defaults to **`secret`**;
* host-bound licensing is strictly opt-in — no fingerprint block is invented, and
  the licence path must be supplied explicitly;
* the hostPath licence mount is skipped when no path is configured;
* the migration converter takes `licensePath` from the request instead of
  assuming one.

### 2.3 Cache engine defaults

`internal/config/renderer.go` set `dataSync=1` but never configured a cache
engine, which DolphinDB rejects:

```
The ChunkNode failed to initialize with exception
[When the feature of dataSync and cache engine must be enable at same time.]
```

Only the sample manifests worked, because they set the values by hand. A cluster
created from a minimal CR could never start its DataNodes.

**Fixed** with `ensureCacheEngineForDataSync()`, applied to `cluster.cfg`,
`datanode.cfg` and `computenode.cfg`. It runs *after* user overrides, so explicit
configuration still wins. Values are whole gigabytes because
`TSDBCacheEngineSize` is parsed as an integer — `"0.5"` is read as `0` and
rejected with `TSDBCacheEngineSize must be greater than 0`.

### 2.4 Credentials are no longer hardcoded

`admin` / `123456` was hardcoded at 8 call sites, and
`spec.security.adminSecretName` (CRD default `dolphindb-admin`) was never read.
Those credentials drive backups, restores, rolling restarts and ACL sync, and on
a cloud deployment the port is reachable from the network.

**Fixed** with `internal/credentials`: the password is read from the named Secret
(key `password`) and falls back to the factory default so existing installations
keep working. The generated `login(...)` script escapes quotes so a password
cannot break out of the script.

### 2.5 A stale fingerprint could never be removed

The reconcile loop handled `nil → set` and `set → set`, but had no branch for
`set → nil`. Once a fingerprint was written it persisted forever, keeping
hostPath mounts (and their startup failures) alive even after the user switched
to Secret-based licensing.

**Fixed** by adding the missing branch.

---

## 3. Helm chart fixes

| Fix | Detail |
| :--- | :--- |
| **Missing `nodes` RBAC** | The operator lists `corev1.NodeList` for placement *and* for cloud detection, but the ClusterRole only granted `configmaps/pvc/pods/secrets/services`. Without it every exposure silently degraded to NodePort. Also added `events`. |
| **Hardcoded container command** | `command: ["/manager"]` only works for images that place binaries at `/`. Now configurable via `image.command` / `dashboard.command`, so `ko`-built images (`/ko-app/...`) work unchanged. |
| **Colliding pod selectors** | The operator's selector (`name` + `instance`) also matched dashboard Pods, so `kubectl logs deploy/<operator>` picked a random Pod. Each Deployment now carries its own `app.kubernetes.io/component`. |

---

## 4. Verification

Performed on EKS 1.34 / `ap-southeast-2` / 2 × `m7i-flex.large`.

**Exposure**

```
POST /api/nodes/.../expose  →  mode=LoadBalancer, provider=aws, pending=true
GET  /api/nodes/.../expose  →  pending=false,
                               http://a589...elb.ap-southeast-2.amazonaws.com:8848
```

* Public Web UI: `HTTP 200`, `<title>DolphinDB</title>`
* Public query: `select sum(id) from t` → `15`
* **Verified after revoking the manually added security-group rule** — the cloud
  provider had added its own `tcp 31723 from 0.0.0.0/0`, i.e. no manual AWS work
  is needed
* `DELETE` released the load balancer and reclaimed the security-group rule
* Cluster-level toggle: only `EnsuringLoadBalancer` / `EnsuredLoadBalancer`
  events, no `DeletingLoadBalancer`, no `SyncLoadBalancerFailed`

**Minimal CRs (zero workarounds)**

A standalone CR with no `standalone.hostname` and no `license` block, and a
cluster CR with no `datanode.config`, both reached `Ready`:

```
NAME              MODE        PHASE   CONTROLLER   DATANODE
ddb-min           standalone  Ready   0            1
ddb-min-cluster   cluster     Ready   1            2
```

* Pod `spec.hostname` = `ip-172-31-2-254` (was a dotted node name)
* Pod volumes contain no hostPath licence
* `datanode.cfg` contains `dataSync=1`, `chunkCacheEngineMemSize=1`,
  `TSDBCacheEngineSize=1` automatically
* Cross-node DFS write/read returned identical results (`rows=9`, `total=45`)

---

## 5. Upgrade notes

**Breaking change — Deployment selectors.** The operator and dashboard
Deployments no longer share a pod selector. `spec.selector` is immutable, so
upgrading from 0.2.x requires deleting the Deployment first:

```bash
kubectl -n <ns> delete deploy <release>-dolphindb-operator
helm upgrade ... # or kubectl apply
```

Pods are stateless, so this is safe.

**Licence default changed.** `license.mode` now defaults to `secret`. Clusters
that relied on the implicit `machineFingerprint` default must now opt in
explicitly:

```yaml
spec:
  license:
    mode: machineFingerprint
    machineFingerprint:
      autoPassthrough: true
      mountMachineId: true
      mountHostEtc: true
      useHostName: true
      licensePath: "/opt/dolphindb/dolphindb.lic"   # no longer inferred
```

**CRDs must be re-applied** because the `license.mode` default and several
descriptions changed:

```bash
kubectl apply -f deploy/helm/dolphindb-operator/crds/
# or
kubectl apply -f config/crd/bases/
```

---

## 6. Known limitations

* **TLS** — exposure does not terminate TLS. Front the port with an Ingress
  controller or an ALB + ACM certificate if TLS is required.
* **Public exposure is a real risk** — `scheme: internet-facing` puts the
  DolphinDB web UI and its REST API on the public internet. Change the admin
  password (see §2.4) and prefer `scheme: internal` for anything non-trivial.
* **NLB provisioning takes 3–5 minutes**, during which DNS resolves to a
  non-routable placeholder. The API reports `pending` and the UI polls.
* **NodePort remains manual** — the operator does not modify cloud security
  groups for NodePort exposure, because that would require cloud credentials in
  the pod. Use `LoadBalancer`.
* **EBS CSI driver** still cannot be installed the documented way on projects
  that deny IRSA; use a different storage class or attach the policy to the node
  role.

---

## 7. Files changed

```
internal/exposure/              new — cloud-aware exposure (14 unit tests)
internal/credentials/           new — admin credential resolution (6 unit tests)
internal/controller/naming.go   new — DNS-safe hostname sanitisation (10 tests)
internal/config/renderer_test.go        new — cache engine / dataSync (5 tests)
internal/controller/dolphindbcluster_controller_test.go  new — hostname, licence, fingerprint (4 tests)
internal/dashboard/exposure_test.go     new — exposure API (7 tests)
docs/cloud-exposure.md          new — exposure guide + troubleshooting
docs/release-notes-0.3.0.md     new — this document

internal/dashboard/server.go            exposure API, cloud-aware detail, single-writer fix
internal/dashboard/ui/index.html        pending state, LB mode, auto-polling
internal/config/renderer.go             cache engine defaults
internal/controller/ddbnode_controller.go      hostname, licence mount guard
internal/controller/dolphindbcluster_controller.go  hostname, licence opt-in, fingerprint clear, credentials
internal/controller/ddbbackup_controller.go     credentials
internal/controller/ddboperation_controller.go  credentials
internal/migration/{converter,executor,types}.go  licence path option, credentials
api/v1alpha1/dolphindbcluster_types.go   license.mode default
config/crd/bases/*.yaml, deploy/helm/dolphindb-operator/crds/*.yaml
deploy/helm/dolphindb-operator/{Chart.yaml,values.yaml,templates/*}
README.md
```

Test suite: 68 assertions across 8 packages. `gofmt`, `go vet`, `go test ./...`
and `helm lint` all clean.
