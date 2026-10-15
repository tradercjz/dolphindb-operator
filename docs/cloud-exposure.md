# Cloud-Aware Service Exposure

How the DolphinDB Operator publishes a database node outside the Kubernetes
cluster, and why it is built the way it is.

---

## 1. The problem this solves

The original implementation exposed a node by creating a `type=NodePort`
Service named `<node>-ext` and reporting

```go
externalURL = fmt.Sprintf("http://%s:%d", node.Status.HostIP, nodePort)
```

`DDBNode.Status.HostIP` is the **pod-IP node address**, which on every managed
Kubernetes is a private address (`172.31.x.x` on EKS). The Service was healthy,
but the URL handed to the user could never be opened:

| Symptom | Reality |
| :--- | :--- |
| UI shows `http://172.31.7.158:31723` | Not routable from outside the VPC |
| `curl <node-public-ip>:<nodePort>` times out | Node security group has no ingress rule for the NodePort |
| Nothing in the codebase touched AWS | No load balancer, no security group, no public address resolution |

The exposure path also ignored `DolphinDBCluster.spec.networking.annotations`, so
the `aws-load-balancer-*` annotations from the AWS samples were dead config.

---

## 2. Design

Exposure is now handled by a small, cloud-aware package:
[`internal/exposure`](../internal/exposure/exposure.go).

```
exposure.Options ──► exposure.BuildService ──► Kubernetes Service
                                                     │
node.spec.providerID ──► DetectProvider ─────────────┤
                                                     ▼
                              exposure.ResolveEndpoint ──► usable URL
```

### Principle: the cloud control plane does the work

`Service.spec.type = LoadBalancer` is reconciled by the **cloud control plane**,
not by an in-cluster controller. That single fact is what makes this design work
on locked-down AWS projects:

* No **AWS Load Balancer Controller** is required.
* No **IRSA** is required — which matters when a Service Control Policy denies
  `iam:*OpenIDConnectProvider*` and IRSA is therefore impossible to set up.
* The cloud provider also **creates the load balancer security group and wires
  the node security group automatically**, so no AWS API calls are needed from
  the operator at all.

Consequently `internal/exposure` has **zero cloud SDK dependencies**: everything
it needs is already in the `Node`, `Service` and `DDBNode` objects.

### Provider detection

`DetectProvider` reads `node.spec.providerID`, which every cloud controller
manager populates:

| providerID prefix | Provider | Default exposure |
| :--- | :--- | :--- |
| `aws://` | `aws` | `LoadBalancer` |
| `gce://` | `gcp` | `LoadBalancer` |
| `azure://` | `azure` | `LoadBalancer` |
| *(empty)* | `bare-metal` | `NodePort` |

A majority vote is used, so one stray node cannot misclassify the cluster.

### Principle: exactly one writer per Service

The cluster-level Access Services are owned by the `DolphinDBCluster` controller.
The dashboard changes exposure by updating the **CR only** and letting the single
owner reconcile.

The first version of this feature also updated the Services directly from the
dashboard, which produced two writers racing on the same object: the controller
could read the Service from its cache while the dashboard was mid-write, decide
the type was still `ClusterIP`, and set it back while the cloud provider was
creating the load balancer. The result was a load balancer that was created and
immediately deleted:

```
Warning SyncLoadBalancerFailed  Service "ddb-standalone" is invalid:
    status.loadBalancer.ingress: Forbidden: may only be used when `spec.type` is 'LoadBalancer'
Normal  DeletingLoadBalancer
```

Nothing is broken by the extra reconcile hop, and the CR write itself triggers
it.

### Address resolution

`ResolveEndpoint` never invents an address:

| Service type | Address used | While waiting |
| :--- | :--- | :--- |
| `LoadBalancer` | `status.loadBalancer.ingress[0].hostname` (or `.ip`) | `pending=true` + reason |
| `NodePort` | node `ExternalIP` → `ExternalDNS` → `InternalIP` | `pending=true` + reason |

If only a private address exists, the endpoint is still returned **together with
a warning message** rather than being silently presented as working.

---

## 3. Using it

### From the dashboard

Click **+ 暴露端口** on a node card. The dashboard:

1. detects the cloud,
2. creates (or upgrades in place) the `<node>-ext` Service of the appropriate
   type,
3. shows progress while the cloud provisions the load balancer, and polls until
   the address appears,
4. displays the real endpoint with its mode (`LoadBalancer · 公网`,
   `LoadBalancer · 内部`, or `NodePort: 31723`).

**关闭外网** deletes the Service; Kubernetes garbage collection removes it, and
the cloud releases the load balancer automatically.

The cluster-level banner works the same way and is driven by the reported state
(`accessURL`, `exposureMode`, `exposurePending`), not by inference - a cluster
that is exposed says so, and the button then releases the load balancer instead of
silently toggling it off.

### From the REST API

```bash
# Expose (defaults to LoadBalancer on a cloud, NodePort elsewhere)
curl -X POST -H 'Content-Type: application/json' \
  -d '{"mode":"loadBalancer","scheme":"internet-facing"}' \
  http://<dashboard>/api/nodes/<ns>/<node>/expose

# Poll without changing anything (used by the UI while provisioning)
curl http://<dashboard>/api/nodes/<ns>/<node>/expose

# Tear down
curl -X DELETE http://<dashboard>/api/nodes/<ns>/<node>/expose

# Cluster level: state the intent explicitly
curl -X POST -H 'Content-Type: application/json' -d '{"enabled":true}' \
  http://<dashboard>/api/clusters/<ns>/<cluster>/expose
curl -X POST -H 'Content-Type: application/json' -d '{"enabled":false}' \
  http://<dashboard>/api/clusters/<ns>/<cluster>/expose
```

> `enabled` may be omitted, in which case the call toggles. That behaviour is kept
> only for backwards compatibility: a blind toggle turns an *exposed* cluster
> **off**, deleting its load balancer. Always prefer an explicit `enabled`.

Request fields:

| Field | Values | Default |
| :--- | :--- | :--- |
| `mode` | `loadBalancer`, `nodePort`, `clusterIP` | provider default |
| `scheme` | `internet-facing`, `internal` | `internet-facing` |
| `nodePort` | explicit NodePort | auto-allocated |
| `annotations` | merged over provider defaults | `{}` |

Response:

```json
{
  "success": true,
  "provider": "aws",
  "providerName": "Amazon EKS",
  "mode": "LoadBalancer",
  "scheme": "internet-facing",
  "nodePort": 8848,
  "externalURL": "http://a5891...elb.ap-southeast-2.amazonaws.com:8848",
  "pending": false,
  "message": ""
}
```

### From the CR

Cluster-level exposure uses `spec.networking`:

```yaml
spec:
  networking:
    serviceType: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
      service.beta.kubernetes.io/aws-load-balancer-scheme: "internal"
```

`networking.annotations` are applied to the cluster, data-node and compute-node
Services by the controller, and are also merged underneath any per-node request
annotations.

---

## 4. AWS specifics

### Which load balancer is created

Default annotations for AWS are set by `exposure.DefaultAnnotations`:

| Annotation | Value | Why |
| :--- | :--- | :--- |
| `service.beta.kubernetes.io/aws-load-balancer-type` | `nlb` | Network Load Balancer fits a TCP database port, and the **in-tree** provider can create it without the AWS Load Balancer Controller / IRSA |
| `service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled` | `true` | Avoids a single-AZ bottleneck |
| `service.beta.kubernetes.io/aws-load-balancer-internal` | `true` | Only when `scheme: internal` |

Verified behaviour on EKS (Kubernetes 1.34, `ap-southeast-2`):

| Configuration | Result |
| :--- | :--- |
| no type annotation | **Classic ELB** created; provider creates a `k8s-elb-*` security group and whitelists it in the cluster SG |
| `aws-load-balancer-type: nlb` | **NLB** created; provider opens the backing NodePort in the cluster SG |

Both are created by the control plane. Neither needs IRSA.

> **Note** — `service.beta.kubernetes.io/aws-load-balancer-scheme` and
> `...-target-type: ip` belong to the *AWS Load Balancer Controller*. They are
> **not** honoured by the in-tree provider. Use
> `aws-load-balancer-internal: "true"` for an internal classic ELB, and pass the
> controller annotations through `annotations:` only if you do run the controller.

### What appears in AWS

* Load balancer (NLB or Classic ELB) — billed hourly plus LCU/GB
* Target group (NLB only)
* A security group, for Classic ELB
* An ingress rule on the cluster security group for the backing NodePort

All of it is removed when the Service is deleted.

### NodePort fallback

`mode: nodePort` remains available. Two things are required for it to be
reachable from outside:

1. a public address on the node (`ExternalIP`) — the operator reports a warning
   if there is none;
2. an ingress rule on the **node** security group for the NodePort range
   (EKS default is `30000-32767`).

The operator does **not** modify security groups itself — that would require
cloud credentials in the pod, which is exactly what the LoadBalancer path avoids.
Prefer `loadBalancer`.

---

## 5. RBAC

`exposure.DetectProvider` lists nodes, so the operator needs `nodes: get,list,watch`.
The Helm chart grants this (plus `events`). If node listing is denied the
operator degrades gracefully to `bare-metal`, i.e. NodePort — the dashboard will
therefore report `provider: bare-metal` on what is obviously a cloud cluster.

Check with:

```bash
kubectl auth can-i list nodes \
  --as=system:serviceaccount:<namespace>:dolphindb-operator
```

---

## 6. Troubleshooting

| Symptom | Cause | Fix |
| :--- | :--- | :--- |
| **The dashboard shows "not exposed" for a cluster that is exposed, and clicking the button makes it worse** | The cluster-level `accessURL` was previously only computed for NodePort, so a load-balanced cluster reported an empty URL. The UI then offered a toggle that switched the exposure *off* | Fixed: the cluster entry point is resolved through the same cloud-aware path, `ClusterDetail` reports `exposureMode`/`exposurePending`, and the API takes an explicit `{"enabled": ...}` instead of only toggling |
| **URL opens in the browser but never loads, or connection times out** | The load balancer is still `provisioning`. An NLB can take **3-5 minutes** after the Service appears, and the DNS name points at a non-routable placeholder (`198.18.x.x`) until then | Wait, then re-resolve DNS. Verify with `aws elbv2 describe-load-balancers --query "LoadBalancers[].State.Code"` - it must read `active`. The UI polls automatically |
| **The link changes every time I click** | The load balancer is being deleted and recreated, so the DNS name changes | Make sure nothing but the CR drives the Service type; see "exactly one writer" above |
| Load balancer created and immediately deleted, repeatedly | Two writers updating the same Service | Fixed by making the CR the only write path; see "exactly one writer" above. If you see `SyncLoadBalancerFailed`, check for anything else mutating the Service (e.g. an external GitOps controller) |
| `pending: true` for many minutes | Cloud still provisioning, or the service controller cannot allocate | `kubectl describe svc <node>-ext` and look for `EnsuringLoadBalancer` events |
| `provider: "bare-metal"` on EKS | Operator cannot list nodes | Fix the `nodes` ClusterRole rule |
| URL contains `172.31.x.x` | NodePort exposure on a node with only a private address | Switch to `loadBalancer`, or add a security group rule for the NodePort |
| `pending` forever with `waiting for Kubernetes to allocate a NodePort` | No NodePort assigned yet | Wait, or request an explicit `nodePort` |

---

## 7. Security

* `LoadBalancer` with `scheme: internet-facing` puts the DolphinDB web UI **and
  its REST API** on the public internet. DolphinDB's default credentials are
  `admin` / `123456`; change them.
* Prefer `scheme: internal` plus a VPN/bastion for anything non-trivial.
* There is no TLS termination in this path. Front the port with an Ingress
  controller or an ALB+ACM certificate if TLS is required.
* `mode: clusterIP` (or **关闭外网**) is the safe state; exposure is opt-in.
