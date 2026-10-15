# Cluster Provisioning: Making the Create Wizard Work on a Cloud

What was wrong with the create wizard on AWS, how it is designed now, and how the
approach compares with other Kubernetes database products.

---

## 1. What was broken

Three of the wizard's inputs were unusable on a fresh EKS cluster, and one of them
was actively harmful. All were reproduced against a live cluster.

### Namespaces and StorageClasses were empty

```
{"error":"Failed to list namespaces: namespaces is forbidden: User
 \"system:serviceaccount:dolphindb-system:dolphindb-operator\" cannot list resource
 \"namespaces\" in API group \"\" at the cluster scope"}

{"error":"Failed to list storage classes: storageclasses.storage.k8s.io is forbidden ..."}
```

Both dropdowns rendered empty, so **the user could not pick a namespace or a
storage class at all**. The cause was the same one that had already broken cloud
detection: the chart's ClusterRole granted only the resources the *reconciler*
needed, and nobody had audited it against what the *dashboard* reads.

### The exposure default was wrong for a cloud

The form had a single checkbox, `开启物理机 NodePort 外部网络直连`, **checked by
default**, which emitted:

```yaml
networking:
  serviceType: "NodePort"
  exposureMode: "NodePortDirect"
```

On a cloud that publishes `http://<node-public-ip>:<nodeport>`. The address is
public, but the node security group does not allow the port and the operator never
opens it, so the link is dead on arrival while looking perfectly healthy.

### The defaults pointed at a machine that does not exist

* the image field was prefilled with `192.168.100.250/dolphindb/dolphindb:v3.00.6`
  — a private registry from the original author's network;
* the generated YAML hardcoded `license.mode: machineFingerprint` with
  `autoPassthrough: true`, i.e. privileged hostPath mounts for a licence file that
  is not present on any cloud node.

---

## 2. Design: the form asks the platform, it does not guess

The browser no longer decides any of this. A single endpoint describes the
installation:

```
GET /api/platform
```

```json
{
  "provider": "aws",
  "providerName": "Amazon EKS",
  "supportsLoadBalancer": true,
  "namespace": "dolphindb-system",
  "namespaces":       [{"name": "dolphindb-system", "writable": true}, ...],
  "storageClasses":   [{"name": "local-path", "provisioner": "rancher.io/local-path", "usable": true},
                       {"name": "ebs-gp3-sc", "provisioner": "ebs.csi.aws.com", "usable": false,
                        "hint": "the CSI driver \"ebs.csi.aws.com\" is not installed in this cluster, so PVCs would stay Pending"}],
  "defaultStorageClass": "local-path",
  "imageRegistry": "dolphindb",
  "imageRepository": "dolphindb",
  "defaultVersion": "3.00.6",
  "defaultServiceType": "LoadBalancer",
  "defaultExpose": {"controller": false, "datanode": false, "computenode": false}
}
```

The four principles behind it:

**1. Only offer choices that will work.** Namespaces the operator cannot create a
`DolphinDBCluster` in are listed but **disabled**, with the reason in the label —
decided by a `SelfSubjectAccessReview`, not by hoping. This is the difference
between "the dropdown is empty" and "this option is unavailable because…".

**2. Validate the storage class, do not just list it.** A `StorageClass` is only
real if its provisioner is installed *and running*. Two signals are combined,
because each alone is wrong:

* `CSINode.spec.drivers` — what **kubelet actually registered**. This is the strong
  signal: a driver appears only while its node plugin is running.
* `CSIDriver` objects — the weak signal. The object **outlives** the driver. On the
  test cluster `efs.csi.aws.com` had a `CSIDriver` object that somebody had applied
  by hand while no plugin ran anywhere, so trusting it alone would have marked an
  EFS class as working.

Measured on a real EKS cluster (a probe PVC per class, left to run):

| Class | Provisioner | Verdict | Observed |
| :--- | :--- | :--- | :--- |
| `local-path` | `rancher.io/local-path` | **usable** | binds |
| `ebs-gp3-sc` | `ebs.csi.aws.com` | **unsuitable** | `ExternalProvisioning: Waiting for a volume to be created either by the external provisioner 'ebs.csi.aws.com'` |
| `gp2` | `kubernetes.io/aws-ebs` | **unsuitable** | same message — **CSI migration forwards the in-tree provisioner to the EBS CSI driver**, so the legacy class fails for the same reason |

Neither EBS class can work without the EBS CSI driver, which on this project cannot
be installed the documented way because that needs IRSA and the account SCP denies
`iam:*OpenIDConnectProvider*`. `local-path` is the working option here.

The preselected default is the **first usable** class. Previously it was "first
alphabetically", which preselected a CSI class whose driver was missing — every PVC
would have stayed `Pending` with no explanation.

**3. Default to the safe, cheap, correct option.** On a cloud: nothing exposed,
`LoadBalancer` when the user asks, `internal` available as a choice, and the cost
stated next to the checkboxes. On bare metal: the historical NodePort behaviour,
preserved.

**4. Configuration that differs per installation belongs to the installation.**
The image registry is a chart value (`dashboard.defaultImage.*` → `DDB_IMAGE_REGISTRY`
etc.), not a string in the browser. An air-gapped or mirrored deployment changes
it once, in `values.yaml`.

### The exposure control

| Platform | Control | Default |
| :--- | :--- | :--- |
| Cloud | one checkbox per component (controller / data node / compute node) + an `internet-facing` / `internal` selector | **all off** |
| Bare metal | the traditional `NodePort` switch, with a note that the firewall is the operator's responsibility | on |

The cloud form states "每个勾选的组件都会创建一个 LoadBalancer（约 US$16/月）"
directly under the checkboxes, because exposing all three components triples the
bill and that should never be a surprise.

---

## 3. How other products do it

| Product | Namespace | Storage | Exposure | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **CloudNativePG** (operator + `kubectl` plugin) | fixed to the operator's namespace, or given explicitly; the plugin offers the namespaces it can write to | `storageClass` free-form, with the default class preselected | **not part of creation** — `spec.service` / a separate `Pooler`/Ingress is added later | exposes the *primary* only by design; encourages `internal` |
| **Percona Everest** | a project/namespace concept in its own UI; the operator reconciles into it | a picker of classes reported by the cluster, with the default marked | an explicit toggle, and **the hostname is shown once the LB is ready**, not at creation time | separates "database" from "exposure" |
| **KubeDB** | namespace picker, then a `StorageClass` picker | picker with the default marked | `Service` type chosen from `ClusterIP` / `NodePort` / `LoadBalancer`, **default `ClusterIP`** | treats exposure as a property, not a checkbox buried in a wizard |
| **Aiven / Confluent / MongoDB Atlas** | n/a (accounts/projects) | plan-dependent | explicit *public* vs *private* networking, **private recommended and often the default** | states the cost of public endpoints |
| **Rancher / KubeSphere app catalogs** | namespace must be selected first; the form is scoped to it | class picker with the default marked | `Service` type picker, default `ClusterIP` | |

Three patterns are consistent across all of them:

1. **Validate, then offer.** Namespaces and storage classes are resolved from the
   cluster and filtered by what actually works, rather than typed or listed blind.
2. **Internal/private is the default.** Exposure is an opt-in with a stated cost,
   and it is often deferred until after the database exists — because the load
   balancer's hostname is only known later.
3. **Installation-level defaults are configuration.** The registry, the default
   storage class and the default exposure live in the installation's settings, not
   in the form.

**What we deliberately did differently:** exposure *is* part of creation, since the
operator reconciles a `DolphinDBCluster` into Services anyway and deferring it would
mean asking the user to do the same thing twice. The cost is that the wizard must
default to "off" and say so, which it now does.

---

## 4. Deliberately not implemented

* **`storageClass: ""` (no persistence)** is not offered. Every DolphinDB node
  needs a data volume; presenting a "no storage" option would produce a cluster
  that cannot hold data. If no class is usable, the wizard still lists them all
  marked unusable so the user learns why.
* **Post-creation exposure.** Exposure is still selected at creation, because the
  per-component controls on the cluster page already cover the later case. The
  wizard preselects nothing, so the common path is "create internal, expose later".
* **Per-installation exposure policy.** A cluster admin cannot currently forbid
  `internet-facing`; that belongs with the SCP/RCP layer rather than here.
* **Region/AZ awareness.** Class and instance-type checks are limited to what the
  Kubernetes API exposes; the wizard does not call the cloud APIs.

---

## 5. Files

```
internal/dashboard/platform.go                    new — /api/platform, availability checks
deploy/helm/dolphindb-operator/templates/clusterrole.yaml   namespaces, storageclasses, csidrivers, SSAR
deploy/helm/dolphindb-operator/templates/dashboard-deployment.yaml  POD_NAMESPACE + image defaults
deploy/helm/dolphindb-operator/values.yaml        dashboard.defaultImage
internal/dashboard/ui/index.html                  platform-driven wizard
```
