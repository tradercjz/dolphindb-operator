# HTTPS for the DolphinDB Endpoint

How the operator turns the exposed DolphinDB endpoint into an HTTPS one, and why
self-signed certificates are the default.

---

## 1. The problem

An exposed endpoint was plain HTTP. Everything on the wire — including the login
payload — was readable, and the DolphinDB web console plus its REST/WebSocket API
were reachable unencrypted from the public internet.

Encrypting it is harder than it looks, because every "obvious" AWS-native route is
blocked on a locked-down account. All of the following was verified on a live
Free Tier project:

| Route | Result |
| :--- | :--- |
| ACM certificate + load balancer TLS | **Needs a domain you control.** ACM only issues for domains you can validate, and this account is Free Tier, so Route 53 **domain registration is refused** (`Free Tier accounts are not supported for this service`). |
| `iam:UploadServerCertificate` (self-signed cert on a load balancer) | **Denied by the account SCP** — so a self-signed certificate cannot be attached to an AWS load balancer at all. |
| CloudFront with its free `*.cloudfront.net` certificate | Works without a domain, but **CloudFront does not support WebSocket**, and the DolphinDB console needs it (`new WebSocket` appears in the UI bundle). The console would break. |
| AWS Load Balancer Controller (ALB + ACM) | Needs **IRSA**, which the SCP blocks. |
| AWS Private CA | Not supported on this experience. |

What *is* available, and was confirmed working: **ACM certificates** (free,
auto-renewing) and **LB TLS termination** from the in-tree cloud provider — both
usable as soon as you own a domain. See §5.

## 2. What the operator does

DolphinDB can terminate TLS itself. `enableHTTPS=true` makes it serve HTTPS on
the same port, using `server.crt` and `serverPrivate.key` from the node's `keys`
directory. That path needs **no certificate authority, no domain, no ACM, no load
balancer controller and no IRSA** — so it works everywhere.

```yaml
apiVersion: dolphindb.io/v1alpha1
kind: DolphinDBCluster
metadata:
  name: ddb-min
spec:
  image: "dolphindb/dolphindb:v3.00.5"
  mode: "standalone"
  security:
    tls:
      enabled: true          # that's all
```

With that, the operator:

1. **Issues a self-signed certificate** into a managed Secret
   (`<cluster>-tls-selfsigned`), valid for 825 days.
2. **Fills the SAN list** with everything clients might use: the exposed load
   balancer hostname, the cluster and headless service DNS names, `localhost` and
   `127.0.0.1`.
3. **Derives `publicName`** — DolphinDB requires a *domain* (never an IP) when
   `enableHTTPS` is on. The order is: `spec.security.tls.publicName`, then the
   exposed load balancer hostname, then `<cluster>.<ns>.svc.cluster.local` so the
   node is bootable before the load balancer exists.
4. **Installs the material** at container start, copying it into
   `<home>/keys` and `<home>/<alias>/keys` (DolphinDB looks in both, depending on
   the mode) with mode `0600` on the key.
5. **Rolls the node** when the certificate is reissued, via a certificate hash
   annotation on the pod template.
6. **Advertises `https://`** on the exposed endpoint, because a cloud load
   balancer forwards TCP untouched — it cannot know the scheme.

### Why the certificate is not mounted as a Secret volume

DolphinDB's `keys` directory also holds its own `aclPrivate.key` /
`aclPublic.key`, which must stay writable, and DolphinDB **regenerates
`server.crt` whenever `serverPrivate.key` is missing**. Mounting a Secret over the
directory would break both. The operator therefore stages the Secret read-only at
`/data/ddb/tls` and the container entrypoint copies the two files into place
before DolphinDB starts.

---

## 3. Using it

### Self-signed (no domain needed)

```yaml
spec:
  security:
    tls:
      enabled: true
```

Result: `https://<endpoint>:8848`.

Browsers will warn that the certificate is not trusted — it is genuinely
self-issued. Traffic **is** encrypted. To remove the warning, import the
certificate as a trusted authority, which is the procedure the DolphinDB
documentation describes for its own self-generated certificate:

```bash
kubectl -n <ns> get secret <cluster>-tls-selfsigned \
  -o jsonpath='{.data.tls\.crt}' | base64 -d > dolphindb.crt
```

Then in Chrome: *Settings → Privacy and security → Security → Manage
certificates → Authorities → Import*. The certificate is generated with
`IsCA: true`, so importing it as an authority is valid, and once imported
`openssl` and browsers report `Verify return code: 0 (ok)`.

### Bring your own certificate

Point `secretName` at any `kubernetes.io/tls` Secret (keys `tls.crt` and
`tls.key`) — a certificate you bought for a domain you own, or one exported from
ACM:

```yaml
spec:
  security:
    tls:
      enabled: true
      secretName: ddb-example-com-tls
      publicName: ddb.example.com   # required for a trusted certificate
```

The operator then does not touch the certificate contents; it only verifies the
Secret exists and hashes it so a rotation rolls the nodes. If you run
`cert-manager`, let it issue into that Secret and enable auto-rotation there.

### Turning it off

Set `enabled: false` (or remove the block). The operator clears the pod's TLS
configuration, the node rolls back to plain HTTP, and the managed Secret is
garbage collected with the cluster.

---

## 4. Behaviour notes

| Situation | Behaviour |
| :--- | :--- |
| Load balancer hostname not known yet | `publicName` falls back to the in-cluster service DNS name, so the node starts. Once the load balancer hostname appears the certificate is reissued for it and the node rolls — automatically, once. |
| Certificate rotated | The pod template's certificate hash changes, so the node is replaced at the next reconcile. |
| `secretName` set but missing/empty | The reconcile fails with an explicit error rather than starting a node that would serve a certificate nobody intended. |
| Data / compute nodes | Not offered HTTPS. DolphinDB documents that they take their certificate from the proxy node in front of them; they are reached through the controller. Enabling it there would mount Secrets for no benefit. |
| Client compatibility | `https://` for the web console and REST, `wss://` for the WebSocket console. The Python/Java clients accept HTTPS on the same port. |

---

## 5. If you do own a domain: let AWS terminate TLS instead

Once a domain is available, ACM plus load balancer TLS termination is the better
operating model, because the certificate is free, auto-renewing and trusted
without any client-side import. The in-tree cloud provider supports it directly —
no AWS Load Balancer Controller and no IRSA:

```yaml
spec:
  networking:
    serviceType: LoadBalancer
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
      service.beta.kubernetes.io/aws-load-balancer-ssl-cert: "arn:aws:acm:<region>:<acct>:certificate/<id>"
      service.beta.kubernetes.io/aws-load-balancer-ssl-ports: "443"
      service.beta.kubernetes.io/aws-load-balancer-backend-protocol: http
```

Steps:

1. `aws acm request-certificate --domain-name ddb.example.com --validation-method DNS`
2. Add the validation CNAME to your DNS.
3. Add a CNAME from `ddb.example.com` to the load balancer hostname.
4. Apply the annotations above.

Verified on EKS: with a bogus certificate ARN the provider reports
`CreateLoadBalancer ... CertificateNotFound` (Classic ELB) or
`error creating listener: CreateListener ... CertificateNotFound` (NLB) — proving
it is really wiring the certificate, and that the SCP does not block
`elasticloadbalancing:CreateListener`.

You can **combine both**: let the load balancer terminate TLS and leave DolphinDB
on plain HTTP inside the cluster, or keep `enableHTTPS` on for end-to-end
encryption. If you do the latter, also upload the same certificate into a
`kubernetes.io/tls` Secret and reference it via `secretName`.

---

## 6. Security checklist

* Encryption does not fix a weak password. The default is `admin/123456` — change
  it, and let the operator read it from `spec.security.adminSecretName`.
* A self-signed certificate protects the wire, not the client: a user who clicks
  through the warning is still vulnerable to a man-in-the-middle. Import the
  certificate, or use a publicly trusted one.
* Prefer `scheme: internal` plus a VPN for anything that does not need to be
  public.
* Consider `spec.security.strictPermissionInspection: true` (the default) and
  keep `enableShellFunction` off.

---

## 7. Current decision and future work

**Decided for now:** ship the self-signed path (§2, §3) and stop there. It needs
no domain, no ACM and no IRSA, it encrypts the wire, and the certificate can be
imported so that a browser stops warning. The operator was verified end to end on
EKS: TLS handshake, HTTPS Web UI, HTTPS query execution, and
`Verify return code: 0 (ok)` when the issued certificate is used as the trust
anchor.

**Accepted gaps.** These are known and deliberate, not oversights:

| Gap | Detail |
| :--- | :--- |
| TLS is not *enforced* | `enableHTTPS=true` makes DolphinDB *also* serve HTTPS on the port; it still accepts plaintext HTTP on the same port. Verified: identical response bodies over `http://` and `https://`. There is no DolphinDB configuration to reject plaintext. |
| TLS 1.2 only | DolphinDB's TLS stack does not negotiate TLS 1.3 (verified: `tls1_3` fails). TLS 1.2 with `ECDHE-RSA-AES256-GCM-SHA384` is currently still acceptable — AEAD, forward secrecy — but it is not the newest. |
| Trust is per client | Every machine that should not see a warning has to import the certificate. Fine for a few operators, not for a public audience. |

**Future work, in rough order of value:**

1. **Bring your own certificate.** The plumbing already exists:
   `spec.security.tls.secretName` accepts any `kubernetes.io/tls` Secret, the
   operator verifies it, hashes it so a rotation rolls the nodes, and leaves the
   contents alone. What is missing is the *acquisition* workflow — nothing in the
   product obtains a certificate for you. Planned: documented ACM-export and
   Let's Encrypt (ACME DNS-01) recipes, and optional cert-manager integration so a
   `Certificate` resource feeds that Secret automatically.
2. **Enforce TLS-only.** Terminate TLS in a thin in-cluster proxy (the pattern the
   DolphinDB documentation itself describes for nginx) and expose only port 443,
   so plaintext requests are rejected. Side benefit: such a proxy speaks TLS 1.3,
   which closes the second gap above. Cost: one more component to run.
3. **ACM + load balancer termination** once a domain exists — the better
   operating model, because the certificate is free, auto-renewing and trusted
   without any client-side import. See §5.
4. **TLS for data and compute nodes.** They are deliberately excluded today
   (DolphinDB documents that they take the certificate from the proxy node in
   front of them). If direct, encrypted access to a data node is needed, this
   becomes relevant — alongside `enableRpcSSL` for inter-node traffic, which
   requires every node to have `enableHTTPS=true`.
