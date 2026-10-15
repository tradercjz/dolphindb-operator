package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/exposure"
	tlspkg "dolphindb.io/operator/internal/tls"
)

const (
	// tlsSecretSuffix names the Secret the operator issues when the user does not
	// bring their own certificate.
	tlsSecretSuffix = "-tls-selfsigned"
	// tlsHostsHashAnnotation records which SAN set a certificate was issued for, so
	// the operator can reissue it when the exposed hostname appears or changes.
	tlsHostsHashAnnotation = "dolphindb.io/tls-hosts-hash"
	// tlsSecretCrtKey / tlsSecretKeyKey are the conventional kubernetes.io/tls keys.
	tlsSecretCrtKey = corev1.TLSCertKey
	tlsSecretKeyKey = corev1.TLSPrivateKeyKey
)

// tlsEnabled reports whether the cluster asked for native HTTPS.
func tlsEnabled(cluster *v1alpha1.DolphinDBCluster) bool {
	return cluster.Spec.Security.TLS != nil && cluster.Spec.Security.TLS.Enabled
}

// roleServesHTTPS reports whether a node role terminates TLS itself.
//
// Standalone and controller nodes are the ones clients reach for the web console
// and the REST/WebSocket API. DolphinDB data and compute nodes take their
// certificate from the proxy node in front of them, which is out of scope here,
// so enabling HTTPS on them would add mounts for no benefit.
func roleServesHTTPS(role v1alpha1.NodeRole) bool {
	return role == v1alpha1.NodeRoleSingle || role == v1alpha1.NodeRoleController
}

// exposedHostname returns the hostname clients use to reach the cluster, if the
// access Service already has one. An in-cluster Service of type LoadBalancer
// receives its hostname from the cloud provider asynchronously, so this is empty
// for the first minute or so after exposure is enabled.
func (r *DolphinDBClusterReconciler) exposedHostname(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) string {
	svc := &corev1.Service{}
	if err := r.Get(ctx, k8sclient.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name}, svc); err != nil {
		return ""
	}
	for _, ing := range svc.Status.LoadBalancer.Ingress {
		if ing.Hostname != "" {
			return ing.Hostname
		}
	}
	return ""
}

// resolvePublicName picks the domain DolphinDB advertises.
//
// DolphinDB requires a domain (never an IP) once enableHTTPS is on, so an
// explicit spec value wins, then the exposed hostname, and finally the in-cluster
// service DNS name - which is a domain and keeps the node bootable before the
// load balancer exists.
func (r *DolphinDBClusterReconciler) resolvePublicName(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) string {
	if t := cluster.Spec.Security.TLS; t != nil {
		if v := strings.TrimSpace(t.PublicName); v != "" {
			return v
		}
	}
	if h := r.exposedHostname(ctx, cluster); h != "" {
		return h
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local", cluster.Name, cluster.Namespace)
}

// tlsHosts is the SAN set for a cluster's certificate.
func tlsHosts(cluster *v1alpha1.DolphinDBCluster, publicName, exposed string) []string {
	ns, name := cluster.Namespace, cluster.Name
	return tlspkg.DesiredHosts(publicName,
		exposed,
		fmt.Sprintf("%s.%s.svc.cluster.local", name, ns),
		fmt.Sprintf("%s.%s.svc", name, ns),
		fmt.Sprintf("%s-headless.%s.svc.cluster.local", name, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local.svc", name, ns),
	)
}

// ensureTLSSecret makes the cluster's TLS material available and returns the
// Secret name, a hash of the certificate contents, and whether the certificate is
// operator-managed (and therefore untrusted by browsers until imported).
//
// The hash is placed on the pod template, so reissuing the certificate rolls the
// nodes automatically.
func (r *DolphinDBClusterReconciler) ensureTLSSecret(
	ctx context.Context,
	cluster *v1alpha1.DolphinDBCluster,
	hosts []string,
) (secretName, hash string, managed bool, err error) {
	spec := cluster.Spec.Security.TLS
	if spec == nil || !spec.Enabled {
		return "", "", false, nil
	}

	// Bring-your-own certificate: verify it exists and hash its contents so a
	// rotation still triggers a rollout.
	if spec.SecretName != "" {
		sec := &corev1.Secret{}
		if err := r.Get(ctx, k8sclient.ObjectKey{Namespace: cluster.Namespace, Name: spec.SecretName}, sec); err != nil {
			return "", "", false, fmt.Errorf("TLS secret %q referenced by spec.security.tls.secretName not found: %w", spec.SecretName, err)
		}
		if len(sec.Data[tlsSecretCrtKey]) == 0 || len(sec.Data[tlsSecretKeyKey]) == 0 {
			return "", "", false, fmt.Errorf("TLS secret %q must contain both %q and %q", spec.SecretName, tlsSecretCrtKey, tlsSecretKeyKey)
		}
		return spec.SecretName, tlspkg.HostsHash(hosts) + "-" + dataHash(sec.Data), false, nil
	}

	name := cluster.Name + tlsSecretSuffix
	wantHash := tlspkg.HostsHash(hosts)

	existing := &corev1.Secret{}
	getErr := r.Get(ctx, k8sclient.ObjectKey{Namespace: cluster.Namespace, Name: name}, existing)
	if getErr == nil {
		if existing.Annotations[tlsHostsHashAnnotation] == wantHash &&
			len(existing.Data[tlsSecretCrtKey]) > 0 && len(existing.Data[tlsSecretKeyKey]) > 0 {
			return name, dataHash(existing.Data), true, nil
		}
	}

	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return "", "", false, getErr
	}

	material, genErr := tlspkg.Generate(hosts, spec.ValidityDays)
	if genErr != nil {
		return "", "", false, genErr
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "dolphindb",
				"app.kubernetes.io/instance":  cluster.Name,
				"app.kubernetes.io/component": "tls",
			},
			Annotations: map[string]string{
				tlsHostsHashAnnotation: wantHash,
			},
			OwnerReferences: []metav1.OwnerReference{ownerRefFor(cluster)},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			tlsSecretCrtKey: material.CertPEM,
			tlsSecretKeyKey: material.KeyPEM,
		},
	}

	switch {
	case apierrors.IsNotFound(getErr):
		if err := r.Create(ctx, desired); err != nil {
			return "", "", false, fmt.Errorf("create self-signed TLS secret: %w", err)
		}
	default:
		existing.Data = desired.Data
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}
		existing.Annotations[tlsHostsHashAnnotation] = wantHash
		existing.Type = corev1.SecretTypeTLS
		if err := r.Update(ctx, existing); err != nil {
			return "", "", false, fmt.Errorf("update self-signed TLS secret: %w", err)
		}
	}

	return name, dataHash(desired.Data), true, nil
}

// dataHash is a short stable digest of the certificate material.
func dataHash(data map[string][]byte) string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	combined := make([]byte, 0, 4096)
	for _, k := range keys {
		combined = append(combined, []byte(k)...)
		combined = append(combined, data[k]...)
	}
	return tlspkg.HostsHash([]string{string(combined)})
}

// ownerRefFor builds a controller owner reference for a cluster.
func ownerRefFor(cluster *v1alpha1.DolphinDBCluster) metav1.OwnerReference {
	controller := true
	block := true
	return metav1.OwnerReference{
		APIVersion:         "dolphindb.io/v1alpha1",
		Kind:               "DolphinDBCluster",
		Name:               cluster.Name,
		UID:                types.UID(cluster.UID),
		Controller:         &controller,
		BlockOwnerDeletion: &block,
	}
}

// dolphinTLSHashAnnotation carries the certificate hash onto the pod so a
// reissued certificate rolls the node.
const dolphinTLSHashAnnotation = "dolphindb.io/tls-hash"

// tlsHashFor returns the certificate hash to stamp on a node's pod, or an empty
// string when the node does not serve HTTPS.
func tlsHashFor(node *v1alpha1.DDBNode) string {
	if node == nil || node.Spec.TLS == nil {
		return ""
	}
	return node.Spec.TLS.Hash
}

// accessServiceAnnotations are the annotations for the cluster Access Services.
//
// The exposure protocol is included because a cloud load balancer forwards TCP
// untouched: whether the endpoint is http:// or https:// is decided entirely by
// DolphinDB's own enableHTTPS setting, and the dashboard needs to know.
func accessServiceAnnotations(cluster *v1alpha1.DolphinDBCluster) map[string]string {
	out := map[string]string{}
	for k, v := range cluster.Spec.Networking.Annotations {
		out[k] = v
	}
	if tlsEnabled(cluster) {
		out[exposure.AnnotationProtocol] = "https"
	}
	return out
}
