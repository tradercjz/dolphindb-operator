// Package exposure implements cloud-aware external exposure for DolphinDB nodes.
//
// # Design rationale
//
// The original implementation only ever created a `type=NodePort` Service and
// advertised `node.Status.HostIP` as the external URL. On any cloud that IP is a
// *private* address, so the URL handed back to the operator was unreachable from
// outside the cluster even though the Service itself was perfectly healthy.
//
// Two facts, verified on a real EKS cluster, drive the design here:
//
//  1. `Service.spec.type = LoadBalancer` is reconciled by the *cloud control
//     plane*, not by an in-cluster controller. On EKS this means neither the AWS
//     Load Balancer Controller nor IRSA is required — which matters for projects
//     whose SCP denies `iam:*OpenIDConnectProvider*` and therefore cannot use
//     IRSA at all. The cloud provider also creates the load balancer security
//     group and wires the node security group automatically (for NLB it opens
//     the backing NodePort).
//
//  2. NodePort still works, but it is only reachable from outside once the node
//     security group allows the port. We therefore never advertise a private
//     address: on a cloud we resolve the node's ExternalIP instead, and report a
//     clear message when no public address exists.
//
// This package is intentionally free of any cloud SDK dependency: everything it
// needs is already present in the Node, Service and DDBNode objects.
package exposure

import (
	"fmt"
	"net"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"dolphindb.io/operator/api/v1alpha1"
)

// Mode is the Kubernetes Service type used to expose a DolphinDB node.
type Mode string

const (
	ModeClusterIP    Mode = "ClusterIP"
	ModeNodePort     Mode = "NodePort"
	ModeLoadBalancer Mode = "LoadBalancer"
)

// ParseMode normalises user input into a Mode, defaulting to def.
func ParseMode(s string, def Mode) Mode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "loadbalancer", "lb", "nlb", "elb":
		return ModeLoadBalancer
	case "nodeport", "node-port":
		return ModeNodePort
	case "clusterip", "cluster-ip", "internal-only":
		return ModeClusterIP
	default:
		return def
	}
}

// Scheme selects between a public and a private load balancer.
type Scheme string

const (
	SchemeInternetFacing Scheme = "internet-facing"
	SchemeInternal       Scheme = "internal"
)

// ParseScheme normalises user input into a Scheme, defaulting to def.
func ParseScheme(s string, def Scheme) Scheme {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "internal", "private":
		return SchemeInternal
	case "internet-facing", "internet", "public", "external":
		return SchemeInternetFacing
	default:
		return def
	}
}

// Provider identifies the cloud (or lack thereof) a cluster runs on.
type Provider string

const (
	ProviderAWS     Provider = "aws"
	ProviderGCP     Provider = "gcp"
	ProviderAzure   Provider = "azure"
	ProviderBare    Provider = "bare-metal"
	ProviderUnknown Provider = "unknown"
)

// Well-known Service annotations. These are the *in-tree* (legacy) cloud
// provider annotations, deliberately chosen because they work without the AWS
// Load Balancer Controller / IRSA. Users may override or extend them through
// DolphinDBCluster.spec.networking.annotations.
const (
	// AnnotationAWSLoadBalancerType selects the load balancer flavour. The value
	// "nlb" makes the in-tree provider create a Network Load Balancer, which is
	// the right fit for a TCP database port and needs no controller.
	AnnotationAWSLoadBalancerType = "service.beta.kubernetes.io/aws-load-balancer-type"
	// AnnotationAWSLoadBalancerInternal marks a Classic ELB as internal.
	AnnotationAWSLoadBalancerInternal = "service.beta.kubernetes.io/aws-load-balancer-internal"
	// AnnotationAWSCrossZone spreads NLB traffic across all healthy targets in
	// every enabled AZ, avoiding a single-AZ bottleneck.
	AnnotationAWSCrossZone = "service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled"
	// AnnotationGCPInternal marks a GCP load balancer as internal.
	AnnotationGCPInternal = "networking.gke.io/load-balancer-type"
)

// DetectProvider infers the cloud provider from node.spec.providerID, which every
// cloud controller manager populates (e.g. "aws:///ap-southeast-2a/i-0abc...").
// It returns ProviderBare when the nodes carry no provider ID at all.
func DetectProvider(nodes []corev1.Node) Provider {
	if len(nodes) == 0 {
		return ProviderBare
	}
	seen := map[Provider]int{}
	for i := range nodes {
		seen[providerFromID(nodes[i].Spec.ProviderID)]++
	}
	// Majority wins, so a single stray node cannot misclassify the cluster.
	best, bestCount := ProviderBare, -1
	for p, c := range seen {
		if c > bestCount || (c == bestCount && p == ProviderAWS) {
			best, bestCount = p, c
		}
	}
	return best
}

func providerFromID(id string) Provider {
	switch {
	case strings.HasPrefix(id, "aws://"):
		return ProviderAWS
	case strings.HasPrefix(id, "gce://"):
		return ProviderGCP
	case strings.HasPrefix(id, "azure://"):
		return ProviderAzure
	default:
		return ProviderBare
	}
}

// SupportsLoadBalancer reports whether the cloud control plane can provision a
// load balancer for a Service. Managed clouds can; bare metal cannot, because
// there is no controller to fulfil the request.
func (p Provider) SupportsLoadBalancer() bool {
	switch p {
	case ProviderAWS, ProviderGCP, ProviderAzure:
		return true
	default:
		return false
	}
}

// DefaultMode returns the exposure mode to use when the user does not choose one.
// On a cloud we prefer a load balancer: it is reachable without touching node
// security groups and is reconciled by the control plane.
func (p Provider) DefaultMode() Mode {
	if p.SupportsLoadBalancer() {
		return ModeLoadBalancer
	}
	return ModeNodePort
}

// DisplayName is a human friendly provider name.
func (p Provider) DisplayName() string {
	switch p {
	case ProviderAWS:
		return "Amazon EKS"
	case ProviderGCP:
		return "Google GKE"
	case ProviderAzure:
		return "Azure AKS"
	case ProviderBare:
		return "Self-managed Kubernetes"
	default:
		return "Unknown platform"
	}
}

// DefaultAnnotations returns the provider specific annotations for a load
// balancer of the requested scheme.
func DefaultAnnotations(p Provider, scheme Scheme) map[string]string {
	out := map[string]string{}
	switch p {
	case ProviderAWS:
		// "nlb" => Network Load Balancer, provisioned by the in-tree cloud
		// provider. No AWS Load Balancer Controller and no IRSA required.
		out[AnnotationAWSLoadBalancerType] = "nlb"
		out[AnnotationAWSCrossZone] = "true"
		if scheme == SchemeInternal {
			out[AnnotationAWSLoadBalancerInternal] = "true"
		}
	case ProviderGCP:
		if scheme == SchemeInternal {
			out[AnnotationGCPInternal] = "internal"
		}
	}
	return out
}

// Options describe the desired exposure of a single node.
type Options struct {
	Mode        Mode
	Scheme      Scheme
	Provider    Provider
	NodePort    int32
	Annotations map[string]string
}

// Normalize fills in defaults so callers can pass a partially specified Options.
func (o Options) Normalize() Options {
	if o.Provider == "" {
		o.Provider = ProviderBare
	}
	if o.Mode == "" {
		o.Mode = o.Provider.DefaultMode()
	}
	if o.Scheme == "" {
		o.Scheme = SchemeInternetFacing
	}
	return o
}

// ServiceName is the deterministic name of the per-node exposure Service.
func ServiceName(nodeName string) string { return nodeName + "-ext" }

// BuildService renders the desired exposure Service for a node.
//
// The Service is owned by the DDBNode so Kubernetes garbage collection removes it
// when the node goes away; the cloud provider then tears down the load balancer.
func BuildService(node *v1alpha1.DDBNode, opts Options) *corev1.Service {
	opts = opts.Normalize()

	port := node.Spec.Port
	if port == 0 {
		port = 8848
	}

	svcPort := corev1.ServicePort{
		Name:       "web-port",
		Port:       port,
		TargetPort: intstr.FromInt(int(port)),
	}
	if opts.Mode == ModeNodePort && opts.NodePort > 0 {
		svcPort.NodePort = opts.NodePort
	}

	annotations := map[string]string{}
	if opts.Mode == ModeLoadBalancer {
		for k, v := range DefaultAnnotations(opts.Provider, opts.Scheme) {
			annotations[k] = v
		}
		// User supplied annotations win over the defaults, so an operator that
		// *does* run the cloud load balancer controller can opt into the modern
		// "external" flavour without us guessing.
		for k, v := range opts.Annotations {
			annotations[k] = v
		}
	}

	trueVal := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "dolphindb.io/v1alpha1",
		Kind:               "DDBNode",
		Name:               node.Name,
		UID:                node.UID,
		Controller:         &trueVal,
		BlockOwnerDeletion: &trueVal,
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceName(node.Name),
			Namespace: node.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "dolphindb",
				"app.kubernetes.io/instance":  node.Spec.ClusterName,
				"app.kubernetes.io/component": string(node.Spec.Role),
				"dolphindb.io/node-alias":     node.Spec.Alias,
				"dolphindb.io/exposed":        "true",
				"dolphindb.io/exposure-mode":  string(opts.Mode),
			},
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceType(opts.Mode),
			Selector: map[string]string{"dolphindb.io/node-alias": node.Spec.Alias},
			Ports:    []corev1.ServicePort{svcPort},
		},
	}
}

// Matches reports whether an existing Service already satisfies the desired
// options. It is used to avoid issuing a no-op Update on every reconcile, which
// would needlessly re-trigger the cloud load balancer reconciler.
func Matches(svc *corev1.Service, desired *corev1.Service) bool {
	if svc.Spec.Type != desired.Spec.Type {
		return false
	}
	for k, v := range desired.Annotations {
		if svc.Annotations[k] != v {
			return false
		}
	}
	for k := range svc.Annotations {
		if strings.HasPrefix(k, "service.beta.kubernetes.io/") && desired.Annotations[k] == "" {
			return false
		}
	}
	if len(svc.Spec.Ports) != 1 || len(desired.Spec.Ports) != 1 {
		return false
	}
	a, b := svc.Spec.Ports[0], desired.Spec.Ports[0]
	return a.Port == b.Port && a.TargetPort == b.TargetPort
}

// Endpoint is the externally usable address of an exposed node.
type Endpoint struct {
	URL      string   `json:"url,omitempty"`
	Address  string   `json:"address,omitempty"`
	Port     int32    `json:"port,omitempty"`
	Mode     Mode     `json:"mode,omitempty"`
	Scheme   Scheme   `json:"scheme,omitempty"`
	Provider Provider `json:"provider,omitempty"`
	// Pending is true while the cloud is still provisioning the load balancer.
	Pending bool `json:"pending,omitempty"`
	// Message explains a pending or unreachable endpoint to the user.
	Message string `json:"message,omitempty"`
}

// ResolveEndpoint turns the live Kubernetes objects into an address a user can
// actually paste into a browser or DolphinDB client.
//
// Precedence: load balancer address > node ExternalIP > node InternalIP. A
// private address is only used as a last resort and always carries a warning.
func ResolveEndpoint(node *v1alpha1.DDBNode, k8sNode *corev1.Node, svc *corev1.Service) Endpoint {
	if svc == nil {
		return Endpoint{Mode: ModeClusterIP, Message: "node is not exposed outside the cluster"}
	}

	mode := Mode(svc.Spec.Type)
	ep := Endpoint{Mode: mode}
	if len(svc.Spec.Ports) == 0 {
		ep.Message = "exposure service has no ports"
		return ep
	}
	svcPort := svc.Spec.Ports[0]

	switch mode {
	case ModeLoadBalancer:
		ep.Scheme = SchemeInternetFacing
		if svc.Annotations[AnnotationAWSLoadBalancerInternal] == "true" {
			ep.Scheme = SchemeInternal
		}
		addr := loadBalancerAddress(svc)
		if addr == "" {
			ep.Pending = true
			ep.Port = svcPort.Port
			ep.Message = "waiting for the cloud provider to finish provisioning the load balancer"
			return ep
		}
		ep.Address = addr
		ep.Port = svcPort.Port
		ep.URL = fmt.Sprintf("http://%s:%d", addr, svcPort.Port)

	case ModeNodePort:
		ep.Port = svcPort.NodePort
		if ep.Port == 0 {
			ep.Pending = true
			ep.Message = "waiting for Kubernetes to allocate a NodePort"
			return ep
		}
		addr, public := nodeAddress(k8sNode)
		if addr == "" {
			ep.Pending = true
			ep.Message = "node address is not known yet"
			return ep
		}
		ep.Address = addr
		ep.URL = fmt.Sprintf("http://%s:%d", addr, ep.Port)
		if !public {
			ep.Message = "node has no external address; open the node security group for this " +
				"NodePort, or expose the node with a LoadBalancer instead"
		}
	}

	return ep
}

func loadBalancerAddress(svc *corev1.Service) string {
	for _, ing := range svc.Status.LoadBalancer.Ingress {
		if ing.Hostname != "" {
			return ing.Hostname
		}
		if ing.IP != "" {
			return ing.IP
		}
	}
	return ""
}

// nodeAddress returns the best address for reaching a node from outside and
// whether that address is publicly routable.
func nodeAddress(n *corev1.Node) (string, bool) {
	if n == nil {
		return "", false
	}
	byType := map[corev1.NodeAddressType][]string{}
	for _, a := range n.Status.Addresses {
		byType[a.Type] = append(byType[a.Type], a.Address)
	}
	if v := firstNonEmpty(byType[corev1.NodeExternalIP]); v != "" {
		return v, !IsPrivateIP(v)
	}
	if v := firstNonEmpty(byType[corev1.NodeExternalDNS]); v != "" {
		return v, true
	}
	if v := firstNonEmpty(byType[corev1.NodeInternalIP]); v != "" {
		return v, false
	}
	if v := firstNonEmpty(byType[corev1.NodeHostName]); v != "" {
		return v, false
	}
	return "", false
}

func firstNonEmpty(vals []string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// IsPrivateIP reports whether addr falls in an RFC1918 / loopback range. Values
// that are not IP addresses (e.g. DNS names) are treated as public, because a
// resolvable DNS name is reachable from outside by definition.
func IsPrivateIP(addr string) bool {
	ip := net.ParseIP(addr)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
}

// SortedAnnotationKeys is a deterministic key list, useful for logs and tests.
func SortedAnnotationKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
