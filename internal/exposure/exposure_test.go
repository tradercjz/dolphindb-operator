package exposure

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"dolphindb.io/operator/api/v1alpha1"
)

func awsNode(name string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{ProviderID: "aws:///ap-southeast-2a/i-0abc"},
	}
}

func testNode() *v1alpha1.DDBNode {
	return &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb-standalone-single-0", Namespace: "dolphindb-system", UID: "uid-1"},
		Spec: v1alpha1.DDBNodeSpec{
			ClusterName: "ddb-standalone",
			Role:        v1alpha1.NodeRoleSingle,
			Alias:       "ddb-standalone-single-0",
			Port:        8848,
		},
	}
}

func TestParseMode(t *testing.T) {
	cases := map[string]Mode{
		"":             ModeNodePort,
		"loadbalancer": ModeLoadBalancer,
		"LoadBalancer": ModeLoadBalancer,
		"nlb":          ModeLoadBalancer,
		"ELB":          ModeLoadBalancer,
		"nodeport":     ModeNodePort,
		"clusterip":    ModeClusterIP,
		"nonsense":     ModeNodePort,
	}
	for in, want := range cases {
		if got := ParseMode(in, ModeNodePort); got != want {
			t.Errorf("ParseMode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseScheme(t *testing.T) {
	if got := ParseScheme("", SchemeInternal); got != SchemeInternal {
		t.Errorf("default scheme not honoured: %q", got)
	}
	if got := ParseScheme("public", SchemeInternal); got != SchemeInternetFacing {
		t.Errorf("public should map to internet-facing, got %q", got)
	}
	if got := ParseScheme("private", SchemeInternetFacing); got != SchemeInternal {
		t.Errorf("private should map to internal, got %q", got)
	}
}

func TestDetectProvider(t *testing.T) {
	tests := []struct {
		name  string
		nodes []corev1.Node
		want  Provider
	}{
		{"empty is bare metal", nil, ProviderBare},
		{"aws", []corev1.Node{awsNode("a"), awsNode("b")}, ProviderAWS},
		{"gce", []corev1.Node{{Spec: corev1.NodeSpec{ProviderID: "gce://proj/zone/inst"}}}, ProviderGCP},
		{"azure", []corev1.Node{{Spec: corev1.NodeSpec{ProviderID: "azure:///subscriptions/x"}}}, ProviderAzure},
		{"no providerID", []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "aftersale2"}}}, ProviderBare},
		{
			"majority wins",
			[]corev1.Node{awsNode("a"), awsNode("b"), {ObjectMeta: metav1.ObjectMeta{Name: "c"}}},
			ProviderAWS,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DetectProvider(tc.nodes); got != tc.want {
				t.Fatalf("DetectProvider = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDefaultModeIsCloudAware(t *testing.T) {
	for _, p := range []Provider{ProviderAWS, ProviderGCP, ProviderAzure} {
		if got := p.DefaultMode(); got != ModeLoadBalancer {
			t.Errorf("%s default mode = %q, want LoadBalancer", p, got)
		}
	}
	if got := ProviderBare.DefaultMode(); got != ModeNodePort {
		t.Errorf("bare metal default mode = %q, want NodePort", got)
	}
}

func TestDefaultAnnotations(t *testing.T) {
	pub := DefaultAnnotations(ProviderAWS, SchemeInternetFacing)
	if pub[AnnotationAWSLoadBalancerType] != "nlb" {
		t.Errorf("expected nlb load balancer type, got %q", pub[AnnotationAWSLoadBalancerType])
	}
	if _, ok := pub[AnnotationAWSLoadBalancerInternal]; ok {
		t.Error("internet-facing LB must not set the internal annotation")
	}
	if pub[AnnotationAWSCrossZone] != "true" {
		t.Error("cross-zone should be enabled by default")
	}

	priv := DefaultAnnotations(ProviderAWS, SchemeInternal)
	if priv[AnnotationAWSLoadBalancerInternal] != "true" {
		t.Error("internal scheme must set the internal annotation")
	}
}

func TestBuildServiceLoadBalancer(t *testing.T) {
	node := testNode()
	svc := BuildService(node, Options{
		Mode:        ModeLoadBalancer,
		Scheme:      SchemeInternetFacing,
		Provider:    ProviderAWS,
		NodePort:    0,
		Annotations: map[string]string{"service.beta.kubernetes.io/aws-load-balancer-attributes": "x"},
	})

	if svc.Name != "ddb-standalone-single-0-ext" {
		t.Errorf("unexpected name %q", svc.Name)
	}
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Errorf("type = %q", svc.Spec.Type)
	}
	if svc.Spec.Selector["dolphindb.io/node-alias"] != "ddb-standalone-single-0" {
		t.Errorf("selector = %v", svc.Spec.Selector)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8848 || svc.Spec.Ports[0].NodePort != 0 {
		t.Errorf("ports = %+v", svc.Spec.Ports)
	}
	// User annotation must survive alongside the defaults.
	if svc.Annotations["service.beta.kubernetes.io/aws-load-balancer-attributes"] != "x" {
		t.Error("user annotations must be preserved")
	}
	if svc.Annotations[AnnotationAWSLoadBalancerType] != "nlb" {
		t.Error("cloud default annotation missing")
	}
	if len(svc.OwnerReferences) != 1 || svc.OwnerReferences[0].Kind != "DDBNode" {
		t.Error("service must be owned by the DDBNode so it is garbage collected")
	}
}

func TestBuildServiceNodePortKeepsRequestedPort(t *testing.T) {
	svc := BuildService(testNode(), Options{Mode: ModeNodePort, NodePort: 31723})
	if svc.Spec.Type != corev1.ServiceTypeNodePort {
		t.Fatalf("type = %q", svc.Spec.Type)
	}
	if svc.Spec.Ports[0].NodePort != 31723 {
		t.Fatalf("nodePort = %d", svc.Spec.Ports[0].NodePort)
	}
	if len(svc.Annotations) != 0 {
		t.Errorf("nodeport mode should not carry LB annotations, got %v", svc.Annotations)
	}
}

func TestBuildServiceDefaultsPort(t *testing.T) {
	node := testNode()
	node.Spec.Port = 0
	svc := BuildService(node, Options{Mode: ModeClusterIP})
	if svc.Spec.Ports[0].Port != 8848 {
		t.Fatalf("default port = %d, want 8848", svc.Spec.Ports[0].Port)
	}
}

func TestMatches(t *testing.T) {
	node := testNode()
	want := BuildService(node, Options{Mode: ModeLoadBalancer, Provider: ProviderAWS, Scheme: SchemeInternetFacing})

	got := want.DeepCopy()
	if !Matches(got, want) {
		t.Error("identical services should match")
	}

	diffType := want.DeepCopy()
	diffType.Spec.Type = corev1.ServiceTypeNodePort
	if Matches(diffType, want) {
		t.Error("different type must not match")
	}

	diffAnn := want.DeepCopy()
	delete(diffAnn.Annotations, AnnotationAWSCrossZone)
	if Matches(diffAnn, want) {
		t.Error("missing default annotation must not match")
	}

	diffPort := want.DeepCopy()
	diffPort.Spec.Ports[0].Port = 9999
	if Matches(diffPort, want) {
		t.Error("different port must not match")
	}
}

func TestResolveEndpointNotExposed(t *testing.T) {
	ep := ResolveEndpoint(testNode(), nil, nil)
	if ep.URL != "" || ep.Mode != ModeClusterIP {
		t.Fatalf("unexpected endpoint %+v", ep)
	}
}

func TestResolveEndpointLoadBalancerPending(t *testing.T) {
	svc := BuildService(testNode(), Options{Mode: ModeLoadBalancer, Provider: ProviderAWS})
	ep := ResolveEndpoint(testNode(), nil, svc)
	if !ep.Pending {
		t.Fatal("load balancer without ingress should be pending")
	}
	if ep.URL != "" {
		t.Fatalf("pending endpoint must not carry a URL, got %q", ep.URL)
	}
	if ep.Port != 8848 {
		t.Fatalf("port = %d", ep.Port)
	}
}

func TestResolveEndpointLoadBalancerReady(t *testing.T) {
	svc := BuildService(testNode(), Options{Mode: ModeLoadBalancer, Provider: ProviderAWS, Scheme: SchemeInternetFacing})
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
		{Hostname: "a2eb43b3.elb.ap-southeast-2.amazonaws.com"},
	}
	ep := ResolveEndpoint(testNode(), nil, svc)
	if ep.Pending {
		t.Fatal("must not be pending once ingress exists")
	}
	want := "http://a2eb43b3.elb.ap-southeast-2.amazonaws.com:8848"
	if ep.URL != want {
		t.Fatalf("URL = %q, want %q", ep.URL, want)
	}
	if ep.Scheme != SchemeInternetFacing {
		t.Fatalf("scheme = %q", ep.Scheme)
	}
}

func TestResolveEndpointLoadBalancerInternal(t *testing.T) {
	svc := BuildService(testNode(), Options{Mode: ModeLoadBalancer, Provider: ProviderAWS, Scheme: SchemeInternal})
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "10.0.0.7"}}
	ep := ResolveEndpoint(testNode(), nil, svc)
	if ep.Scheme != SchemeInternal {
		t.Fatalf("scheme = %q, want internal", ep.Scheme)
	}
	if ep.URL != "http://10.0.0.7:8848" {
		t.Fatalf("URL = %q", ep.URL)
	}
}

func TestResolveEndpointNodePortUsesExternalIP(t *testing.T) {
	svc := BuildService(testNode(), Options{Mode: ModeNodePort, NodePort: 31723})
	k8sNode := &corev1.Node{
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "172.31.2.254"},
				{Type: corev1.NodeExternalIP, Address: "16.176.229.160"},
			},
		},
	}
	ep := ResolveEndpoint(testNode(), k8sNode, svc)
	if ep.URL != "http://16.176.229.160:31723" {
		t.Fatalf("URL = %q, want the public node address", ep.URL)
	}
	if ep.Message != "" {
		t.Fatalf("public address should not warn, got %q", ep.Message)
	}
}

// Regression test: the original code advertised node.Status.HostIP, which on any
// cloud is the private address, producing a URL that could never be reached.
func TestResolveEndpointNodePortPrivateAddressWarns(t *testing.T) {
	svc := BuildService(testNode(), Options{Mode: ModeNodePort, NodePort: 31723})
	svc.Spec.Ports[0].NodePort = 31723
	k8sNode := &corev1.Node{
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "172.31.2.254"}},
		},
	}
	ep := ResolveEndpoint(testNode(), k8sNode, svc)
	if ep.Address != "172.31.2.254" {
		t.Fatalf("address = %q", ep.Address)
	}
	if ep.Message == "" {
		t.Fatal("a private-only address must carry an explanatory message")
	}
}

func TestResolveEndpointNodePortWaitingForAllocation(t *testing.T) {
	svc := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Type:  corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{{Port: 8848, TargetPort: intstr.FromInt(8848)}},
		},
	}
	ep := ResolveEndpoint(testNode(), nil, svc)
	if !ep.Pending {
		t.Fatal("NodePort without an allocated port should be pending")
	}
}

func TestIsPrivateIP(t *testing.T) {
	for _, addr := range []string{"172.31.2.254", "10.0.0.1", "192.168.1.1", "127.0.0.1", "169.254.1.1"} {
		if !IsPrivateIP(addr) {
			t.Errorf("%s should be private", addr)
		}
	}
	for _, addr := range []string{"16.176.229.160", "8.8.8.8", "a2eb.elb.amazonaws.com"} {
		if IsPrivateIP(addr) {
			t.Errorf("%s should not be private", addr)
		}
	}
}
