package controller

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/exposure"
)

func clusterWithNetworking(net v1alpha1.NetworkingSpec) *v1alpha1.DolphinDBCluster {
	return &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb", Namespace: "ddb"},
		Spec:       v1alpha1.DolphinDBClusterSpec{Networking: net},
	}
}

// Regression test: the CRD used to default serviceType to NodePort for every
// cluster, so on a cloud each cluster published a URL that looked public while
// the node security group blocked the port. Nothing may be exposed by default.
func TestResolveExposureDefaultsToNothingOnCloud(t *testing.T) {
	got := resolveExposure(clusterWithNetworking(v1alpha1.NetworkingSpec{}), exposure.ProviderAWS)
	want := componentExposure{
		Controller:  corev1.ServiceTypeClusterIP,
		DataNode:    corev1.ServiceTypeClusterIP,
		ComputeNode: corev1.ServiceTypeClusterIP,
	}
	if got != want {
		t.Fatalf("resolveExposure = %+v, want %+v", got, want)
	}
	if n := got.ExposedComponents(); len(n) != 0 {
		t.Fatalf("nothing should be exposed, got %v", n)
	}
}

// Bare metal keeps the historical behaviour, where the operator has no privileged
// knowledge of the firewall and NodePort is the normal way to publish a service.
func TestResolveExposureDefaultsToNodePortOnBareMetal(t *testing.T) {
	got := resolveExposure(clusterWithNetworking(v1alpha1.NetworkingSpec{}), exposure.ProviderBare)
	want := componentExposure{
		Controller:  corev1.ServiceTypeNodePort,
		DataNode:    corev1.ServiceTypeNodePort,
		ComputeNode: corev1.ServiceTypeNodePort,
	}
	if got != want {
		t.Fatalf("resolveExposure = %+v, want %+v", got, want)
	}
}

func TestResolveExposurePerComponentSelection(t *testing.T) {
	// A controller for the Web console plus a data node for real work: the two
	// things a user actually needs, and two load balancers rather than three.
	cluster := clusterWithNetworking(v1alpha1.NetworkingSpec{
		Expose: &v1alpha1.ExposeSpec{Controller: true, DataNode: true},
	})
	got := resolveExposure(cluster, exposure.ProviderAWS)
	if got.Controller != corev1.ServiceTypeLoadBalancer {
		t.Errorf("controller = %q, want LoadBalancer", got.Controller)
	}
	if got.DataNode != corev1.ServiceTypeLoadBalancer {
		t.Errorf("datanode = %q, want LoadBalancer", got.DataNode)
	}
	if got.ComputeNode != corev1.ServiceTypeClusterIP {
		t.Errorf("computenode = %q, want it left internal", got.ComputeNode)
	}
	if names := got.ExposedComponents(); !reflect.DeepEqual(names, []string{"controller", "datanode"}) {
		t.Fatalf("ExposedComponents = %v", names)
	}
}

func TestResolveExposureHonoursExplicitServiceType(t *testing.T) {
	cluster := clusterWithNetworking(v1alpha1.NetworkingSpec{
		Expose:      &v1alpha1.ExposeSpec{DataNode: true},
		ServiceType: corev1.ServiceTypeNodePort,
	})
	got := resolveExposure(cluster, exposure.ProviderAWS)
	if got.DataNode != corev1.ServiceTypeNodePort {
		t.Fatalf("data node = %q, want the explicitly requested NodePort", got.DataNode)
	}
}

// A cluster created before per-component exposure existed carries a cluster-wide
// serviceType; it must keep working unchanged.
func TestResolveExposureLegacyClusterWideServiceType(t *testing.T) {
	cluster := clusterWithNetworking(v1alpha1.NetworkingSpec{ServiceType: corev1.ServiceTypeNodePort})
	got := resolveExposure(cluster, exposure.ProviderAWS)
	want := componentExposure{
		Controller:  corev1.ServiceTypeNodePort,
		DataNode:    corev1.ServiceTypeNodePort,
		ComputeNode: corev1.ServiceTypeNodePort,
	}
	if got != want {
		t.Fatalf("legacy cluster-wide serviceType must still apply everywhere, got %+v", got)
	}
}

func TestExposeSpecNames(t *testing.T) {
	if got := (*v1alpha1.ExposeSpec)(nil).Names(); got != nil {
		t.Errorf("nil spec should list nothing, got %v", got)
	}
	if got := (&v1alpha1.ExposeSpec{}).Any(); got {
		t.Error("an empty spec exposes nothing")
	}
	e := &v1alpha1.ExposeSpec{Controller: true, ComputeNode: true}
	if got := e.Names(); !reflect.DeepEqual(got, []string{"controller", "computenode"}) {
		t.Errorf("Names = %v", got)
	}
	if !e.Any() {
		t.Error("Any should be true when a component is selected")
	}
}
