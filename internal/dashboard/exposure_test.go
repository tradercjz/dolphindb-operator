package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/exposure"
)

func exposureTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

// awsWorkerNode mimics an EKS worker node: the provider ID is what makes
// exposure.DetectProvider recognise the cluster as running on AWS.
func awsWorkerNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "ip-172-31-2-254.ap-southeast-2.compute.internal"},
		Spec: corev1.NodeSpec{
			ProviderID: "aws:///ap-southeast-2a/i-0149329f1fee056dc",
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "172.31.2.254"},
				{Type: corev1.NodeExternalIP, Address: "16.176.229.160"},
			},
		},
	}
}

func bareMetalNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "aftersale2"},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "192.168.100.44"},
			},
		},
	}
}

func standaloneNode() *v1alpha1.DDBNode {
	return &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-standalone-single-0",
			Namespace: "dolphindb-system",
			UID:       "uid-standalone",
		},
		Spec: v1alpha1.DDBNodeSpec{
			ClusterName: "ddb-standalone",
			Role:        v1alpha1.NodeRoleSingle,
			Alias:       "ddb-standalone-single-0",
			Port:        8848,
			NodeName:    "ip-172-31-2-254.ap-southeast-2.compute.internal",
		},
		Status: v1alpha1.DDBNodeStatus{
			HostIP:   "172.31.7.158",
			NodeName: "ip-172-31-2-254.ap-southeast-2.compute.internal",
		},
	}
}

func newExposureServer(t *testing.T, objs ...client.Object) *Server {
	t.Helper()
	cli := fake.NewClientBuilder().WithScheme(exposureTestScheme()).WithObjects(objs...).Build()
	return NewServer(cli, nil, ":0", "default")
}

func callExpose(t *testing.T, srv *Server, method, body string) (int, map[string]interface{}) {
	t.Helper()
	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "/api/nodes/dolphindb-system/ddb-standalone-single-0/expose", rdr)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleNodeSubRoutes(w, req)

	out := map[string]interface{}{}
	resp := w.Result()
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// On a managed cloud the default exposure must be a cloud load balancer, because
// that is the only path that yields a reachable address without any manual
// security group work.
func TestExposeOnAWSDefaultsToLoadBalancer(t *testing.T) {
	srv := newExposureServer(t, awsWorkerNode(), standaloneNode())

	code, body := callExpose(t, srv, http.MethodPost, `{}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, body = %v", code, body)
	}
	if body["provider"] != "aws" {
		t.Errorf("provider = %v, want aws", body["provider"])
	}
	if body["mode"] != string(exposure.ModeLoadBalancer) {
		t.Errorf("mode = %v, want LoadBalancer", body["mode"])
	}
	// A freshly created load balancer has no address yet: the API must say so
	// instead of returning a bogus URL.
	if body["pending"] != true {
		t.Errorf("expected pending=true, got %v", body["pending"])
	}
	if body["externalURL"] != "" {
		t.Errorf("pending exposure must not carry a URL, got %v", body["externalURL"])
	}
	if msg, _ := body["message"].(string); msg == "" {
		t.Error("pending exposure should explain why")
	}

	svc := &corev1.Service{}
	if err := srv.client.Get(context.Background(), client.ObjectKey{Namespace: "dolphindb-system", Name: "ddb-standalone-single-0-ext"}, svc); err != nil {
		t.Fatalf("exposure service not created: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Errorf("service type = %q", svc.Spec.Type)
	}
	if svc.Spec.Selector["dolphindb.io/node-alias"] != "ddb-standalone-single-0" {
		t.Errorf("selector = %v", svc.Spec.Selector)
	}
	// NLB is chosen because the in-tree cloud provider can provision it without
	// the AWS Load Balancer Controller and without IRSA.
	if got := svc.Annotations[exposure.AnnotationAWSLoadBalancerType]; got != "nlb" {
		t.Errorf("aws-load-balancer-type = %q, want nlb", got)
	}
}

// An existing NodePort exposure must be upgraded in place. Recreating the Service
// would change the address and leak the previous load balancer.
func TestExposeUpgradesExistingNodePortInPlace(t *testing.T) {
	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-standalone-single-0-ext",
			Namespace: "dolphindb-system",
			UID:       "svc-uid",
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"dolphindb.io/node-alias": "ddb-standalone-single-0"},
			Ports: []corev1.ServicePort{
				{Name: "web-port", Port: 8848, NodePort: 31723},
			},
		},
	}
	srv := newExposureServer(t, awsWorkerNode(), standaloneNode(), existing)

	code, body := callExpose(t, srv, http.MethodPost, `{}`)
	if code != http.StatusOK {
		t.Fatalf("expected 200 for an upgrade, got %d (%v)", code, body)
	}

	svc := &corev1.Service{}
	if err := srv.client.Get(context.Background(), client.ObjectKey{Namespace: "dolphindb-system", Name: "ddb-standalone-single-0-ext"}, svc); err != nil {
		t.Fatal(err)
	}
	if svc.UID != "svc-uid" {
		t.Error("the existing Service must be updated, not replaced")
	}
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Errorf("service type = %q, want LoadBalancer", svc.Spec.Type)
	}
}

// Once the cloud has allocated an address, the API must report it.
func TestExposeReportsLoadBalancerAddressOnceReady(t *testing.T) {
	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-standalone-single-0-ext",
			Namespace: "dolphindb-system",
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{"dolphindb.io/node-alias": "ddb-standalone-single-0"},
			Ports: []corev1.ServicePort{
				{Name: "web-port", Port: 8848},
			},
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{
					{Hostname: "a589122b65c494b5b90a55bcb5b283d6-e6b07c809dfba952.elb.ap-southeast-2.amazonaws.com"},
				},
			},
		},
	}
	srv := newExposureServer(t, awsWorkerNode(), standaloneNode(), existing)

	code, body := callExpose(t, srv, http.MethodGet, "")
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	want := "http://a589122b65c494b5b90a55bcb5b283d6-e6b07c809dfba952.elb.ap-southeast-2.amazonaws.com:8848"
	if body["externalURL"] != want {
		t.Fatalf("externalURL = %v, want %v", body["externalURL"], want)
	}
	if body["pending"] == true {
		t.Error("a provisioned load balancer must not report pending")
	}
}

// Regression test for the original defect: on bare metal there is no cloud load
// balancer, so the fallback is NodePort - and the URL must use the node address,
// never DDBNode.Status.HostIP (which is the pod subnet address on every cloud).
func TestExposeOnBareMetalUsesNodePortWithNodeAddress(t *testing.T) {
	node := standaloneNode()
	node.Spec.NodeName = "aftersale2"
	node.Status.NodeName = "aftersale2"
	srv := newExposureServer(t, bareMetalNode(), node)

	code, body := callExpose(t, srv, http.MethodPost, `{"nodePort":31723}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, body = %v", code, body)
	}
	if body["provider"] != "bare-metal" {
		t.Errorf("provider = %v", body["provider"])
	}
	if body["mode"] != string(exposure.ModeNodePort) {
		t.Errorf("mode = %v, want NodePort", body["mode"])
	}
	if body["externalURL"] != "http://192.168.100.44:31723" {
		t.Errorf("externalURL = %v", body["externalURL"])
	}
	// The private address must trigger a warning, because NodePort is only
	// reachable once the node firewall/security group allows it.
	if msg, _ := body["message"].(string); msg == "" {
		t.Error("a private-only NodePort address should be flagged")
	}
}

// Explicitly requesting nodePort on a cloud must be honoured, and the returned
// URL must use the node's public address.
func TestExposeNodePortOnCloudUsesPublicNodeAddress(t *testing.T) {
	srv := newExposureServer(t, awsWorkerNode(), standaloneNode())

	code, body := callExpose(t, srv, http.MethodPost, `{"mode":"nodePort","nodePort":31723}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d, body = %v", code, body)
	}
	if body["mode"] != string(exposure.ModeNodePort) {
		t.Fatalf("mode = %v", body["mode"])
	}
	url, _ := body["externalURL"].(string)
	if !strings.HasPrefix(url, "http://16.176.229.160:31723") {
		t.Fatalf("expected the public node address, got %q", url)
	}
	if strings.Contains(url, "172.31.") {
		t.Fatalf("URL must never expose the private node address: %q", url)
	}
}

// Until the cluster allocates a NodePort there is nothing to advertise, so the
// API must report progress rather than fabricate an address.
func TestExposeNodePortWaitsForAllocation(t *testing.T) {
	srv := newExposureServer(t, awsWorkerNode(), standaloneNode())

	code, body := callExpose(t, srv, http.MethodPost, `{"mode":"nodePort"}`)
	if code != http.StatusCreated {
		t.Fatalf("status = %d", code)
	}
	if body["pending"] != true {
		t.Errorf("expected pending=true before allocation, got %v", body)
	}
	if body["externalURL"] != "" {
		t.Errorf("no URL should be advertised before allocation, got %v", body["externalURL"])
	}
}

func TestUnexposeDeletesService(t *testing.T) {
	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-standalone-single-0-ext",
			Namespace: "dolphindb-system",
		},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
	}
	srv := newExposureServer(t, awsWorkerNode(), standaloneNode(), existing)

	code, body := callExpose(t, srv, http.MethodDelete, "")
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if body["success"] != true {
		t.Fatalf("body = %v", body)
	}
	svc := &corev1.Service{}
	err := srv.client.Get(context.Background(), client.ObjectKey{Namespace: "dolphindb-system", Name: "ddb-standalone-single-0-ext"}, svc)
	if err == nil {
		t.Fatal("service should have been deleted")
	}
}

// ---------------------------- cluster-level exposure ----------------------------

func standaloneClusterCR() *v1alpha1.DolphinDBCluster {
	return &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb-min", Namespace: "dolphindb-system"},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:       v1alpha1.ModeStandalone,
			Version:    "3.00.5",
			Standalone: &v1alpha1.StandaloneSpec{Port: 8848},
			Networking: v1alpha1.NetworkingSpec{
				ServiceType:  corev1.ServiceTypeLoadBalancer,
				ExposureMode: v1alpha1.ExposureGateway,
			},
		},
	}
}

func lbService(name string, ready bool) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "dolphindb-system"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{"app.kubernetes.io/instance": name},
			Ports:    []corev1.ServicePort{{Name: "ddb-port", Port: 8848}},
		},
	}
	if ready {
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
			{Hostname: "a4a6d7b4fcfb14b01a863a78b5694ce1-886b29f6972a8073.elb.ap-southeast-2.amazonaws.com"},
		}
	}
	return svc
}

func callClusterDetail(t *testing.T, srv *Server) map[string]interface{} {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/clusters/dolphindb-system/ddb-min", nil)
	w := httptest.NewRecorder()
	srv.getClusterDetail(w, req, "dolphindb-system", "ddb-min")
	out := map[string]interface{}{}
	resp := w.Result()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

// Regression test: the cluster-level accessURL was only computed for NodePort, so
// a cluster exposed through a cloud load balancer reported an empty accessURL.
// The UI then claimed the cluster was ClusterIP-only and offered an "enable"
// button that actually toggled the exposure off.
func TestClusterDetailReportsLoadBalancerAccessURL(t *testing.T) {
	srv := newExposureServer(t, awsWorkerNode(), standaloneClusterCR(), lbService("ddb-min", true))

	body := callClusterDetail(t, srv)

	want := "http://a4a6d7b4fcfb14b01a863a78b5694ce1-886b29f6972a8073.elb.ap-southeast-2.amazonaws.com:8848"
	if body["accessURL"] != want {
		t.Fatalf("accessURL = %v, want %v", body["accessURL"], want)
	}
	if body["serviceType"] != "LoadBalancer" {
		t.Errorf("serviceType = %v", body["serviceType"])
	}
	if body["exposureMode"] != string(exposure.ModeLoadBalancer) {
		t.Errorf("exposureMode = %v", body["exposureMode"])
	}
	if body["exposurePending"] == true {
		t.Error("a provisioned load balancer must not be reported as pending")
	}
}

func TestClusterDetailReportsPendingLoadBalancer(t *testing.T) {
	srv := newExposureServer(t, awsWorkerNode(), standaloneClusterCR(), lbService("ddb-min", false))

	body := callClusterDetail(t, srv)

	if v, ok := body["accessURL"]; ok && v != "" {
		t.Errorf("no address should be advertised yet, got %v", v)
	}
	if body["exposurePending"] != true {
		t.Errorf("expected exposurePending=true, got %v", body["exposurePending"])
	}
	if msg, _ := body["exposureMessage"].(string); msg == "" {
		t.Error("pending state should explain itself")
	}
}

func callClusterExpose(t *testing.T, srv *Server, body string) (int, map[string]interface{}) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/clusters/dolphindb-system/ddb-min/expose", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.handleClusterSubRoutes(w, req)
	out := map[string]interface{}{}
	resp := w.Result()
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// Regression test: with an explicit desired state, asking to enable an already
// exposed cluster must be a no-op rather than turning it off (which would delete
// the cloud load balancer).
func TestClusterExposeIsIdempotentWhenEnabledExplicitly(t *testing.T) {
	cluster := standaloneClusterCR()
	srv := newExposureServer(t, awsWorkerNode(), cluster)

	code, body := callClusterExpose(t, srv, `{"enabled":true}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d (%v)", code, body)
	}
	if body["enabled"] != true {
		t.Fatalf("enabled = %v, want true", body["enabled"])
	}

	stored := &v1alpha1.DolphinDBCluster{}
	if err := srv.client.Get(context.Background(), client.ObjectKey{Namespace: "dolphindb-system", Name: "ddb-min"}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Networking.ServiceType != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("service type = %q, want it left as LoadBalancer", stored.Spec.Networking.ServiceType)
	}

	// Second identical request must not flip it off.
	if _, body = callClusterExpose(t, srv, `{"enabled":true}`); body["enabled"] != true {
		t.Fatalf("second enable returned enabled=%v", body["enabled"])
	}
	if err := srv.client.Get(context.Background(), client.ObjectKey{Namespace: "dolphindb-system", Name: "ddb-min"}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Networking.ServiceType != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("second enable changed the type to %q", stored.Spec.Networking.ServiceType)
	}
}

func TestClusterExposeCanBeDisabledExplicitly(t *testing.T) {
	srv := newExposureServer(t, awsWorkerNode(), standaloneClusterCR())

	code, body := callClusterExpose(t, srv, `{"enabled":false}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if body["enabled"] != false {
		t.Fatalf("enabled = %v", body["enabled"])
	}
	stored := &v1alpha1.DolphinDBCluster{}
	if err := srv.client.Get(context.Background(), client.ObjectKey{Namespace: "dolphindb-system", Name: "ddb-min"}, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Networking.ServiceType != corev1.ServiceTypeClusterIP {
		t.Fatalf("service type = %q, want ClusterIP", stored.Spec.Networking.ServiceType)
	}
}
