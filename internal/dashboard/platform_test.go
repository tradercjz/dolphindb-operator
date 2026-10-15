package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"dolphindb.io/operator/internal/exposure"
)

// A StorageClass is only usable if its provisioner is actually installed. Listing
// one that cannot provision leaves the user with PVCs stuck in Pending and no
// explanation, which is exactly what happened with an EBS class on a cluster
// without the EBS CSI driver.
func TestClassifyStorageClass(t *testing.T) {
	installed := map[string]bool{"ebs.csi.aws.com": true}

	tests := []struct {
		name        string
		provisioner string
		drivers     map[string]bool
		wantUsable  bool
	}{
		{"csi driver installed", "ebs.csi.aws.com", installed, true},
		{"csi driver missing", "ebs.csi.aws.com", map[string]bool{}, false},
		{"another csi driver missing", "pd.csi.storage.gke.io", map[string]bool{}, false},
		{"legacy aws provisioner", "kubernetes.io/aws-ebs", map[string]bool{}, false},
		{"local path", "rancher.io/local-path", map[string]bool{}, true},
		{"unknown non-csi provisioner", "example.com/nfs", map[string]bool{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			usable, hint := classifyStorageClass(tc.provisioner, tc.drivers)
			if usable != tc.wantUsable {
				t.Fatalf("classifyStorageClass(%q) usable = %v, want %v (hint %q)",
					tc.provisioner, usable, tc.wantUsable, hint)
			}
			// An unusable class must always explain itself.
			if !usable && hint == "" {
				t.Errorf("an unusable class must carry a hint: %q", tc.provisioner)
			}
			if usable && hint != "" {
				t.Errorf("a usable class should not carry a blocker hint: %q", tc.provisioner)
			}
		})
	}
}

func platformServer(t *testing.T, objs ...runtime.Object) *Server {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	// The namespace is left empty on purpose: this mirrors the usual deployment,
	// where the dashboard watches every namespace and must fall back to its own.
	t.Setenv("POD_NAMESPACE", "dolphindb-system")
	return NewServer(cli, nil, ":0", "")
}

func awsNodeForPlatform() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "ip-10-0-0-1.ap-southeast-2.compute.internal"},
		Spec:       corev1.NodeSpec{ProviderID: "aws:///ap-southeast-2a/i-0abcdef"},
	}
}

func callPlatform(t *testing.T, srv *Server) PlatformInfo {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/platform", nil)
	w := httptest.NewRecorder()
	srv.handlePlatform(w, req)
	resp := w.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var info PlatformInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return info
}

// Regression test for the whole complaint: namespaces and storage classes came back
// empty, and the exposure default was NodePort, which is unreachable on a cloud.
func TestPlatformReportsCloudSafeDefaults(t *testing.T) {
	scLocal := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "local-path"}, Provisioner: "rancher.io/local-path"}
	scEBS := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "ebs-gp3-sc"}, Provisioner: "ebs.csi.aws.com"}
	srv := platformServer(t, awsNodeForPlatform(), scLocal, scEBS,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dolphindb-system"}},
	)

	info := callPlatform(t, srv)

	if info.Provider != exposure.ProviderAWS {
		t.Fatalf("provider = %q", info.Provider)
	}
	if !info.SupportsLoadBalancer || info.DefaultServiceType != string(corev1.ServiceTypeLoadBalancer) {
		t.Fatalf("a cloud should default to LoadBalancer, got %q", info.DefaultServiceType)
	}
	if info.DefaultExpose.Any() {
		t.Fatalf("nothing may be exposed by default on a cloud, got %+v", info.DefaultExpose)
	}
	if info.Namespace != "dolphindb-system" {
		t.Fatalf("namespace = %q, want the pod's own namespace", info.Namespace)
	}
	if len(info.Namespaces) != 2 {
		t.Fatalf("expected the two namespaces, got %+v", info.Namespaces)
	}
	// The operator's own namespace must sort first so it is preselected.
	if info.Namespaces[0].Name != "dolphindb-system" {
		t.Errorf("first namespace = %q", info.Namespaces[0].Name)
	}

	// The default must be a class that can actually bind: the EBS class has no
	// installed driver, so `local-path` wins even though "ebs-gp3-sc" sorts first.
	if info.DefaultStorageClass != "local-path" {
		t.Fatalf("defaultStorageClass = %q, want the usable class", info.DefaultStorageClass)
	}
	var ebs *StorageClassSummary
	for i := range info.StorageClasses {
		if info.StorageClasses[i].Name == "ebs-gp3-sc" {
			ebs = &info.StorageClasses[i]
		}
	}
	if ebs == nil {
		t.Fatal("the unusable class should still be listed so the user learns why")
	}
	if ebs.Usable {
		t.Error("a CSI class without its driver must not be reported usable")
	}
	if ebs.Hint == "" {
		t.Error("an unusable class must explain itself")
	}
	if info.ImageRegistry == "" || info.ImageRepository == "" || info.DefaultVersion == "" {
		t.Errorf("image defaults must be populated, got %+v", info)
	}
}

// Bare metal keeps the historical behaviour: everything published over NodePort.
func TestPlatformKeepsBareMetalDefaults(t *testing.T) {
	bare := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "aftersale2"}}
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "longhorn"}, Provisioner: "driver.longhorn.io"}
	srv := platformServer(t, bare, sc)

	info := callPlatform(t, srv)

	if info.SupportsLoadBalancer {
		t.Fatal("bare metal cannot provision a load balancer")
	}
	if info.DefaultServiceType != string(corev1.ServiceTypeNodePort) {
		t.Fatalf("defaultServiceType = %q", info.DefaultServiceType)
	}
	if !info.DefaultExpose.Controller || !info.DefaultExpose.DataNode || !info.DefaultExpose.ComputeNode {
		t.Fatalf("bare metal should keep exposing every component, got %+v", info.DefaultExpose)
	}
	if info.DefaultStorageClass != "longhorn" {
		t.Fatalf("defaultStorageClass = %q", info.DefaultStorageClass)
	}
}

// With no class marked default and none usable, the picker must still be populated.
func TestPlatformFallsBackWhenNoClassIsUsable(t *testing.T) {
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "ebs-gp3-sc"}, Provisioner: "ebs.csi.aws.com"}
	srv := platformServer(t, awsNodeForPlatform(), sc)

	info := callPlatform(t, srv)

	if len(info.StorageClasses) != 1 {
		t.Fatalf("classes = %+v", info.StorageClasses)
	}
	if info.DefaultStorageClass != "" {
		t.Fatalf("no class is usable, so nothing should be preselected, got %q", info.DefaultStorageClass)
	}
}
