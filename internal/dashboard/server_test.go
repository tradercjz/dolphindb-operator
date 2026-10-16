package dashboard

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"dolphindb.io/operator/api/v1alpha1"
)

func TestDashboardHealthz(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	srv := NewServer(cli, nil, ":0", "default")

	req := httptest.NewRequest(http.MethodGet, "/api/healthz", nil)
	w := httptest.NewRecorder()

	srv.handleHealthz(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var data map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if data["status"] != "ok" {
		t.Errorf("expected status ok, got %s", data["status"])
	}
}

func TestDashboardListAndCreateClusters(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	testCluster := &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ddb",
			Namespace: "default",
		},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:    v1alpha1.ModeCluster,
			Version: "3.00.5",
			Controller: &v1alpha1.ControllerGroupSpec{
				Replicas: 1,
				Port:     8900,
			},
			DataNode: &v1alpha1.DataNodeGroupSpec{
				Replicas: 2,
				Port:     8901,
			},
		},
		Status: v1alpha1.DolphinDBClusterStatus{
			Phase: v1alpha1.PhaseReady,
			Controller: v1alpha1.TopologyStatus{
				Desired: 1,
				Ready:   1,
			},
			DataNode: v1alpha1.TopologyStatus{
				Desired: 2,
				Ready:   2,
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testCluster).Build()
	srv := NewServer(cli, nil, ":0", "default")

	// 1. List clusters
	req := httptest.NewRequest(http.MethodGet, "/api/clusters", nil)
	w := httptest.NewRecorder()
	srv.handleClusters(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var clusters []ClusterSummary
	if err := json.NewDecoder(resp.Body).Decode(&clusters); err != nil {
		t.Fatalf("failed to decode clusters: %v", err)
	}
	if len(clusters) != 1 || clusters[0].Name != "test-ddb" {
		t.Fatalf("expected 1 cluster 'test-ddb', got %d", len(clusters))
	}

	// 2. Create new cluster via POST
	createBody := CreateClusterRequest{
		Name:      "new-cluster",
		Namespace: "default",
		Mode:      "cluster",
		Version:   "3.00.5",
		Controller: &NodeGroupConfig{
			Replicas: 1,
		},
		DataNode: &NodeGroupConfig{
			Replicas: 2,
		},
	}
	bodyBytes, _ := json.Marshal(createBody)
	postReq := httptest.NewRequest(http.MethodPost, "/api/clusters", bytes.NewReader(bodyBytes))
	postW := httptest.NewRecorder()
	srv.handleClusters(postW, postReq)

	postResp := postW.Result()
	if postResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", postResp.StatusCode)
	}

	// 3. Delete cluster
	delReq := httptest.NewRequest(http.MethodDelete, "/api/clusters/default/new-cluster", nil)
	delW := httptest.NewRecorder()
	srv.deleteCluster(delW, delReq, "default", "new-cluster")
	if delW.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on delete, got %d", delW.Result().StatusCode)
	}
}

func TestDashboardNamespacesAndStorageClasses(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "prod-zone"}}
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "longhorn-nvme"},
		Provisioner: "driver.longhorn.io",
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns, sc).Build()
	srv := NewServer(cli, nil, ":0", "default")

	// Namespaces
	reqNS := httptest.NewRequest(http.MethodGet, "/api/namespaces", nil)
	wNS := httptest.NewRecorder()
	srv.handleNamespaces(wNS, reqNS)
	if wNS.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", wNS.Result().StatusCode)
	}

	// StorageClasses
	reqSC := httptest.NewRequest(http.MethodGet, "/api/storageclasses", nil)
	wSC := httptest.NewRecorder()
	srv.handleStorageClasses(wSC, reqSC)
	if wSC.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", wSC.Result().StatusCode)
	}
}

func TestDashboardSuspendAndResume(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	testCluster := &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ddb",
			Namespace: "default",
		},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:      v1alpha1.ModeCluster,
			Version:   "3.00.5",
			Suspended: false,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(testCluster).Build()
	srv := NewServer(cli, nil, ":0", "default")

	// 1. Suspend cluster
	reqSuspend := httptest.NewRequest(http.MethodPost, "/api/clusters/default/test-ddb/suspend", nil)
	wSuspend := httptest.NewRecorder()
	srv.handleClusterSubRoutes(wSuspend, reqSuspend)

	if wSuspend.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on suspend, got %d", wSuspend.Result().StatusCode)
	}

	// Verify cluster is now suspended in client
	var updated v1alpha1.DolphinDBCluster
	if err := cli.Get(reqSuspend.Context(), client.ObjectKey{Namespace: "default", Name: "test-ddb"}, &updated); err != nil {
		t.Fatal(err)
	}
	if !updated.Spec.Suspended {
		t.Fatalf("expected cluster.Spec.Suspended = true")
	}

	// 2. Resume cluster
	reqResume := httptest.NewRequest(http.MethodPost, "/api/clusters/default/test-ddb/resume", nil)
	wResume := httptest.NewRecorder()
	srv.handleClusterSubRoutes(wResume, reqResume)

	if wResume.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on resume, got %d", wResume.Result().StatusCode)
	}

	if err := cli.Get(reqResume.Context(), client.ObjectKey{Namespace: "default", Name: "test-ddb"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Suspended {
		t.Fatalf("expected cluster.Spec.Suspended = false after resume")
	}
}

