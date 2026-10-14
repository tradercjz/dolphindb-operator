package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"dolphindb.io/operator/api/v1alpha1"
)

func TestDashboardHealthz(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	srv := NewServer(cli, ":0", "default")

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

func TestDashboardListClusters(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)

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
	srv := NewServer(cli, ":0", "default")

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

	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusters))
	}

	if clusters[0].Name != "test-ddb" || clusters[0].ReadyNodes != 3 {
		t.Errorf("unexpected cluster summary: %+v", clusters[0])
	}
}
