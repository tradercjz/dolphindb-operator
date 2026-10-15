package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"dolphindb.io/operator/api/v1alpha1"
)

func newClusterReconciler(t *testing.T, objs ...client.Object) *DolphinDBClusterReconciler {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return &DolphinDBClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
		Scheme: s,
	}
}

func standaloneCluster() *v1alpha1.DolphinDBCluster {
	return &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-standalone",
			Namespace: "ddb",
			UID:       "uid-standalone",
		},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:       v1alpha1.ModeStandalone,
			Version:    "3.00.5",
			Standalone: &v1alpha1.StandaloneSpec{Port: 8848},
		},
	}
}

// Regression test for the EKS failure: a plain standalone CR used to pass the
// Kubernetes node name straight through as the Pod hostname, which Kubernetes
// rejects because cloud node names contain dots.
func TestEnsureDDBNodeSanitizesStandaloneHostname(t *testing.T) {
	cluster := standaloneCluster()
	r := newClusterReconciler(t, cluster)
	ctx := context.Background()

	eksNode := "ip-172-31-2-254.ap-southeast-2.compute.internal"
	if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleSingle, 0, 8848, "dolphindb/dolphindb:3.00.5", nil, eksNode); err != nil {
		t.Fatalf("ensureDDBNode: %v", err)
	}

	node := &v1alpha1.DDBNode{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-standalone-single-0"}, node); err != nil {
		t.Fatalf("expected the DDBNode to be created: %v", err)
	}
	if strings.Contains(node.Spec.Hostname, ".") {
		t.Fatalf("hostname %q must not contain dots", node.Spec.Hostname)
	}
	if node.Spec.Hostname != "ip-172-31-2-254" {
		t.Fatalf("hostname = %q, want ip-172-31-2-254", node.Spec.Hostname)
	}
}

// A CR that does not ask for host-bound licensing must not get privileged
// hostPath mounts, and must not inherit a licence path from anywhere.
func TestEnsureDDBNodeDoesNotInventHostBoundLicensing(t *testing.T) {
	cluster := standaloneCluster()
	cluster.Spec.License = v1alpha1.LicenseSpec{Mode: v1alpha1.LicenseModeSecret}
	r := newClusterReconciler(t, cluster)
	ctx := context.Background()

	if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleSingle, 0, 8848, "img", nil, "aftersale2"); err != nil {
		t.Fatalf("ensureDDBNode: %v", err)
	}
	node := &v1alpha1.DDBNode{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-standalone-single-0"}, node); err != nil {
		t.Fatal(err)
	}
	if node.Spec.Fingerprint != nil {
		t.Fatalf("fingerprint must stay nil, got %+v", node.Spec.Fingerprint)
	}
	if node.Spec.Hostname != "aftersale2" {
		t.Fatalf("hostname = %q", node.Spec.Hostname)
	}
}

// An explicitly configured fingerprint block is honoured verbatim, including the
// licence path the user chose.
func TestEnsureDDBNodeHonoursExplicitFingerprint(t *testing.T) {
	cluster := standaloneCluster()
	cluster.Spec.License = v1alpha1.LicenseSpec{
		Mode: v1alpha1.LicenseModeMachineFingerprint,
		MachineFingerprint: &v1alpha1.MachineFingerprintSpec{
			AutoPassthrough: true,
			MountMachineId:  true,
			LicensePath:     "/opt/dolphindb/dolphindb.lic",
		},
	}
	r := newClusterReconciler(t, cluster)
	ctx := context.Background()

	if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleSingle, 0, 8848, "img", nil, "aftersale2"); err != nil {
		t.Fatalf("ensureDDBNode: %v", err)
	}
	node := &v1alpha1.DDBNode{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-standalone-single-0"}, node); err != nil {
		t.Fatal(err)
	}
	if node.Spec.Fingerprint == nil {
		t.Fatal("fingerprint should be propagated")
	}
	if node.Spec.Fingerprint.LicensePath != "/opt/dolphindb/dolphindb.lic" {
		t.Fatalf("license path = %q", node.Spec.Fingerprint.LicensePath)
	}
}

// Regression test: a fingerprint that was set once could never be removed,
// because the update path had no branch for "existing set, desired unset". That
// left stale hostPath mounts (and their startup failures) in place forever.
func TestEnsureDDBNodeClearsStaleFingerprint(t *testing.T) {
	cluster := &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb-cluster", Namespace: "ddb", UID: "uid-cluster"},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:    v1alpha1.ModeCluster,
			Version: "3.00.5",
			License: v1alpha1.LicenseSpec{Mode: v1alpha1.LicenseModeSecret},
			Controller: &v1alpha1.ControllerGroupSpec{
				Replicas: 1,
				Port:     8900,
			},
		},
	}
	stale := &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb-cluster-ctl-0", Namespace: "ddb"},
		Spec: v1alpha1.DDBNodeSpec{
			ClusterName: "ddb-cluster",
			Role:        v1alpha1.NodeRoleController,
			Alias:       "ddb-cluster-ctl-0",
			Port:        8900,
			Fingerprint: &v1alpha1.MachineFingerprintSpec{
				AutoPassthrough: true,
				MountMachineId:  true,
				LicensePath:     "/home/someone/dolphindb.lic",
			},
		},
	}
	r := newClusterReconciler(t, cluster, stale)
	ctx := context.Background()

	if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleController, 0, 8900, "img", nil, ""); err != nil {
		t.Fatalf("ensureDDBNode: %v", err)
	}

	node := &v1alpha1.DDBNode{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-cluster-ctl-0"}, node); err != nil {
		t.Fatal(err)
	}
	if node.Spec.Fingerprint != nil {
		t.Fatalf("stale fingerprint must be cleared, got %+v", node.Spec.Fingerprint)
	}
}
