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

func clusterModeCluster(controllerReplicas int32) *v1alpha1.DolphinDBCluster {
	return &v1alpha1.DolphinDBCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-ha",
			Namespace: "ddb",
			UID:       "uid-ha",
		},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Mode:       v1alpha1.ModeCluster,
			Version:    "3.00.5",
			Controller: &v1alpha1.ControllerGroupSpec{Replicas: controllerReplicas, Port: 8900},
			DataNode:   &v1alpha1.DataNodeGroupSpec{Replicas: 1},
		},
	}
}

func ctlNode(name, cluster string, index int32) *v1alpha1.DDBNode {
	return &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ddb",
			Labels: map[string]string{
				"app.kubernetes.io/instance":  cluster,
				"app.kubernetes.io/component": string(v1alpha1.NodeRoleController),
			},
		},
		Spec: v1alpha1.DDBNodeSpec{
			Alias:       name,
			ClusterName: cluster,
			Role:        v1alpha1.NodeRoleController,
			Index:       index,
		},
	}
}

// Regression: reducing spec.controller.replicas used to leave the excess
// controller DDBNodes running forever; operators had to delete them by hand.
func TestReconcileControllerScaleDownRemovesHighestIndex(t *testing.T) {
	cluster := clusterModeCluster(1) // scaled 3 -> 1
	r := newClusterReconciler(t, cluster,
		ctlNode("ddb-ha-ctl-0", "ddb-ha", 0),
		ctlNode("ddb-ha-ctl-1", "ddb-ha", 1),
		ctlNode("ddb-ha-ctl-2", "ddb-ha", 2))

	handled, _, err := r.reconcileControllerScaleDown(context.Background(), cluster)
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	for _, name := range []string{"ddb-ha-ctl-2"} {
		var n v1alpha1.DDBNode
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: name}, &n); err == nil {
			t.Errorf("%s should have been deleted", name)
		}
	}
	for _, name := range []string{"ddb-ha-ctl-0", "ddb-ha-ctl-1"} {
		var n v1alpha1.DDBNode
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: name}, &n); err != nil {
			t.Errorf("%s should survive: %v", name, err)
		}
	}
}

func TestReconcileControllerScaleDownRejectsBelowOne(t *testing.T) {
	cluster := clusterModeCluster(0)
	r := newClusterReconciler(t, cluster,
		ctlNode("ddb-ha-ctl-0", "ddb-ha", 0),
		ctlNode("ddb-ha-ctl-1", "ddb-ha", 1))

	handled, _, err := r.reconcileControllerScaleDown(context.Background(), cluster)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if handled {
		t.Fatal("scale-down below 1 controller must be rejected without doing work")
	}
	var n v1alpha1.DDBNode
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-ctl-1"}, &n); err != nil {
		t.Fatal("no node may be deleted when the request is rejected")
	}
}

func TestRestartOnControllerTopologyChangeRecordsWithoutBouncingOnAdoption(t *testing.T) {
	cluster := clusterModeCluster(2)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ddb-ha-ctl-0", Namespace: "ddb"}}
	r := newClusterReconciler(t, cluster, pod)

	if err := r.restartOnControllerTopologyChange(context.Background(), cluster); err != nil {
		t.Fatalf("err=%v", err)
	}
	if got := cluster.Annotations[annControllerTopology]; got != "ctl:2,dn:1" {
		t.Fatalf("annotation = %q, want ctl:2,dn:1", got)
	}
	var p corev1.Pod
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-ctl-0"}, &p); err != nil {
		t.Fatal("adoption must not restart pods")
	}
}

func TestRestartOnControllerTopologyChangeRestartsWhenChanged(t *testing.T) {
	cluster := clusterModeCluster(1)
	cluster.Annotations = map[string]string{annControllerTopology: "ctl:2,dn:1"} // controller count changed 2 -> 1
	r := newClusterReconciler(t, cluster,
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ddb-ha-ctl-0", Namespace: "ddb", Labels: map[string]string{"app.kubernetes.io/name": "dolphindb", "app.kubernetes.io/instance": "ddb-ha"}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ddb-ha-dn-0", Namespace: "ddb", Labels: map[string]string{"app.kubernetes.io/name": "dolphindb", "app.kubernetes.io/instance": "ddb-ha"}}})

	if err := r.restartOnControllerTopologyChange(context.Background(), cluster); err != nil {
		t.Fatalf("err=%v", err)
	}
	var p corev1.Pod
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-ctl-0"}, &p); err == nil {
		t.Error("master controller pod must be restarted too (it re-reads the peer list)")
	}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-dn-0"}, &p); err == nil {
		t.Error("datanode pod must be restarted to re-read controllerSite")
	}
	if got := cluster.Annotations[annControllerTopology]; got != "ctl:1,dn:1" {
		t.Fatalf("annotation = %q, want ctl:1,dn:1", got)
	}
}

func TestRestartOnControllerTopologyChangeIsNoopWhenCurrent(t *testing.T) {
	cluster := clusterModeCluster(2)
	cluster.Annotations = map[string]string{annControllerTopology: "ctl:2,dn:1"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ddb-ha-ctl-0", Namespace: "ddb"}}
	r := newClusterReconciler(t, cluster, pod)

	if err := r.restartOnControllerTopologyChange(context.Background(), cluster); err != nil {
		t.Fatalf("err=%v", err)
	}
	var p corev1.Pod
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-ctl-0"}, &p); err != nil {
		t.Fatal("matching topology must not restart pods")
	}
}

// Regression: adding a datanode used to leave the controller blind — it reads
// cluster.nodes at startup only, so writes failed with "found 0" datanodes
// until someone bounced the controller by hand.
func TestRestartOnTopologyDatanodeOnlyRestartsControllersOnly(t *testing.T) {
	cluster := clusterModeCluster(1)
	cluster.Spec.DataNode.Replicas = 2 // datanodes scaled 1 -> 2
	cluster.Annotations = map[string]string{annControllerTopology: "ctl:1,dn:1"}
	ctlPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "ddb-ha-ctl-0", Namespace: "ddb",
		Labels: map[string]string{"app.kubernetes.io/name": "dolphindb", "app.kubernetes.io/instance": "ddb-ha", "app.kubernetes.io/component": "controller"}}}
	dnPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "ddb-ha-dn-1", Namespace: "ddb",
		Labels: map[string]string{"app.kubernetes.io/name": "dolphindb", "app.kubernetes.io/instance": "ddb-ha", "app.kubernetes.io/component": "datanode"}}}
	r := newClusterReconciler(t, cluster, ctlPod, dnPod)

	if err := r.restartOnControllerTopologyChange(context.Background(), cluster); err != nil {
		t.Fatalf("err=%v", err)
	}
	var p corev1.Pod
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-ctl-0"}, &p); err == nil {
		t.Error("controller pod must be restarted to re-read cluster.nodes")
	}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-dn-1"}, &p); err != nil {
		t.Error("surviving datanode pod must NOT be restarted for a datanode-only change")
	}
	if got := cluster.Annotations[annControllerTopology]; got != "ctl:1,dn:2" {
		t.Fatalf("annotation = %q, want ctl:1,dn:2", got)
	}
}

func TestReconcileSuspendedTogglesStoppedAnnotation(t *testing.T) {
	cluster := clusterModeCluster(1)
	cluster.Spec.Suspended = true

	nodeCtl := &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-ha-ctl-0",
			Namespace: "ddb",
			Labels: map[string]string{
				"app.kubernetes.io/instance":  "ddb-ha",
				"app.kubernetes.io/component": "controller",
			},
		},
		Spec: v1alpha1.DDBNodeSpec{ClusterName: "ddb-ha", Index: 0, Role: v1alpha1.NodeRoleController},
	}
	nodeDn := &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-ha-dn-0",
			Namespace: "ddb",
			Labels: map[string]string{
				"app.kubernetes.io/instance":  "ddb-ha",
				"app.kubernetes.io/component": "datanode",
			},
		},
		Spec: v1alpha1.DDBNodeSpec{ClusterName: "ddb-ha", Index: 0, Role: v1alpha1.NodeRoleDataNode},
	}

	r := newClusterReconciler(t, cluster, nodeCtl, nodeDn)
	ctx := context.Background()

	handled, _, err := r.reconcileSuspended(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handled {
		t.Fatal("expected reconcileSuspended to return handled=true when cluster is suspended")
	}

	// Verify child nodes received stopped annotation
	var ctl v1alpha1.DDBNode
	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-ctl-0"}, &ctl); err != nil {
		t.Fatal(err)
	}
	if ctl.Annotations["dolphindb.io/stopped"] != "true" {
		t.Fatalf("ctl node missing dolphindb.io/stopped annotation, got %v", ctl.Annotations)
	}

	if cluster.Status.Phase != v1alpha1.PhaseSuspended {
		t.Fatalf("expected PhaseSuspended, got %v", cluster.Status.Phase)
	}

	// Test resumption
	cluster.Spec.Suspended = false
	handled, _, err = r.reconcileSuspended(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handled {
		t.Fatal("expected reconcileSuspended to return handled=false when cluster is resumed")
	}

	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-ctl-0"}, &ctl); err != nil {
		t.Fatal(err)
	}
	if ctl.Annotations != nil && ctl.Annotations["dolphindb.io/stopped"] == "true" {
		t.Fatalf("ctl node should have stopped annotation removed on resume")
	}
}

func TestReconcileComputeNodeScaleDown(t *testing.T) {
	cluster := clusterModeCluster(1)
	cluster.Spec.ComputeNode = &v1alpha1.ComputeNodeGroupSpec{Replicas: 1}

	cn0 := &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-ha-cn-0",
			Namespace: "ddb",
			Labels: map[string]string{
				"app.kubernetes.io/instance":  "ddb-ha",
				"app.kubernetes.io/component": "computenode",
			},
		},
		Spec: v1alpha1.DDBNodeSpec{ClusterName: "ddb-ha", Index: 0, Role: v1alpha1.NodeRoleComputeNode},
	}
	cn1 := &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ddb-ha-cn-1",
			Namespace: "ddb",
			Labels: map[string]string{
				"app.kubernetes.io/instance":  "ddb-ha",
				"app.kubernetes.io/component": "computenode",
			},
		},
		Spec: v1alpha1.DDBNodeSpec{ClusterName: "ddb-ha", Index: 1, Role: v1alpha1.NodeRoleComputeNode},
	}

	r := newClusterReconciler(t, cluster, cn0, cn1)
	ctx := context.Background()

	handled, _, err := r.reconcileComputeNodeScaleDown(ctx, cluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handled {
		t.Fatal("expected reconcileComputeNodeScaleDown to handle scale down from 2 to 1")
	}

	var checkCn1 v1alpha1.DDBNode
	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-cn-1"}, &checkCn1); err == nil {
		t.Fatal("expected cn-1 to be deleted during scale down")
	}

	var checkCn0 v1alpha1.DDBNode
	if err := r.Get(ctx, client.ObjectKey{Namespace: "ddb", Name: "ddb-ha-cn-0"}, &checkCn0); err != nil {
		t.Fatal("expected cn-0 to be retained during scale down")
	}
}

