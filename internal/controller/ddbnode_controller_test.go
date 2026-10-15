package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"dolphindb.io/operator/api/v1alpha1"
)

func podTestReconciler(t *testing.T) *DDBNodeReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return &DDBNodeReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
	}
}

func nodeOfRole(role v1alpha1.NodeRole) *v1alpha1.DDBNode {
	return &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{Name: "ddb-" + string(role) + "-0", Namespace: "ddb"},
		Spec: v1alpha1.DDBNodeSpec{
			ClusterName: "ddb",
			Role:        role,
			Alias:       "ddb-" + string(role) + "-0",
			Port:        8848,
			Version:     "3.00.5",
			Image:       "img",
		},
	}
}

func pvcNamed(name string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ddb"},
	}
}

// Regression test for silent data loss in cluster mode.
//
// DolphinDB keeps its redo log and recover log under `<home>/log`. The pod builder
// decided whether to mount the node home on a PVC with `if i == 0`, where `i`
// indexes the whole PVC list - which always starts with the meta volume. So a data
// node's home stayed on the container's ephemeral layer:
//
//	[startup::redoLogReplay] The log dir [...] is empty. No need to check redo log.
//
// and deleting the pod discarded acknowledged writes. Measured on a live cluster:
// six rows written, pod recreated, two rows left.
func TestBuildPodMountsHomeOnPersistentStorage(t *testing.T) {
	cases := []struct {
		role viteRole
		pvcs []corev1.PersistentVolumeClaim
	}{
		// A single node has a data volume but no meta volume.
		{viteRole{v1alpha1.NodeRoleSingle, "/data/ddb/server/data"}, []corev1.PersistentVolumeClaim{
			pvcNamed("data-0-x"),
		}},
		// A controller has a meta volume but no data volume.
		{viteRole{v1alpha1.NodeRoleController, "/data/ddb/server/data"}, []corev1.PersistentVolumeClaim{
			pvcNamed("meta-x"),
		}},
		// A data node has both; the data volume is NOT first in the list.
		{viteRole{v1alpha1.NodeRoleDataNode, "/data/ddb/server/clusterDemo/data"}, []corev1.PersistentVolumeClaim{
			pvcNamed("meta-x"), pvcNamed("data-0-x"),
		}},
		// A compute node has only a meta volume.
		{viteRole{v1alpha1.NodeRoleComputeNode, "/data/ddb/server/clusterDemo/data"}, []corev1.PersistentVolumeClaim{
			pvcNamed("meta-x"),
		}},
	}

	r := podTestReconciler(t)
	for _, tc := range cases {
		t.Run(string(tc.role.role), func(t *testing.T) {
			node := nodeOfRole(tc.role.role)
			pod := r.buildPod(context.Background(), node, tc.pvcs)

			if got := homeMountPath(node); got != tc.role.home {
				t.Fatalf("homeMountPath = %q, want %q", got, tc.role.home)
			}
			if !podHasPersistentHome(node, pod) {
				t.Fatalf("%s: home %q is not backed by a PVC; the redo log would be lost on restart",
					tc.role.role, tc.role.home)
			}
			// The home mount must be the expected path, not just any PVC mount.
			found := false
			for _, m := range pod.Spec.Containers[0].VolumeMounts {
				if m.MountPath == tc.role.home {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s: no mount at %q, mounts: %+v", tc.role.role, tc.role.home,
					pod.Spec.Containers[0].VolumeMounts)
			}
		})
	}
}

type viteRole struct {
	role v1alpha1.NodeRole
	home string
}

// The data volume must still be mounted at its DolphinDB volume path, and a node
// with several volumes must get one mount per volume.
func TestBuildPodKeepsVolumeMounts(t *testing.T) {
	r := podTestReconciler(t)
	node := nodeOfRole(v1alpha1.NodeRoleDataNode)
	node.Spec.Storage.DataVolumeCount = 2
	pvcs := []corev1.PersistentVolumeClaim{
		pvcNamed("meta-x"), pvcNamed("data-0-x"), pvcNamed("data-1-x"),
	}
	pod := r.buildPod(context.Background(), node, pvcs)

	mounts := map[string]bool{}
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		mounts[m.MountPath] = true
	}
	for _, want := range []string{
		"/data/ddb/server/clusterDemo/data",
		"/var/lib/dolphindb/data/vol-0",
		"/var/lib/dolphindb/data/vol-1",
	} {
		if !mounts[want] {
			t.Errorf("missing mount %q; have %v", want, mounts)
		}
	}
}

// A pod built before the fix has the home on the ephemeral layer and must be
// replaced, otherwise running clusters would keep the bug.
func TestPodHasPersistentHomeDetectsLegacyPod(t *testing.T) {
	node := nodeOfRole(v1alpha1.NodeRoleDataNode)
	legacy := &corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{Name: "vol-0", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "meta-x"}}},
				{Name: "vol-1", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-0-x"}}},
			},
			Containers: []corev1.Container{{
				VolumeMounts: []corev1.VolumeMount{
					{Name: "vol-0", MountPath: "/var/lib/dolphindb/meta"},
					{Name: "vol-1", MountPath: "/var/lib/dolphindb/data/vol-0"},
				},
			}},
		},
	}
	if podHasPersistentHome(node, legacy) {
		t.Fatal("a pod whose home is not on a PVC must be reported as such, so it gets recreated")
	}
}
