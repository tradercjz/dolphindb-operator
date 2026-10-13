package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"dolphindb.io/operator/api/v1alpha1"
)

// DDBNodeReconciler reconciles a DDBNode object
type DDBNodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbnodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbnodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

func (r *DDBNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	node := &v1alpha1.DDBNode{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling DDBNode", "alias", node.Spec.Alias, "role", node.Spec.Role, "phase", node.Status.Phase)

	// 1. Ensure Persistent Volume Claims
	pvcs, err := r.ensurePVCs(ctx, node)
	if err != nil {
		logger.Error(err, "Failed to ensure PVCs for DDBNode")
		return ctrl.Result{}, err
	}

	// 2. Ensure Pod
	pod, err := r.ensurePod(ctx, node, pvcs)
	if err != nil {
		logger.Error(err, "Failed to ensure Pod for DDBNode")
		return ctrl.Result{}, err
	}

	// 3. Update Status
	return ctrl.Result{}, r.updateStatus(ctx, node, pod, pvcs)
}

func (r *DDBNodeReconciler) ensurePVCs(ctx context.Context, node *v1alpha1.DDBNode) ([]corev1.PersistentVolumeClaim, error) {
	var pvcs []corev1.PersistentVolumeClaim

	// Meta PVC for redo logs, chunk metadata, raft logs
	metaSize := node.Spec.Storage.MetaVolumeSize
	if metaSize == "" {
		metaSize = "20Gi"
	}
	metaPVCName := fmt.Sprintf("meta-%s", node.Name)
	metaPVC, err := r.createPVCIfNotExist(ctx, node, metaPVCName, metaSize, node.Spec.Storage.StorageClassName)
	if err != nil {
		return nil, err
	}
	pvcs = append(pvcs, *metaPVC)

	// Data PVCs (JBOD)
	dataVolCount := node.Spec.Storage.DataVolumeCount
	if dataVolCount <= 0 {
		dataVolCount = 1
	}
	dataSize := node.Spec.Storage.DataVolumeSize
	if dataSize == "" {
		dataSize = "100Gi"
	}
	for i := 0; i < dataVolCount; i++ {
		dataPVCName := fmt.Sprintf("data-%d-%s", i, node.Name)
		pvc, err := r.createPVCIfNotExist(ctx, node, dataPVCName, dataSize, node.Spec.Storage.StorageClassName)
		if err != nil {
			return nil, err
		}
		pvcs = append(pvcs, *pvc)
	}

	return pvcs, nil
}

func (r *DDBNodeReconciler) createPVCIfNotExist(ctx context.Context, node *v1alpha1.DDBNode, name, sizeStr string, scName *string) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: name}, pvc)
	if err == nil {
		return pvc, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	qty, err := resource.ParseQuantity(sizeStr)
	if err != nil {
		qty = resource.MustParse("20Gi")
	}

	newPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: node.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "dolphindb",
				"app.kubernetes.io/instance":  node.Spec.ClusterName,
				"app.kubernetes.io/component": string(node.Spec.Role),
				"dolphindb.io/node-alias":     node.Spec.Alias,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: qty,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(node, newPVC, r.Scheme); err != nil {
		return nil, err
	}

	if err := r.Create(ctx, newPVC); err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}
	return newPVC, nil
}

func (r *DDBNodeReconciler) ensurePod(ctx context.Context, node *v1alpha1.DDBNode, pvcs []corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: node.Name}, pod)
	if err == nil {
		return pod, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	// Build new Pod
	newPod := r.buildPod(node, pvcs)
	if err := ctrl.SetControllerReference(node, newPod, r.Scheme); err != nil {
		return nil, err
	}

	if err := r.Create(ctx, newPod); err != nil {
		return nil, err
	}
	return newPod, nil
}

func (r *DDBNodeReconciler) buildPod(node *v1alpha1.DDBNode, pvcs []corev1.PersistentVolumeClaim) *corev1.Pod {
	headlessSvc := fmt.Sprintf("%s-headless", node.Spec.ClusterName)

	var volumes []corev1.Volume
	var ddbMounts []corev1.VolumeMount

	// Mount ConfigMap
	cfgVolName := "ddb-config"
	cmName := fmt.Sprintf("%s-config", node.Spec.ClusterName)
	if node.Spec.ConfigVolumeName != "" {
		cmName = node.Spec.ConfigVolumeName
	}
	volumes = append(volumes, corev1.Volume{
		Name: cfgVolName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
			},
		},
	})
	ddbMounts = append(ddbMounts, corev1.VolumeMount{
		Name:      cfgVolName,
		MountPath: "/dolphindb/config",
	})

	// Mount PVCs
	for i, pvc := range pvcs {
		volName := fmt.Sprintf("vol-%d", i)
		volumes = append(volumes, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.Name,
				},
			},
		})
		if strings.HasPrefix(pvc.Name, "meta-") {
			ddbMounts = append(ddbMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: "/var/lib/dolphindb/meta",
			})
		} else if strings.HasPrefix(pvc.Name, "data-") {
			volIdx := strings.Split(pvc.Name, "-")[1]
			ddbMounts = append(ddbMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: fmt.Sprintf("/var/lib/dolphindb/data/vol-%s", volIdx),
			})
		}
	}

	// Hardware fingerprint passthrough (if enabled)
	podHostname := node.Name
	if node.Spec.Hostname != "" {
		podHostname = node.Spec.Hostname
	}
	if node.Spec.Fingerprint != nil && node.Spec.Fingerprint.MountMachineId {
		midVolName := "host-machine-id"
		volumes = append(volumes, corev1.Volume{
			Name: midVolName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/etc/machine-id",
				},
			},
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      midVolName,
			MountPath: "/etc/machine-id",
			ReadOnly:  true,
		})
	}
	if node.Spec.Fingerprint != nil && node.Spec.Fingerprint.MountHostEtc {
		etcVolName := "host-etc"
		volumes = append(volumes, corev1.Volume{
			Name: etcVolName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/etc",
				},
			},
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      etcVolName,
			MountPath: "/dolphindb/etc",
			ReadOnly:  true,
		})
	}

	// Build DolphinDB start command line
	args := []string{
		"-home", "/var/lib/dolphindb",
		"-console", "0",
		"-stdoutLog", "1",
		"-mode", string(node.Spec.Role),
	}
	domain := fmt.Sprintf("%s.%s.%s.svc.cluster.local", node.Name, headlessSvc, node.Namespace)
	args = append(args, "-localSite", fmt.Sprintf("%s:%d:%s", domain, node.Spec.Port, node.Spec.Alias))

	if node.Spec.Role != v1alpha1.NodeRoleSingle {
		ctlDomain := fmt.Sprintf("%s-ctl-0.%s.%s.svc.cluster.local", node.Spec.ClusterName, headlessSvc, node.Namespace)
		args = append(args, "-controllerSite", fmt.Sprintf("%s:8900:%s-ctl-0", ctlDomain, node.Spec.ClusterName))
		args = append(args, "-config", fmt.Sprintf("/dolphindb/config/%s.cfg", node.Spec.Role))
		args = append(args, "-clusterConfig", "/dolphindb/config/cluster.cfg")
		if node.Spec.Role == v1alpha1.NodeRoleController {
			args = append(args, "-nodesFile", "/dolphindb/config/cluster.nodes")
		}
	} else {
		args = append(args, "-config", "/dolphindb/config/dolphindb.cfg")
	}

	graceSeconds := int64(300)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      node.Name,
			Namespace: node.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "dolphindb",
				"app.kubernetes.io/instance":  node.Spec.ClusterName,
				"app.kubernetes.io/component": string(node.Spec.Role),
				"dolphindb.io/node-alias":     node.Spec.Alias,
			},
		},
		Spec: corev1.PodSpec{
			Hostname:                      podHostname,
			Subdomain:                     headlessSvc,
			NodeName:                      node.Spec.NodeName,
			NodeSelector:                  node.Spec.NodeSelector,
			Tolerations:                   node.Spec.Tolerations,
			TerminationGracePeriodSeconds: &graceSeconds,
			Volumes:                       volumes,
			Containers: []corev1.Container{
				{
					Name:         "dolphindb",
					Image:        node.Spec.Image,
					Command:      []string{"/dolphindb/server/dolphindb"},
					Args:         args,
					Resources:    node.Spec.Resources,
					VolumeMounts: ddbMounts,
					Ports: []corev1.ContainerPort{
						{Name: "ddb-port", ContainerPort: node.Spec.Port},
					},
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"/bin/sh", "-c", fmt.Sprintf("/ddb-agent -mode prestop -ddbPort %d", node.Spec.Port)},
							},
						},
					},
				},
			},
		},
	}

	return pod
}

func (r *DDBNodeReconciler) updateStatus(ctx context.Context, node *v1alpha1.DDBNode, pod *corev1.Pod, pvcs []corev1.PersistentVolumeClaim) error {
	node.Status.PodName = pod.Name
	node.Status.PodIP = pod.Status.PodIP
	node.Status.HostIP = pod.Status.HostIP
	node.Status.NodeName = pod.Spec.NodeName

	switch pod.Status.Phase {
	case corev1.PodRunning:
		node.Status.Phase = v1alpha1.NodePhaseRunning
		node.Status.DDBState = 1
	case corev1.PodPending:
		node.Status.Phase = v1alpha1.NodePhaseCreating
	case corev1.PodFailed:
		node.Status.Phase = v1alpha1.NodePhaseFailed
	default:
		node.Status.Phase = v1alpha1.NodePhasePending
	}

	var bindings []v1alpha1.VolumeBinding
	for _, pvc := range pvcs {
		bindings = append(bindings, v1alpha1.VolumeBinding{
			Name:    pvc.Name,
			PVCName: pvc.Name,
			Bound:   pvc.Status.Phase == corev1.ClaimBound,
		})
	}
	node.Status.Volumes = bindings

	return r.Status().Update(ctx, node)
}

func (r *DDBNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DDBNode{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}
