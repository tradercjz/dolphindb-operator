package controller

import (
	"context"
	"fmt"
	"strconv"
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
	"dolphindb.io/operator/internal/metrics"
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

	// 0. Check if node is explicitly stopped by Day-2 operation
	if node.Annotations != nil && node.Annotations["dolphindb.io/stopped"] == "true" {
		pod := &corev1.Pod{}
		err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: node.Name}, pod)
		if err == nil {
			logger.Info("Node is marked as stopped, deleting Pod", "node", node.Name)
			if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		if node.Status.Phase != v1alpha1.NodePhaseOffline || node.Status.PodIP != "" {
			node.Status.Phase = v1alpha1.NodePhaseOffline
			node.Status.PodName = ""
			node.Status.PodIP = ""
			node.Status.DDBState = 0
			return ctrl.Result{}, r.Status().Update(ctx, node)
		}
		return ctrl.Result{}, nil
	}

	// 1. Ensure Persistent Volume Claims
	pvcs, err := r.ensurePVCs(ctx, node)
	if err != nil {
		logger.Error(err, "Failed to ensure PVCs for DDBNode")
		return ctrl.Result{}, err
	}

	// 2. Ensure per-node Headless Service (allows reliable node-level DNS regardless of pod hostname)
	if node.Spec.Role != v1alpha1.NodeRoleSingle {
		if _, err := r.ensureService(ctx, node); err != nil {
			logger.Error(err, "Failed to ensure Service for DDBNode")
			return ctrl.Result{}, err
		}
	}

	// 3. Ensure Pod
	pod, err := r.ensurePod(ctx, node, pvcs)
	if err != nil {
		logger.Error(err, "Failed to ensure Pod for DDBNode")
		return ctrl.Result{}, err
	}

	// 4. Update Status
	return ctrl.Result{}, r.updateStatus(ctx, node, pod, pvcs)
}

func (r *DDBNodeReconciler) ensurePVCs(ctx context.Context, node *v1alpha1.DDBNode) ([]corev1.PersistentVolumeClaim, error) {
	var pvcs []corev1.PersistentVolumeClaim

	// If explicit existing PVC claims are given (e.g. from legacy migration adoption), adopt them directly!
	if len(node.Spec.Storage.ExistingClaims) > 0 {
		for _, claimName := range node.Spec.Storage.ExistingClaims {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: claimName}, pvc); err == nil {
				pvcs = append(pvcs, *pvc)
			}
		}
		if len(pvcs) > 0 {
			return pvcs, nil
		}
	}

	// Meta PVC for redo logs, chunk metadata, raft logs (only for cluster nodes)
	if node.Spec.Role != v1alpha1.NodeRoleSingle {
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
	}

	// Data PVCs (JBOD) - only for DataNode and Single node (ComputeNode and Controller do not store DFS data)
	if node.Spec.Role == v1alpha1.NodeRoleDataNode || node.Spec.Role == v1alpha1.NodeRoleSingle {
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

	pvcAnnotations := make(map[string]string)
	if node.Spec.NodeName != "" {
		pvcAnnotations["volume.kubernetes.io/selected-node"] = node.Spec.NodeName
	}

	newPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   node.Namespace,
			Annotations: pvcAnnotations,
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

func (r *DDBNodeReconciler) ensureService(ctx context.Context, node *v1alpha1.DDBNode) (*corev1.Service, error) {
	svc := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: node.Name}, svc)
	if err == nil {
		needsUpdate := false
		if svc.Spec.Selector == nil || svc.Spec.Selector["dolphindb.io/node-alias"] != node.Spec.Alias {
			if svc.Spec.Selector == nil {
				svc.Spec.Selector = make(map[string]string)
			}
			svc.Spec.Selector["dolphindb.io/node-alias"] = node.Spec.Alias
			needsUpdate = true
		}
		if len(svc.Spec.Ports) > 0 && svc.Spec.Ports[0].Port != node.Spec.Port {
			svc.Spec.Ports[0].Port = node.Spec.Port
			needsUpdate = true
		}
		if needsUpdate {
			if err := r.Update(ctx, svc); err != nil {
				return nil, err
			}
		}
		return svc, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	newSvc := &corev1.Service{
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
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector: map[string]string{
				"dolphindb.io/node-alias": node.Spec.Alias,
			},
			Ports: []corev1.ServicePort{
				{
					Name: "ddb-port",
					Port: node.Spec.Port,
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(node, newSvc, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, newSvc); err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}
	return newSvc, nil
}

func (r *DDBNodeReconciler) ensurePod(ctx context.Context, node *v1alpha1.DDBNode, pvcs []corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: node.Name}, pod)
	if err == nil {
		// Detect if pod spec has diverged on hardware fingerprint or node placement
		needsRecreate := false
		if node.Spec.Fingerprint != nil && (node.Spec.Fingerprint.MountMachineId || node.Spec.Fingerprint.MachineIdSecretName != "" || node.Spec.Fingerprint.VirtualMachineId != "") {
			hasMachineId := false
			for _, v := range pod.Spec.Volumes {
				if v.Name == "host-machine-id" || v.Name == "secret-machine-id" {
					hasMachineId = true
					break
				}
			}
			if !hasMachineId {
				needsRecreate = true
			}
		}
		if node.Spec.Fingerprint != nil {
			hasHostHn := false
			for _, v := range pod.Spec.Volumes {
				if v.Name == "host-hostname" {
					hasHostHn = true
					break
				}
			}
			if !hasHostHn {
				needsRecreate = true
			}
		}
		if node.Spec.Fingerprint != nil && node.Spec.Fingerprint.AutoPassthrough {
			hasLic := false
			for _, v := range pod.Spec.Volumes {
				if v.Name == "host-dolphindb-license" || v.Name == "ddb-license" {
					hasLic = true
					break
				}
			}
			if !hasLic {
				needsRecreate = true
			}
		}
		expectedHostname := node.Name
		if node.Spec.Role == v1alpha1.NodeRoleSingle {
			if node.Spec.Hostname != "" {
				expectedHostname = node.Spec.Hostname
			} else if node.Spec.Fingerprint != nil {
				if node.Spec.Fingerprint.Hostname != "" {
					expectedHostname = node.Spec.Fingerprint.Hostname
				} else if node.Spec.Fingerprint.UseHostName && node.Spec.NodeName != "" {
					expectedHostname = node.Spec.NodeName
				} else if node.Spec.NodeName != "" {
					expectedHostname = node.Spec.NodeName
				}
			}
		}
		if expectedHostname != "" && pod.Spec.Hostname != expectedHostname {
			needsRecreate = true
		}
		if node.Spec.NodeName != "" && pod.Spec.NodeName != "" && pod.Spec.NodeName != node.Spec.NodeName {
			needsRecreate = true
		}
		if pod.Labels != nil && node.Spec.Alias != "" && pod.Labels["dolphindb.io/node-alias"] != node.Spec.Alias {
			needsRecreate = true
		}
		desiredArgs := r.buildContainerArgs(node)
		if len(pod.Spec.Containers) > 0 {
			if pod.Spec.Containers[0].Image != node.Spec.Image {
				needsRecreate = true
			}
			if !slicesEqual(pod.Spec.Containers[0].Args, desiredArgs) {
				needsRecreate = true
			}
			hasLogMountInPod := false
			for _, vm := range pod.Spec.Containers[0].VolumeMounts {
				if vm.MountPath == "/data/ddb/server/log" {
					hasLogMountInPod = true
					break
				}
			}
			if len(pvcs) > 0 && !hasLogMountInPod {
				needsRecreate = true
			}
		}
		if len(pvcs) > 0 {
			for _, expPVC := range pvcs {
				matched := false
				for _, v := range pod.Spec.Volumes {
					if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == expPVC.Name {
						matched = true
						break
					}
				}
				if !matched {
					needsRecreate = true
					break
				}
			}
		}
		if needsRecreate && pod.DeletionTimestamp == nil {
			log.FromContext(ctx).Info("Recreating pod due to updated spec / fingerprint / volume bindings",
				"pod", pod.Name,
				"podHostname", pod.Spec.Hostname,
				"expectedHostname", expectedHostname,
				"podNodeName", pod.Spec.NodeName,
				"nodeSpecNodeName", node.Spec.NodeName,
			)
			if err := r.Delete(ctx, pod); err != nil {
				return nil, err
			}
			return pod, nil
		}
		// Dynamically reconcile mutable labels & annotations (e.g., Prometheus scraping, legacy Grafana labels)
		updated := false
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		if pod.Labels["dolphindb.io/dolphindb.name"] != node.Spec.ClusterName {
			pod.Labels["dolphindb.io/dolphindb.name"] = node.Spec.ClusterName
			updated = true
		}
		compType := getLegacyComponentType(node.Spec.Role)
		if pod.Labels["dolphindb.io/component.type"] != compType {
			pod.Labels["dolphindb.io/component.type"] = compType
			updated = true
		}
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		if pod.Annotations["prometheus.io/scrape"] != "true" {
			pod.Annotations["prometheus.io/scrape"] = "true"
			pod.Annotations["prometheus.io/port"] = strconv.Itoa(int(node.Spec.Port))
			pod.Annotations["prometheus.io/path"] = "/metrics"
			updated = true
		}
		if updated {
			if err := r.Update(ctx, pod); err != nil {
				return nil, err
			}
		}
		return pod, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	// Build new Pod
	newPod := r.buildPod(ctx, node, pvcs)
	if err := ctrl.SetControllerReference(node, newPod, r.Scheme); err != nil {
		return nil, err
	}

	if err := r.Create(ctx, newPod); err != nil {
		return nil, err
	}
	return newPod, nil
}

func (r *DDBNodeReconciler) buildPod(ctx context.Context, node *v1alpha1.DDBNode, pvcs []corev1.PersistentVolumeClaim) *corev1.Pod {
	headlessSvc := fmt.Sprintf("%s-headless", node.Spec.ClusterName)

	var volumes []corev1.Volume
	var ddbMounts []corev1.VolumeMount

	// Mount ConfigMap via an emptyDir staging so that DolphinDB process has full write access
	// (enabling Web UI "saveClusterNodesConfigs" to write cluster.cfg without Read-only filesystem error)
	cfgSrcVolName := "ddb-config-source"
	cfgWorkVolName := "ddb-config"
	cmName := fmt.Sprintf("%s-config", node.Spec.ClusterName)
	if node.Spec.ConfigVolumeName != "" {
		cmName = node.Spec.ConfigVolumeName
	}
	volumes = append(volumes,
		corev1.Volume{
			Name: cfgSrcVolName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
				},
			},
		},
		corev1.Volume{
			Name: cfgWorkVolName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	)
	ddbMounts = append(ddbMounts,
		corev1.VolumeMount{
			Name:      cfgSrcVolName,
			MountPath: "/data/ddb/config-source",
			ReadOnly:  true,
		},
		corev1.VolumeMount{
			Name:      cfgWorkVolName,
			MountPath: "/data/ddb/server/config",
		},
	)

	// Mount License: check Secret first (supports per-node <nodeName>.lic or dolphindb.lic), fallback to host passthrough
	secretName := node.Spec.LicenseSecretName
	if secretName == "" && (node.Spec.Fingerprint == nil || !node.Spec.Fingerprint.AutoPassthrough) {
		secretName = "dolphindb-license"
	}
	hasSecretLicense := false
	secretKey := "dolphindb.lic"
	if secretName != "" {
		sec := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: secretName}, sec); err == nil && sec.Data != nil {
			nodeKey := fmt.Sprintf("%s.lic", node.Spec.NodeName)
			if node.Spec.NodeName != "" && len(sec.Data[nodeKey]) > 0 {
				secretKey = nodeKey
				hasSecretLicense = true
			} else if len(sec.Data["dolphindb.lic"]) > 0 {
				secretKey = "dolphindb.lic"
				hasSecretLicense = true
			}
		}
	}

	if hasSecretLicense {
		licVolName := "ddb-license"
		volumes = append(volumes, corev1.Volume{
			Name: licVolName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
					Items: []corev1.KeyToPath{
						{Key: secretKey, Path: "dolphindb.lic"},
					},
				},
			},
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      licVolName,
			MountPath: "/data/ddb/license",
		})
	} else if node.Spec.Fingerprint != nil && node.Spec.Fingerprint.AutoPassthrough && node.Spec.Fingerprint.LicensePath != "" {
		// Host-path licence passthrough is strictly opt-in. There is deliberately no
		// fallback path here: the previous hardcoded default pointed at a directory
		// inside the original author's home directory, so on any other machine the
		// kubelet failed with "not a directory" and the container never started.
		licVolName := "host-dolphindb-license"
		licPath := node.Spec.Fingerprint.LicensePath
		volumes = append(volumes, corev1.Volume{
			Name: licVolName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: licPath,
				},
			},
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      licVolName,
			MountPath: "/data/ddb/server/dolphindb.lic",
		})
	}

	// Mount ACL public key for DataNode and ComputeNode in cluster mode
	if node.Spec.Role == v1alpha1.NodeRoleDataNode || node.Spec.Role == v1alpha1.NodeRoleComputeNode {
		aclSecName := fmt.Sprintf("%s-acl-keys", node.Spec.ClusterName)
		aclVolName := "ddb-acl-key"
		volumes = append(volumes, corev1.Volume{
			Name: aclVolName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: aclSecName,
				},
			},
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      aclVolName,
			MountPath: "/data/ddb/server/data/keys",
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      aclVolName,
			MountPath: fmt.Sprintf("/data/ddb/server/data/%s/keys", node.Spec.Alias),
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      aclVolName,
			MountPath: fmt.Sprintf("/data/ddb/server/clusterDemo/data/%s/keys", node.Spec.Alias),
		})
	}

	// Mount PVCs
	hasLogMount := false
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
		if strings.HasPrefix(pvc.Name, "log-") || strings.Contains(pvc.Name, "-log-") {
			ddbMounts = append(ddbMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: "/data/ddb/server/log",
			})
			hasLogMount = true
		} else if strings.HasPrefix(pvc.Name, "meta-") {
			if node.Spec.Role == v1alpha1.NodeRoleSingle {
				ddbMounts = append(ddbMounts, corev1.VolumeMount{
					Name:      volName,
					MountPath: "/data/ddb/server/meta",
				})
			} else {
				ddbMounts = append(ddbMounts, corev1.VolumeMount{
					Name:      volName,
					MountPath: "/var/lib/dolphindb/meta",
				})
			}
		} else if strings.HasPrefix(pvc.Name, "data-") {
			if node.Spec.Role == v1alpha1.NodeRoleSingle || node.Spec.Role == v1alpha1.NodeRoleController {
				ddbMounts = append(ddbMounts, corev1.VolumeMount{
					Name:      volName,
					MountPath: "/data/ddb/server/data",
				})
				ddbMounts = append(ddbMounts, corev1.VolumeMount{
					Name:      volName,
					MountPath: "/data/ddb/server/clusterDemo/data",
				})
				ddbMounts = append(ddbMounts, corev1.VolumeMount{
					Name:      volName,
					MountPath: "/var/lib/dolphindb/data/vol-0",
				})
			} else {
				volIdx := fmt.Sprintf("%d", i)
				parts := strings.Split(pvc.Name, "-")
				if len(parts) >= 2 {
					if _, err := strconv.Atoi(parts[1]); err == nil {
						volIdx = parts[1]
					}
				}
				if i == 0 {
					ddbMounts = append(ddbMounts, corev1.VolumeMount{
						Name:      volName,
						MountPath: "/data/ddb/server/data",
					})
					ddbMounts = append(ddbMounts, corev1.VolumeMount{
						Name:      volName,
						MountPath: "/data/ddb/server/clusterDemo/data",
					})
				}
				ddbMounts = append(ddbMounts, corev1.VolumeMount{
					Name:      volName,
					MountPath: fmt.Sprintf("/var/lib/dolphindb/data/vol-%s", volIdx),
				})
			}
		} else {
			mountPath := "/data/ddb/server/data"
			if node.Spec.Role != v1alpha1.NodeRoleSingle {
				mountPath = fmt.Sprintf("/var/lib/dolphindb/data/vol-%d", i)
			}
			ddbMounts = append(ddbMounts, corev1.VolumeMount{
				Name:      volName,
				MountPath: mountPath,
			})
		}
	}

	// Ensure /data/ddb/server/log is mounted to persistent storage or emptyDir
	if !hasLogMount {
		if len(pvcs) > 0 {
			ddbMounts = append(ddbMounts, corev1.VolumeMount{
				Name:      "vol-0",
				MountPath: "/data/ddb/server/log",
				SubPath:   "log",
			})
		} else {
			logVolName := "ddb-log"
			volumes = append(volumes, corev1.Volume{
				Name: logVolName,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			})
			ddbMounts = append(ddbMounts, corev1.VolumeMount{
				Name:      logVolName,
				MountPath: "/data/ddb/server/log",
			})
		}
	}

	// Hardware fingerprint passthrough (if enabled).
	//
	// Any node name that ends up as the Pod hostname must be sanitised: managed
	// node names are DNS names containing dots, which Kubernetes rejects outright.
	podHostname := node.Name
	if node.Spec.Role == v1alpha1.NodeRoleSingle {
		if node.Spec.Hostname != "" {
			podHostname = SanitizeHostname(node.Spec.Hostname)
		} else if node.Spec.Fingerprint != nil {
			if node.Spec.Fingerprint.Hostname != "" {
				podHostname = SanitizeHostname(node.Spec.Fingerprint.Hostname)
			} else if node.Spec.Fingerprint.UseHostName && node.Spec.NodeName != "" {
				podHostname = SanitizeHostname(node.Spec.NodeName)
			} else if node.Spec.NodeName != "" {
				podHostname = SanitizeHostname(node.Spec.NodeName)
			}
		}
	}

	podNodeName := node.Spec.NodeName
	if podNodeName == "" && node.Spec.Fingerprint != nil && node.Spec.Fingerprint.NodeName != "" {
		podNodeName = node.Spec.Fingerprint.NodeName
	}
	midMounted := false
	midSecretName := secretName
	if node.Spec.Fingerprint != nil && node.Spec.Fingerprint.MachineIdSecretName != "" {
		midSecretName = node.Spec.Fingerprint.MachineIdSecretName
	}
	if midSecretName != "" {
		sec := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: midSecretName}, sec); err == nil && sec.Data != nil {
			if len(sec.Data["machine-id"]) > 0 {
				midVolName := "secret-machine-id"
				volumes = append(volumes, corev1.Volume{
					Name: midVolName,
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: midSecretName,
							Items: []corev1.KeyToPath{
								{Key: "machine-id", Path: "machine-id"},
							},
						},
					},
				})
				ddbMounts = append(ddbMounts, corev1.VolumeMount{
					Name:      midVolName,
					MountPath: "/etc/machine-id",
					SubPath:   "machine-id",
					ReadOnly:  true,
				})
				midMounted = true
			}
		}
	}

	if !midMounted && node.Spec.Fingerprint != nil && node.Spec.Fingerprint.MountMachineId {
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
	if node.Spec.Fingerprint != nil {
		hnVolName := "host-hostname"
		volumes = append(volumes, corev1.Volume{
			Name: hnVolName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/etc/hostname",
				},
			},
		})
		ddbMounts = append(ddbMounts, corev1.VolumeMount{
			Name:      hnVolName,
			MountPath: "/etc/hostname",
			ReadOnly:  true,
		})
	}

	// Build DolphinDB start command line
	args := r.buildContainerArgs(node)

	probeFunc := "isDataNodeInitialized()"
	if node.Spec.Role != v1alpha1.NodeRoleDataNode {
		probeFunc = "isNodeInitialized()"
	}
	probeCmd := fmt.Sprintf(`wget -q -O - --header="Content-Type: application/json" --post-data='{"functionName":"executeCode","params":[{"name":"script","form":"scalar","type":"string","value":%q}]}' http://127.0.0.1:%d | grep -q '"value":"1"'`, probeFunc, node.Spec.Port)

	readinessProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/sh", "-c", probeCmd},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      5,
		SuccessThreshold:    1,
		FailureThreshold:    5,
	}

	startupProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/sh", "-c", probeCmd},
			},
		},
		InitialDelaySeconds: 3,
		PeriodSeconds:       3,
		TimeoutSeconds:      5,
		SuccessThreshold:    1,
		FailureThreshold:    60, // allows up to 180s for node initialization and redo log playback
	}

	lifecycle := &corev1.Lifecycle{
		PreStop: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"/bin/sh",
					"-c",
					`kill -15 1 || true; for i in $(seq 1 15); do kill -0 1 2>/dev/null || exit 0; sleep 1; done; kill -9 1 || true`,
				},
			},
		},
	}

	graceSeconds := int64(30)
	compType := getLegacyComponentType(node.Spec.Role)
	podLabels := map[string]string{
		"app.kubernetes.io/name":      "dolphindb",
		"app.kubernetes.io/instance":  node.Spec.ClusterName,
		"app.kubernetes.io/component": string(node.Spec.Role),
		"dolphindb.io/node-alias":     node.Spec.Alias,
		"dolphindb.io/dolphindb.name": node.Spec.ClusterName,
		"dolphindb.io/component.type": compType,
	}
	if node.Spec.Role == v1alpha1.NodeRoleController {
		if node.Spec.Index == 0 {
			podLabels["dolphindb.io/leader"] = "true"
		} else {
			podLabels["dolphindb.io/leader"] = "false"
		}
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      node.Name,
			Namespace: node.Namespace,
			Labels:    podLabels,
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   strconv.Itoa(int(node.Spec.Port)),
				"prometheus.io/path":   "/metrics",
			},
		},
		Spec: corev1.PodSpec{
			Hostname:                      podHostname,
			Subdomain:                     headlessSvc,
			NodeName:                      podNodeName,
			NodeSelector:                  node.Spec.NodeSelector,
			Affinity:                      buildPodAntiAffinity(node),
			Tolerations:                   node.Spec.Tolerations,
			TerminationGracePeriodSeconds: &graceSeconds,
			Volumes:                       volumes,
			Containers: []corev1.Container{
				{
					Name:       "dolphindb",
					Image:      node.Spec.Image,
					WorkingDir: "/data/ddb/server",
					Command: []string{
						"/bin/sh",
						"-c",
						`cp -f /data/ddb/config-source/* /data/ddb/server/config/ 2>/dev/null || true; [ -f /data/ddb/license/dolphindb.lic ] && ln -sf /data/ddb/license/dolphindb.lic /data/ddb/server/dolphindb.lic 2>/dev/null || true; exec /data/ddb/server/dolphindb "$@"`,
						"--",
					},
					Args:           args,
					Resources:      node.Spec.Resources,
					VolumeMounts:   ddbMounts,
					Lifecycle:      lifecycle,
					StartupProbe:   startupProbe,
					ReadinessProbe: readinessProbe,
					Ports: []corev1.ContainerPort{
						{Name: "ddb-port", ContainerPort: node.Spec.Port},
					},
				},
			},
		},
	}

	return pod
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *DDBNodeReconciler) updateStatus(ctx context.Context, node *v1alpha1.DDBNode, pod *corev1.Pod, pvcs []corev1.PersistentVolumeClaim) error {
	node.Status.PodName = pod.Name
	node.Status.PodIP = pod.Status.PodIP
	node.Status.HostIP = pod.Status.HostIP
	node.Status.NodeName = pod.Spec.NodeName

	switch pod.Status.Phase {
	case corev1.PodRunning:
		if isPodReady(pod) {
			node.Status.Phase = v1alpha1.NodePhaseRunning
			node.Status.DDBState = 1
		} else {
			node.Status.Phase = v1alpha1.NodePhaseCreating
			node.Status.DDBState = 0
		}
	case corev1.PodPending:
		node.Status.Phase = v1alpha1.NodePhaseCreating
		node.Status.DDBState = 0
	case corev1.PodFailed:
		node.Status.Phase = v1alpha1.NodePhaseFailed
		node.Status.DDBState = 0
	default:
		node.Status.Phase = v1alpha1.NodePhasePending
		node.Status.DDBState = 0
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

	// Update Prometheus metrics
	clusterName := node.Spec.ClusterName
	if clusterName == "" {
		clusterName = "standalone"
	}
	metrics.NodeState.WithLabelValues(clusterName, node.Namespace, node.Name, string(node.Spec.Role)).Set(float64(node.Status.DDBState))

	return r.Status().Update(ctx, node)
}

func (r *DDBNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DDBNode{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}

func buildPodAntiAffinity(node *v1alpha1.DDBNode) *corev1.Affinity {
	mode := node.Spec.AntiAffinity
	if mode == "" {
		if node.Spec.Role != v1alpha1.NodeRoleSingle {
			mode = v1alpha1.AntiAffinityPreferred
		} else {
			return nil
		}
	}
	if mode == v1alpha1.AntiAffinityDisabled {
		return nil
	}

	labelSelector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "app.kubernetes.io/instance",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{node.Spec.ClusterName},
			},
			{
				Key:      "app.kubernetes.io/component",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{string(node.Spec.Role)},
			},
		},
	}

	if mode == v1alpha1.AntiAffinityZoneRequired {
		return &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: labelSelector,
						TopologyKey:   "topology.kubernetes.io/zone",
					},
					{
						LabelSelector: labelSelector,
						TopologyKey:   "kubernetes.io/hostname",
					},
				},
			},
		}
	}

	if mode == v1alpha1.AntiAffinityZonePreferred {
		return &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{
						Weight: 100,
						PodAffinityTerm: corev1.PodAffinityTerm{
							LabelSelector: labelSelector,
							TopologyKey:   "topology.kubernetes.io/zone",
						},
					},
					{
						Weight: 50,
						PodAffinityTerm: corev1.PodAffinityTerm{
							LabelSelector: labelSelector,
							TopologyKey:   "kubernetes.io/hostname",
						},
					},
				},
			},
		}
	}

	if mode == v1alpha1.AntiAffinityRequired {
		return &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: labelSelector,
						TopologyKey:   "kubernetes.io/hostname",
					},
				},
			},
		}
	}

	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: labelSelector,
						TopologyKey:   "kubernetes.io/hostname",
					},
				},
			},
		},
	}
}

func getLegacyComponentType(role v1alpha1.NodeRole) string {
	switch role {
	case v1alpha1.NodeRoleController:
		return "Controllernode"
	case v1alpha1.NodeRoleComputeNode:
		return "Computenode"
	case v1alpha1.NodeRoleSingle:
		return "Standalone"
	default:
		return "Datanode"
	}
}

func (r *DDBNodeReconciler) buildContainerArgs(node *v1alpha1.DDBNode) []string {
	homeDir := "/data/ddb/server/data"
	if node.Spec.Role == v1alpha1.NodeRoleDataNode || node.Spec.Role == v1alpha1.NodeRoleComputeNode {
		homeDir = fmt.Sprintf("/data/ddb/server/clusterDemo/data/%s", node.Spec.Alias)
	}

	stdoutLog := int32(2)
	maxLogSize := int32(200)
	logFile := fmt.Sprintf("/data/ddb/server/log/%s.log", node.Spec.Alias)
	logLevel := "INFO"

	if node.Spec.Logging != nil {
		if node.Spec.Logging.StdoutLog != nil {
			stdoutLog = *node.Spec.Logging.StdoutLog
		}
		if node.Spec.Logging.LogFile != "" {
			logFile = node.Spec.Logging.LogFile
		}
		if node.Spec.Logging.MaxLogSize > 0 {
			maxLogSize = node.Spec.Logging.MaxLogSize
		}
		if node.Spec.Logging.LogLevel != "" {
			logLevel = node.Spec.Logging.LogLevel
		}
	}

	args := []string{
		"-home", homeDir,
		"-console", "0",
		"-stdoutLog", strconv.Itoa(int(stdoutLog)),
		"-mode", string(node.Spec.Role),
		"-lanCluster", "0",
	}

	if stdoutLog != 1 {
		args = append(args, "-logFile", logFile, "-maxLogSize", strconv.Itoa(int(maxLogSize)))
	}
	if logLevel != "" {
		args = append(args, "-logLevel", logLevel)
	}

	if (node.Spec.Fingerprint != nil && node.Spec.Fingerprint.AutoPassthrough) || node.Spec.LicenseSecretName != "" {
		args = append(args, "-license", "/data/ddb/server/dolphindb.lic")
	}
	domain := fmt.Sprintf("%s.%s.svc.cluster.local", node.Name, node.Namespace)
	args = append(args, "-localSite", fmt.Sprintf("%s:%d:%s", domain, node.Spec.Port, node.Spec.Alias))

	if node.Spec.Role != v1alpha1.NodeRoleSingle {
		if node.Spec.Role != v1alpha1.NodeRoleController {
			ctlSite := node.Spec.ControllerSite
			if ctlSite == "" {
				ctlDomain := fmt.Sprintf("%s-ctl-0.%s.svc.cluster.local", node.Spec.ClusterName, node.Namespace)
				ctlSite = fmt.Sprintf("%s:8900:%s-ctl-0", ctlDomain, node.Spec.ClusterName)
			}
			args = append(args, "-controllerSite", ctlSite)
			if node.Spec.Sites != "" {
				args = append(args, "-sites", node.Spec.Sites)
			}
		}
		args = append(args, "-config", fmt.Sprintf("/data/ddb/server/config/%s.cfg", node.Spec.Role))
		args = append(args, "-clusterConfig", "/data/ddb/server/config/cluster.cfg")
		if node.Spec.Role == v1alpha1.NodeRoleController {
			args = append(args, "-nodesFile", "/data/ddb/server/config/cluster.nodes")
		}
	} else {
		args = append(args, "-config", "/data/ddb/server/config/dolphindb.cfg")
	}

	return args
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
