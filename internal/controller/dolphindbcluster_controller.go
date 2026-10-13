package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/config"
)

// DolphinDBClusterReconciler reconciles a DolphinDBCluster object
type DolphinDBClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbnodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *DolphinDBClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling DolphinDBCluster", "name", cluster.Name, "mode", cluster.Spec.Mode)

	// 1. Ensure Headless Service for stable DNS
	if err := r.ensureHeadlessService(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Ensure Access Service (ClusterIP / NodePort)
	if err := r.ensureAccessService(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Render configurations & create ConfigMap
	if err := r.ensureConfigMap(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Reconcile child DDBNode instances
	if err := r.reconcileNodes(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Update Cluster Status & Conditions
	return ctrl.Result{}, r.updateClusterStatus(ctx, cluster)
}

func (r *DolphinDBClusterReconciler) ensureHeadlessService(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	svcName := fmt.Sprintf("%s-headless", cluster.Name)
	svc := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: svcName}, svc)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	var ports []corev1.ServicePort
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		port := int32(8848)
		if cluster.Spec.Standalone != nil && cluster.Spec.Standalone.Port != 0 {
			port = cluster.Spec.Standalone.Port
		}
		ports = append(ports, corev1.ServicePort{Name: "ddb-port", Port: port, TargetPort: intstr.FromInt(int(port))})
	} else {
		ports = append(ports,
			corev1.ServicePort{Name: "ctl-port", Port: 8900, TargetPort: intstr.FromInt(8900)},
			corev1.ServicePort{Name: "dn-port", Port: 8901, TargetPort: intstr.FromInt(8901)},
			corev1.ServicePort{Name: "cn-port", Port: 8902, TargetPort: intstr.FromInt(8902)},
		)
	}

	newSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "dolphindb",
				"app.kubernetes.io/instance": cluster.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector: map[string]string{
				"app.kubernetes.io/instance": cluster.Name,
			},
			Ports: ports,
		},
	}

	if err := ctrl.SetControllerReference(cluster, newSvc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, newSvc)
}

func (r *DolphinDBClusterReconciler) ensureAccessService(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	svcName := cluster.Name
	svc := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: svcName}, svc)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	svcType := corev1.ServiceTypeClusterIP
	if cluster.Spec.Networking.ServiceType != "" {
		svcType = cluster.Spec.Networking.ServiceType
	}

	var ports []corev1.ServicePort
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		port := int32(8848)
		if cluster.Spec.Standalone != nil && cluster.Spec.Standalone.Port != 0 {
			port = cluster.Spec.Standalone.Port
		}
		ports = append(ports, corev1.ServicePort{Name: "ddb-port", Port: port, TargetPort: intstr.FromInt(int(port))})
	} else {
		ports = append(ports,
			corev1.ServicePort{Name: "ctl-port", Port: 8900, TargetPort: intstr.FromInt(8900)},
			corev1.ServicePort{Name: "dn-port", Port: 8901, TargetPort: intstr.FromInt(8901)},
		)
	}

	newSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "dolphindb",
				"app.kubernetes.io/instance": cluster.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Type: svcType,
			Selector: map[string]string{
				"app.kubernetes.io/instance": cluster.Name,
			},
			Ports: ports,
		},
	}

	if err := ctrl.SetControllerReference(cluster, newSvc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, newSvc)
}

func (r *DolphinDBClusterReconciler) ensureConfigMap(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	cmName := fmt.Sprintf("%s-config", cluster.Name)
	renderer := config.NewConfigRenderer(cluster)
	rendered, err := renderer.RenderAll()
	if err != nil {
		return err
	}

	data := make(map[string]string)
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		data["dolphindb.cfg"] = rendered.StandaloneCfg
	} else {
		data["controller.cfg"] = rendered.ControllerCfg
		data["controllers.cfg"] = rendered.ControllersCfg
		data["cluster.cfg"] = rendered.ClusterCfg
		data["cluster.nodes"] = rendered.ClusterNodes
	}

	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: cmName}, cm)
	if errors.IsNotFound(err) {
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/name":     "dolphindb",
					"app.kubernetes.io/instance": cluster.Name,
				},
			},
			Data: data,
		}
		if err := ctrl.SetControllerReference(cluster, newCM, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, newCM)
	} else if err == nil {
		cm.Data = data
		return r.Update(ctx, cm)
	}
	return err
}

func (r *DolphinDBClusterReconciler) reconcileNodes(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	image := cluster.Spec.Image
	if image == "" {
		image = fmt.Sprintf("%s/dolphindb:%s", cluster.Spec.ImageRepository, cluster.Spec.Version)
		if cluster.Spec.ImageRepository == "" || cluster.Spec.ImageRepository == "dolphindb" {
			image = fmt.Sprintf("dolphindb/dolphindb:%s", cluster.Spec.Version)
		}
	}

	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		return r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleSingle, 0, 8848, image, nil)
	}

	// 1. Controller Nodes
	if cluster.Spec.Controller != nil {
		for i := int32(0); i < cluster.Spec.Controller.Replicas; i++ {
			var binding *v1alpha1.NodeBinding
			for _, b := range cluster.Spec.Controller.NodeBindings {
				if b.Index == int(i) {
					binding = &b
					break
				}
			}
			port := cluster.Spec.Controller.Port
			if port == 0 {
				port = 8900
			}
			if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleController, i, port, image, binding); err != nil {
				return err
			}
		}
	}

	// 2. DataNodes
	if cluster.Spec.DataNode != nil {
		for i := int32(0); i < cluster.Spec.DataNode.Replicas; i++ {
			var binding *v1alpha1.NodeBinding
			for _, b := range cluster.Spec.DataNode.NodeBindings {
				if b.Index == int(i) {
					binding = &b
					break
				}
			}
			port := cluster.Spec.DataNode.Port
			if port == 0 {
				port = 8901
			}
			if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleDataNode, i, port, image, binding); err != nil {
				return err
			}
		}
	}

	// 3. ComputeNodes
	if cluster.Spec.ComputeNode != nil {
		for i := int32(0); i < cluster.Spec.ComputeNode.Replicas; i++ {
			port := cluster.Spec.ComputeNode.Port
			if port == 0 {
				port = 8902
			}
			if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleComputeNode, i, port, image, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *DolphinDBClusterReconciler) ensureDDBNode(
	ctx context.Context,
	cluster *v1alpha1.DolphinDBCluster,
	role v1alpha1.NodeRole,
	index int32,
	port int32,
	image string,
	binding *v1alpha1.NodeBinding,
) error {
	name := fmt.Sprintf("%s-%s-%d", cluster.Name, rolePrefix(role), index)
	alias := name

	existing := &v1alpha1.DDBNode{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	// Build new DDBNode
	nodeSpec := v1alpha1.DDBNodeSpec{
		ClusterName: cluster.Name,
		Role:        role,
		Index:       index,
		Alias:       alias,
		Port:        port,
		Version:     cluster.Spec.Version,
		Image:       image,
	}

	if cluster.Spec.License.SecretName != "" {
		nodeSpec.LicenseSecretName = cluster.Spec.License.SecretName
	}

	if cluster.Spec.License.MachineFingerprint != nil {
		nodeSpec.Fingerprint = cluster.Spec.License.MachineFingerprint
		if nodeSpec.NodeName == "" && cluster.Spec.License.MachineFingerprint.NodeName != "" {
			nodeSpec.NodeName = cluster.Spec.License.MachineFingerprint.NodeName
		}
		if nodeSpec.Hostname == "" && cluster.Spec.License.MachineFingerprint.Hostname != "" {
			nodeSpec.Hostname = cluster.Spec.License.MachineFingerprint.Hostname
		}
	}

	if binding != nil {
		nodeSpec.NodeName = binding.NodeName
		nodeSpec.Hostname = binding.Hostname
	}

	// Apply resources & storage from group spec
	switch role {
	case v1alpha1.NodeRoleController:
		if cluster.Spec.Controller != nil {
			nodeSpec.Resources = cluster.Spec.Controller.Resources
			nodeSpec.Storage = cluster.Spec.Controller.Storage
			nodeSpec.AntiAffinity = cluster.Spec.Controller.AntiAffinity
		}
	case v1alpha1.NodeRoleDataNode:
		if cluster.Spec.DataNode != nil {
			nodeSpec.Resources = cluster.Spec.DataNode.Resources
			nodeSpec.Storage = cluster.Spec.DataNode.Storage
			nodeSpec.AntiAffinity = cluster.Spec.DataNode.AntiAffinity
		}
	case v1alpha1.NodeRoleComputeNode:
		if cluster.Spec.ComputeNode != nil {
			nodeSpec.Resources = cluster.Spec.ComputeNode.Resources
			nodeSpec.Storage = cluster.Spec.ComputeNode.Storage
		}
	case v1alpha1.NodeRoleSingle:
		if cluster.Spec.Standalone != nil {
			nodeSpec.Resources = cluster.Spec.Standalone.Resources
			nodeSpec.Storage = cluster.Spec.Standalone.Storage
			if cluster.Spec.Standalone.NodeName != "" {
				nodeSpec.NodeName = cluster.Spec.Standalone.NodeName
			}
			if cluster.Spec.Standalone.Hostname != "" {
				nodeSpec.Hostname = cluster.Spec.Standalone.Hostname
			}
		}
	}

	nodeSpec.NodeSelector = cluster.Spec.Scheduling.NodeSelector
	nodeSpec.Tolerations = cluster.Spec.Scheduling.Tolerations

	newNode := &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "dolphindb",
				"app.kubernetes.io/instance":  cluster.Name,
				"app.kubernetes.io/component": string(role),
				"dolphindb.io/node-alias":     alias,
			},
		},
		Spec: nodeSpec,
	}

	if err := ctrl.SetControllerReference(cluster, newNode, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, newNode)
}

func (r *DolphinDBClusterReconciler) updateClusterStatus(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	var nodeList v1alpha1.DDBNodeList
	if err := r.List(ctx, &nodeList, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance": cluster.Name,
	}); err != nil {
		return err
	}

	var ctlReady, dnReady, cnReady int32
	var masterName string

	for _, n := range nodeList.Items {
		switch n.Spec.Role {
		case v1alpha1.NodeRoleController:
			if n.Status.Phase == v1alpha1.NodePhaseRunning {
				ctlReady++
				if masterName == "" {
					masterName = n.Name
				}
			}
		case v1alpha1.NodeRoleDataNode:
			if n.Status.Phase == v1alpha1.NodePhaseRunning {
				dnReady++
			}
		case v1alpha1.NodeRoleComputeNode:
			if n.Status.Phase == v1alpha1.NodePhaseRunning {
				cnReady++
			}
		case v1alpha1.NodeRoleSingle:
			if n.Status.Phase == v1alpha1.NodePhaseRunning {
				dnReady++
			}
		}
	}

	desiredCtl := int32(0)
	if cluster.Spec.Controller != nil {
		desiredCtl = cluster.Spec.Controller.Replicas
	}
	desiredDn := int32(0)
	if cluster.Spec.DataNode != nil {
		desiredDn = cluster.Spec.DataNode.Replicas
	}
	desiredCn := int32(0)
	if cluster.Spec.ComputeNode != nil {
		desiredCn = cluster.Spec.ComputeNode.Replicas
	}

	cluster.Status.Controller = v1alpha1.TopologyStatus{Desired: desiredCtl, Ready: ctlReady}
	cluster.Status.DataNode = v1alpha1.TopologyStatus{Desired: desiredDn, Ready: dnReady}
	cluster.Status.ComputeNode = v1alpha1.TopologyStatus{Desired: desiredCn, Ready: cnReady}
	cluster.Status.MasterController = masterName

	if (desiredCtl == 0 || ctlReady == desiredCtl) && (desiredDn == 0 || dnReady == desiredDn) && (desiredCn == 0 || cnReady == desiredCn) && (ctlReady+dnReady > 0) {
		cluster.Status.Phase = v1alpha1.PhaseReady
	} else {
		cluster.Status.Phase = v1alpha1.PhaseCreating
	}

	return r.Status().Update(ctx, cluster)
}

func rolePrefix(role v1alpha1.NodeRole) string {
	switch role {
	case v1alpha1.NodeRoleController:
		return "ctl"
	case v1alpha1.NodeRoleDataNode:
		return "dn"
	case v1alpha1.NodeRoleComputeNode:
		return "cn"
	default:
		return "single"
	}
}

func (r *DolphinDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DolphinDBCluster{}).
		Owns(&v1alpha1.DDBNode{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
