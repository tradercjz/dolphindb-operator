package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

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
	"dolphindb.io/operator/internal/ddbclient"
	"dolphindb.io/operator/internal/flowengine"
	"dolphindb.io/operator/internal/metrics"
	"dolphindb.io/operator/internal/paramdict"
)

// DolphinDBClusterReconciler reconciles a DolphinDBCluster object
type DolphinDBClusterReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	FlowEngine *flowengine.FlowEngine
}

// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbnodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

func (r *DolphinDBClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling DolphinDBCluster", "name", cluster.Name, "mode", cluster.Spec.Mode)

	// Initialize FlowEngine if nil
	if r.FlowEngine == nil {
		r.FlowEngine = flowengine.NewFlowEngine()
	}

	// 0.1 Check if an ActiveFlow is currently in progress
	if cluster.Status.ActiveFlow != "" {
		flowName := strings.Split(cluster.Status.ActiveFlow, ":")[0]
		r.ensureFlowRegistered(cluster, flowName)
		res, done := r.FlowEngine.Run(ctx, cluster, flowName)
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		if !done {
			requeue := res.RequeueAfter
			if requeue == 0 {
				requeue = 5 * time.Second
			}
			return ctrl.Result{RequeueAfter: requeue}, nil
		}
	}

	// 0.2 Check for scale-down before general reconciliation
	handled, res, err := r.reconcileScaleDown(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if handled {
		return res, nil
	}

	// 0.3 Check for live dynamic config updates
	if _, err := r.reconcileDynamicConfig(ctx, cluster); err != nil {
		logger.Error(err, "failed to reconcile dynamic config")
	}

	// 0.4 Reverse-sync configuration modifications made from Web UI / Controller to K8s ConfigMap & CR
	if changed, err := r.reconcileWebUIConfigSync(ctx, cluster); err != nil {
		logger.Error(err, "failed to reverse-sync Web UI configuration")
	} else if changed {
		return ctrl.Result{Requeue: true}, nil
	}

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

	// 5. Update Cluster Status & Conditions (schedule periodic recheck every 30s to detect Web UI modifications)
	if err := r.updateClusterStatus(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
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
		ctlPort := int32(8900)
		if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
			ctlPort = cluster.Spec.Controller.Port
		}
		dnPort := int32(8848)
		if cluster.Spec.DataNode != nil && cluster.Spec.DataNode.Port > 0 {
			dnPort = cluster.Spec.DataNode.Port
		}
		ports = append(ports,
			corev1.ServicePort{Name: "ctl-port", Port: ctlPort, TargetPort: intstr.FromInt(int(ctlPort))},
			corev1.ServicePort{Name: "dn-port", Port: dnPort, TargetPort: intstr.FromInt(int(dnPort))},
		)
		if cluster.Spec.ComputeNode != nil && cluster.Spec.ComputeNode.Replicas > 0 {
			cnPort := int32(8902)
			if cluster.Spec.ComputeNode.Port > 0 {
				cnPort = cluster.Spec.ComputeNode.Port
			}
			ports = append(ports, corev1.ServicePort{Name: "cn-port", Port: cnPort, TargetPort: intstr.FromInt(int(cnPort))})
		}
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
	svcType := corev1.ServiceTypeClusterIP
	if cluster.Spec.Networking.ServiceType != "" {
		svcType = cluster.Spec.Networking.ServiceType
	}

	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		port := int32(8848)
		if cluster.Spec.Standalone != nil && cluster.Spec.Standalone.Port != 0 {
			port = cluster.Spec.Standalone.Port
		}
		selector := map[string]string{
			"app.kubernetes.io/instance": cluster.Name,
		}
		ports := []corev1.ServicePort{
			{Name: "ddb-port", Port: port, TargetPort: intstr.FromInt(int(port))},
		}
		return r.reconcileSingleAccessService(ctx, cluster, cluster.Name, svcType, selector, ports)
	}

	// 1. Primary Access Service (<clusterName>): Routes to Controller pods (Web UI and management API)
	ctlPort := int32(8900)
	if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
		ctlPort = cluster.Spec.Controller.Port
	}
	ctlSelector := map[string]string{
		"app.kubernetes.io/instance":  cluster.Name,
		"app.kubernetes.io/component": "controller",
	}
	if cluster.Spec.Controller != nil && cluster.Spec.Controller.Replicas > 1 {
		ctlSelector["dolphindb.io/leader"] = "true"
	}
	ctlPorts := []corev1.ServicePort{
		{Name: "ctl-port", Port: ctlPort, TargetPort: intstr.FromInt(int(ctlPort))},
	}
	if err := r.reconcileSingleAccessService(ctx, cluster, cluster.Name, svcType, ctlSelector, ctlPorts); err != nil {
		return err
	}

	// 2. DataNode Group Service (<clusterName>-dn): Routes to DataNode pods (for external data queries/ingestion)
	if cluster.Spec.DataNode != nil && cluster.Spec.DataNode.Replicas > 0 {
		dnPort := int32(8848)
		if cluster.Spec.DataNode.Port > 0 {
			dnPort = cluster.Spec.DataNode.Port
		}
		dnSelector := map[string]string{
			"app.kubernetes.io/instance":  cluster.Name,
			"app.kubernetes.io/component": "datanode",
		}
		dnPorts := []corev1.ServicePort{
			{Name: "dn-port", Port: dnPort, TargetPort: intstr.FromInt(int(dnPort))},
		}
		if err := r.reconcileSingleAccessService(ctx, cluster, fmt.Sprintf("%s-dn", cluster.Name), svcType, dnSelector, dnPorts); err != nil {
			return err
		}
	}

	// 3. ComputeNode Group Service (<clusterName>-cn): Routes to ComputeNode pods
	if cluster.Spec.ComputeNode != nil && cluster.Spec.ComputeNode.Replicas > 0 {
		cnPort := int32(8902)
		if cluster.Spec.ComputeNode.Port > 0 {
			cnPort = cluster.Spec.ComputeNode.Port
		}
		cnSelector := map[string]string{
			"app.kubernetes.io/instance":  cluster.Name,
			"app.kubernetes.io/component": "computenode",
		}
		cnPorts := []corev1.ServicePort{
			{Name: "cn-port", Port: cnPort, TargetPort: intstr.FromInt(int(cnPort))},
		}
		if err := r.reconcileSingleAccessService(ctx, cluster, fmt.Sprintf("%s-cn", cluster.Name), svcType, cnSelector, cnPorts); err != nil {
			return err
		}
	}

	return nil
}

func (r *DolphinDBClusterReconciler) reconcileSingleAccessService(
	ctx context.Context,
	cluster *v1alpha1.DolphinDBCluster,
	svcName string,
	svcType corev1.ServiceType,
	selector map[string]string,
	desiredPorts []corev1.ServicePort,
) error {
	svc := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: svcName}, svc)
	if err == nil {
		needsUpdate := false

		if svc.Spec.Type != svcType {
			svc.Spec.Type = svcType
			needsUpdate = true
		}

		if !mapsEqual(svc.Spec.Selector, selector) {
			svc.Spec.Selector = selector
			needsUpdate = true
		}

		// Sync custom annotations (e.g. Alibaba Cloud SLB / AWS NLB annotations)
		if len(cluster.Spec.Networking.Annotations) > 0 {
			if svc.Annotations == nil {
				svc.Annotations = make(map[string]string)
			}
			for k, v := range cluster.Spec.Networking.Annotations {
				if svc.Annotations[k] != v {
					svc.Annotations[k] = v
					needsUpdate = true
				}
			}
		}

		portsMismatch := len(svc.Spec.Ports) != len(desiredPorts)
		if !portsMismatch {
			for i, dp := range desiredPorts {
				sp := svc.Spec.Ports[i]
				if sp.Name != dp.Name || sp.Port != dp.Port || sp.TargetPort != dp.TargetPort {
					portsMismatch = true
					break
				}
			}
		}

		if portsMismatch {
			updatedPorts := make([]corev1.ServicePort, len(desiredPorts))
			for i, dp := range desiredPorts {
				updatedPorts[i] = dp
				if svcType == corev1.ServiceTypeNodePort {
					for _, sp := range svc.Spec.Ports {
						if (sp.Name == dp.Name || sp.Port == dp.Port) && sp.NodePort > 0 {
							updatedPorts[i].NodePort = sp.NodePort
							break
						}
					}
				}
			}
			svc.Spec.Ports = updatedPorts
			needsUpdate = true
		}

		if needsUpdate {
			return r.Update(ctx, svc)
		}
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	var svcAnnotations map[string]string
	if len(cluster.Spec.Networking.Annotations) > 0 {
		svcAnnotations = make(map[string]string)
		for k, v := range cluster.Spec.Networking.Annotations {
			svcAnnotations[k] = v
		}
	}

	newSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        svcName,
			Namespace:   cluster.Namespace,
			Annotations: svcAnnotations,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "dolphindb",
				"app.kubernetes.io/instance": cluster.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: selector,
			Ports:    desiredPorts,
		},
	}

	if err := ctrl.SetControllerReference(cluster, newSvc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, newSvc)
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
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
		data["datanode.cfg"] = rendered.DataNodeCfg
		data["computenode.cfg"] = rendered.ComputeNodeCfg
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

func (r *DolphinDBClusterReconciler) getAvailableK8sNodes(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) []string {
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return nil
	}
	var nodeNames []string
	for _, n := range nodeList.Items {
		if n.Spec.Unschedulable {
			continue
		}
		isReady := false
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
				isReady = true
				break
			}
		}
		if !isReady {
			continue
		}
		if cluster.Spec.Scheduling.NodeSelector != nil {
			matches := true
			for k, v := range cluster.Spec.Scheduling.NodeSelector {
				if n.Labels[k] != v {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}
		}
		nodeNames = append(nodeNames, n.Name)
	}
	sort.Strings(nodeNames)
	return nodeNames
}

func (r *DolphinDBClusterReconciler) reconcileNodes(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	image := cluster.Spec.Image
	if image == "" {
		image = fmt.Sprintf("%s/dolphindb:%s", cluster.Spec.ImageRepository, cluster.Spec.Version)
		if cluster.Spec.ImageRepository == "" || cluster.Spec.ImageRepository == "dolphindb" {
			image = fmt.Sprintf("dolphindb/dolphindb:%s", cluster.Spec.Version)
		}
	}

	availableNodes := r.getAvailableK8sNodes(ctx, cluster)

	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		defaultNode := ""
		if len(availableNodes) > 0 {
			defaultNode = availableNodes[0]
			for _, nd := range availableNodes {
				if nd == "aftersale2" {
					defaultNode = "aftersale2"
					break
				}
			}
		}
		return r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleSingle, 0, 8848, image, nil, defaultNode)
	}

	// 1. Controller Nodes: distribute across available physical nodes
	if cluster.Spec.Controller != nil {
		for i := int32(0); i < cluster.Spec.Controller.Replicas; i++ {
			var binding *v1alpha1.NodeBinding
			for _, b := range cluster.Spec.Controller.NodeBindings {
				if b.Index == int(i) {
					binding = &b
					break
				}
			}
			defaultNode := ""
			if len(availableNodes) > 0 {
				defaultNode = availableNodes[int(i)%len(availableNodes)]
			}
			port := cluster.Spec.Controller.Port
			if port == 0 {
				port = 8900
			}
			if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleController, i, port, image, binding, defaultNode); err != nil {
				return err
			}
		}
	}

	// 2. In cluster mode, sync ACL public key from Controller to Secret before launching DataNodes
	if err := r.syncAclPublicKey(ctx, cluster); err != nil {
		log.FromContext(ctx).Info("Waiting for Controller to initialize and export ACL public key", "error", err.Error())
		return fmt.Errorf("waiting for controller acl key: %w", err)
	}

	// 3. DataNodes: distribute across available physical nodes
	if cluster.Spec.DataNode != nil {
		for i := int32(0); i < cluster.Spec.DataNode.Replicas; i++ {
			var binding *v1alpha1.NodeBinding
			for _, b := range cluster.Spec.DataNode.NodeBindings {
				if b.Index == int(i) {
					binding = &b
					break
				}
			}
			defaultNode := ""
			if len(availableNodes) > 0 {
				defaultNode = availableNodes[int(i)%len(availableNodes)]
			}
			port := cluster.Spec.DataNode.Port
			if port == 0 {
				port = 8901
			}
			if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleDataNode, i, port, image, binding, defaultNode); err != nil {
				return err
			}
		}
	}

	// 4. ComputeNodes: distribute across available physical nodes
	if cluster.Spec.ComputeNode != nil {
		for i := int32(0); i < cluster.Spec.ComputeNode.Replicas; i++ {
			defaultNode := ""
			if len(availableNodes) > 0 {
				defaultNode = availableNodes[(int(i)+len(availableNodes)-1)%len(availableNodes)]
			}
			port := cluster.Spec.ComputeNode.Port
			if port == 0 {
				port = 8902
			}
			if err := r.ensureDDBNode(ctx, cluster, v1alpha1.NodeRoleComputeNode, i, port, image, nil, defaultNode); err != nil {
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
	defaultNodeName string,
) error {
	name := fmt.Sprintf("%s-%s-%d", cluster.Name, rolePrefix(role), index)
	alias := name

	renderer := config.NewConfigRenderer(cluster)
	if role == v1alpha1.NodeRoleController {
		alias = renderer.ControllerAlias(index)
	} else if role == v1alpha1.NodeRoleDataNode {
		alias = renderer.DataNodeAlias(index)
	}
	if binding != nil && binding.Alias != "" {
		alias = binding.Alias
	}

	var expectedSites string
	var expectedCtlSite string
	if cluster.Spec.Mode == v1alpha1.ModeCluster {
		expectedSites = renderer.RenderSites()
		ctlPort := int32(8900)
		if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
			ctlPort = cluster.Spec.Controller.Port
		}
		ctlAlias := renderer.ControllerAlias(0)
		ctlDomain := renderer.NodeDomain(fmt.Sprintf("%s-ctl-0", cluster.Name))
		expectedCtlSite = fmt.Sprintf("%s:%d:%s", ctlDomain, ctlPort, ctlAlias)
	}

	// Build desired nodeSpec
	nodeSpec := v1alpha1.DDBNodeSpec{
		ClusterName:    cluster.Name,
		Role:           role,
		Index:          index,
		Alias:          alias,
		Port:           port,
		Version:        cluster.Spec.Version,
		Image:          image,
		ControllerSite: expectedCtlSite,
		Sites:          expectedSites,
	}

	if cluster.Spec.License.SecretName != "" {
		nodeSpec.LicenseSecretName = cluster.Spec.License.SecretName
	}

	if cluster.Spec.License.MachineFingerprint != nil {
		nodeSpec.Fingerprint = cluster.Spec.License.MachineFingerprint.DeepCopy()
		if nodeSpec.Fingerprint.LicensePath == "" {
			nodeSpec.Fingerprint.LicensePath = "/home/jzchen/dolphindb.lic"
		}
		if cluster.Spec.Mode == v1alpha1.ModeStandalone {
			if nodeSpec.NodeName == "" && cluster.Spec.License.MachineFingerprint.NodeName != "" {
				nodeSpec.NodeName = cluster.Spec.License.MachineFingerprint.NodeName
			}
			if nodeSpec.Hostname == "" && cluster.Spec.License.MachineFingerprint.Hostname != "" {
				nodeSpec.Hostname = cluster.Spec.License.MachineFingerprint.Hostname
			}
		}
	} else if cluster.Spec.License.Mode == v1alpha1.LicenseModeMachineFingerprint || cluster.Spec.License.Mode == "" {
		// Auto-default fingerprint if mode is machineFingerprint
		nodeSpec.Fingerprint = &v1alpha1.MachineFingerprintSpec{
			AutoPassthrough: true,
			MountMachineId:  true,
			MountHostEtc:    true,
			UseHostName:     true,
			LicensePath:     "/home/jzchen/dolphindb.lic",
		}
	}

	// Apply node placement
	if binding != nil && binding.NodeName != "" {
		nodeSpec.NodeName = binding.NodeName
		if binding.Hostname != "" {
			nodeSpec.Hostname = binding.Hostname
		} else if cluster.Spec.Mode == v1alpha1.ModeStandalone {
			nodeSpec.Hostname = binding.NodeName
		}
		if binding.LicenseSecretName != "" {
			nodeSpec.LicenseSecretName = binding.LicenseSecretName
		}
	} else if nodeSpec.NodeName == "" && defaultNodeName != "" {
		nodeSpec.NodeName = defaultNodeName
		if cluster.Spec.Mode == v1alpha1.ModeStandalone && nodeSpec.Hostname == "" {
			nodeSpec.Hostname = defaultNodeName
		}
	}

	// Apply resources, storage & anti-affinity from group spec
	switch role {
	case v1alpha1.NodeRoleController:
		if cluster.Spec.Controller != nil {
			nodeSpec.Resources = cluster.Spec.Controller.Resources
			nodeSpec.Storage = cluster.Spec.Controller.Storage
			if binding != nil && binding.ClaimName != "" {
				nodeSpec.Storage.ExistingClaims = []string{binding.ClaimName}
			}
			nodeSpec.AntiAffinity = cluster.Spec.Controller.AntiAffinity
			if nodeSpec.AntiAffinity == "" {
				nodeSpec.AntiAffinity = v1alpha1.AntiAffinityPreferred
			}
		}
	case v1alpha1.NodeRoleDataNode:
		if cluster.Spec.DataNode != nil {
			nodeSpec.Resources = cluster.Spec.DataNode.Resources
			nodeSpec.Storage = cluster.Spec.DataNode.Storage
			if binding != nil && binding.ClaimName != "" {
				nodeSpec.Storage.ExistingClaims = []string{binding.ClaimName}
			}
			nodeSpec.AntiAffinity = cluster.Spec.DataNode.AntiAffinity
			if nodeSpec.AntiAffinity == "" {
				nodeSpec.AntiAffinity = v1alpha1.AntiAffinityPreferred
			}
		}
	case v1alpha1.NodeRoleComputeNode:
		if cluster.Spec.ComputeNode != nil {
			nodeSpec.Resources = cluster.Spec.ComputeNode.Resources
			nodeSpec.Storage = cluster.Spec.ComputeNode.Storage
			nodeSpec.AntiAffinity = v1alpha1.AntiAffinityPreferred
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

	existing := &v1alpha1.DDBNode{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, existing)
	if err == nil {
		needsUpdate := false
		if existing.Spec.Sites != expectedSites {
			existing.Spec.Sites = expectedSites
			needsUpdate = true
		}
		if existing.Spec.ControllerSite != expectedCtlSite {
			existing.Spec.ControllerSite = expectedCtlSite
			needsUpdate = true
		}
		if existing.Spec.Fingerprint == nil && nodeSpec.Fingerprint != nil {
			existing.Spec.Fingerprint = nodeSpec.Fingerprint
			needsUpdate = true
		} else if existing.Spec.Fingerprint != nil && nodeSpec.Fingerprint != nil {
			if existing.Spec.Fingerprint.LicensePath != nodeSpec.Fingerprint.LicensePath {
				existing.Spec.Fingerprint.LicensePath = nodeSpec.Fingerprint.LicensePath
				needsUpdate = true
			}
			if existing.Spec.Fingerprint.UseHostName != nodeSpec.Fingerprint.UseHostName {
				existing.Spec.Fingerprint.UseHostName = nodeSpec.Fingerprint.UseHostName
				needsUpdate = true
			}
		}
		if existing.Spec.NodeName != nodeSpec.NodeName {
			existing.Spec.NodeName = nodeSpec.NodeName
			needsUpdate = true
		}
		if existing.Spec.Hostname != nodeSpec.Hostname {
			existing.Spec.Hostname = nodeSpec.Hostname
			needsUpdate = true
		}
		if existing.Spec.AntiAffinity != nodeSpec.AntiAffinity {
			existing.Spec.AntiAffinity = nodeSpec.AntiAffinity
			needsUpdate = true
		}
		if existing.Spec.Alias != alias {
			existing.Spec.Alias = alias
			if existing.Labels == nil {
				existing.Labels = make(map[string]string)
			}
			existing.Labels["dolphindb.io/node-alias"] = alias
			needsUpdate = true
		}
		if existing.Spec.Port != port {
			existing.Spec.Port = port
			needsUpdate = true
		}
		claimsChanged := len(existing.Spec.Storage.ExistingClaims) != len(nodeSpec.Storage.ExistingClaims)
		if !claimsChanged {
			for idx := range nodeSpec.Storage.ExistingClaims {
				if existing.Spec.Storage.ExistingClaims[idx] != nodeSpec.Storage.ExistingClaims[idx] {
					claimsChanged = true
					break
				}
			}
		}
		if claimsChanged {
			existing.Spec.Storage.ExistingClaims = nodeSpec.Storage.ExistingClaims
			needsUpdate = true
		}
		if existing.Spec.LicenseSecretName == "" && nodeSpec.LicenseSecretName != "" {
			existing.Spec.LicenseSecretName = nodeSpec.LicenseSecretName
			needsUpdate = true
		}
		if needsUpdate {
			return r.Update(ctx, existing)
		}
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

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
		isNodeReady := n.Status.Phase == v1alpha1.NodePhaseRunning && n.Status.DDBState == 1
		switch n.Spec.Role {
		case v1alpha1.NodeRoleController:
			if isNodeReady {
				ctlReady++
			}
		case v1alpha1.NodeRoleDataNode:
			if isNodeReady {
				dnReady++
			}
		case v1alpha1.NodeRoleComputeNode:
			if isNodeReady {
				cnReady++
			}
		case v1alpha1.NodeRoleSingle:
			if isNodeReady {
				dnReady++
			}
		}
	}

	// Detect Leader Controller in HA cluster and maintain dolphindb.io/leader label on pods
	if cluster.Spec.Mode == v1alpha1.ModeCluster && cluster.Spec.Controller != nil {
		if cluster.Spec.Controller.Replicas > 1 {
			var leaderAlias string
			for i := int32(0); i < cluster.Spec.Controller.Replicas; i++ {
				ctlPodName := fmt.Sprintf("%s-ctl-%d", cluster.Name, i)
				ctlPod := &corev1.Pod{}
				if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: ctlPodName}, ctlPod); err == nil {
					if ctlPod.Status.Phase == corev1.PodRunning && ctlPod.Status.PodIP != "" {
						ctlPort := int32(8900)
						if cluster.Spec.Controller.Port > 0 {
							ctlPort = cluster.Spec.Controller.Port
						}
						c := ddbclient.NewClient(fmt.Sprintf("http://%s:%d", ctlPod.Status.PodIP, ctlPort), "admin", "123456")
						alias, err := c.GetLeaderControllerAlias(ctx)
						if err == nil && alias != "" {
							leaderAlias = alias
							break
						}
					}
				}
			}

			for _, n := range nodeList.Items {
				if n.Spec.Role == v1alpha1.NodeRoleController {
					isLeader := (leaderAlias != "" && n.Spec.Alias == leaderAlias)
					if leaderAlias == "" && n.Spec.Index == 0 {
						isLeader = true
					}
					if isLeader {
						masterName = n.Name
					}
					pod := &corev1.Pod{}
					if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: n.Name}, pod); err == nil {
						desiredLeaderLabel := "false"
						if isLeader {
							desiredLeaderLabel = "true"
						}
						if pod.Labels == nil {
							pod.Labels = make(map[string]string)
						}
						if pod.Labels["dolphindb.io/leader"] != desiredLeaderLabel {
							pod.Labels["dolphindb.io/leader"] = desiredLeaderLabel
							_ = r.Update(ctx, pod)
						}
					}
				}
			}
		} else {
			ctlPodName := fmt.Sprintf("%s-ctl-0", cluster.Name)
			masterName = ctlPodName
			pod := &corev1.Pod{}
			if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: ctlPodName}, pod); err == nil {
				if pod.Labels == nil {
					pod.Labels = make(map[string]string)
				}
				if pod.Labels["dolphindb.io/leader"] != "true" {
					pod.Labels["dolphindb.io/leader"] = "true"
					_ = r.Update(ctx, pod)
				}
			}
		}
	} else if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		masterName = fmt.Sprintf("%s-single-0", cluster.Name)
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

	if cluster.Status.ActiveFlow != "" {
		if strings.HasPrefix(cluster.Status.ActiveFlow, "ScaleDown") {
			cluster.Status.Phase = v1alpha1.PhaseScaling
		} else if strings.HasPrefix(cluster.Status.ActiveFlow, "RollingRestart") {
			cluster.Status.Phase = v1alpha1.PhaseRolling
		} else {
			cluster.Status.Phase = v1alpha1.PhaseScaling
		}
	} else if (desiredCtl == 0 || ctlReady == desiredCtl) && (desiredDn == 0 || dnReady == desiredDn) && (desiredCn == 0 || cnReady == desiredCn) && (ctlReady+dnReady > 0) {
		cluster.Status.Phase = v1alpha1.PhaseReady
	} else {
		cluster.Status.Phase = v1alpha1.PhaseCreating
	}

	// Update Prometheus metrics
	metrics.ClusterNodesDesired.WithLabelValues(cluster.Name, cluster.Namespace, "controller").Set(float64(desiredCtl))
	metrics.ClusterNodesDesired.WithLabelValues(cluster.Name, cluster.Namespace, "datanode").Set(float64(desiredDn))
	metrics.ClusterNodesDesired.WithLabelValues(cluster.Name, cluster.Namespace, "computenode").Set(float64(desiredCn))
	metrics.ClusterNodesReady.WithLabelValues(cluster.Name, cluster.Namespace, "controller").Set(float64(ctlReady))
	metrics.ClusterNodesReady.WithLabelValues(cluster.Name, cluster.Namespace, "datanode").Set(float64(dnReady))
	metrics.ClusterNodesReady.WithLabelValues(cluster.Name, cluster.Namespace, "computenode").Set(float64(cnReady))

	healthVal := 0.0
	if cluster.Status.Phase == v1alpha1.PhaseReady {
		healthVal = 1.0
	}
	metrics.ClusterHealthStatus.WithLabelValues(cluster.Name, cluster.Namespace, string(cluster.Status.Phase)).Set(healthVal)

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

func (r *DolphinDBClusterReconciler) syncAclPublicKey(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	ctlPodName := fmt.Sprintf("%s-ctl-0", cluster.Name)
	ctlPod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: ctlPodName}, ctlPod); err != nil {
		return err
	}
	if ctlPod.Status.Phase != corev1.PodRunning || ctlPod.Status.PodIP == "" {
		return fmt.Errorf("controller pod not running yet")
	}

	ctlPort := int32(8900)
	if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
		ctlPort = cluster.Spec.Controller.Port
	}

	url := fmt.Sprintf("http://%s:%d", ctlPod.Status.PodIP, ctlPort)
	payload := map[string]interface{}{
		"functionName": "executeCode",
		"params": []map[string]string{
			{
				"name":  "script",
				"form":  "scalar",
				"type":  "string",
				"value": `login("admin", "123456"); concat(funcByName("getAclPublicKey")())`,
			},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	clientHttp := &http.Client{
		Transport: &http.Transport{
			Proxy:             nil,
			DisableKeepAlives: true,
		},
		Timeout: 5 * time.Second,
	}
	resp, err := clientHttp.Post(url, "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var ddbResp struct {
		ResultCode string `json:"resultCode"`
		Msg        string `json:"msg"`
		Object     []struct {
			Value string `json:"value"`
		} `json:"object"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ddbResp); err != nil {
		return err
	}
	if ddbResp.ResultCode != "0" || len(ddbResp.Object) == 0 || ddbResp.Object[0].Value == "" {
		return fmt.Errorf("failed to get acl public key: %s", ddbResp.Msg)
	}

	keyData := []byte(ddbResp.Object[0].Value)
	secretName := fmt.Sprintf("%s-acl-keys", cluster.Name)
	existingSec := &corev1.Secret{}
	err = r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: secretName}, existingSec)
	if err == nil {
		if bytes.Equal(existingSec.Data["aclPublic.key"], keyData) {
			return nil
		}
		if existingSec.Data == nil {
			existingSec.Data = make(map[string][]byte)
		}
		existingSec.Data["aclPublic.key"] = keyData
		return r.Update(ctx, existingSec)
	}
	if !errors.IsNotFound(err) {
		return err
	}

	newSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":     "dolphindb",
				"app.kubernetes.io/instance": cluster.Name,
			},
		},
		Data: map[string][]byte{
			"aclPublic.key": keyData,
		},
	}
	if err := ctrl.SetControllerReference(cluster, newSec, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, newSec)
}

func (r *DolphinDBClusterReconciler) getDDBClient(cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client {
	ctlPodName := fmt.Sprintf("%s-ctl-0", cluster.Name)
	if cluster.Status.MasterController != "" {
		ctlPodName = cluster.Status.MasterController
	}
	ctlPod := &corev1.Pod{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: cluster.Namespace, Name: ctlPodName}, ctlPod); err != nil {
		return nil
	}
	if ctlPod.Status.Phase != corev1.PodRunning || ctlPod.Status.PodIP == "" {
		return nil
	}
	ctlPort := int32(8900)
	if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
		ctlPort = cluster.Spec.Controller.Port
	}
	baseURL := fmt.Sprintf("http://%s:%d", ctlPod.Status.PodIP, ctlPort)
	return ddbclient.NewClient(baseURL, "admin", "123456")
}

func (r *DolphinDBClusterReconciler) reconcileScaleDown(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) (bool, ctrl.Result, error) {
	if cluster.Spec.Mode == v1alpha1.ModeStandalone || cluster.Spec.DataNode == nil {
		return false, ctrl.Result{}, nil
	}

	// List current DataNode DDBNodes
	var nodeList v1alpha1.DDBNodeList
	if err := r.List(ctx, &nodeList, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance":  cluster.Name,
		"app.kubernetes.io/component": string(v1alpha1.NodeRoleDataNode),
	}); err != nil {
		return false, ctrl.Result{}, err
	}

	desired := cluster.Spec.DataNode.Replicas
	current := int32(len(nodeList.Items))
	if current <= desired {
		return false, ctrl.Result{}, nil
	}

	// Safety check: ensure target replicas does not violate dfsReplicationFactor
	repFactor := int32(1)
	if cluster.Spec.Controller != nil {
		if rfStr, ok := cluster.Spec.Controller.Config["dfsReplicationFactor"]; ok {
			if rf, err := strconv.Atoi(rfStr); err == nil && rf > 0 {
				repFactor = int32(rf)
			}
		} else if cluster.Spec.Controller.Replicas >= 3 {
			repFactor = int32(2)
		}
	}

	if desired < repFactor {
		log.FromContext(ctx).Error(
			fmt.Errorf("cannot scale down datanodes to %d: must be >= dfsReplicationFactor (%d)", desired, repFactor),
			"Scale-down request rejected to prevent chunk replica allocation failure",
			"desired", desired, "dfsReplicationFactor", repFactor,
		)
		return false, ctrl.Result{}, nil
	}

	// Sort nodes by index descending to pick highest index for removal
	sort.Slice(nodeList.Items, func(i, j int) bool {
		return nodeList.Items[i].Spec.Index > nodeList.Items[j].Spec.Index
	})

	candidate := nodeList.Items[0]
	candidateName := candidate.Name
	candidateAlias := candidate.Spec.Alias
	survivorAlias := fmt.Sprintf("%s-dn-0", cluster.Name)

	log.FromContext(ctx).Info("Detected DataNode scale-down, initiating lossless DAG scale-down flow",
		"candidate", candidateName, "survivor", survivorAlias)

	flow := flowengine.BuildScaleDownFlow(r.Client, r.getDDBClient, candidateName, candidateAlias, survivorAlias)
	if r.FlowEngine == nil {
		r.FlowEngine = flowengine.NewFlowEngine()
	}
	r.FlowEngine.RegisterFlow(flow)

	res, done := r.FlowEngine.Run(ctx, cluster, flow.Name)
	if err := r.Status().Update(ctx, cluster); err != nil {
		return true, ctrl.Result{}, err
	}

	if !done {
		requeue := res.RequeueAfter
		if requeue == 0 {
			requeue = 5 * time.Second
		}
		return true, ctrl.Result{RequeueAfter: requeue}, nil
	}

	return true, ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

func (r *DolphinDBClusterReconciler) reconcileDynamicConfig(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) (bool, error) {
	if cluster.Spec.DataNode == nil || len(cluster.Spec.DataNode.Config) == 0 {
		return false, nil
	}

	lastAppliedKey := "dolphindb.io/last-applied-datanode-config"
	var lastApplied map[string]string
	if cluster.Annotations != nil && cluster.Annotations[lastAppliedKey] != "" {
		_ = json.Unmarshal([]byte(cluster.Annotations[lastAppliedKey]), &lastApplied)
	}
	if lastApplied == nil {
		lastApplied = make(map[string]string)
	}

	diff := paramdict.ClassifyDiff(lastApplied, cluster.Spec.DataNode.Config)
	if !diff.HasChanges {
		return false, nil
	}

	if diff.IsOnlyDynamic() {
		log.FromContext(ctx).Info("Detected only dynamic config changes, applying live via setDynamicConfig", "diff", diff.DynamicDiff)
		cli := r.getDDBClient(cluster)
		if cli != nil {
			for k, v := range diff.DynamicDiff {
				script := fmt.Sprintf("pnodeRun(setDynamicConfig{%q, %s});", k, formatDDBValue(v))
				_, _ = cli.ExecuteScript(ctx, script)
			}
		}

		// Update annotation to remember applied config
		newBytes, _ := json.Marshal(cluster.Spec.DataNode.Config)
		if cluster.Annotations == nil {
			cluster.Annotations = make(map[string]string)
		}
		cluster.Annotations[lastAppliedKey] = string(newBytes)
		return true, r.Update(ctx, cluster)
	}

	// Contains static changes: update annotation so we don't re-trigger
	newBytes, _ := json.Marshal(cluster.Spec.DataNode.Config)
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[lastAppliedKey] = string(newBytes)
	_ = r.Update(ctx, cluster)
	return false, nil
}

func (r *DolphinDBClusterReconciler) reconcileWebUIConfigSync(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) (bool, error) {
	if cluster.Spec.Mode == v1alpha1.ModeStandalone || cluster.Status.Phase != v1alpha1.PhaseReady {
		return false, nil
	}

	cli := r.getDDBClient(cluster)
	if cli == nil {
		log.FromContext(ctx).Info("reconcileWebUIConfigSync: ddbclient is nil", "cluster", cluster.Name)
		return false, nil
	}

	liveConfigs, err := cli.GetClusterNodesConfigs(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "reconcileWebUIConfigSync: GetClusterNodesConfigs failed", "cluster", cluster.Name)
		return false, nil
	}
	log.FromContext(ctx).Info("reconcileWebUIConfigSync: live configs fetched", "count", len(liveConfigs))

	if cluster.Spec.DataNode == nil {
		return false, nil
	}
	if cluster.Spec.DataNode.Config == nil {
		cluster.Spec.DataNode.Config = make(map[string]string)
	}

	updated := false
	for k, v := range liveConfigs {
		if isSystemManagedKey(k) {
			continue
		}
		curVal, exists := cluster.Spec.DataNode.Config[k]
		if !exists || curVal != v {
			log.FromContext(ctx).Info("Detected Web UI cluster configuration change",
				"key", k, "old", curVal, "new", v)
			cluster.Spec.DataNode.Config[k] = v
			updated = true
		}
	}

	if !updated {
		return false, nil
	}

	// Update ConfigMap so new config is immediately written to ddb-config-source
	if err := r.ensureConfigMap(ctx, cluster); err != nil {
		return false, err
	}

	// Update annotation to remember applied config
	newBytes, _ := json.Marshal(cluster.Spec.DataNode.Config)
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations["dolphindb.io/last-applied-datanode-config"] = string(newBytes)

	log.FromContext(ctx).Info("Persisted Web UI configuration changes to K8s CR and ConfigMap",
		"cluster", cluster.Name)
	return true, r.Update(ctx, cluster)
}

func isSystemManagedKey(k string) bool {
	switch k {
	case "sites", "lanCluster", "dataSync", "mode", "clusterName",
		"streamingHAMode", "strictPermissionInspection",
		"newValuePartitionPolicy", "enableChunkGranularityConfig":
		return true
	}
	if strings.Contains(k, ".volumes") || strings.Contains(k, ".redoLogDir") || strings.Contains(k, ".chunkMetaDir") {
		return true
	}
	return false
}

func (r *DolphinDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DolphinDBCluster{}).
		Owns(&v1alpha1.DDBNode{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}

func formatDDBValue(v string) string {
	v = strings.TrimSpace(v)
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return v
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return v
	}
	if strings.EqualFold(v, "true") || strings.EqualFold(v, "false") {
		return strings.ToLower(v)
	}
	return fmt.Sprintf("%q", v)
}

func (r *DolphinDBClusterReconciler) ensureFlowRegistered(cluster *v1alpha1.DolphinDBCluster, flowName string) {
	if r.FlowEngine == nil {
		r.FlowEngine = flowengine.NewFlowEngine()
	}
	if _, exists := r.FlowEngine.Flows[flowName]; exists {
		return
	}

	if strings.HasPrefix(flowName, "ScaleDown-") {
		candidateAlias := strings.TrimPrefix(flowName, "ScaleDown-")
		candidateName := candidateAlias
		survivorAlias := fmt.Sprintf("%s-dn-0", cluster.Name)
		flow := flowengine.BuildScaleDownFlow(r.Client, r.getDDBClient, candidateName, candidateAlias, survivorAlias)
		r.FlowEngine.RegisterFlow(flow)
	} else if strings.HasPrefix(flowName, "RollingRestart") {
		var targetNodes []string
		var targetAliases []string
		if cluster.Spec.DataNode != nil {
			for i := int32(0); i < cluster.Spec.DataNode.Replicas; i++ {
				name := fmt.Sprintf("%s-dn-%d", cluster.Name, i)
				targetNodes = append(targetNodes, name)
				targetAliases = append(targetAliases, name)
			}
		}
		flow := flowengine.BuildRollingRestartFlow(r.Client, r.getDDBClient, targetNodes, targetAliases)
		r.FlowEngine.RegisterFlow(flow)
	}
}
