package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
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
	"dolphindb.io/operator/internal/credentials"
	"dolphindb.io/operator/internal/ddbclient"
	"dolphindb.io/operator/internal/exposure"
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

	// 0.35 Align the admin password BEFORE any step that needs to log in.
	// reconcileWebUIConfigSync (0.4) authenticates with the declared Secret; if
	// the controller still has the factory default, that login fails and used to
	// abort the whole reconcile — so the rotation inside syncAclPublicKey (5.x)
	// never ran and the cluster was stuck forever. This call is best-effort: when
	// the controller pod does not exist yet (brand-new cluster) it is a no-op and
	// the rotation is retried once the controller is up.
	r.ensureAdminPasswordForCluster(ctx, cluster)

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
	handled, res, err := r.reconcileControllerScaleDown(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if handled {
		return res, nil
	}
	handled, res, err = r.reconcileScaleDown(ctx, cluster)
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
	// Exposure is resolved per component: an exposed component needs its own Service
	// and therefore its own cloud load balancer.
	plan := resolveExposure(cluster, r.provider(ctx))
	svcType := plan.Controller

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
		if err := r.reconcileSingleAccessService(ctx, cluster, fmt.Sprintf("%s-dn", cluster.Name), plan.DataNode, dnSelector, dnPorts); err != nil {
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
		if err := r.reconcileSingleAccessService(ctx, cluster, fmt.Sprintf("%s-cn", cluster.Name), plan.ComputeNode, cnSelector, cnPorts); err != nil {
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

		if svcType == corev1.ServiceTypeLoadBalancer && svc.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyTypeLocal {
			svc.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyTypeLocal
			needsUpdate = true
		}

		if !mapsEqual(svc.Spec.Selector, selector) {
			svc.Spec.Selector = selector
			needsUpdate = true
		}

		// Sync custom annotations (e.g. Alibaba Cloud SLB / AWS NLB annotations) and
		// the exposure protocol, which changes when native HTTPS is toggled.
		for k, v := range accessServiceAnnotations(cluster, r.provider(ctx)) {
			if svc.Annotations == nil {
				svc.Annotations = make(map[string]string)
			}
			if svc.Annotations[k] != v {
				svc.Annotations[k] = v
				needsUpdate = true
			}
		}
		if !tlsEnabled(cluster) && svc.Annotations[exposure.AnnotationProtocol] != "" {
			delete(svc.Annotations, exposure.AnnotationProtocol)
			needsUpdate = true
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

	svcAnnotations := accessServiceAnnotations(cluster, r.provider(ctx))

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
	if svcType == corev1.ServiceTypeLoadBalancer {
		newSvc.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyTypeLocal
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

func loggingEqual(a, b *v1alpha1.LoggingSpec) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	aStdout := int32(2)
	if a.StdoutLog != nil {
		aStdout = *a.StdoutLog
	}
	bStdout := int32(2)
	if b.StdoutLog != nil {
		bStdout = *b.StdoutLog
	}
	if aStdout != bStdout {
		return false
	}
	if a.LogFile != b.LogFile || a.MaxLogSize != b.MaxLogSize || a.LogLevel != b.LogLevel {
		return false
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

	// 1.5 If the controller count changed (scale up or down), restart node pods
	// once so every node re-reads the controllerSite list rendered from the spec.
	if err := r.restartOnControllerTopologyChange(ctx, cluster); err != nil {
		return err
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

	stdoutLogDefault := int32(2)
	maxLogSizeDefault := int32(200)
	logLevelDefault := "INFO"
	if cluster.Spec.Logging != nil {
		nodeSpec.Logging = cluster.Spec.Logging.DeepCopy()
		if nodeSpec.Logging.StdoutLog == nil {
			nodeSpec.Logging.StdoutLog = &stdoutLogDefault
		}
		if nodeSpec.Logging.MaxLogSize <= 0 {
			nodeSpec.Logging.MaxLogSize = maxLogSizeDefault
		}
		if nodeSpec.Logging.LogLevel == "" {
			nodeSpec.Logging.LogLevel = logLevelDefault
		}
	} else {
		nodeSpec.Logging = &v1alpha1.LoggingSpec{
			StdoutLog:  &stdoutLogDefault,
			MaxLogSize: maxLogSizeDefault,
			LogLevel:   logLevelDefault,
		}
	}

	if cluster.Spec.License.SecretName != "" {
		nodeSpec.LicenseSecretName = cluster.Spec.License.SecretName
	}

	if cluster.Spec.License.MachineFingerprint != nil {
		// Host-bound licensing is strictly opt-in: the fingerprint block carries the
		// operator's licence path and cannot be guessed, so no path is invented here.
		nodeSpec.Fingerprint = cluster.Spec.License.MachineFingerprint.DeepCopy()
		if cluster.Spec.Mode == v1alpha1.ModeStandalone {
			if nodeSpec.NodeName == "" && cluster.Spec.License.MachineFingerprint.NodeName != "" {
				nodeSpec.NodeName = cluster.Spec.License.MachineFingerprint.NodeName
			}
			if nodeSpec.Hostname == "" && cluster.Spec.License.MachineFingerprint.Hostname != "" {
				nodeSpec.Hostname = cluster.Spec.License.MachineFingerprint.Hostname
			}
		}
	}

	// Apply node placement
	if binding != nil && binding.NodeName != "" {
		nodeSpec.NodeName = binding.NodeName
		if binding.Hostname != "" {
			nodeSpec.Hostname = binding.Hostname
		} else if cluster.Spec.Mode == v1alpha1.ModeStandalone {
			nodeSpec.Hostname = SanitizeHostname(binding.NodeName)
		}
		if binding.LicenseSecretName != "" {
			nodeSpec.LicenseSecretName = binding.LicenseSecretName
		}
	} else if nodeSpec.NodeName == "" && defaultNodeName != "" {
		nodeSpec.NodeName = defaultNodeName
		if cluster.Spec.Mode == v1alpha1.ModeStandalone && nodeSpec.Hostname == "" {
			nodeSpec.Hostname = SanitizeHostname(defaultNodeName)
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

	// Native HTTPS: make sure the certificate exists before the pod that consumes
	// it is created, and hand the node everything it needs (secret, advertised
	// domain, and a hash that rolls the node when the certificate is reissued).
	if tlsEnabled(cluster) && roleServesHTTPS(role) {
		publicName := r.resolvePublicName(ctx, cluster)
		tlsSecret, tlsHash, tlsManaged, err := r.ensureTLSSecret(ctx, cluster, tlsHosts(cluster, publicName, r.exposedHostname(ctx, cluster)))
		if err != nil {
			return err
		}
		nodeSpec.TLS = &v1alpha1.NodeTLSSpec{
			SecretName: tlsSecret,
			PublicName: publicName,
			Hash:       tlsHash,
			Managed:    tlsManaged,
		}
	}

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
		} else if existing.Spec.Fingerprint != nil && nodeSpec.Fingerprint == nil {
			// Host-bound licensing is opt-in, so removing it from the CR has to be
			// reflected here too. Without this branch a stale fingerprint persisted
			// forever, which kept hostPath mounts (and their startup failures) alive
			// even after the user switched to a Secret-based licence.
			existing.Spec.Fingerprint = nil
			needsUpdate = true
		}

		// Same for TLS: it must be possible to turn HTTPS off again, and a rotated
		// certificate has to reach the pod template so the node rolls.
		switch {
		case existing.Spec.TLS == nil && nodeSpec.TLS != nil:
			existing.Spec.TLS = nodeSpec.TLS
			needsUpdate = true
		case existing.Spec.TLS != nil && nodeSpec.TLS == nil:
			existing.Spec.TLS = nil
			needsUpdate = true
		case existing.Spec.TLS != nil && nodeSpec.TLS != nil:
			if existing.Spec.TLS.SecretName != nodeSpec.TLS.SecretName ||
				existing.Spec.TLS.PublicName != nodeSpec.TLS.PublicName ||
				existing.Spec.TLS.Hash != nodeSpec.TLS.Hash {
				existing.Spec.TLS = nodeSpec.TLS
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
		if !loggingEqual(existing.Spec.Logging, nodeSpec.Logging) {
			existing.Spec.Logging = nodeSpec.Logging.DeepCopy()
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
						adminUser, adminPass := credentials.Resolve(ctx, r.Client, cluster.Namespace, cluster.Spec.Security.AdminSecretName)
						c := ddbclient.NewClient(fmt.Sprintf("http://%s:%d", ctlPod.Status.PodIP, ctlPort), adminUser, adminPass)
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

// syncAclPublicKey publishes the cluster's shared ACL keypair.
//
// DolphinDB uses this RSA pair for inter-node message encryption and keeps it at
// `<HOME_DIR>/<NodeAlias>/keys` (aclPublic.key + aclPrivate.key). Two behaviours
// matter, both measured on a live cluster:
//
//   - a node that finds only ONE of the two files treats the pair as missing and
//     generates its own, silently replacing whatever it was given. Handing data
//     nodes just the controller's public key therefore produced
//     `Failed to decrypt the given message by RSA private key` and the data nodes
//     never started;
//   - the controller regenerates its pair whenever its home directory is recreated,
//     which invalidates anything that was distributed beforehand.
//
// So the operator owns one pair for the whole cluster and publishes both halves.
// Every node installs it at start-up; nothing depends on which node generated what.
func (r *DolphinDBClusterReconciler) syncAclPublicKey(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	secretName := fmt.Sprintf("%s-acl-keys", cluster.Name)

	existingSec := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: secretName}, existingSec)
	if err == nil && len(existingSec.Data["aclPrivate.key"]) > 0 && len(existingSec.Data["aclPublic.key"]) > 0 {
		return nil // already published
	}
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	keyPEM, pubPEM, genErr := generateACLKeyPair()
	if genErr != nil {
		return genErr
	}

	data := map[string][]byte{"aclPrivate.key": keyPEM, "aclPublic.key": pubPEM}
	if err == nil {
		if existingSec.Data == nil {
			existingSec.Data = map[string][]byte{}
		}
		existingSec.Data["aclPrivate.key"] = keyPEM
		existingSec.Data["aclPublic.key"] = pubPEM
		return r.Update(ctx, existingSec)
	}

	newSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "dolphindb",
				"app.kubernetes.io/instance":  cluster.Name,
				"app.kubernetes.io/component": "acl-keys",
			},
		},
		Data: data,
	}
	if err := ctrl.SetControllerReference(cluster, newSec, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, newSec)
}

// generateACLKeyPair returns an RSA keypair in the PEM encodings DolphinDB writes
// and reads: PKCS#1 ("RSA PRIVATE KEY") and PKIX ("PUBLIC KEY").
func generateACLKeyPair() (keyPEM, pubPEM []byte, err error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	keyDER := x509.MarshalPKCS1PrivateKey(key)
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), nil
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
	adminUser, adminPass := credentials.Resolve(context.Background(), r.Client, cluster.Namespace, cluster.Spec.Security.AdminSecretName)
	return ddbclient.NewClient(baseURL, adminUser, adminPass)
}

// annControllerTopology records the controller replica count the node pods were
// last started with. Pods read the controllerSite list (rendered from spec) only
// at startup, so any change to the controller count — up or down — needs a pod
// restart to take effect; the annotation makes that restart happen exactly once.
const annControllerTopology = "dolphindb.io/controller-topology"

// reconcileControllerScaleDown removes excess controller DDBNodes when
// spec.controller.replicas is reduced. reconcileScaleDown only handles data
// nodes; controllers used to stay forever, so a 3->1 scale-down required
// deleting DDBNodes by hand. Controllers are removed one per pass, highest
// index first, and never below one. Once the excess are gone the surviving
// pods restart once (see restartOnControllerTopologyChange) to re-read the
// shrunken controllerSite list.
func (r *DolphinDBClusterReconciler) reconcileControllerScaleDown(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) (bool, ctrl.Result, error) {
	if cluster.Spec.Mode == v1alpha1.ModeStandalone || cluster.Spec.Controller == nil {
		return false, ctrl.Result{}, nil
	}

	var nodeList v1alpha1.DDBNodeList
	if err := r.List(ctx, &nodeList, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance":  cluster.Name,
		"app.kubernetes.io/component": string(v1alpha1.NodeRoleController),
	}); err != nil {
		return false, ctrl.Result{}, err
	}

	desired := cluster.Spec.Controller.Replicas
	current := int32(len(nodeList.Items))
	if current <= desired {
		return false, ctrl.Result{}, nil
	}
	if desired < 1 {
		log.FromContext(ctx).Error(fmt.Errorf("a cluster needs at least one controller"),
			"Controller scale-down request rejected", "desired", desired)
		return false, ctrl.Result{}, nil
	}

	highest := nodeList.Items[0]
	for _, n := range nodeList.Items[1:] {
		if n.Spec.Index > highest.Spec.Index {
			highest = n
		}
	}

	log.FromContext(ctx).Info("Detected controller scale-down, removing highest-index controller",
		"candidate", highest.Name, "current", current, "desired", desired)

	if err := r.Delete(ctx, &highest); err != nil && !errors.IsNotFound(err) {
		return true, ctrl.Result{}, err
	}
	// One controller per pass: the next reconcile re-renders the config for the
	// survivors, and when current == desired the topology restart finishes the job.
	return true, ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// restartOnControllerTopologyChange restarts node pods once after the cluster
// topology (controller or datanode count) changed:
//
//   - controller count changed: every node restarts, because all nodes carry the
//     controllerSite list and only read it at startup. Without this, a removed
//     controller stays in the datanodes' config (they heartbeat into the void)
//     and an added controller never joins Raft.
//   - only the datanode count changed: the controllers restart, because they read
//     cluster.nodes at startup only — a newly added datanode stays "Unknown chunk
//     node" forever otherwise (observed live: writes failed with "found 0"
//     datanodes until the controller was bounced).
func (r *DolphinDBClusterReconciler) restartOnControllerTopologyChange(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) error {
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		return nil
	}
	ctlReplicas := int32(0)
	dnReplicas := int32(0)
	if cluster.Spec.Controller != nil {
		ctlReplicas = cluster.Spec.Controller.Replicas
	} else {
		return nil
	}
	if cluster.Spec.DataNode != nil {
		dnReplicas = cluster.Spec.DataNode.Replicas
	}
	desired := fmt.Sprintf("ctl:%d,dn:%d", ctlReplicas, dnReplicas)
	current := cluster.Annotations[annControllerTopology]
	if current == "" {
		// First sighting of this cluster: record without restarting, so adopting
		// an existing cluster does not bounce every pod.
		return r.setControllerTopology(ctx, cluster, desired)
	}
	if current == desired {
		return nil
	}

	ctlChanged := strings.Split(current, ",")[0] != strings.Split(desired, ",")[0]

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		"app.kubernetes.io/name":     "dolphindb",
		"app.kubernetes.io/instance": cluster.Name,
	}); err != nil {
		return err
	}
	for i := range pods.Items {
		if !ctlChanged && pods.Items[i].Labels["app.kubernetes.io/component"] != string(v1alpha1.NodeRoleController) {
			continue // datanode-only change: only the controllers must re-read cluster.nodes
		}
		if err := r.Delete(ctx, &pods.Items[i]); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	log.FromContext(ctx).Info("Cluster topology changed, restarted node pods",
		"from", current, "to", desired, "controllersOnly", !ctlChanged)
	return r.setControllerTopology(ctx, cluster, desired)
}

func (r *DolphinDBClusterReconciler) setControllerTopology(ctx context.Context, cluster *v1alpha1.DolphinDBCluster, topology string) error {
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[annControllerTopology] = topology
	return r.Update(ctx, cluster)
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
