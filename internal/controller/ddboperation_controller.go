package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/credentials"
	"dolphindb.io/operator/internal/ddbclient"
)

// DDBOperationReconciler reconciles a DDBOperation object
type DDBOperationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=dolphindb.io,resources=ddboperations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddboperations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbnodes,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete

func (r *DDBOperationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	op := &v1alpha1.DDBOperation{}
	if err := r.Get(ctx, req.NamespacedName, op); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. If operation has already finalized, terminate reconciliation
	if op.Status.Phase == v1alpha1.OpPhaseSucceeded || op.Status.Phase == v1alpha1.OpPhaseFailed {
		return ctrl.Result{}, nil
	}

	// 2. Initialize status
	if op.Status.Phase == "" {
		op.Status.Phase = v1alpha1.OpPhasePending
		now := metav1.Now()
		op.Status.StartTime = &now
		op.Status.Progress = "0%"
		op.Status.CurrentStep = "Initialized"
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, op)
	}

	logger.Info("Reconciling DDBOperation", "name", op.Name, "type", op.Spec.Type, "cluster", op.Spec.Cluster, "phase", op.Status.Phase)

	// 3. Find target cluster
	cluster := &v1alpha1.DolphinDBCluster{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: op.Namespace, Name: op.Spec.Cluster}, cluster); err != nil {
		if errors.IsNotFound(err) {
			op.Status.Phase = v1alpha1.OpPhaseFailed
			op.Status.Message = fmt.Sprintf("Cluster %q not found in namespace %q", op.Spec.Cluster, op.Namespace)
			now := metav1.Now()
			op.Status.CompletionTime = &now
			return ctrl.Result{}, r.Status().Update(ctx, op)
		}
		return ctrl.Result{}, err
	}

	// 4. Dispatch based on operation type
	switch op.Spec.Type {
	case v1alpha1.OpReloadLicense:
		return r.reconcileReloadLicense(ctx, op, cluster)
	case v1alpha1.OpStopNode:
		return r.reconcileStopNode(ctx, op, cluster)
	case v1alpha1.OpStartNode:
		return r.reconcileStartNode(ctx, op, cluster)
	case v1alpha1.OpRebalanceChunks:
		return r.reconcileRebalanceChunks(ctx, op, cluster)
	case v1alpha1.OpRollingRestart:
		return r.reconcileRollingRestart(ctx, op, cluster)
	case v1alpha1.OpDecommissionNode:
		return r.reconcileDecommissionNode(ctx, op, cluster)
	default:
		op.Status.Phase = v1alpha1.OpPhaseFailed
		op.Status.Message = fmt.Sprintf("Unsupported operation type: %s", op.Spec.Type)
		now := metav1.Now()
		op.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}
}

func (r *DDBOperationReconciler) getDDBClient(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client {
	targetPodName := fmt.Sprintf("%s-ctl-0", cluster.Name)
	port := int32(8900)
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		targetPodName = fmt.Sprintf("%s-single-0", cluster.Name)
		port = 8848
		if cluster.Spec.Standalone != nil && cluster.Spec.Standalone.Port > 0 {
			port = cluster.Spec.Standalone.Port
		}
	} else if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
		port = cluster.Spec.Controller.Port
	}

	pod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: targetPodName}, pod); err != nil {
		return nil
	}
	if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		return nil
	}

	baseURL := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port)
	user, pass := credentials.Resolve(ctx, r.Client, cluster.Namespace, cluster.Spec.Security.AdminSecretName)
	return ddbclient.NewClient(baseURL, user, pass)
}

func (r *DDBOperationReconciler) reconcileReloadLicense(ctx context.Context, op *v1alpha1.DDBOperation, cluster *v1alpha1.DolphinDBCluster) (ctrl.Result, error) {
	cli := r.getDDBClient(ctx, cluster)
	if cli == nil {
		op.Status.CurrentStep = "WaitingControllerClient"
		op.Status.Message = "DolphinDB controller endpoint is not yet reachable"
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	lic, err := cli.ReloadLicense(ctx)
	if err != nil {
		op.Status.Phase = v1alpha1.OpPhaseFailed
		op.Status.Message = fmt.Sprintf("Failed to reload license: %v", err)
		now := metav1.Now()
		op.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	now := metav1.Now()
	op.Status.Phase = v1alpha1.OpPhaseSucceeded
	op.Status.Progress = "100%"
	op.Status.CurrentStep = "Completed"
	op.Status.CompletionTime = &now
	op.Status.Message = fmt.Sprintf("License successfully reloaded: expiration=%s, maxCores=%s, maxMemory=%sGB, auth=%s, client=%s",
		lic["expiration"], lic["maxCoresPerNode"], lic["maxMemoryPerNode"], lic["authorization"], lic["clientName"])
	return ctrl.Result{}, r.Status().Update(ctx, op)
}

func (r *DDBOperationReconciler) resolveTargetNode(ctx context.Context, op *v1alpha1.DDBOperation, cluster *v1alpha1.DolphinDBCluster) (*v1alpha1.DDBNode, error) {
	target := ""
	if len(op.Spec.TargetNodes) > 0 {
		target = op.Spec.TargetNodes[0]
	} else if op.Spec.Parameters != nil {
		target = op.Spec.Parameters["node"]
	}
	if target == "" {
		return nil, fmt.Errorf("no target node specified in spec.targetNodes or spec.parameters['node']")
	}

	// 1. Try finding by CR name
	node := &v1alpha1.DDBNode{}
	err := r.Get(ctx, client.ObjectKey{Namespace: op.Namespace, Name: target}, node)
	if err == nil {
		return node, nil
	}

	// 2. Try finding by alias or prefix
	var nodeList v1alpha1.DDBNodeList
	if err := r.List(ctx, &nodeList, client.InNamespace(op.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance": cluster.Name,
	}); err != nil {
		return nil, err
	}

	for _, n := range nodeList.Items {
		if n.Spec.Alias == target || n.Name == target {
			return &n, nil
		}
	}

	return nil, fmt.Errorf("target node %q not found in cluster %s", target, cluster.Name)
}

func (r *DDBOperationReconciler) reconcileStopNode(ctx context.Context, op *v1alpha1.DDBOperation, cluster *v1alpha1.DolphinDBCluster) (ctrl.Result, error) {
	node, err := r.resolveTargetNode(ctx, op, cluster)
	if err != nil {
		op.Status.Phase = v1alpha1.OpPhaseFailed
		op.Status.Message = err.Error()
		now := metav1.Now()
		op.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	// If annotation is not present, add it
	if node.Annotations == nil || node.Annotations["dolphindb.io/stopped"] != "true" {
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}
		node.Annotations["dolphindb.io/stopped"] = "true"
		if err := r.Update(ctx, node); err != nil {
			return ctrl.Result{}, err
		}
		op.Status.Phase = v1alpha1.OpPhaseRunning
		op.Status.CurrentStep = fmt.Sprintf("AnnotatedStop-%s", node.Name)
		op.Status.Progress = "50%"
		op.Status.Message = fmt.Sprintf("Marked node %s as stopped; awaiting pod termination", node.Name)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, r.Status().Update(ctx, op)
	}

	// Verify pod is deleted or offline
	pod := &corev1.Pod{}
	err = r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: node.Name}, pod)
	if errors.IsNotFound(err) || node.Status.Phase == v1alpha1.NodePhaseOffline {
		now := metav1.Now()
		op.Status.Phase = v1alpha1.OpPhaseSucceeded
		op.Status.Progress = "100%"
		op.Status.CurrentStep = "Completed"
		op.Status.CompletionTime = &now
		op.Status.Message = fmt.Sprintf("Node %s stopped successfully (Phase: Offline, Pod deleted)", node.Name)
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	op.Status.CurrentStep = fmt.Sprintf("TerminatingPod-%s", node.Name)
	op.Status.Message = fmt.Sprintf("Pod %s is currently terminating...", node.Name)
	return ctrl.Result{RequeueAfter: 2 * time.Second}, r.Status().Update(ctx, op)
}

func (r *DDBOperationReconciler) reconcileStartNode(ctx context.Context, op *v1alpha1.DDBOperation, cluster *v1alpha1.DolphinDBCluster) (ctrl.Result, error) {
	node, err := r.resolveTargetNode(ctx, op, cluster)
	if err != nil {
		op.Status.Phase = v1alpha1.OpPhaseFailed
		op.Status.Message = err.Error()
		now := metav1.Now()
		op.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	// If annotation is present, remove it to allow pod creation
	if node.Annotations != nil && node.Annotations["dolphindb.io/stopped"] == "true" {
		delete(node.Annotations, "dolphindb.io/stopped")
		if err := r.Update(ctx, node); err != nil {
			return ctrl.Result{}, err
		}
		op.Status.Phase = v1alpha1.OpPhaseRunning
		op.Status.CurrentStep = fmt.Sprintf("TriggeredStart-%s", node.Name)
		op.Status.Progress = "30%"
		op.Status.Message = fmt.Sprintf("Removed stop annotation from %s; awaiting node pod recreation", node.Name)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	// Check if pod is running and ready via probe
	pod := &corev1.Pod{}
	err = r.Get(ctx, client.ObjectKey{Namespace: node.Namespace, Name: node.Name}, pod)
	if err != nil || pod.Status.Phase != corev1.PodRunning || !isPodReady(pod) {
		op.Status.CurrentStep = fmt.Sprintf("WaitingPodRunning-%s", node.Name)
		op.Status.Progress = "60%"
		op.Status.Message = fmt.Sprintf("Waiting for pod %s to enter Running and Ready state (initialization check)", node.Name)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	// In cluster mode, verify node state in cluster perf
	if cluster.Spec.Mode != v1alpha1.ModeStandalone {
		cli := r.getDDBClient(ctx, cluster)
		if cli != nil {
			nodes, err := cli.GetClusterPerf(ctx)
			if err == nil {
				for _, n := range nodes {
					if n.Name == node.Spec.Alias && n.State == 1 {
						now := metav1.Now()
						op.Status.Phase = v1alpha1.OpPhaseSucceeded
						op.Status.Progress = "100%"
						op.Status.CurrentStep = "Completed"
						op.Status.CompletionTime = &now
						op.Status.Message = fmt.Sprintf("Node %s started and verified healthy in cluster (state=1)", node.Name)
						return ctrl.Result{}, r.Status().Update(ctx, op)
					}
				}
			}
		}
		op.Status.CurrentStep = fmt.Sprintf("WaitingClusterJoin-%s", node.Spec.Alias)
		op.Status.Progress = "80%"
		op.Status.Message = fmt.Sprintf("Pod is Running; waiting for node %s to report state=1 to controller", node.Spec.Alias)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	now := metav1.Now()
	op.Status.Phase = v1alpha1.OpPhaseSucceeded
	op.Status.Progress = "100%"
	op.Status.CurrentStep = "Completed"
	op.Status.CompletionTime = &now
	op.Status.Message = fmt.Sprintf("Node %s started and pod is Running", node.Name)
	return ctrl.Result{}, r.Status().Update(ctx, op)
}

func (r *DDBOperationReconciler) reconcileRebalanceChunks(ctx context.Context, op *v1alpha1.DDBOperation, cluster *v1alpha1.DolphinDBCluster) (ctrl.Result, error) {
	cli := r.getDDBClient(ctx, cluster)
	if cli == nil {
		op.Status.CurrentStep = "WaitingControllerClient"
		op.Status.Message = "Controller client is not yet available"
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	recovering, _ := cli.HasActiveRecoveryTasks(ctx)
	if recovering {
		op.Status.CurrentStep = "WaitingActiveRecovery"
		op.Status.Message = "Waiting for existing recovery tasks to complete before rebalancing"
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.Status().Update(ctx, op)
	}

	moved, summary, err := cli.RebalanceClusterChunks(ctx)
	if err != nil {
		op.Status.Phase = v1alpha1.OpPhaseFailed
		op.Status.Message = fmt.Sprintf("Rebalancing failed: %v", err)
		now := metav1.Now()
		op.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	now := metav1.Now()
	op.Status.Phase = v1alpha1.OpPhaseSucceeded
	op.Status.Progress = "100%"
	op.Status.CurrentStep = "Completed"
	op.Status.CompletionTime = &now
	op.Status.Message = fmt.Sprintf("Rebalance completed (chunks moved: %d): %s", moved, summary)
	return ctrl.Result{}, r.Status().Update(ctx, op)
}

func (r *DDBOperationReconciler) reconcileRollingRestart(ctx context.Context, op *v1alpha1.DDBOperation, cluster *v1alpha1.DolphinDBCluster) (ctrl.Result, error) {
	cli := r.getDDBClient(ctx, cluster)
	if cli == nil {
		op.Status.CurrentStep = "WaitingControllerClient"
		op.Status.Message = "Controller client is not yet available"
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	// 1. Determine list of targets to restart
	var targetNodes []string
	if len(op.Spec.TargetNodes) > 0 {
		targetNodes = op.Spec.TargetNodes
	} else {
		var nodeList v1alpha1.DDBNodeList
		if err := r.List(ctx, &nodeList, client.InNamespace(op.Namespace), client.MatchingLabels{
			"app.kubernetes.io/instance": cluster.Name,
		}); err != nil {
			return ctrl.Result{}, err
		}

		// Sort DataNodes first, Controllers last
		var dns []string
		var ctls []string
		for _, n := range nodeList.Items {
			if n.Spec.Role == v1alpha1.NodeRoleDataNode {
				dns = append(dns, n.Name)
			} else {
				ctls = append(ctls, n.Name)
			}
		}
		sort.Strings(dns)
		sort.Strings(ctls)
		targetNodes = append(dns, ctls...)
	}

	if len(targetNodes) == 0 {
		op.Status.Phase = v1alpha1.OpPhaseSucceeded
		now := metav1.Now()
		op.Status.CompletionTime = &now
		op.Status.Message = "No nodes found to restart"
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	// 2. Determine which node is currently being processed
	// State machine stored in op.Status.CurrentStep (e.g., "Restarting:ddb-cluster-dn-0:1/2")
	var currentIdx int
	stepStr := op.Status.CurrentStep
	if strings.HasPrefix(stepStr, "Restarting:") {
		parts := strings.Split(stepStr, ":")
		if len(parts) >= 2 {
			currentNode := parts[1]
			for idx, name := range targetNodes {
				if name == currentNode {
					currentIdx = idx
					break
				}
			}
		}
	}

	if currentIdx >= len(targetNodes) {
		now := metav1.Now()
		op.Status.Phase = v1alpha1.OpPhaseSucceeded
		op.Status.Progress = "100%"
		op.Status.CurrentStep = "Completed"
		op.Status.CompletionTime = &now
		op.Status.Message = fmt.Sprintf("Successfully completed rolling restart across %d nodes", len(targetNodes))
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	nodeToRestart := targetNodes[currentIdx]
	op.Status.Phase = v1alpha1.OpPhaseRunning
	op.Status.CurrentStep = fmt.Sprintf("Restarting:%s:%d/%d", nodeToRestart, currentIdx+1, len(targetNodes))
	op.Status.Progress = fmt.Sprintf("%d/%d", currentIdx+1, len(targetNodes))

	// Find the DDBNode CR
	nodeCR := &v1alpha1.DDBNode{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: op.Namespace, Name: nodeToRestart}, nodeCR); err != nil {
		op.Status.Message = fmt.Sprintf("Awaiting node %s: %v", nodeToRestart, err)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, r.Status().Update(ctx, op)
	}

	restartAnnotationKey := "dolphindb.io/restarted-by-op"
	if nodeCR.Annotations == nil || nodeCR.Annotations[restartAnnotationKey] != op.Name {
		if nodeCR.Annotations == nil {
			nodeCR.Annotations = make(map[string]string)
		}
		nodeCR.Annotations[restartAnnotationKey] = op.Name
		if err := r.Update(ctx, nodeCR); err != nil {
			return ctrl.Result{}, err
		}
		// Delete pod to trigger restart
		pod := &corev1.Pod{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: op.Namespace, Name: nodeToRestart}, pod); err == nil {
			_ = r.Delete(ctx, pod)
		}
		op.Status.Message = fmt.Sprintf("Triggered pod recreation for %s", nodeToRestart)
		return ctrl.Result{RequeueAfter: 4 * time.Second}, r.Status().Update(ctx, op)
	}

	// Wait for Pod to become Running and Ready
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Namespace: op.Namespace, Name: nodeToRestart}, pod)
	if err != nil || pod.Status.Phase != corev1.PodRunning || !isPodReady(pod) {
		op.Status.Message = fmt.Sprintf("Waiting for pod %s to be running and ready (probe check)", nodeToRestart)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	// In cluster mode, verify node state in cluster perf and replication tasks
	if cluster.Spec.Mode != v1alpha1.ModeStandalone {
		perf, err := cli.GetClusterPerf(ctx)
		if err != nil {
			op.Status.Message = fmt.Sprintf("Querying cluster status after restart: %v", err)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
		}

		online := false
		for _, n := range perf {
			if n.Name == nodeToRestart {
				if n.State == 1 {
					online = true
				}
				break
			}
		}
		// If it's a controller pod, getting perf itself means it's online
		if strings.Contains(nodeToRestart, "ctl-") {
			online = true
		}

		if !online {
			op.Status.Message = fmt.Sprintf("Waiting for %s to register state=1 in cluster", nodeToRestart)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
		}

		recovering, _ := cli.HasActiveRecoveryTasks(ctx)
		if recovering {
			op.Status.Message = fmt.Sprintf("Node %s is online; catching up data replicas", nodeToRestart)
			return ctrl.Result{RequeueAfter: 4 * time.Second}, r.Status().Update(ctx, op)
		}
	}

	// Node successfully restarted! Move to next
	nextIdx := currentIdx + 1
	if nextIdx >= len(targetNodes) {
		now := metav1.Now()
		op.Status.Phase = v1alpha1.OpPhaseSucceeded
		op.Status.Progress = "100%"
		op.Status.CurrentStep = "Completed"
		op.Status.CompletionTime = &now
		op.Status.Message = fmt.Sprintf("Successfully completed rolling restart across %d nodes", len(targetNodes))
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	nextNode := targetNodes[nextIdx]
	op.Status.CurrentStep = fmt.Sprintf("Restarting:%s:%d/%d", nextNode, nextIdx+1, len(targetNodes))
	op.Status.Progress = fmt.Sprintf("%d/%d", nextIdx+1, len(targetNodes))
	op.Status.Message = fmt.Sprintf("Node %s verified online; proceeding to %s", nodeToRestart, nextNode)
	return ctrl.Result{RequeueAfter: 2 * time.Second}, r.Status().Update(ctx, op)
}

func (r *DDBOperationReconciler) reconcileDecommissionNode(ctx context.Context, op *v1alpha1.DDBOperation, cluster *v1alpha1.DolphinDBCluster) (ctrl.Result, error) {
	node, err := r.resolveTargetNode(ctx, op, cluster)
	if err != nil {
		op.Status.Phase = v1alpha1.OpPhaseFailed
		op.Status.Message = err.Error()
		now := metav1.Now()
		op.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, op)
	}

	cli := r.getDDBClient(ctx, cluster)
	if cli == nil {
		op.Status.CurrentStep = "WaitingControllerClient"
		op.Status.Message = "Controller client not yet available"
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	candidateAlias := node.Spec.Alias
	survivorAlias := fmt.Sprintf("%s-dn-0", cluster.Name)
	if candidateAlias == survivorAlias {
		survivorAlias = fmt.Sprintf("%s-dn-1", cluster.Name)
	}

	// Check remaining chunks
	chunks, err := cli.GetChunksOnNode(ctx, candidateAlias)
	if err != nil {
		op.Status.Message = fmt.Sprintf("Checking chunks on %s: %v", candidateAlias, err)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, op)
	}

	if len(chunks) > 0 {
		op.Status.Phase = v1alpha1.OpPhaseRunning
		op.Status.CurrentStep = fmt.Sprintf("DrainingChunks-%s", candidateAlias)
		op.Status.Progress = fmt.Sprintf("%d chunks remain", len(chunks))

		// Check survivor chunks
		survivorChunks, _ := cli.GetChunksOnNode(ctx, survivorAlias)
		survivorSet := make(map[string]bool)
		for _, sc := range survivorChunks {
			survivorSet[sc] = true
		}

		for _, c := range chunks {
			if survivorSet[c] {
				_ = cli.DeleteReplicas(ctx, c, candidateAlias)
			} else {
				_ = cli.CopyReplicas(ctx, candidateAlias, survivorAlias, c)
			}
		}

		op.Status.Message = fmt.Sprintf("Dispatched chunk migrations from %s to %s (%d chunks remaining)", candidateAlias, survivorAlias, len(chunks))
		return ctrl.Result{RequeueAfter: 4 * time.Second}, r.Status().Update(ctx, op)
	}

	// 0 chunks remain: delete DDBNode CR and Pod
	_ = r.Delete(ctx, node)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      node.Name,
			Namespace: node.Namespace,
		},
	}
	_ = r.Delete(ctx, pod)

	now := metav1.Now()
	op.Status.Phase = v1alpha1.OpPhaseSucceeded
	op.Status.Progress = "100%"
	op.Status.CurrentStep = "Completed"
	op.Status.CompletionTime = &now
	op.Status.Message = fmt.Sprintf("Node %s successfully drained and decommissioned", candidateAlias)
	return ctrl.Result{}, r.Status().Update(ctx, op)
}

func (r *DDBOperationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DDBOperation{}).
		Complete(r)
}
