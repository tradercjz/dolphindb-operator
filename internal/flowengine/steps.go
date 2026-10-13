package flowengine

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/ddbclient"
)

// CheckClusterHealthyStep ensures all active cluster nodes are online and no recovery is running
type CheckClusterHealthyStep struct {
	GetDDBClient func(cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client
}

func (s *CheckClusterHealthyStep) Name() string {
	return "CheckClusterHealthy"
}

func (s *CheckClusterHealthyStep) Execute(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) StepResult {
	cli := s.GetDDBClient(cluster)
	if cli == nil {
		return WaitResult(3*time.Second, "Controller endpoint not available yet")
	}

	nodes, err := cli.GetClusterPerf(ctx)
	if err != nil {
		return WaitResult(3*time.Second, fmt.Sprintf("Failed to query cluster perf: %v", err))
	}

	for _, n := range nodes {
		if n.State != 1 {
			return WaitResult(3*time.Second, fmt.Sprintf("Node %s is not online (state=%d)", n.Name, n.State))
		}
	}

	recovering, err := cli.HasActiveRecoveryTasks(ctx)
	if err != nil {
		return WaitResult(3*time.Second, fmt.Sprintf("Failed to query recovery tasks: %v", err))
	}
	if recovering {
		return WaitResult(5*time.Second, "Cluster has active chunk replication/recovery in progress")
	}

	return DoneResult("Cluster is fully healthy and ready for operation")
}

// RollingRestartNodeStep restarts a single node pod and waits for its health & catch-up
type RollingRestartNodeStep struct {
	K8sClient    client.Client
	GetDDBClient func(cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client
	NodeName     string
	NodeAlias    string
}

func (s *RollingRestartNodeStep) Name() string {
	return fmt.Sprintf("RollingRestart-%s", s.NodeName)
}

func (s *RollingRestartNodeStep) Execute(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) StepResult {
	pod := &corev1.Pod{}
	err := s.K8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: s.NodeName}, pod)
	if errors.IsNotFound(err) {
		return WaitResult(3*time.Second, fmt.Sprintf("Pod %s is recreating", s.NodeName))
	}
	if err != nil {
		return ErrorResult(err)
	}

	// Check if this pod has been triggered for restart
	restartAnnotationKey := "dolphindb.io/restarted-flow"
	expectedVal := cluster.Status.ActiveFlow
	if pod.Annotations == nil || pod.Annotations[restartAnnotationKey] != expectedVal {
		// Delete the pod to trigger rolling restart with new configs
		if err := s.K8sClient.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			return ErrorResult(fmt.Errorf("failed to delete pod %s for restart: %w", s.NodeName, err))
		}
		return WaitResult(4*time.Second, fmt.Sprintf("Triggered pod recreation for %s", s.NodeName))
	}

	// Verify pod is running
	if pod.Status.Phase != corev1.PodRunning {
		return WaitResult(3*time.Second, fmt.Sprintf("Pod %s is %s", s.NodeName, pod.Status.Phase))
	}

	// Verify node is online in cluster perf
	cli := s.GetDDBClient(cluster)
	if cli == nil {
		return WaitResult(3*time.Second, "Controller client not ready")
	}

	nodes, err := cli.GetClusterPerf(ctx)
	if err != nil {
		return WaitResult(3*time.Second, fmt.Sprintf("Querying cluster status after restarting %s: %v", s.NodeName, err))
	}

	found := false
	for _, n := range nodes {
		if n.Name == s.NodeAlias {
			found = true
			if n.State != 1 {
				return WaitResult(3*time.Second, fmt.Sprintf("Node %s is online in pod but not yet surviving in cluster (state=%d)", s.NodeAlias, n.State))
			}
			break
		}
	}
	if !found {
		return WaitResult(3*time.Second, fmt.Sprintf("Waiting for node %s to register with controller", s.NodeAlias))
	}

	// Wait for any recovery/catch-up tasks
	recovering, _ := cli.HasActiveRecoveryTasks(ctx)
	if recovering {
		return WaitResult(4*time.Second, fmt.Sprintf("Node %s is catching up data replicas", s.NodeAlias))
	}

	return DoneResult(fmt.Sprintf("Node %s successfully restarted and verified online", s.NodeName))
}

// DrainDataNodeChunksStep safely copies all data chunks from candidate to survivor
type DrainDataNodeChunksStep struct {
	GetDDBClient   func(cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client
	CandidateAlias string
	SurvivorAlias  string
}

func (s *DrainDataNodeChunksStep) Name() string {
	return fmt.Sprintf("DrainChunks-%s", s.CandidateAlias)
}

func (s *DrainDataNodeChunksStep) Execute(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) StepResult {
	cli := s.GetDDBClient(cluster)
	if cli == nil {
		return WaitResult(3*time.Second, "Controller client not available")
	}

	chunks, err := cli.GetChunksOnNode(ctx, s.CandidateAlias)
	if err != nil {
		return WaitResult(3*time.Second, fmt.Sprintf("Querying chunks on node %s: %v", s.CandidateAlias, err))
	}

	if len(chunks) == 0 {
		return DoneResult(fmt.Sprintf("Node %s has 0 chunks, safe to decommission", s.CandidateAlias))
	}

	// Check if replication tasks are already in flight
	recovering, err := cli.HasActiveRecoveryTasks(ctx)
	if err == nil && recovering {
		return WaitResult(5*time.Second, fmt.Sprintf("Chunk replication in progress (%d chunks on %s)", len(chunks), s.CandidateAlias))
	}

	// Check survivor chunks to see which have already arrived on survivor
	survivorChunks, err := cli.GetChunksOnNode(ctx, s.SurvivorAlias)
	if err != nil {
		return WaitResult(3*time.Second, fmt.Sprintf("Querying chunks on survivor %s: %v", s.SurvivorAlias, err))
	}
	survivorSet := make(map[string]bool)
	for _, sc := range survivorChunks {
		survivorSet[sc] = true
	}

	var needCopy []string
	var canDelete []string
	for _, c := range chunks {
		if survivorSet[c] {
			canDelete = append(canDelete, c)
		} else {
			needCopy = append(needCopy, c)
		}
	}

	// If some chunks have arrived on survivor, delete their replicas from candidate!
	if len(canDelete) > 0 {
		for _, c := range canDelete {
			_ = cli.DeleteReplicas(ctx, c, s.CandidateAlias)
		}
	}

	// Dispatch copyReplicas for chunks not yet on survivor
	if len(needCopy) > 0 {
		for _, c := range needCopy {
			if err := cli.CopyReplicas(ctx, s.CandidateAlias, s.SurvivorAlias, c); err != nil {
				return WaitResult(3*time.Second, fmt.Sprintf("Migrating chunk %s to %s failed: %v", c, s.SurvivorAlias, err))
			}
		}
		return WaitResult(5*time.Second, fmt.Sprintf("Dispatched replica migration for %d chunks from %s to %s", len(needCopy), s.CandidateAlias, s.SurvivorAlias))
	}

	return WaitResult(3*time.Second, fmt.Sprintf("Cleaned %d evacuated replicas on %s", len(canDelete), s.CandidateAlias))
}

// CleanNodeReplicaMetadataStep removes drained chunk metadata from the decommissioned node
type CleanNodeReplicaMetadataStep struct {
	GetDDBClient   func(cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client
	CandidateAlias string
}

func (s *CleanNodeReplicaMetadataStep) Name() string {
	return fmt.Sprintf("CleanReplicaMeta-%s", s.CandidateAlias)
}

func (s *CleanNodeReplicaMetadataStep) Execute(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) StepResult {
	cli := s.GetDDBClient(cluster)
	if cli == nil {
		return WaitResult(3*time.Second, "Controller client not available")
	}

	chunks, err := cli.GetChunksOnNode(ctx, s.CandidateAlias)
	if err != nil {
		return WaitResult(3*time.Second, fmt.Sprintf("Checking remaining chunks: %v", err))
	}

	if len(chunks) == 0 {
		return DoneResult(fmt.Sprintf("Metadata cleaned, 0 replicas remain on %s", s.CandidateAlias))
	}

	for _, c := range chunks {
		_ = cli.DeleteReplicas(ctx, c, s.CandidateAlias)
	}

	return WaitResult(3*time.Second, fmt.Sprintf("Cleaning remaining %d replica entries on %s", len(chunks), s.CandidateAlias))
}

// DeleteDDBNodeCRStep deletes the decommissioned DDBNode resource and its Pod
type DeleteDDBNodeCRStep struct {
	K8sClient client.Client
	NodeName  string
}

func (s *DeleteDDBNodeCRStep) Name() string {
	return fmt.Sprintf("DeleteNodeCR-%s", s.NodeName)
}

func (s *DeleteDDBNodeCRStep) Execute(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) StepResult {
	nodeCR := &v1alpha1.DDBNode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.NodeName,
			Namespace: cluster.Namespace,
		},
	}
	if err := s.K8sClient.Delete(ctx, nodeCR); err != nil && !errors.IsNotFound(err) {
		return ErrorResult(fmt.Errorf("failed to delete DDBNode %s: %w", s.NodeName, err))
	}

	// Delete Pod if still present
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.NodeName,
			Namespace: cluster.Namespace,
		},
	}
	_ = s.K8sClient.Delete(ctx, pod)

	return DoneResult(fmt.Sprintf("DDBNode %s deleted safely", s.NodeName))
}
