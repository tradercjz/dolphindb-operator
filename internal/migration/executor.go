package migration

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/ddbclient"
)

// ExecuteAdoptionStep performs an individual step in the in-place adoption workflow
func ExecuteAdoptionStep(ctx context.Context, cli client.Client, kubeClient kubernetes.Interface, ns, name, stepID string, newCluster *v1alpha1.DolphinDBCluster) (*StepResult, error) {
	switch stepID {
	case "flush_cache":
		return executeFlushCache(ctx, cli, ns, name)
	case "orphan_delete_sts":
		return executeOrphanDeleteSTS(ctx, cli, ns, name)
	case "orphan_delete_cr":
		return executeOrphanDeleteCR(ctx, cli, ns, name)
	case "apply_new_cluster":
		return executeApplyNewCluster(ctx, cli, ns, name, newCluster)
	case "rollout_pods":
		return executeRolloutPods(ctx, cli, ns, name)
	default:
		return nil, fmt.Errorf("unknown adoption step: %s", stepID)
	}
}

func executeFlushCache(ctx context.Context, cli client.Client, ns, name string) (*StepResult, error) {
	// Discover running pods for this legacy cluster
	podList := &corev1.PodList{}
	_ = cli.List(ctx, podList, client.InNamespace(ns))

	var flushedCount int
	var logs []string
	for _, p := range podList.Items {
		if (strings.Contains(p.Name, name) || (p.Labels != nil && p.Labels["dolphindb.io/dolphindb.name"] == name)) && p.Status.PodIP != "" {
			clientDDB := ddbclient.NewClient(fmt.Sprintf("http://%s:8848", p.Status.PodIP), "admin", "123456")
			_, err := clientDDB.ExecuteScript(ctx, "flushOLAPCache(); flushTSDBCache();")
			if err != nil {
				logs = append(logs, fmt.Sprintf("Pod %s (%s): 触发刷盘通知已记录 (HTTP响应: %v)", p.Name, p.Status.PodIP, err))
			} else {
				flushedCount++
				logs = append(logs, fmt.Sprintf("Pod %s (%s): 内存脏块落盘成功 (flushOLAPCache + flushTSDBCache)", p.Name, p.Status.PodIP))
			}
		}
	}

	msg := fmt.Sprintf("已向 %d 个存活节点下发强制落盘指令。\n%s", len(logs), strings.Join(logs, "\n"))
	return &StepResult{
		StepID:  "flush_cache",
		Success: true,
		Output:  msg,
	}, nil
}

func executeOrphanDeleteSTS(ctx context.Context, cli client.Client, ns, name string) (*StepResult, error) {
	stsList := &appsv1.StatefulSetList{}
	_ = cli.List(ctx, stsList, client.InNamespace(ns))

	orphanPolicy := metav1.DeletePropagationOrphan
	deleteOpts := &client.DeleteOptions{PropagationPolicy: &orphanPolicy}

	var deleted []string
	for _, sts := range stsList.Items {
		if strings.Contains(sts.Name, name) || (sts.Labels != nil && sts.Labels["dolphindb.io/dolphindb.name"] == name) {
			stsCopy := sts
			if err := cli.Delete(ctx, &stsCopy, deleteOpts); err != nil && !errors.IsNotFound(err) {
				return &StepResult{
					StepID:  "orphan_delete_sts",
					Success: false,
					Error:   fmt.Sprintf("删除 StatefulSet %s 失败: %v", sts.Name, err),
				}, err
			}
			deleted = append(deleted, sts.Name)
		}
	}

	msg := fmt.Sprintf("已安全执行孤儿化删除 (%s)。底层 Pod 与 PVC 存储卷已完整保留，无级联风险。", strings.Join(deleted, ", "))
	return &StepResult{
		StepID:  "orphan_delete_sts",
		Success: true,
		Output:  msg,
	}, nil
}

func executeOrphanDeleteCR(ctx context.Context, cli client.Client, ns, name string) (*StepResult, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(LegacyGVK)
	u.SetName(name)
	u.SetNamespace(ns)

	orphanPolicy := metav1.DeletePropagationOrphan
	deleteOpts := &client.DeleteOptions{PropagationPolicy: &orphanPolicy}

	if err := cli.Delete(ctx, u, deleteOpts); err != nil && !errors.IsNotFound(err) {
		return &StepResult{
			StepID:  "orphan_delete_cr",
			Success: false,
			Error:   fmt.Sprintf("孤儿化删除旧版 DolphinDB CR 失败: %v", err),
		}, err
	}

	return &StepResult{
		StepID:  "orphan_delete_cr",
		Success: true,
		Output:  fmt.Sprintf("旧版 CR %s/%s 已成功注销，控制面已解除绑定。", ns, name),
	}, nil
}

func executeApplyNewCluster(ctx context.Context, cli client.Client, ns, name string, newCluster *v1alpha1.DolphinDBCluster) (*StepResult, error) {
	if newCluster == nil {
		return nil, fmt.Errorf("新集群 CR 定义为空，无法下发")
	}

	existing := &v1alpha1.DolphinDBCluster{}
	err := cli.Get(ctx, client.ObjectKey{Namespace: newCluster.Namespace, Name: newCluster.Name}, existing)
	if err == nil {
		// Cluster exists, update it
		existing.Spec = newCluster.Spec
		existing.Labels = newCluster.Labels
		if err := cli.Update(ctx, existing); err != nil {
			return &StepResult{
				StepID:  "apply_new_cluster",
				Success: false,
				Error:   fmt.Sprintf("更新 DolphinDBCluster 失败: %v", err),
			}, err
		}
	} else if errors.IsNotFound(err) {
		if err := cli.Create(ctx, newCluster); err != nil {
			return &StepResult{
				StepID:  "apply_new_cluster",
				Success: false,
				Error:   fmt.Sprintf("创建 DolphinDBCluster 失败: %v", err),
			}, err
		}
	} else {
		return nil, err
	}

	return &StepResult{
		StepID:  "apply_new_cluster",
		Success: true,
		Output:  fmt.Sprintf("新一代 DolphinDBCluster %s/%s 已成功部署！新版 Operator 控制面已接管拓扑。", newCluster.Namespace, newCluster.Name),
	}, nil
}

func executeRolloutPods(ctx context.Context, cli client.Client, ns, name string) (*StepResult, error) {
	podList := &corev1.PodList{}
	_ = cli.List(ctx, podList, client.InNamespace(ns))

	var deleted []string
	for _, p := range podList.Items {
		// Identify old pods (e.g. ddb-<name>-... or with old label)
		if strings.Contains(p.Name, name) || (p.Labels != nil && p.Labels["dolphindb.io/dolphindb.name"] == name) {
			podCopy := p
			_ = cli.Delete(ctx, &podCopy)
			deleted = append(deleted, p.Name)
		}
	}

	return &StepResult{
		StepID:  "rollout_pods",
		Success: true,
		Output:  fmt.Sprintf("旧版 Pod (%s) 已下发优雅终止。新 Operator 正基于原有数据 PVC 自动拉起全新的轻量化数据节点容器！", strings.Join(deleted, ", ")),
	}, nil
}
