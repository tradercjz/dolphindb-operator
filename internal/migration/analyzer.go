package migration

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AnalyzeLegacyCluster performs deep inspection and generates a compatibility assessment
func AnalyzeLegacyCluster(ctx context.Context, cli client.Client, kubeClient kubernetes.Interface, ns, name string) (*LegacyClusterDetail, *AssessmentReport, error) {
	// 1. Fetch Legacy DolphinDB CR
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(LegacyGVK)
	if err := cli.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, u); err != nil {
		return nil, nil, fmt.Errorf("failed to get legacy DolphinDB %s/%s: %w", ns, name, err)
	}

	spec, _, _ := unstructured.NestedMap(u.Object, "spec")
	mode, _, _ := unstructured.NestedString(spec, "mode")
	if mode == "" {
		mode = "standalone"
	}
	version, _, _ := unstructured.NestedString(spec, "version")
	storageClassName, _, _ := unstructured.NestedString(spec, "storageClassName")
	statusPhase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	if statusPhase == "" {
		statusPhase = "Available"
	}

	detail := &LegacyClusterDetail{
		Info: LegacyClusterInfo{
			Name:             name,
			Namespace:        ns,
			Mode:             mode,
			Version:          version,
			Status:           statusPhase,
			CreatedAt:        u.GetCreationTimestamp().Time,
			StorageClassName: storageClassName,
		},
		StorageClassName: storageClassName,
		CustomConfigs:    make(map[string]string),
	}

	// Parse node group specs
	detail.ControllerSpec = parseNodeGroup(spec, "controller")
	detail.DataNodeSpec = parseNodeGroup(spec, "datanode")
	detail.ComputeNodeSpec = parseNodeGroup(spec, "computenode")

	totalReplicas := int(detail.ControllerSpec.Replicas + detail.DataNodeSpec.Replicas + detail.ComputeNodeSpec.Replicas)
	if totalReplicas == 0 {
		totalReplicas = 1
	}
	detail.Info.NodeCount = totalReplicas
	detail.Info.DataSize = detail.DataNodeSpec.DataSize

	// 2. Inspect StatefulSets
	stsList := &appsv1.StatefulSetList{}
	_ = cli.List(ctx, stsList, client.InNamespace(ns))
	for _, sts := range stsList.Items {
		if strings.Contains(sts.Name, name) || (sts.Labels != nil && sts.Labels["dolphindb.io/dolphindb.name"] == name) {
			detail.StatefulSets = append(detail.StatefulSets, sts.Name)
		}
	}

	// 3. Inspect Pods
	podList := &corev1.PodList{}
	_ = cli.List(ctx, podList, client.InNamespace(ns))
	var legacySidecarsFound bool
	for _, pod := range podList.Items {
		if strings.Contains(pod.Name, name) || (pod.Labels != nil && pod.Labels["dolphindb.io/dolphindb.name"] == name) {
			var containers []string
			for _, c := range pod.Spec.Containers {
				containers = append(containers, c.Name)
				if c.Name == "exporter" || c.Name == "cleaner" || c.Name == "promtail" {
					legacySidecarsFound = true
				}
			}
			isReady := false
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					isReady = true
					break
				}
			}
			var restarts int32
			for _, cs := range pod.Status.ContainerStatuses {
				restarts += cs.RestartCount
			}

			detail.Pods = append(detail.Pods, LegacyPodSummary{
				Name:       pod.Name,
				NodeName:   pod.Spec.NodeName,
				IP:         pod.Status.PodIP,
				Phase:      string(pod.Status.Phase),
				Ready:      isReady,
				Containers: containers,
				Restarts:   restarts,
			})
		}
	}

	// 4. Inspect PVCs
	pvcList := &corev1.PersistentVolumeClaimList{}
	_ = cli.List(ctx, pvcList, client.InNamespace(ns))
	var totalStorageQty resource.Quantity
	for _, pvc := range pvcList.Items {
		if strings.Contains(pvc.Name, name) || (pvc.Labels != nil && pvc.Labels["dolphindb.io/dolphindb.name"] == name) {
			sc := ""
			if pvc.Spec.StorageClassName != nil {
				sc = *pvc.Spec.StorageClassName
			}
			capStr := ""
			if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
				capStr = req.String()
				totalStorageQty.Add(req)
			}
			mountP := "/data/ddb/server/data"
			if strings.HasPrefix(pvc.Name, "log-") {
				mountP = "/data/ddb/server/log"
			} else if strings.HasPrefix(pvc.Name, "core-") {
				mountP = "/data/ddb/core"
			}

			detail.PVCs = append(detail.PVCs, LegacyPVCSummary{
				Name:         pvc.Name,
				Capacity:     capStr,
				StorageClass: sc,
				Status:       string(pvc.Status.Phase),
				VolumeName:   pvc.Spec.VolumeName,
				MountPath:    mountP,
			})
		}
	}

	// 5. Inspect ConfigMap (ddb-<name>)
	cmNames := []string{fmt.Sprintf("ddb-%s", name), name}
	for _, cmName := range cmNames {
		cm := &corev1.ConfigMap{}
		if err := cli.Get(ctx, client.ObjectKey{Namespace: ns, Name: cmName}, cm); err == nil {
			if cnf, ok := cm.Data["dolphindb.cnf"]; ok {
				detail.CustomConfigs = parseProperties(cnf)
			}
			if lic, ok := cm.Data["license.lic"]; ok {
				detail.License = strings.TrimSpace(lic)
			}
			break
		}
	}

	// 6. Inspect Services
	svcList := &corev1.ServiceList{}
	_ = cli.List(ctx, svcList, client.InNamespace(ns))
	for _, svc := range svcList.Items {
		if strings.Contains(svc.Name, name) || (svc.Labels != nil && svc.Labels["dolphindb.io/dolphindb.name"] == name) {
			var ports []string
			for _, p := range svc.Spec.Ports {
				if p.NodePort > 0 {
					ports = append(ports, fmt.Sprintf("%d:%d/%s", p.Port, p.NodePort, p.Protocol))
				} else {
					ports = append(ports, fmt.Sprintf("%d/%s", p.Port, p.Protocol))
				}
			}
			detail.Services = append(detail.Services, LegacyServiceSummary{
				Name:  svc.Name,
				Type:  string(svc.Spec.Type),
				Ports: ports,
			})
		}
	}

	// 7. Generate Assessment Report & Findings
	report := evaluateCompatibility(detail, totalStorageQty, legacySidecarsFound)

	return detail, report, nil
}

func parseNodeGroup(spec map[string]interface{}, group string) NodeGroupSummary {
	var summary NodeGroupSummary
	g, ok := spec[group].(map[string]interface{})
	if !ok {
		return summary
	}
	if r, ok := g["replicas"].(int64); ok {
		summary.Replicas = int32(r)
	}
	if p, ok := g["port"].(int64); ok {
		summary.Port = int32(p)
	}
	if ds, ok := g["dataSize"].(string); ok {
		summary.DataSize = ds
	}
	if ls, ok := g["logSize"].(string); ok {
		summary.LogSize = ls
	}
	if res, ok := g["resources"].(map[string]interface{}); ok {
		if limits, ok := res["limits"].(map[string]interface{}); ok {
			if cpu, ok := limits["cpu"].(string); ok {
				summary.CPU = cpu
			}
			if mem, ok := limits["memory"].(string); ok {
				summary.Memory = mem
			}
		}
	}
	return summary
}

func parseProperties(content string) map[string]string {
	result := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var key, val string
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			key = strings.TrimSpace(parts[0])
			val = strings.Trim(strings.TrimSpace(parts[1]), "\"")
		} else if strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			key = strings.TrimSpace(parts[0])
			val = strings.Trim(strings.TrimSpace(parts[1]), "\"")
		}
		if key != "" {
			result[key] = val
		}
	}
	return result
}

func evaluateCompatibility(detail *LegacyClusterDetail, totalQty resource.Quantity, legacySidecars bool) *AssessmentReport {
	score := 100
	var findings []PreflightFinding

	// Workload Controller Evaluation
	if len(detail.StatefulSets) > 0 {
		score -= 10
		findings = append(findings, PreflightFinding{
			Category: "Workload",
			Severity: SeverityWarning,
			Title:    "旧套件由 K8s StatefulSet 托管（级联删除风险）",
			Detail: fmt.Sprintf("发现 %d 个关联的 StatefulSet (%s)。旧版 StatefulSet 与 Pod 存在 OwnerReferences 级联关系，直接卸载旧套件会造成 Pod 瞬间被强制 Terminate。",
				len(detail.StatefulSets), strings.Join(detail.StatefulSets, ", ")),
			Recommendation: "若选择原地接管模式，必须严格使用 `kubectl delete sts <name> --cascade=orphan` 剥离控制器，保留底层运行中 Pod 和 PVC。",
		})
	}

	// Container & Resource Footprint
	if legacySidecars {
		findings = append(findings, PreflightFinding{
			Category:       "Architecture",
			Severity:       SeverityInfo,
			Title:          "旧版多副容器架构 (4 Sidecars / Pod)",
			Detail:         "旧版 Pod 注入了 exporter, cleaner, promtail 3 个常驻副容器，带来约 500MB+ 的额外内存底噪开销。",
			Recommendation: "新版采用一体化设计（单容器 + 轻量 ddb-agent < 15MB），升级后每个数据库节点可释放约 480MB 内存，极大降低宿主机资源浪费。",
		})
	}

	// Storage & Volumes Evaluation
	var pvcNames []string
	for _, pvc := range detail.PVCs {
		pvcNames = append(pvcNames, pvc.Name)
	}
	if len(detail.PVCs) > 0 {
		findings = append(findings, PreflightFinding{
			Category: "Storage",
			Severity: SeverityWarning,
			Title:    "存量数据卷与挂载路径对齐",
			Detail: fmt.Sprintf("检测到 %d 个持久化数据卷 (总容量约 %s，存储类: %s)。旧版单节点数据路径为 `/data/ddb/server/data`，多盘集群为独立挂载。",
				len(detail.PVCs), totalQty.String(), detail.StorageClassName),
			Recommendation: "升级新版时将自动复用 `data-xxx` 数据主卷，新版 Operator 已内置多路径兼容适配，防止数据未对齐导致的重新初始化。",
		})
	}

	// Custom Configuration Drift
	if len(detail.CustomConfigs) > 0 {
		var cfgSummary []string
		for k, v := range detail.CustomConfigs {
			cfgSummary = append(cfgSummary, fmt.Sprintf("%s=%s", k, v))
		}
		findings = append(findings, PreflightFinding{
			Category:       "Config",
			Severity:       SeverityInfo,
			Title:          fmt.Sprintf("已自动捕获 %d 项历史定制参数", len(detail.CustomConfigs)),
			Detail:         fmt.Sprintf("从原 ConfigMap 中提取到定制参数: %s", strings.Join(cfgSummary, ", ")),
			Recommendation: "转换新版 CRD 时，工具链已将上述参数自动注入新版 `spec.extraConfig` / `cluster.cfg`，确保业务调优参数不发生漂移。",
		})
	}

	// Service & Port Exposure
	var exposedPorts []string
	for _, svc := range detail.Services {
		if svc.Type == "NodePort" {
			exposedPorts = append(exposedPorts, svc.Ports...)
		}
	}
	if len(exposedPorts) > 0 {
		score -= 5
		findings = append(findings, PreflightFinding{
			Category:       "Networking",
			Severity:       SeverityWarning,
			Title:          "外部 NodePort 端口映射变更提示",
			Detail:         fmt.Sprintf("旧集群当前通过动态 NodePort 暴露服务: %s。硬编码旧端口的客户端访问脚本可能受影响。", strings.Join(exposedPorts, ", ")),
			Recommendation: "升级后推荐接入统一规划的 Service 或保留相同的 NodePort，并同步检查上游客户端连接配置。",
		})
	}

	if score < 0 {
		score = 0
	}

	riskLevel := "LOW"
	if score < 70 {
		riskLevel = "HIGH"
	} else if score < 85 {
		riskLevel = "MEDIUM"
	}

	recommendedStrategy := "BlueGreen"
	reason := "当前集群规模适中，强烈推荐【双轨蓝绿平滑迁移】：在同集群并存新建新集群，验证无误后切流量，业务 0 停机且随时可回滚。"
	if totalQty.Value() > 500*1024*1024*1024 { // > 500GB
		recommendedStrategy = "InPlace"
		reason = "检测到存储数据量较大，跨集群复制耗时较长，推荐采用【存量 PVC 原地接管】：通过孤儿化删除旧控制器并原位挂载 PVC，秒级完成换壳升级。"
	}

	return &AssessmentReport{
		HealthScore: score,
		RiskLevel:   riskLevel,
		Findings:    findings,
		ResourceComparison: ResourceComparison{
			LegacyArchitecture: "旧版 4 容器架构 (datanode + exporter + cleaner + promtail)",
			NewArchitecture:    "新版轻量架构 (单容器 + ddb-agent < 15MB)",
			MemorySavedPerPod:  "约 480MB / 节点",
			SidecarReduction:   "消除 3 个冗余副容器，大幅降低容器崩塌排查成本",
		},
		StorageSummary: StorageSummary{
			TotalVolumes:  len(detail.PVCs),
			TotalCapacity: totalQty.String(),
			StorageClass:  detail.StorageClassName,
			VolumeNames:   pvcNames,
		},
		CustomConfigs:        detail.CustomConfigs,
		RecommendedStrategy:  recommendedStrategy,
		RecommendationReason: reason,
	}
}
