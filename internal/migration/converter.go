package migration

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"dolphindb.io/operator/api/v1alpha1"
)

// ConvertLegacyCluster translates legacy cluster configuration into the new DolphinDBCluster CR
func ConvertLegacyCluster(detail *LegacyClusterDetail, opts ConversionOptions) (*ConversionResult, error) {
	targetName := opts.NewClusterName
	if targetName == "" {
		if opts.TargetMode == "in-place" {
			targetName = detail.Info.Name
		} else {
			targetName = fmt.Sprintf("%s-v2", detail.Info.Name)
		}
	}

	targetNs := opts.TargetNamespace
	if targetNs == "" {
		targetNs = detail.Info.Namespace
	}

	targetVer := opts.TargetVersion
	if targetVer == "" {
		if detail.Info.Version != "" {
			targetVer = strings.TrimPrefix(detail.Info.Version, "v")
		} else {
			targetVer = "3.00.6"
		}
	} else {
		targetVer = strings.TrimPrefix(targetVer, "v")
	}

	var scName *string
	if detail.StorageClassName != "" {
		sc := detail.StorageClassName
		scName = &sc
	}

	cluster := v1alpha1.DolphinDBCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "dolphindb.io/v1alpha1",
			Kind:       "DolphinDBCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetName,
			Namespace: targetNs,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "ddb-operator",
				"dolphindb.io/migrated-from":   detail.Info.Name,
				"dolphindb.io/migration-mode":  opts.TargetMode,
			},
		},
		Spec: v1alpha1.DolphinDBClusterSpec{
			Version:         targetVer,
			ImageRepository: "dolphindb",
			License: v1alpha1.LicenseSpec{
				Mode:       v1alpha1.LicenseModeMachineFingerprint,
				SecretName: "",
				AutoReload: true,
				MachineFingerprint: &v1alpha1.MachineFingerprintSpec{
					AutoPassthrough: true,
					MountMachineId:  true,
					MountHostEtc:    true,
					UseHostName:     true,
					LicensePath:     "/home/jzchen/dolphindb.lic",
				},
			},
			Networking: v1alpha1.NetworkingSpec{
				ExposureMode: v1alpha1.ExposureNodePortDirect,
				ServiceType:  corev1.ServiceTypeNodePort,
			},
		},
	}

	// Identify data PVCs for adoption if in-place mode
	var existingDataClaims []string
	if opts.TargetMode == "in-place" {
		if len(opts.ExistingPVCs) > 0 {
			existingDataClaims = opts.ExistingPVCs
		} else {
			for _, pvc := range detail.PVCs {
				if strings.HasPrefix(pvc.Name, "data-") {
					existingDataClaims = append(existingDataClaims, pvc.Name)
				}
			}
		}
	}

	// 1. Standalone Mode Mapping
	if detail.Info.Mode == "standalone" {
		cluster.Spec.Mode = v1alpha1.ModeStandalone

		port := int32(8848)
		if detail.DataNodeSpec.Port > 0 {
			port = detail.DataNodeSpec.Port
		}

		res := buildResources(detail.DataNodeSpec.CPU, detail.DataNodeSpec.Memory)

		dataSize := detail.DataNodeSpec.DataSize
		if dataSize == "" {
			dataSize = "20Gi"
		}

		storage := v1alpha1.StorageSpec{
			StorageClassName: scName,
			DataVolumeCount:  1,
			DataVolumeSize:   dataSize,
		}
		if opts.TargetMode == "in-place" && len(existingDataClaims) > 0 {
			storage.ExistingClaims = existingDataClaims
		}

		cluster.Spec.Standalone = &v1alpha1.StandaloneSpec{
			Port:      port,
			Resources: res,
			Storage:   storage,
			Config:    detail.CustomConfigs,
		}
	} else {
		// 2. Cluster Mode Mapping
		cluster.Spec.Mode = v1alpha1.ModeCluster

		// Controller
		ctlReplicas := detail.ControllerSpec.Replicas
		if ctlReplicas <= 0 {
			ctlReplicas = 1
		}
		ctlPort := detail.ControllerSpec.Port
		if ctlPort <= 0 {
			ctlPort = 8900
		}
		ctlRes := buildResources(detail.ControllerSpec.CPU, detail.ControllerSpec.Memory)

		// DataNode baseline specs
		dnReplicas := detail.DataNodeSpec.Replicas
		if dnReplicas <= 0 {
			dnReplicas = 1
		}
		dnPort := detail.DataNodeSpec.Port
		if dnPort <= 0 {
			dnPort = 8901
		}
		dnRes := buildResources(detail.DataNodeSpec.CPU, detail.DataNodeSpec.Memory)
		dnDataSize := detail.DataNodeSpec.DataSize
		if dnDataSize == "" {
			dnDataSize = "100Gi"
		}

		// In-place migration bindings and paths
		var ctlBindings []v1alpha1.NodeBinding
		var dnBindings []v1alpha1.NodeBinding
		ctlConfig := make(map[string]string)
		dnConfig := make(map[string]string)
		for k, v := range detail.CustomConfigs {
			dnConfig[k] = v
		}

		if opts.TargetMode == "in-place" {
			// In-place migration preserves legacy node aliases and path layouts
			dnConfig["volumes"] = "/data/ddb/server/clusterDemo/data/<ALIAS>/storage"
			dnConfig["redoLogDir"] = "/data/ddb/server/clusterDemo/data/<ALIAS>/redolog"
			dnConfig["chunkMetaDir"] = "/data/ddb/server/clusterDemo/data/<ALIAS>/storage"
			if dnConfig["maxMemSize"] == "" {
				dnConfig["maxMemSize"] = "4"
			}
			if dnConfig["OLAPCacheEngineSize"] == "" {
				dnConfig["OLAPCacheEngineSize"] = "1"
			}
			if dnConfig["TSDBCacheEngineSize"] == "" {
				dnConfig["TSDBCacheEngineSize"] = "1"
			}
			dnConfig["dataSync"] = "1"

			// Generate Controller NodeBindings with legacy claim adoption
			for i := int32(0); i < ctlReplicas; i++ {
				claimName := fmt.Sprintf("data-ddb-%s-ctr-%d-0", detail.Info.Name, i)
				nodeName := "aftersale2"
				for _, p := range detail.Pods {
					if strings.Contains(p.Name, fmt.Sprintf("ctr-%d", i)) && p.NodeName != "" {
						nodeName = p.NodeName
						break
					}
				}
				ctlBindings = append(ctlBindings, v1alpha1.NodeBinding{
					Index:     int(i),
					NodeName:  nodeName,
					Alias:     fmt.Sprintf("controller%d", i),
					ClaimName: claimName,
				})
			}

			// Generate DataNode NodeBindings with legacy claim adoption
			for i := int32(0); i < dnReplicas; i++ {
				claimName := fmt.Sprintf("data-ddb-%s-dn-%d-0", detail.Info.Name, i)
				nodeName := "aftersale2"
				for _, p := range detail.Pods {
					if strings.Contains(p.Name, fmt.Sprintf("dn-%d", i)) && p.NodeName != "" {
						nodeName = p.NodeName
						break
					}
				}
				dnBindings = append(dnBindings, v1alpha1.NodeBinding{
					Index:     int(i),
					NodeName:  nodeName,
					Alias:     fmt.Sprintf("node%d", i),
					ClaimName: claimName,
				})
			}
		}

		cluster.Spec.Controller = &v1alpha1.ControllerGroupSpec{
			Replicas:     ctlReplicas,
			Port:         ctlPort,
			AntiAffinity: v1alpha1.AntiAffinityPreferred,
			Resources:    ctlRes,
			Storage: v1alpha1.StorageSpec{
				StorageClassName: scName,
				MetaVolumeSize:   "20Gi",
			},
			NodeBindings: ctlBindings,
			Config:       ctlConfig,
		}

		// DataNode
		dnStorage := v1alpha1.StorageSpec{
			StorageClassName: scName,
			MetaVolumeSize:   "20Gi",
			DataVolumeCount:  1,
			DataVolumeSize:   dnDataSize,
		}

		cluster.Spec.DataNode = &v1alpha1.DataNodeGroupSpec{
			Replicas:     dnReplicas,
			Port:         dnPort,
			AntiAffinity: v1alpha1.AntiAffinityPreferred,
			Resources:    dnRes,
			Storage:      dnStorage,
			NodeBindings: dnBindings,
			Config:       dnConfig,
		}

		// ComputeNode (if any)
		if detail.ComputeNodeSpec.Replicas > 0 {
			cnPort := detail.ComputeNodeSpec.Port
			if cnPort <= 0 {
				cnPort = 8902
			}
			cnRes := buildResources(detail.ComputeNodeSpec.CPU, detail.ComputeNodeSpec.Memory)
			cluster.Spec.ComputeNode = &v1alpha1.ComputeNodeGroupSpec{
				Replicas:  detail.ComputeNodeSpec.Replicas,
				Port:      cnPort,
				Resources: cnRes,
				Storage: v1alpha1.StorageSpec{
					StorageClassName: scName,
					DataVolumeSize:   "20Gi",
				},
			}
		}
	}

	// Serialize to clean YAML
	yamlBytes, err := yaml.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal target cluster to YAML: %w", err)
	}

	// Generate Data Sync Script (for Blue-Green mode)
	syncScript := generateSyncScript(detail.Info.Name, targetName, detail.Info.Namespace)

	// Generate Step-by-Step Execution Plan (for In-Place Adoption mode)
	var adoptionSteps []AdoptionStep
	var stsNames []string
	if len(detail.StatefulSets) > 0 {
		stsNames = detail.StatefulSets
	} else {
		stsNames = []string{fmt.Sprintf("ddb-%s-dn-0", detail.Info.Name)}
	}

	var podNames []string
	for _, p := range detail.Pods {
		podNames = append(podNames, p.Name)
	}

	adoptionSteps = []AdoptionStep{
		{
			StepID:               "flush_cache",
			Title:                "步骤 1：业务停写与强制刷盘 (Checkpoint & Flush Cache)",
			Description:          "向旧集群数据库节点下发刷盘指令，确保内存中的 OLAP 和 TSDB 脏数据块全部落盘到底层 PVC 存储中，保证事务零丢失。",
			Command:              fmt.Sprintf("curl -s http://<DDB_NODE_IP>:8848/execute -d 'expr=flushOLAPCache();flushTSDBCache()'"),
			Status:               "pending",
			RequiresConfirmation: true,
		},
		{
			StepID:               "orphan_delete_sts",
			Title:                "步骤 2：孤儿化安全删除旧版 StatefulSet 控制器",
			Description:          "【关键操作】使用 --cascade=orphan 剥离 StatefulSet 控制器，此时运行中 Pod 与绑定的 PVC 存储卷安然无恙，杜绝级联误删。",
			Command:              fmt.Sprintf("kubectl delete sts %s --cascade=orphan -n %s", strings.Join(stsNames, " "), detail.Info.Namespace),
			Status:               "pending",
			RequiresConfirmation: true,
		},
		{
			StepID:               "orphan_delete_cr",
			Title:                "步骤 3：孤儿化删除旧版 DolphinDB CR 资源",
			Description:          "安全清理旧版 dolphindbs.dolphindb.dolphindb.io 资源对象，解除控制面争抢。",
			Command:              fmt.Sprintf("kubectl delete dolphindb %s --cascade=orphan -n %s", detail.Info.Name, detail.Info.Namespace),
			Status:               "pending",
			RequiresConfirmation: false,
		},
		{
			StepID:               "apply_new_cluster",
			Title:                "步骤 4：部署新一代 DolphinDBCluster 资源",
			Description:          "将生成的新版 CRD 应用至集群。新版 Operator 将感知并接管已存在的 PVC 卷，自动建立健康生命周期监控。",
			Command:              fmt.Sprintf("kubectl apply -f %s.yaml -n %s", targetName, targetNs),
			Status:               "pending",
			RequiresConfirmation: true,
		},
		{
			StepID:               "rollout_pods",
			Title:                "步骤 5：安全销毁旧 Pod，拉起新版轻量 Pod 接管",
			Description:          "销毁旧版带有 4 个冗余 Sidecar 的 Pod，新 Operator 会毫秒级拉起带有 ddb-agent 的轻量新 Pod，并重新挂载原有 PVC 恢复业务！",
			Command:              fmt.Sprintf("kubectl delete pod %s -n %s", strings.Join(podNames, " "), detail.Info.Namespace),
			Status:               "pending",
			RequiresConfirmation: true,
		},
	}

	var warnings []string
	if opts.TargetMode == "in-place" {
		warnings = append(warnings,
			"【重要提示】原地接管将直接重用存量数据卷，请务必先完成步骤 1 刷盘后再删除旧控制器。",
			"删除 StatefulSet 时必须携带 `--cascade=orphan` 参数，严禁执行普通级联删除！",
			"新 Pod 启动后将继承原盘的所有分区数据，首次启动耗时取决于 Redo Log 回放量。",
		)
	} else {
		warnings = append(warnings,
			"双轨蓝绿模式将在集群中部署全新实例，业务 0 停机，且随时可快速回滚到原集群。",
			"数据同步可通过下方提供的 DolphinDB 专属同步脚本在线执行，建议在业务低峰期执行初始全量同步。",
		)
	}

	return &ConversionResult{
		TargetMode:    opts.TargetMode,
		TargetCluster: cluster,
		TargetYAML:    string(yamlBytes),
		SyncScript:    syncScript,
		AdoptionSteps: adoptionSteps,
		WarningNotes:  warnings,
	}, nil
}

func buildResources(cpuStr, memStr string) corev1.ResourceRequirements {
	limits := corev1.ResourceList{}
	requests := corev1.ResourceList{}

	if cpuStr != "" {
		if q, err := resource.ParseQuantity(cpuStr); err == nil {
			limits[corev1.ResourceCPU] = q
			requests[corev1.ResourceCPU] = resource.MustParse("500m")
		}
	} else {
		limits[corev1.ResourceCPU] = resource.MustParse("2")
		requests[corev1.ResourceCPU] = resource.MustParse("500m")
	}

	if memStr != "" {
		if q, err := resource.ParseQuantity(memStr); err == nil {
			if q.Cmp(resource.MustParse("8Gi")) < 0 {
				limits[corev1.ResourceMemory] = resource.MustParse("8Gi")
			} else {
				limits[corev1.ResourceMemory] = q
			}
			requests[corev1.ResourceMemory] = resource.MustParse("2Gi")
		}
	} else {
		limits[corev1.ResourceMemory] = resource.MustParse("8Gi")
		requests[corev1.ResourceMemory] = resource.MustParse("2Gi")
	}

	return corev1.ResourceRequirements{
		Limits:   limits,
		Requests: requests,
	}
}

func generateSyncScript(oldCluster, newCluster, namespace string) string {
	return fmt.Sprintf(`// ==============================================================================
// DolphinDB 跨集群双轨蓝绿数据平滑同步脚本 (Blue-Green Data Migration Script)
// 源旧集群: %s (Namespace: %s)
// 目标新集群: %s
// ==============================================================================

// ------------------------------------------------------------------------------
// 方案 1：全量冷备/热备导入 (适合 TB 级海量历史行情数据，保证静态数据零丢失)
// ------------------------------------------------------------------------------

// 1.1 在【源旧集群】节点执行 Checkpoint 刷盘并导出 DFS 数据库:
def backupDfsDatabase(backupPath, dbUrl) {
    print("Step 1: 正在落盘内存脏块...");
    flushOLAPCache();
    flushTSDBCache();
    print("Step 2: 正在备份 DFS 数据库 " + dbUrl + " 到 " + backupPath + "...");
    backup(backupPath, dbUrl, true);
    print("全量备份完成！请确保新集群可以访问该共享备份目录，或通过 scp/rsync 同步文件。");
}

// 1.2 在【目标新集群】节点执行全量恢复:
def restoreDfsDatabase(backupPath, dbUrl) {
    print("Step 3: 新集群正在恢复元数据与分区数据...");
    restore(backupPath, dbUrl, "*", 0, true);
    print("DFS 数据库恢复成功！");
}

// ------------------------------------------------------------------------------
// 方案 2：分布式网络流在线同步 (适合中小数据量或实时追加，两集群网络互通)
// ------------------------------------------------------------------------------
def syncTableOnline(legacyHost, legacyPort, legacyUser, legacyPwd, dbUrl, tableName) {
    print("正在连接旧集群 " + legacyHost + ":" + string(legacyPort) + "...");
    conn = xdb(legacyHost, legacyPort, legacyUser, legacyPwd);
    
    // 跨集群拉取数据表流并直接写入新集群
    t = conn(expr(tableName, dbUrl));
    loadTable(dbUrl, tableName).append!(t);
    print("数据表 " + tableName + " 在线同步完成！");
}

// 示例调用:
// 在旧集群执行: backupDfsDatabase("/data/ddb/server/data/backup", "dfs://kline");
// 在新集群执行: restoreDfsDatabase("/data/ddb/server/data/backup", "dfs://kline");
`, oldCluster, namespace, newCluster)
}
