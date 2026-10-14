package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"dolphindb.io/operator/api/v1alpha1"
)

// RenderedConfigs contains all rendered configuration files for a cluster
type RenderedConfigs struct {
	ControllerCfg  string
	ControllersCfg string
	ClusterCfg     string
	ClusterNodes   string
	DataNodeCfg    string
	ComputeNodeCfg string
	StandaloneCfg  string
}

// ConfigRenderer generates DolphinDB configuration files based on CR specifications
type ConfigRenderer struct {
	Cluster *v1alpha1.DolphinDBCluster
}

// NewConfigRenderer creates a new renderer instance
func NewConfigRenderer(cluster *v1alpha1.DolphinDBCluster) *ConfigRenderer {
	return &ConfigRenderer{Cluster: cluster}
}

// HeadlessServiceName returns the domain of the cluster's headless service
func (r *ConfigRenderer) HeadlessServiceName() string {
	return fmt.Sprintf("%s-headless", r.Cluster.Name)
}

// NodeDomain returns the FQDN for a node in the cluster
func (r *ConfigRenderer) NodeDomain(podName string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", podName, r.Cluster.Namespace)
}

// ControllerAlias returns standard alias for controller index
func (r *ConfigRenderer) ControllerAlias(index int32) string {
	if r.Cluster.Spec.Controller != nil {
		for _, b := range r.Cluster.Spec.Controller.NodeBindings {
			if b.Index == int(index) && b.Alias != "" {
				return b.Alias
			}
		}
	}
	return fmt.Sprintf("%s-ctl-%d", r.Cluster.Name, index)
}

// DataNodeAlias returns standard alias for datanode index
func (r *ConfigRenderer) DataNodeAlias(index int32) string {
	if r.Cluster.Spec.DataNode != nil {
		for _, b := range r.Cluster.Spec.DataNode.NodeBindings {
			if b.Index == int(index) && b.Alias != "" {
				return b.Alias
			}
		}
	}
	return fmt.Sprintf("%s-dn-%d", r.Cluster.Name, index)
}

// ComputeNodeAlias returns standard alias for computenode index
func (r *ConfigRenderer) ComputeNodeAlias(index int32) string {
	return fmt.Sprintf("%s-cn-%d", r.Cluster.Name, index)
}

// RenderAll generates all configuration files according to the cluster mode
func (r *ConfigRenderer) RenderAll() (*RenderedConfigs, error) {
	out := &RenderedConfigs{}

	if r.Cluster.Spec.Mode == v1alpha1.ModeStandalone {
		out.StandaloneCfg = r.renderStandalone()
		return out, nil
	}

	out.ClusterNodes = r.renderClusterNodes()
	out.ControllerCfg = r.renderControllerCfg()
	out.ControllersCfg = r.renderControllersCfg()
	out.ClusterCfg = r.renderClusterCfg()
	out.DataNodeCfg = r.renderDataNodeCfg()
	out.ComputeNodeCfg = r.renderComputeNodeCfg()

	return out, nil
}

// renderStandalone produces dolphindb.cfg for standalone mode
func (r *ConfigRenderer) renderStandalone() string {
	cfg := make(map[string]string)
	cfg["mode"] = "single"
	cfg["lanCluster"] = "0"
	cfg["dataSync"] = "0"
	cfg["maxConnections"] = "512"
	cfg["workerNum"] = "4"
	cfg["maxBatchJobWorker"] = "4"
	cfg["subExecutors"] = "4"

	if r.Cluster.Spec.Standalone != nil {
		port := r.Cluster.Spec.Standalone.Port
		if port == 0 {
			port = 8848
		}
		cfg["localSite"] = fmt.Sprintf("localhost:%d:local%d", port, port)

		for k, v := range r.Cluster.Spec.Standalone.Config {
			cfg[k] = v
		}
	} else {
		cfg["localSite"] = "localhost:8848:local8848"
	}

	return formatProps(cfg)
}

// renderClusterNodes produces cluster.nodes
// RULE: The first line must NOT be empty. CSV format: localSite,mode
func (r *ConfigRenderer) renderClusterNodes() string {
	var lines []string
	lines = append(lines, "localSite,mode")

	ctlReplicas := int32(1)
	ctlPort := int32(8900)
	if r.Cluster.Spec.Controller != nil {
		ctlReplicas = r.Cluster.Spec.Controller.Replicas
		if r.Cluster.Spec.Controller.Port > 0 {
			ctlPort = r.Cluster.Spec.Controller.Port
		}
	}

	// Register Controllers
	for i := int32(0); i < ctlReplicas; i++ {
		podName := fmt.Sprintf("%s-ctl-%d", r.Cluster.Name, i)
		alias := r.ControllerAlias(i)
		domain := r.NodeDomain(podName)
		lines = append(lines, fmt.Sprintf("%s:%d:%s,controller", domain, ctlPort, alias))
	}

	// Register DataNodes
	if r.Cluster.Spec.DataNode != nil {
		dnPort := int32(8901)
		if r.Cluster.Spec.DataNode.Port > 0 {
			dnPort = r.Cluster.Spec.DataNode.Port
		}
		for i := int32(0); i < r.Cluster.Spec.DataNode.Replicas; i++ {
			podName := fmt.Sprintf("%s-dn-%d", r.Cluster.Name, i)
			alias := r.DataNodeAlias(i)
			domain := r.NodeDomain(podName)
			lines = append(lines, fmt.Sprintf("%s:%d:%s,datanode", domain, dnPort, alias))
		}
	}

	// Register ComputeNodes
	if r.Cluster.Spec.ComputeNode != nil {
		cnPort := int32(8902)
		if r.Cluster.Spec.ComputeNode.Port > 0 {
			cnPort = r.Cluster.Spec.ComputeNode.Port
		}
		for i := int32(0); i < r.Cluster.Spec.ComputeNode.Replicas; i++ {
			podName := fmt.Sprintf("%s-cn-%d", r.Cluster.Name, i)
			alias := r.ComputeNodeAlias(i)
			domain := r.NodeDomain(podName)
			lines = append(lines, fmt.Sprintf("%s:%d:%s,computenode", domain, cnPort, alias))
		}
	}

	return strings.Join(lines, "\n") + "\n"
}

// renderControllerCfg produces controller.cfg
func (r *ConfigRenderer) renderControllerCfg() string {
	cfg := make(map[string]string)
	cfg["mode"] = "controller"
	cfg["lanCluster"] = "0"
	cfg["dataSync"] = "1"
	cfg["workerNum"] = "4"
	cfg["maxConnections"] = "2048"

	ctlReplicas := int32(1)
	if r.Cluster.Spec.Controller != nil {
		ctlReplicas = r.Cluster.Spec.Controller.Replicas
		if ctlReplicas >= 3 {
			cfg["dfsHAMode"] = "Raft"
			cfg["dfsReplicationFactor"] = "2"
			cfg["dfsReplicaReliabilityLevel"] = "1"
		} else {
			cfg["dfsReplicationFactor"] = "1"
			cfg["dfsReplicaReliabilityLevel"] = "0"
		}

		for k, v := range r.Cluster.Spec.Controller.Config {
			cfg[k] = v
		}
	}

	// License configuration
	if r.Cluster.Spec.License.Mode == v1alpha1.LicenseModeServer && r.Cluster.Spec.License.ServerAddress != "" {
		cfg["licenseServerSite"] = r.Cluster.Spec.License.ServerAddress
	}

	// Public name mapping if configured
	if r.Cluster.Spec.Networking.PublicIP != "" {
		cfg["publicName"] = r.Cluster.Spec.Networking.PublicIP
	}

	return formatProps(cfg)
}

// renderControllersCfg produces controllers.cfg for controller Raft group
func (r *ConfigRenderer) renderControllersCfg() string {
	ctlReplicas := int32(1)
	ctlPort := int32(8900)
	if r.Cluster.Spec.Controller != nil {
		ctlReplicas = r.Cluster.Spec.Controller.Replicas
		if r.Cluster.Spec.Controller.Port > 0 {
			ctlPort = r.Cluster.Spec.Controller.Port
		}
	}

	var sites []string
	for i := int32(0); i < ctlReplicas; i++ {
		podName := fmt.Sprintf("%s-ctl-%d", r.Cluster.Name, i)
		alias := r.ControllerAlias(i)
		domain := r.NodeDomain(podName)
		sites = append(sites, fmt.Sprintf("%s:%d:%s:controller", domain, ctlPort, alias))
	}

	return fmt.Sprintf("sites=%s\n", strings.Join(sites, ","))
}

// renderClusterProps generates the shared baseline cluster configuration properties
func (r *ConfigRenderer) renderClusterProps() map[string]string {
	cfg := make(map[string]string)
	cfg["lanCluster"] = "0"
	cfg["dataSync"] = "1"
	cfg["maxConnections"] = "2048"
	cfg["workerNum"] = "4"
	cfg["maxBatchJobWorker"] = "4"
	cfg["subExecutors"] = "4"
	cfg["newValuePartitionPolicy"] = "add"
	cfg["enableChunkGranularityConfig"] = "true"
	cfg["sites"] = r.RenderSites()

	// Streaming HA configuration
	if r.Cluster.Spec.StreamingHA.Enabled {
		cfg["streamingHAMode"] = "raft"
		cName := r.Cluster.Spec.StreamingHA.ClusterName
		if cName == "" {
			cName = strings.ReplaceAll(r.Cluster.Name, "-", "")
		}
		cfg["clusterName"] = cName
	}

	// Security baselines
	if r.Cluster.Spec.Security.StrictPermissionInspection {
		cfg["strictPermissionInspection"] = "true"
	}
	if !r.Cluster.Spec.Security.EnableShellFunction {
		cfg["enableShellFunction"] = "false"
	}

	return cfg
}

// renderClusterCfg produces cluster.cfg for DataNodes and ComputeNodes
func (r *ConfigRenderer) renderClusterCfg() string {
	cfg := r.renderClusterProps()

	var customVolsPattern string
	var customRedoPattern string
	var customMetaPattern string

	// Merge DataNode user config overrides
	if r.Cluster.Spec.DataNode != nil {
		for k, v := range r.Cluster.Spec.DataNode.Config {
			if k == "volumes" {
				customVolsPattern = v
			} else if k == "redoLogDir" {
				customRedoPattern = v
			} else if k == "chunkMetaDir" {
				customMetaPattern = v
			} else if !strings.Contains(v, "<ALIAS>") {
				cfg[k] = v
			}
		}

		dataVolCount := 1
		if r.Cluster.Spec.DataNode.Storage.DataVolumeCount > 0 {
			dataVolCount = r.Cluster.Spec.DataNode.Storage.DataVolumeCount
		}

		for i := int32(0); i < r.Cluster.Spec.DataNode.Replicas; i++ {
			alias := r.DataNodeAlias(i)

			// 1. volumes
			if customVolsPattern != "" {
				cfg[fmt.Sprintf("%s.volumes", alias)] = strings.ReplaceAll(customVolsPattern, "<ALIAS>", alias)
			} else {
				var vols []string
				for vIdx := 0; vIdx < dataVolCount; vIdx++ {
					vols = append(vols, fmt.Sprintf("/var/lib/dolphindb/data/vol-%d/%s", vIdx, alias))
				}
				cfg[fmt.Sprintf("%s.volumes", alias)] = strings.Join(vols, ",")
			}

			// 2. redoLogDir
			if customRedoPattern != "" {
				cfg[fmt.Sprintf("%s.redoLogDir", alias)] = strings.ReplaceAll(customRedoPattern, "<ALIAS>", alias)
			} else {
				cfg[fmt.Sprintf("%s.redoLogDir", alias)] = fmt.Sprintf("/var/lib/dolphindb/meta/redoLog/%s", alias)
			}

			// 3. chunkMetaDir
			if customMetaPattern != "" {
				cfg[fmt.Sprintf("%s.chunkMetaDir", alias)] = strings.ReplaceAll(customMetaPattern, "<ALIAS>", alias)
			} else {
				cfg[fmt.Sprintf("%s.chunkMetaDir", alias)] = fmt.Sprintf("/var/lib/dolphindb/meta/chunkMeta/%s", alias)
			}

			// 4. streamingHA persistenceDir
			if r.Cluster.Spec.StreamingHA.Enabled {
				cfg[fmt.Sprintf("%s.persistenceDir", alias)] = fmt.Sprintf("/var/lib/dolphindb/meta/streaming/%s", alias)
			}
		}
	}

	// Merge ComputeNode specific cache configs if compute nodes exist
	if r.Cluster.Spec.ComputeNode != nil && r.Cluster.Spec.ComputeNode.Replicas > 0 {
		for i := int32(0); i < r.Cluster.Spec.ComputeNode.Replicas; i++ {
			alias := r.ComputeNodeAlias(i)
			cfg[fmt.Sprintf("%s.computeNodeCacheDir", alias)] = fmt.Sprintf("/var/lib/dolphindb/cache/%s", alias)
		}
		if r.Cluster.Spec.ComputeNode.ComputeGroup != "" {
			cfg["computeGroup"] = r.Cluster.Spec.ComputeNode.ComputeGroup
		}
		for k, v := range r.Cluster.Spec.ComputeNode.Config {
			if !strings.Contains(v, "<ALIAS>") {
				cfg[k] = v
			}
		}
	}

	return formatProps(cfg)
}

// formatProps formats key-values deterministically
func formatProps(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("%s=%s\n", k, m[k]))
	}
	return sb.String()
}

// CalculateMaxMemSize implements official formula: limits * (1.0 - memoryReservedRatio)
func CalculateMaxMemSize(memLimitBytes int64, reservedRatio float64) int64 {
	if reservedRatio <= 0 || reservedRatio >= 1.0 {
		reservedRatio = 0.15
	}
	usable := float64(memLimitBytes) * (1.0 - reservedRatio)
	gb := int64(usable / (1024 * 1024 * 1024))
	if gb < 1 {
		return 1
	}
	return gb
}

// ParseMemoryString parses Gi/G string to gigabytes
func ParseMemoryString(val string) (int64, error) {
	val = strings.TrimSpace(strings.ToUpper(val))
	val = strings.TrimSuffix(val, "B")
	if strings.HasSuffix(val, "GI") {
		num, err := strconv.ParseInt(strings.TrimSuffix(val, "GI"), 10, 64)
		return num, err
	}
	if strings.HasSuffix(val, "G") {
		num, err := strconv.ParseInt(strings.TrimSuffix(val, "G"), 10, 64)
		return num, err
	}
	return strconv.ParseInt(val, 10, 64)
}

func (r *ConfigRenderer) renderDataNodeCfg() string {
	cfg := r.renderClusterProps()
	cfg["mode"] = "datanode"
	cfg["lanCluster"] = "0"
	cfg["sites"] = r.RenderSites()
	if r.Cluster.Spec.DataNode != nil {
		for k, v := range r.Cluster.Spec.DataNode.Config {
			if !strings.Contains(v, "<ALIAS>") && k != "volumes" && k != "redoLogDir" && k != "chunkMetaDir" {
				cfg[k] = v
			}
		}
	}
	return formatProps(cfg)
}

func (r *ConfigRenderer) renderComputeNodeCfg() string {
	cfg := r.renderClusterProps()
	cfg["mode"] = "computenode"
	cfg["lanCluster"] = "0"
	cfg["sites"] = r.RenderSites()
	if r.Cluster.Spec.ComputeNode != nil {
		if r.Cluster.Spec.ComputeNode.ComputeGroup != "" {
			cfg["computeGroup"] = r.Cluster.Spec.ComputeNode.ComputeGroup
		}
		for k, v := range r.Cluster.Spec.ComputeNode.Config {
			if !strings.Contains(v, "<ALIAS>") {
				cfg[k] = v
			}
		}
	}
	return formatProps(cfg)
}

// RenderSites produces comma-separated sites configuration:
// DataNodes: domain:port:alias
// ComputeNodes: domain:port:alias
// Controllers: domain:port:alias:controller
func (r *ConfigRenderer) RenderSites() string {
	var sites []string

	// 1. DataNodes
	if r.Cluster.Spec.DataNode != nil {
		dnPort := int32(8901)
		if r.Cluster.Spec.DataNode.Port > 0 {
			dnPort = r.Cluster.Spec.DataNode.Port
		}
		for i := int32(0); i < r.Cluster.Spec.DataNode.Replicas; i++ {
			podName := fmt.Sprintf("%s-dn-%d", r.Cluster.Name, i)
			alias := r.DataNodeAlias(i)
			domain := r.NodeDomain(podName)
			sites = append(sites, fmt.Sprintf("%s:%d:%s", domain, dnPort, alias))
		}
	}

	// 2. ComputeNodes
	if r.Cluster.Spec.ComputeNode != nil {
		cnPort := int32(8902)
		if r.Cluster.Spec.ComputeNode.Port > 0 {
			cnPort = r.Cluster.Spec.ComputeNode.Port
		}
		for i := int32(0); i < r.Cluster.Spec.ComputeNode.Replicas; i++ {
			podName := fmt.Sprintf("%s-cn-%d", r.Cluster.Name, i)
			alias := r.ComputeNodeAlias(i)
			domain := r.NodeDomain(podName)
			sites = append(sites, fmt.Sprintf("%s:%d:%s", domain, cnPort, alias))
		}
	}

	// 3. Controllers
	ctlReplicas := int32(1)
	ctlPort := int32(8900)
	if r.Cluster.Spec.Controller != nil {
		ctlReplicas = r.Cluster.Spec.Controller.Replicas
		if r.Cluster.Spec.Controller.Port > 0 {
			ctlPort = r.Cluster.Spec.Controller.Port
		}
	}
	for i := int32(0); i < ctlReplicas; i++ {
		podName := fmt.Sprintf("%s-ctl-%d", r.Cluster.Name, i)
		alias := r.ControllerAlias(i)
		domain := r.NodeDomain(podName)
		sites = append(sites, fmt.Sprintf("%s:%d:%s:controller", domain, ctlPort, alias))
	}

	return strings.Join(sites, ",")
}
