package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// ClusterNodesDesired tracks the desired number of DolphinDB nodes
	ClusterNodesDesired = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dolphindb_cluster_nodes_desired",
			Help: "Number of desired DolphinDB nodes by role",
		},
		[]string{"cluster", "namespace", "role"},
	)

	// ClusterNodesReady tracks the ready number of DolphinDB nodes
	ClusterNodesReady = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dolphindb_cluster_nodes_ready",
			Help: "Number of ready DolphinDB nodes by role",
		},
		[]string{"cluster", "namespace", "role"},
	)

	// ClusterHealthStatus tracks cluster readiness (1=Ready, 0=Not Ready)
	ClusterHealthStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dolphindb_cluster_health",
			Help: "Health status of DolphinDB cluster (1=Ready, 0=Not Ready)",
		},
		[]string{"cluster", "namespace", "phase"},
	)

	// NodeState tracks individual node live state (1=alive, 0=dead)
	NodeState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dolphindb_node_state",
			Help: "Internal DolphinDB node state from controller (1=alive, 0=dead)",
		},
		[]string{"cluster", "namespace", "node", "role"},
	)

	// BackupTotalChunks records chunks backed up
	BackupTotalChunks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dolphindb_backup_chunks_total",
			Help: "Total chunks successfully backed up in last execution",
		},
		[]string{"cluster", "namespace", "backup"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		ClusterNodesDesired,
		ClusterNodesReady,
		ClusterHealthStatus,
		NodeState,
		BackupTotalChunks,
	)
}
