package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterMode defines the mode of DolphinDB deployment
// +kubebuilder:validation:Enum=standalone;cluster
type ClusterMode string

const (
	ModeStandalone ClusterMode = "standalone"
	ModeCluster    ClusterMode = "cluster"
)

// LicenseMode defines the mode of License
// +kubebuilder:validation:Enum=server;secret;machineFingerprint
type LicenseMode string

const (
	LicenseModeServer             LicenseMode = "server"
	LicenseModeSecret             LicenseMode = "secret"
	LicenseModeMachineFingerprint LicenseMode = "machineFingerprint"
)

// AntiAffinityMode defines pod anti-affinity strength
// +kubebuilder:validation:Enum=Disabled;Preferred;Required;ZonePreferred;ZoneRequired
type AntiAffinityMode string

const (
	AntiAffinityDisabled      AntiAffinityMode = "Disabled"
	AntiAffinityPreferred     AntiAffinityMode = "Preferred"
	AntiAffinityRequired      AntiAffinityMode = "Required"
	AntiAffinityZonePreferred AntiAffinityMode = "ZonePreferred"
	AntiAffinityZoneRequired  AntiAffinityMode = "ZoneRequired"
)

// MachineFingerprintSpec defines hardware fingerprint passthrough settings
type MachineFingerprintSpec struct {
	// AutoPassthrough automatically mounts /etc/machine-id and sets hostname to node name
	// +kubebuilder:default=true
	AutoPassthrough bool `json:"autoPassthrough,omitempty"`

	// MountMachineId mounts host /etc/machine-id to container
	// +kubebuilder:default=true
	MountMachineId bool `json:"mountMachineId,omitempty"`

	// VirtualMachineId specifies a fixed 32-hex machine-id to inject into container /etc/machine-id (for cloud VMs/drift resistance)
	VirtualMachineId string `json:"virtualMachineId,omitempty"`

	// MachineIdSecretName specifies the secret containing machine-id key to mount into /etc/machine-id
	MachineIdSecretName string `json:"machineIdSecretName,omitempty"`

	// MountHostEtc mounts host /etc to container /dolphindb/etc
	// +kubebuilder:default=true
	MountHostEtc bool `json:"mountHostEtc,omitempty"`

	// MountDMI mounts host /sys/class/dmi/id for system UUID
	// +kubebuilder:default=false
	MountDMI bool `json:"mountDMI,omitempty"`

	// UseHostName sets container hostname to host node name
	// +kubebuilder:default=true
	UseHostName bool `json:"useHostName,omitempty"`

	// Hostname manual override if not using host node name
	Hostname string `json:"hostname,omitempty"`

	// NodeName manual node pinning if specified
	NodeName string `json:"nodeName,omitempty"`

	// LicensePath optional host license file path (defaults to /home/jzchen/dolphindb.lic)
	LicensePath string `json:"licensePath,omitempty"`
}

// LicenseSpec defines licensing configuration
type LicenseSpec struct {
	// Mode defines the license type: server, secret, or machineFingerprint
	// +kubebuilder:default="machineFingerprint"
	Mode LicenseMode `json:"mode,omitempty"`

	// SecretName is the K8s Secret containing dolphindb.lic
	// +kubebuilder:default="dolphindb-license"
	SecretName string `json:"secretName,omitempty"`

	// ServerAddress is the DolphinDB License Server site (host:port) when mode is server
	ServerAddress string `json:"serverAddress,omitempty"`

	// AutoReload hot-reloads license across all nodes via updateLicense() RPC when Secret changes
	// +kubebuilder:default=true
	AutoReload bool `json:"autoReload,omitempty"`

	// MachineFingerprint hardware fingerprint settings for host-bound licenses
	MachineFingerprint *MachineFingerprintSpec `json:"machineFingerprint,omitempty"`
}

// SecuritySpec defines credentials and security policies
type SecuritySpec struct {
	// AdminSecretName stores the admin password (secret key: password)
	// +kubebuilder:default="dolphindb-admin"
	AdminSecretName string `json:"adminSecretName,omitempty"`

	// StrictPermissionInspection enables strict ACL inspection
	// +kubebuilder:default=true
	StrictPermissionInspection bool `json:"strictPermissionInspection,omitempty"`

	// EnableShellFunction allows or denies the dangerous shell() function in DDB
	// +kubebuilder:default=false
	EnableShellFunction bool `json:"enableShellFunction,omitempty"`
}

// ExposureMode defines how DolphinDB is exposed externally
// +kubebuilder:validation:Enum=Gateway;NodePortDirect;ClusterIP
type ExposureMode string

const (
	ExposureGateway        ExposureMode = "Gateway"
	ExposureNodePortDirect ExposureMode = "NodePortDirect"
	ExposureClusterIP      ExposureMode = "ClusterIP"
)

// NetworkingSpec defines external access and publicName mapping
type NetworkingSpec struct {
	// ExposureMode defines the external exposure strategy
	// +kubebuilder:default="NodePortDirect"
	ExposureMode ExposureMode `json:"exposureMode,omitempty"`

	// ServiceType defines K8s service type
	// +kubebuilder:default="NodePort"
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`

	// PublicIP or domain to inject into publicName config for Web UI redirect
	PublicIP string `json:"publicIP,omitempty"`

	// Annotations allows passing custom annotations to external Services (e.g. Alibaba Cloud SLB / AWS NLB annotations)
	Annotations map[string]string `json:"annotations,omitempty"`
}

// StreamingHASpec defines streaming replication and Raft groups
type StreamingHASpec struct {
	// Enabled toggles streaming Raft HA
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// RaftGroupId specifies the streaming Raft group ID
	// +kubebuilder:default=2
	RaftGroupId int `json:"raftGroupId,omitempty"`

	// ClusterName normalized name without hyphens required by DDB streaming HA
	ClusterName string `json:"clusterName,omitempty"`
}

// FailoverSpec defines automatic node recovery parameters
type FailoverSpec struct {
	// AutoFailover enables replacement of permanently dead nodes
	// +kubebuilder:default=false
	AutoFailover bool `json:"autoFailover,omitempty"`

	// FailoverPeriod is the wait duration before declaring a node dead (e.g. 15m)
	// +kubebuilder:default="15m"
	FailoverPeriod string `json:"failoverPeriod,omitempty"`

	// MaxFailoverCount maximum number of dead nodes that can be automatically replaced
	// +kubebuilder:default=1
	MaxFailoverCount int `json:"maxFailoverCount,omitempty"`
}

// StorageSpec defines storage layout and sizing for a node group
type StorageSpec struct {
	// StorageClassName to use for PVCs
	StorageClassName *string `json:"storageClassName,omitempty"`

	// MetaVolumeSize for redo logs, chunk metadata and raft state
	// +kubebuilder:default="20Gi"
	MetaVolumeSize string `json:"metaVolumeSize,omitempty"`

	// DataVolumeCount number of independent data volumes (JBOD)
	// +kubebuilder:default=1
	DataVolumeCount int `json:"dataVolumeCount,omitempty"`

	// DataVolumeSize size of each data volume
	// +kubebuilder:default="100Gi"
	DataVolumeSize string `json:"dataVolumeSize,omitempty"`

	// ExistingClaims specifies names of pre-existing PVCs to adopt
	ExistingClaims []string `json:"existingClaims,omitempty"`
}

// NodeBinding maps an indexed instance to a specific physical node
type NodeBinding struct {
	// Index is the zero-based replica index (e.g., 0 for dn-0)
	Index int `json:"index"`

	// NodeName is the K8s node name
	NodeName string `json:"nodeName"`

	// Hostname override for license fingerprint
	Hostname string `json:"hostname,omitempty"`

	// LicenseSecretName override license secret per node
	LicenseSecretName string `json:"licenseSecretName,omitempty"`

	// Alias is the DolphinDB node alias override (e.g. node0, controller0)
	Alias string `json:"alias,omitempty"`

	// ClaimName specifies an existing PVC to adopt for this replica
	ClaimName string `json:"claimName,omitempty"`
}

// StandaloneSpec defines the single-node deployment profile
type StandaloneSpec struct {
	// Port listening port
	// +kubebuilder:default=8848
	Port int32 `json:"port,omitempty"`

	// NodeName targeted physical host if pinned
	NodeName string `json:"nodeName,omitempty"`

	// Hostname container hostname override
	Hostname string `json:"hostname,omitempty"`

	// Resources compute resource limits and requests
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage size and class for standalone data & metadata
	Storage StorageSpec `json:"storage,omitempty"`

	// Config key-value configuration overrides for dolphindb.cfg
	Config map[string]string `json:"config,omitempty"`
}

// ControllerGroupSpec defines configuration for controller nodes
type ControllerGroupSpec struct {
	// Replicas number of controllers (1 for standalone/single cluster, 3 or 5 for Raft HA)
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas"`

	// Port listening port
	// +kubebuilder:default=8900
	Port int32 `json:"port,omitempty"`

	// AntiAffinity spreading across physical hosts
	// +kubebuilder:default="Preferred"
	AntiAffinity AntiAffinityMode `json:"antiAffinity,omitempty"`

	// MemoryReservedRatio ratio of memory reserved for OS PageCache (e.g., 0.15 = 15%)
	// +kubebuilder:default="0.15"
	MemoryReservedRatio string `json:"memoryReservedRatio,omitempty"`

	// Resources CPU/Memory limits and requests
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage size and class for raft metadata and configs
	Storage StorageSpec `json:"storage,omitempty"`

	// NodeBindings explicit node pinning per replica index
	NodeBindings []NodeBinding `json:"nodeBindings,omitempty"`

	// Config key-value overrides for controller.cfg
	Config map[string]string `json:"config,omitempty"`
}

// DataNodeGroupSpec defines configuration for data nodes
type DataNodeGroupSpec struct {
	// Replicas number of data nodes
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas"`

	// Port listening port
	// +kubebuilder:default=8901
	Port int32 `json:"port,omitempty"`

	// AntiAffinity spreading across physical hosts
	// +kubebuilder:default="Required"
	AntiAffinity AntiAffinityMode `json:"antiAffinity,omitempty"`

	// MemoryReservedRatio ratio of memory reserved for OS PageCache
	// +kubebuilder:default="0.15"
	MemoryReservedRatio string `json:"memoryReservedRatio,omitempty"`

	// Resources CPU/Memory limits and requests
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage layout (meta volume + JBOD data volumes)
	Storage StorageSpec `json:"storage,omitempty"`

	// NodeBindings explicit node pinning per replica index
	NodeBindings []NodeBinding `json:"nodeBindings,omitempty"`

	// Config key-value overrides for cluster.cfg (applied to data nodes)
	Config map[string]string `json:"config,omitempty"`
}

// ComputeNodeGroupSpec defines configuration for compute nodes
type ComputeNodeGroupSpec struct {
	// Replicas number of compute nodes (0..N)
	// +kubebuilder:default=0
	Replicas int32 `json:"replicas"`

	// Port listening port
	// +kubebuilder:default=8902
	Port int32 `json:"port,omitempty"`

	// ComputeGroup group name for tenant or query isolation
	ComputeGroup string `json:"computeGroup,omitempty"`

	// Resources CPU/Memory limits and requests
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage local cache disk sizing
	Storage StorageSpec `json:"storage,omitempty"`

	// Config key-value overrides for cluster.cfg (applied to compute nodes)
	Config map[string]string `json:"config,omitempty"`
}

// ScriptsSpec defines lifecycle DOS script extensions
type ScriptsSpec struct {
	// InitScript executed during engine initialization (dolphindb.dos)
	InitScript string `json:"initScript,omitempty"`

	// StartupScript executed after engine startup (startup.dos)
	StartupScript string `json:"startupScript,omitempty"`

	// PostStartScript executed after scheduled job engine starts (postStart.dos)
	PostStartScript string `json:"postStartScript,omitempty"`
}

// SchedulingSpec defines global scheduling policies
type SchedulingSpec struct {
	// NodeSelector restricts pods to nodes with specified labels
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allows pods to run on tainted nodes
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

// DolphinDBClusterSpec defines the desired state of DolphinDBCluster
type DolphinDBClusterSpec struct {
	// Version of DolphinDB Server image (e.g., 3.00.5)
	// +kubebuilder:default="3.00.5"
	Version string `json:"version"`

	// ImageRepository base repository (e.g., 192.168.100.250/dolphindb)
	// +kubebuilder:default="dolphindb"
	ImageRepository string `json:"imageRepository,omitempty"`

	// Image optional full image name override (e.g., 192.168.100.250/dolphindb/dolphindb:v3.00.5)
	Image string `json:"image,omitempty"`

	// Mode deployment mode: standalone or cluster
	// +kubebuilder:default="cluster"
	Mode ClusterMode `json:"mode"`

	// Standalone configuration (used when mode == standalone)
	Standalone *StandaloneSpec `json:"standalone,omitempty"`

	// Controller group configuration
	Controller *ControllerGroupSpec `json:"controller,omitempty"`

	// DataNode group configuration
	DataNode *DataNodeGroupSpec `json:"datanode,omitempty"`

	// ComputeNode group configuration
	ComputeNode *ComputeNodeGroupSpec `json:"computenode,omitempty"`

	// License configuration (server, secret, or machineFingerprint)
	License LicenseSpec `json:"license,omitempty"`

	// Security configuration (admin credentials & ACL)
	Security SecuritySpec `json:"security,omitempty"`

	// Networking external access configuration
	Networking NetworkingSpec `json:"networking,omitempty"`

	// StreamingHA streaming Raft configuration
	StreamingHA StreamingHASpec `json:"streamingHA,omitempty"`

	// Failover safeguard configuration
	Failover FailoverSpec `json:"failover,omitempty"`

	// Scripts lifecycle DOS scripts
	Scripts ScriptsSpec `json:"scripts,omitempty"`

	// PreloadModules modules and plugins to preload at startup
	PreloadModules []string `json:"preloadModules,omitempty"`

	// Scheduling global node selection and tolerations
	Scheduling SchedulingSpec `json:"scheduling,omitempty"`
}

// ClusterPhase defines the lifecycle phase of the cluster
type ClusterPhase string

const (
	PhasePending   ClusterPhase = "Pending"
	PhaseCreating  ClusterPhase = "Creating"
	PhaseReady     ClusterPhase = "Ready"
	PhaseScaling   ClusterPhase = "Scaling"
	PhaseUpgrading ClusterPhase = "Upgrading"
	PhaseRolling   ClusterPhase = "RollingRestart"
	PhaseDegraded  ClusterPhase = "Degraded"
)

// TopologyStatus summaries running instances
type TopologyStatus struct {
	Desired int32 `json:"desired"`
	Ready   int32 `json:"ready"`
}

// DolphinDBClusterStatus defines the observed state of DolphinDBCluster
type DolphinDBClusterStatus struct {
	// Phase current high-level state
	Phase ClusterPhase `json:"phase,omitempty"`

	// ObservedGeneration generation observed by operator
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ActiveFlow current DAG task flow in progress
	ActiveFlow string `json:"activeFlow,omitempty"`

	// FlowProgress percentage completion of active flow
	FlowProgress string `json:"flowProgress,omitempty"`

	// MasterController current active Raft master controller pod
	MasterController string `json:"masterController,omitempty"`

	// Controller topology status
	Controller TopologyStatus `json:"controller,omitempty"`

	// DataNode topology status
	DataNode TopologyStatus `json:"datanode,omitempty"`

	// ComputeNode topology status
	ComputeNode TopologyStatus `json:"computenode,omitempty"`

	// Conditions standard condition array
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ddbc,categories=dolphindb
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Controller",type=string,JSONPath=`.status.controller.ready`
// +kubebuilder:printcolumn:name="DataNode",type=string,JSONPath=`.status.datanode.ready`
// +kubebuilder:printcolumn:name="Master",type=string,JSONPath=`.status.masterController`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DolphinDBCluster is the Schema for the dolphindbclusters API
type DolphinDBCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DolphinDBClusterSpec   `json:"spec,omitempty"`
	Status DolphinDBClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DolphinDBClusterList contains a list of DolphinDBCluster
type DolphinDBClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DolphinDBCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DolphinDBCluster{}, &DolphinDBClusterList{})
}
