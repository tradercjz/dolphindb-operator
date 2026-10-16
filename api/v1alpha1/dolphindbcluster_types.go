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

	// LicensePath is the absolute path of the licence file on the host, used only
	// for host-bound (machineFingerprint) licensing. It has no default: supply it
	// explicitly, otherwise the licence is taken from a Secret or from the image.
	LicensePath string `json:"licensePath,omitempty"`
}

// LicenseSpec defines licensing configuration
type LicenseSpec struct {
	// Mode defines the license type: server, secret, or machineFingerprint
	//
	// The default is `secret` rather than `machineFingerprint`: host-bound
	// licensing requires hostPath mounts (/etc/machine-id, /etc/hostname) and a
	// licence file that exists on the node, which is rarely true on Kubernetes and
	// never true on a managed cloud. Opting in explicitly is safer than surprising
	// users with privileged mounts that fail to start.
	// +kubebuilder:default="secret"
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

	// TLS enables DolphinDB's native HTTPS for the Web UI and REST/WebSocket API.
	TLS *TLSSpec `json:"tls,omitempty"`
}

// TLSSpec configures DolphinDB native HTTPS (enableHTTPS).
//
// DolphinDB terminates TLS itself, on the same port as plain HTTP, using a
// certificate found in the node's `<home>/keys` directory (server.crt +
// serverPrivate.key). That makes it the one HTTPS option that needs neither a
// registered domain, nor ACM, nor a cloud load balancer controller, nor IRSA - it
// works identically on a laptop and on a locked-down cloud project.
//
// Because `enableHTTPS` requires `publicName` to be a *domain* (not an IP),
// the operator derives one when the field is left empty: the exposed load
// balancer hostname once it exists, otherwise the in-cluster service DNS name.
type TLSSpec struct {
	// Enabled turns on DolphinDB native HTTPS.
	Enabled bool `json:"enabled,omitempty"`

	// SecretName references an existing kubernetes.io/tls Secret (keys tls.crt and
	// tls.key). Use this with a certificate you own - for example one issued for a
	// domain you control, or an ACM-exported certificate. When empty the operator
	// issues and rotates its own self-signed certificate.
	SecretName string `json:"secretName,omitempty"`

	// PublicName is the external domain clients use. DolphinDB requires a domain
	// (never an IP) when enableHTTPS is on. When empty the operator derives one.
	PublicName string `json:"publicName,omitempty"`

	// ValidityDays is the lifetime of the operator-managed self-signed certificate.
	// +kubebuilder:default=825
	ValidityDays int `json:"validityDays,omitempty"`
}

// LoggingSpec defines runtime logging configuration
type LoggingSpec struct {
	// StdoutLog controls stdout vs file logging:
	// 0: output to log file only (no stdout)
	// 1: output to stdout only (no file)
	// 2: output to both log file and stdout (dual write, default)
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2
	StdoutLog *int32 `json:"stdoutLog,omitempty"`

	// LogFile custom path for log file (defaults to /data/ddb/server/log/<alias>.log)
	LogFile string `json:"logFile,omitempty"`

	// MaxLogSize single log file size limit in MB before rotation (defaults to 200MB)
	// +kubebuilder:default=200
	MaxLogSize int32 `json:"maxLogSize,omitempty"`

	// LogLevel logging severity: DEBUG, INFO, WARN, ERROR (defaults to INFO)
	// +kubebuilder:default="INFO"
	LogLevel string `json:"logLevel,omitempty"`
}

// ExposureMode defines how DolphinDB is exposed externally
// +kubebuilder:validation:Enum=Gateway;NodePortDirect;ClusterIP
type ExposureMode string

const (
	ExposureGateway        ExposureMode = "Gateway"
	ExposureNodePortDirect ExposureMode = "NodePortDirect"
	ExposureClusterIP      ExposureMode = "ClusterIP"
)

// ExposeSpec selects which cluster components are reachable from outside.
//
// Defaults come from the platform: on a managed cloud nothing is exposed until it
// is asked for, because every exposed component costs a load balancer, whereas on
// bare metal the traditional NodePort behaviour is preserved.
type ExposeSpec struct {
	// Controller exposes port 8900: the Web console and the management API.
	// A controller holds cluster metadata and heartbeats; it cannot run DFS
	// operations itself, so this is the *management* entry point.
	Controller bool `json:"controller,omitempty"`

	// DataNode exposes port 8901, where DFS reads and writes actually execute.
	// Needed for real data work: a controller rejects database()/loadTable().
	DataNode bool `json:"datanode,omitempty"`

	// ComputeNode exposes port 8902, the stateless query tier.
	ComputeNode bool `json:"computenode,omitempty"`
}

// Any reports whether at least one component is selected.
func (e *ExposeSpec) Any() bool {
	return e != nil && (e.Controller || e.DataNode || e.ComputeNode)
}

// Names lists the selected components, for messages and status.
func (e *ExposeSpec) Names() []string {
	if e == nil {
		return nil
	}
	var out []string
	if e.Controller {
		out = append(out, "controller")
	}
	if e.DataNode {
		out = append(out, "datanode")
	}
	if e.ComputeNode {
		out = append(out, "computenode")
	}
	return out
}

// NetworkingSpec defines external access and publicName mapping
type NetworkingSpec struct {
	// ExposureMode defines the external exposure strategy
	// +kubebuilder:default="NodePortDirect"
	ExposureMode ExposureMode `json:"exposureMode,omitempty"`

	// ServiceType is the Kubernetes Service type used for the components selected by
	// `expose`.
	//
	// Leave it empty to let the operator decide. The old default was NodePort, which
	// on a cloud produced an access URL that looked public but was firewalled off by
	// the node security group - the operator never opens that group, so the link was
	// dead on arrival.
	ServiceType corev1.ServiceType `json:"serviceType,omitempty"`

	// Expose selects which components get an external entry point.
	//
	// Each selected component needs its own Service, and therefore its own cloud
	// load balancer: a Kubernetes Service can only target one pod set, so a single
	// load balancer cannot serve controllers and data nodes at once.
	Expose *ExposeSpec `json:"expose,omitempty"`

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

	// Logging runtime logging configuration (defaults to dual write: stdoutLog=2)
	Logging *LoggingSpec `json:"logging,omitempty"`

	// Scheduling global node selection and tolerations
	Scheduling SchedulingSpec `json:"scheduling,omitempty"`

	// Suspended pauses the cluster, gracefully stopping all node pods while preserving PVCs and data.
	// When true, all compute and storage node pods are terminated to achieve scale-to-zero and eliminate cloud compute costs.
	// When toggled back to false, the operator resumes all nodes and restores service.
	// +kubebuilder:default=false
	Suspended bool `json:"suspended,omitempty"`
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
	PhaseSuspended ClusterPhase = "Suspended"
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
