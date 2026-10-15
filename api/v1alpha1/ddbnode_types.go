package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeRole represents the role of a DolphinDB instance
// +kubebuilder:validation:Enum=controller;datanode;computenode;single
type NodeRole string

const (
	NodeRoleController  NodeRole = "controller"
	NodeRoleDataNode    NodeRole = "datanode"
	NodeRoleComputeNode NodeRole = "computenode"
	NodeRoleSingle      NodeRole = "single"
)

// DDBNodePhase represents the lifecycle state of a DDBNode
type DDBNodePhase string

const (
	NodePhasePending  DDBNodePhase = "Pending"
	NodePhaseCreating DDBNodePhase = "Creating"
	NodePhaseRunning  DDBNodePhase = "Running"
	NodePhaseEvicting DDBNodePhase = "Evicting"
	NodePhaseOffline  DDBNodePhase = "Offline"
	NodePhaseFailed   DDBNodePhase = "Failed"
)

// DDBNodeSpec defines the desired state of a single DDBNode instance
type DDBNodeSpec struct {
	// ClusterName references the parent DolphinDBCluster
	ClusterName string `json:"clusterName"`

	// Role of this instance
	Role NodeRole `json:"role"`

	// Index ordinal index within the role (e.g., 0, 1, 2)
	Index int32 `json:"index"`

	// Alias is the unique DolphinDB site alias (e.g., quant-dn-0)
	Alias string `json:"alias"`

	// Port listening port
	Port int32 `json:"port"`

	// Version image version
	Version string `json:"version"`

	// Image full container image
	Image string `json:"image"`

	// NodeName targeted physical host if pinned
	NodeName string `json:"nodeName,omitempty"`

	// Hostname override if pinned
	Hostname string `json:"hostname,omitempty"`

	// ControllerSite is the FQDN:port:alias of the controller master
	ControllerSite string `json:"controllerSite,omitempty"`

	// Sites is the comma-separated list of all cluster sites
	Sites string `json:"sites,omitempty"`

	// Resources CPU and Memory requirements
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Storage specification for meta and data PVCs
	Storage StorageSpec `json:"storage,omitempty"`

	// Fingerprint hardware fingerprint mount settings
	Fingerprint *MachineFingerprintSpec `json:"fingerprint,omitempty"`

	// AntiAffinity spreading strength
	AntiAffinity AntiAffinityMode `json:"antiAffinity,omitempty"`

	// Tolerations pod tolerations
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector pod node selector
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// ConfigVolumeName configmap or secret containing node config
	ConfigVolumeName string `json:"configVolumeName,omitempty"`

	// LicenseSecretName name of secret containing dolphindb.lic
	LicenseSecretName string `json:"licenseSecretName,omitempty"`

	// Logging runtime logging configuration
	Logging *LoggingSpec `json:"logging,omitempty"`

	// Paused when true, container process is kept stopped without deleting PVCs
	// +kubebuilder:default=false
	Paused bool `json:"paused,omitempty"`
}

// VolumeBinding tracks PVC status
type VolumeBinding struct {
	Name      string `json:"name"`
	PVCName   string `json:"pvcName"`
	MountPath string `json:"mountPath"`
	Bound     bool   `json:"bound"`
}

// DDBNodeStatus defines the observed state of DDBNode
type DDBNodeStatus struct {
	// Phase current lifecycle phase
	Phase DDBNodePhase `json:"phase,omitempty"`

	// PodName name of the managed Pod
	PodName string `json:"podName,omitempty"`

	// PodIP IP of the managed Pod
	PodIP string `json:"podIP,omitempty"`

	// HostIP node IP where pod is scheduled
	HostIP string `json:"hostIP,omitempty"`

	// NodeName physical node where pod is placed
	NodeName string `json:"nodeName,omitempty"`

	// IsMaster true if this controller is the active Raft Master
	IsMaster bool `json:"isMaster,omitempty"`

	// DDBState internal state in getClusterPerf() (0=init, 1=alive, 2=dead)
	DDBState int32 `json:"ddbState,omitempty"`

	// ChunksCount total number of DFS chunks stored on this node
	ChunksCount int32 `json:"chunksCount,omitempty"`

	// Volumes list of managed PVCs
	Volumes []VolumeBinding `json:"volumes,omitempty"`

	// Conditions condition array
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ddbn,categories=dolphindb
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.role`
// +kubebuilder:printcolumn:name="Alias",type=string,JSONPath=`.spec.alias`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.nodeName`
// +kubebuilder:printcolumn:name="PodIP",type=string,JSONPath=`.status.podIP`
// +kubebuilder:printcolumn:name="DDBState",type=integer,JSONPath=`.status.ddbState`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DDBNode is the Schema for the ddbnodes API
type DDBNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DDBNodeSpec   `json:"spec,omitempty"`
	Status DDBNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DDBNodeList contains a list of DDBNode
type DDBNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DDBNode `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DDBNode{}, &DDBNodeList{})
}
