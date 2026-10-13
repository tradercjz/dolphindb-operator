package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OperationType defines supported Day-2 operations
// +kubebuilder:validation:Enum=DecommissionNode;StopNode;StartNode;RebalanceChunks;RollingRestart;ReloadLicense
type OperationType string

const (
	OpDecommissionNode OperationType = "DecommissionNode"
	OpStopNode         OperationType = "StopNode"
	OpStartNode        OperationType = "StartNode"
	OpRebalanceChunks  OperationType = "RebalanceChunks"
	OpRollingRestart   OperationType = "RollingRestart"
	OpReloadLicense    OperationType = "ReloadLicense"
)

// OperationPhase represents current state of the operation
type OperationPhase string

const (
	OpPhasePending   OperationPhase = "Pending"
	OpPhaseRunning   OperationPhase = "Running"
	OpPhaseSucceeded OperationPhase = "Succeeded"
	OpPhaseFailed    OperationPhase = "Failed"
)

// DDBOperationSpec defines the desired operation to execute
type DDBOperationSpec struct {
	// Cluster target DolphinDBCluster name
	Cluster string `json:"cluster"`

	// Type of operation to perform
	Type OperationType `json:"type"`

	// TargetNodes list of node aliases to operate on (e.g., ["quant-prod-dn-1"])
	TargetNodes []string `json:"targetNodes,omitempty"`

	// Parameters extra key-value parameters for the operation
	Parameters map[string]string `json:"parameters,omitempty"`
}

// DDBOperationStatus defines the progress and execution state
type DDBOperationStatus struct {
	// Phase current execution status
	Phase OperationPhase `json:"phase,omitempty"`

	// CurrentStep description of current active step (e.g., "3/4 WaitChunkEviction")
	CurrentStep string `json:"currentStep,omitempty"`

	// Progress human readable progress percentage or fraction
	Progress string `json:"progress,omitempty"`

	// Message details of current step or error message
	Message string `json:"message,omitempty"`

	// StartTime timestamp when operation began
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime timestamp when operation concluded
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions condition array
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ddbop,categories=dolphindb
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Step",type=string,JSONPath=`.status.currentStep`
// +kubebuilder:printcolumn:name="Progress",type=string,JSONPath=`.status.progress`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DDBOperation is the Schema for the ddboperations API
type DDBOperation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DDBOperationSpec   `json:"spec,omitempty"`
	Status DDBOperationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DDBOperationList contains a list of DDBOperation
type DDBOperationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DDBOperation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DDBOperation{}, &DDBOperationList{})
}
