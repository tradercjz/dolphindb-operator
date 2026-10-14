package migration

import (
	"time"

	"dolphindb.io/operator/api/v1alpha1"
)

// LegacyClusterInfo contains basic metadata of an identified legacy DolphinDB cluster
type LegacyClusterInfo struct {
	Name             string    `json:"name"`
	Namespace        string    `json:"namespace"`
	Mode             string    `json:"mode"`
	Version          string    `json:"version"`
	Status           string    `json:"status"`
	CreatedAt        time.Time `json:"createdAt"`
	NodeCount        int       `json:"nodeCount"`
	DataSize         string    `json:"dataSize"`
	StorageClassName string    `json:"storageClassName"`
}

// NodeGroupSummary describes resource limits and ports of a node group
type NodeGroupSummary struct {
	Replicas int32  `json:"replicas"`
	Port     int32  `json:"port"`
	CPU      string `json:"cpu"`
	Memory   string `json:"memory"`
	DataSize string `json:"dataSize"`
	LogSize  string `json:"logSize"`
}

// LegacyPodSummary summarizes a running pod under the legacy StatefulSet
type LegacyPodSummary struct {
	Name       string   `json:"name"`
	NodeName   string   `json:"nodeName"`
	IP         string   `json:"ip"`
	Phase      string   `json:"phase"`
	Ready      bool     `json:"ready"`
	Containers []string `json:"containers"`
	Restarts   int32    `json:"restarts"`
}

// LegacyPVCSummary describes an existing persistent volume claim
type LegacyPVCSummary struct {
	Name         string `json:"name"`
	Capacity     string `json:"capacity"`
	StorageClass string `json:"storageClass"`
	Status       string `json:"status"`
	VolumeName   string `json:"volumeName"`
	MountPath    string `json:"mountPath"`
}

// LegacyServiceSummary describes legacy services (e.g. NodePorts)
type LegacyServiceSummary struct {
	Name  string   `json:"name"`
	Type  string   `json:"type"`
	Ports []string `json:"ports"`
}

// LegacyClusterDetail is the comprehensive inspection result of a legacy cluster
type LegacyClusterDetail struct {
	Info             LegacyClusterInfo      `json:"info"`
	ControllerSpec   NodeGroupSummary       `json:"controllerSpec"`
	DataNodeSpec     NodeGroupSummary       `json:"dataNodeSpec"`
	ComputeNodeSpec  NodeGroupSummary       `json:"computeNodeSpec"`
	StorageClassName string                 `json:"storageClassName"`
	Pods             []LegacyPodSummary     `json:"pods"`
	PVCs             []LegacyPVCSummary     `json:"pvcs"`
	CustomConfigs    map[string]string      `json:"customConfigs"`
	License          string                 `json:"license"`
	StatefulSets     []string               `json:"statefulSets"`
	Services         []LegacyServiceSummary `json:"services"`
}

// FindingSeverity indicates the risk level of an assessment finding
type FindingSeverity string

const (
	SeverityInfo     FindingSeverity = "INFO"
	SeverityWarning  FindingSeverity = "WARNING"
	SeverityCritical FindingSeverity = "CRITICAL"
)

// PreflightFinding represents an individual risk or observation
type PreflightFinding struct {
	Category       string          `json:"category"` // Workload, Storage, Networking, Config, Architecture
	Severity       FindingSeverity `json:"severity"`
	Title          string          `json:"title"`
	Detail         string          `json:"detail"`
	Recommendation string          `json:"recommendation"`
}

// ResourceComparison highlights resource footprint optimization
type ResourceComparison struct {
	LegacyArchitecture string `json:"legacyArchitecture"`
	NewArchitecture    string `json:"newArchitecture"`
	MemorySavedPerPod  string `json:"memorySavedPerPod"`
	SidecarReduction   string `json:"sidecarReduction"`
}

// StorageSummary aggregates storage consumption
type StorageSummary struct {
	TotalVolumes  int      `json:"totalVolumes"`
	TotalCapacity string   `json:"totalCapacity"`
	StorageClass  string   `json:"storageClass"`
	VolumeNames   []string `json:"volumeNames"`
}

// AssessmentReport is the pre-flight compatibility evaluation
type AssessmentReport struct {
	HealthScore          int                 `json:"healthScore"` // 0-100
	RiskLevel            string              `json:"riskLevel"`   // LOW, MEDIUM, HIGH
	Findings             []PreflightFinding  `json:"findings"`
	ResourceComparison   ResourceComparison  `json:"resourceComparison"`
	StorageSummary       StorageSummary      `json:"storageSummary"`
	CustomConfigs        map[string]string   `json:"customConfigs"`
	RecommendedStrategy  string              `json:"recommendedStrategy"` // BlueGreen or InPlace
	RecommendationReason string              `json:"recommendationReason"`
}

// ConversionOptions specifies parameters for converting to the new CRD
type ConversionOptions struct {
	TargetMode      string   `json:"targetMode"` // "blue-green" or "in-place"
	TargetVersion   string   `json:"targetVersion"`
	NewClusterName  string   `json:"newClusterName"`
	TargetNamespace string   `json:"targetNamespace"`
	UseSecretLicense bool    `json:"useSecretLicense"`
	ExistingPVCs    []string `json:"existingPVCs"`
}

// AdoptionStep is an actionable step for in-place migration
type AdoptionStep struct {
	StepID               string `json:"stepId"`
	Title                string `json:"title"`
	Description          string `json:"description"`
	Command              string `json:"command"`
	Status               string `json:"status"` // pending, running, completed, failed, skipped
	Message              string `json:"message"`
	RequiresConfirmation bool   `json:"requiresConfirmation"`
}

// ConversionResult contains the generated artifacts and scripts
type ConversionResult struct {
	TargetMode    string                     `json:"targetMode"`
	TargetCluster v1alpha1.DolphinDBCluster  `json:"targetCluster"`
	TargetYAML    string                     `json:"targetYaml"`
	SyncScript    string                     `json:"syncScript"`
	AdoptionSteps []AdoptionStep             `json:"adoptionSteps"`
	WarningNotes  []string                   `json:"warningNotes"`
}

// StepResult represents the outcome of an automated execution step
type StepResult struct {
	StepID  string `json:"stepId"`
	Success bool   `json:"success"`
	Output  string `json:"output"`
	Error   string `json:"error,omitempty"`
}
