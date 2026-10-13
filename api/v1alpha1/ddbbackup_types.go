package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupPhase defines the state of a backup execution
type BackupPhase string

const (
	BackupPhasePending   BackupPhase = "Pending"
	BackupPhaseRunning   BackupPhase = "Running"
	BackupPhaseCompleted BackupPhase = "Completed"
	BackupPhaseFailed    BackupPhase = "Failed"
)

// DDBBackupSpec defines the desired backup action
type DDBBackupSpec struct {
	// Cluster target DolphinDBCluster name
	Cluster string `json:"cluster"`

	// Databases list of DFS database URLs to back up (e.g., ["dfs://k8sScaleTestDB"])
	Databases []string `json:"databases"`

	// Tables optional specific table names to back up
	Tables []string `json:"tables,omitempty"`

	// BackupPath destination path for backup files (defaults to /data/ddb/backup/<cluster>/<name>)
	BackupPath string `json:"backupPath,omitempty"`

	// Force whether to overwrite existing backup files
	Force bool `json:"force,omitempty"`

	// Parallel whether to execute backup in parallel
	Parallel bool `json:"parallel,omitempty"`

	// Snapshot whether to take snapshot backup
	Snapshot *bool `json:"snapshot,omitempty"`

	// Schedule optional cron expression for recurring backups (e.g., "0 2 * * *")
	Schedule string `json:"schedule,omitempty"`
}

// DDBBackupStatus defines the backup execution state
type DDBBackupStatus struct {
	// Phase current execution status
	Phase BackupPhase `json:"phase,omitempty"`

	// BackupPath resolved directory path where backup was stored
	BackupPath string `json:"backupPath,omitempty"`

	// Node node alias where backup was executed
	Node string `json:"node,omitempty"`

	// TotalChunks total number of chunks backed up
	TotalChunks int `json:"totalChunks,omitempty"`

	// StartTime timestamp when backup began
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime timestamp when backup concluded
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message human readable description or error message
	Message string `json:"message,omitempty"`

	// LastBackupTime timestamp of last completed cron backup
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ddbbackup,categories=dolphindb
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Chunks",type=integer,JSONPath=`.status.totalChunks`
// +kubebuilder:printcolumn:name="Path",type=string,JSONPath=`.status.backupPath`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DDBBackup is the Schema for the ddbbackups API
type DDBBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DDBBackupSpec   `json:"spec,omitempty"`
	Status DDBBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DDBBackupList contains a list of DDBBackup
type DDBBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DDBBackup `json:"items"`
}

// RestorePhase defines the state of a restore execution
type RestorePhase string

const (
	RestorePhasePending   RestorePhase = "Pending"
	RestorePhaseRunning   RestorePhase = "Running"
	RestorePhaseCompleted RestorePhase = "Completed"
	RestorePhaseFailed    RestorePhase = "Failed"
)

// DDBRestoreSpec defines the desired restore action
type DDBRestoreSpec struct {
	// Cluster target DolphinDBCluster name
	Cluster string `json:"cluster"`

	// BackupName name of DDBBackup CR to restore from (optional if BackupPath is given)
	BackupName string `json:"backupName,omitempty"`

	// BackupPath source directory path of backup files
	BackupPath string `json:"backupPath,omitempty"`

	// Database DFS database URL to restore (e.g. "dfs://k8sScaleTestDB")
	Database string `json:"database"`

	// Table optional table name to restore
	Table string `json:"table,omitempty"`

	// TargetDatabase optional destination database URL if restoring to a different DB
	TargetDatabase string `json:"targetDatabase,omitempty"`

	// TargetTable optional destination table name if restoring to a different table
	TargetTable string `json:"targetTable,omitempty"`

	// Node optional node alias where backup files reside
	Node string `json:"node,omitempty"`
}

// DDBRestoreStatus defines the restore execution state
type DDBRestoreStatus struct {
	// Phase current execution status
	Phase RestorePhase `json:"phase,omitempty"`

	// StartTime timestamp when restore began
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime timestamp when restore concluded
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message human readable description or error message
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ddbrestore,categories=dolphindb
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.database`
// +kubebuilder:printcolumn:name="TargetDB",type=string,JSONPath=`.spec.targetDatabase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DDBRestore is the Schema for the ddbrestores API
type DDBRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DDBRestoreSpec   `json:"spec,omitempty"`
	Status DDBRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DDBRestoreList contains a list of DDBRestore
type DDBRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DDBRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DDBBackup{}, &DDBBackupList{})
	SchemeBuilder.Register(&DDBRestore{}, &DDBRestoreList{})
}
