package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
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

// S3StorageSpec defines AWS S3 or S3-compatible object storage destination/source
type S3StorageSpec struct {
	// Bucket target S3 bucket name (e.g., "my-dolphindb-backups")
	Bucket string `json:"bucket"`

	// Region optional AWS region (e.g., "ap-southeast-2", defaults to cluster region)
	Region string `json:"region,omitempty"`

	// Prefix optional prefix/directory path in the bucket (e.g., "dolphindb-backups")
	Prefix string `json:"prefix,omitempty"`

	// Endpoint optional custom S3 endpoint URL for MinIO, Ceph, or Cloudflare R2
	Endpoint string `json:"endpoint,omitempty"`

	// SecretRef optional reference to K8s Secret containing AWS credentials (keys: accessKeyId, secretAccessKey). If omitted, IAM Role / Instance Profile is used.
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}

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

	// S3 optional S3 storage configuration for archiving backups directly to S3
	S3 *S3StorageSpec `json:"s3,omitempty"`

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

	// S3Location full S3 URI if uploaded to S3 (e.g., "s3://my-bucket/backups/db-20261016.zip")
	S3Location string `json:"s3Location,omitempty"`

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

	// NextBackupTime timestamp of next scheduled backup
	NextBackupTime *metav1.Time `json:"nextBackupTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ddbbackup,categories=dolphindb
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Chunks",type=integer,JSONPath=`.status.totalChunks`
// +kubebuilder:printcolumn:name="Path",type=string,JSONPath=`.status.backupPath`
// +kubebuilder:printcolumn:name="Next",type=string,JSONPath=`.status.nextBackupTime`
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

	// S3 optional S3 storage configuration to restore from
	S3 *S3StorageSpec `json:"s3,omitempty"`

	// S3Key specific S3 object key to restore from (e.g. "backups/test_1rep_db_20261016.zip")
	S3Key string `json:"s3Key,omitempty"`

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
