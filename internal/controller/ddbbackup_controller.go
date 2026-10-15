package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/robfig/cron/v3"

	"dolphindb.io/operator/api/v1alpha1"
	"dolphindb.io/operator/internal/credentials"
	"dolphindb.io/operator/internal/ddbclient"
	"dolphindb.io/operator/internal/metrics"
)

// DDBBackupReconciler reconciles a DDBBackup object
type DDBBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dolphindb.io,resources=dolphindbclusters,verbs=get;list;watch

func (r *DDBBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	backup := &v1alpha1.DDBBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. Check if cron schedule is defined
	var cronSched cron.Schedule
	if backup.Spec.Schedule != "" {
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		sched, err := parser.Parse(backup.Spec.Schedule)
		if err != nil {
			backup.Status.Phase = v1alpha1.BackupPhaseFailed
			backup.Status.Message = fmt.Sprintf("Invalid cron schedule %q: %v", backup.Spec.Schedule, err)
			return ctrl.Result{}, r.Status().Update(ctx, backup)
		}
		cronSched = sched
	} else {
		// Non-scheduled backup terminates if already completed or failed
		if backup.Status.Phase == v1alpha1.BackupPhaseCompleted || backup.Status.Phase == v1alpha1.BackupPhaseFailed {
			return ctrl.Result{}, nil
		}
	}

	now := time.Now()

	// 2. Scheduled backup timing check
	if cronSched != nil {
		if backup.Status.LastBackupTime != nil {
			nextTime := cronSched.Next(backup.Status.LastBackupTime.Time)
			if now.Before(nextTime) {
				backup.Status.NextBackupTime = &metav1.Time{Time: nextTime}
				requeueWait := time.Until(nextTime)
				if requeueWait <= 0 {
					requeueWait = 1 * time.Second
				}
				logger.Info("Cron backup scheduled for future execution", "name", backup.Name, "nextTime", nextTime, "wait", requeueWait)
				return ctrl.Result{RequeueAfter: requeueWait}, r.Status().Update(ctx, backup)
			}
			// Due for execution! Proceed below.
		} else if backup.Status.StartTime != nil && backup.Status.Phase == v1alpha1.BackupPhasePending {
			// First run has been scheduled, check if due
			nextTime := cronSched.Next(backup.CreationTimestamp.Time)
			if now.Before(nextTime) {
				backup.Status.NextBackupTime = &metav1.Time{Time: nextTime}
				requeueWait := time.Until(nextTime)
				if requeueWait <= 0 {
					requeueWait = 1 * time.Second
				}
				return ctrl.Result{RequeueAfter: requeueWait}, r.Status().Update(ctx, backup)
			}
		}
	}

	// 3. Initialize status if not yet initialized
	if backup.Status.Phase == "" {
		backup.Status.Phase = v1alpha1.BackupPhasePending
		nowMeta := metav1.Now()
		backup.Status.StartTime = &nowMeta
		if cronSched != nil {
			nextTime := cronSched.Next(backup.CreationTimestamp.Time)
			backup.Status.NextBackupTime = &metav1.Time{Time: nextTime}
			if now.Before(nextTime) {
				backup.Status.Message = fmt.Sprintf("Scheduled next backup at %s", nextTime.Format("2006-01-02 15:04:05"))
				return ctrl.Result{RequeueAfter: time.Until(nextTime)}, r.Status().Update(ctx, backup)
			}
		}
		backup.Status.Message = "Initialized backup request"
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, backup)
	}

	logger.Info("Reconciling DDBBackup execution", "name", backup.Name, "cluster", backup.Spec.Cluster, "phase", backup.Status.Phase)

	// Fetch target cluster
	cluster := &v1alpha1.DolphinDBCluster{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.Cluster}, cluster); err != nil {
		if errors.IsNotFound(err) {
			backup.Status.Phase = v1alpha1.BackupPhaseFailed
			backup.Status.Message = fmt.Sprintf("Cluster %q not found", backup.Spec.Cluster)
			nowMeta := metav1.Now()
			backup.Status.CompletionTime = &nowMeta
			return ctrl.Result{}, r.Status().Update(ctx, backup)
		}
		return ctrl.Result{}, err
	}

	// Obtain client
	cli := r.getDDBClient(ctx, cluster)
	if cli == nil {
		backup.Status.Message = "Awaiting DolphinDB controller endpoint..."
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, backup)
	}

	// Select active DataNode for RPC execution
	activeDN, err := r.findActiveDataNode(ctx, cli, cluster)
	if err != nil {
		if cronSched != nil {
			nowMeta := metav1.Now()
			backup.Status.Phase = v1alpha1.BackupPhaseFailed
			backup.Status.CompletionTime = &nowMeta
			nextTime := cronSched.Next(time.Now())
			backup.Status.NextBackupTime = &metav1.Time{Time: nextTime}
			backup.Status.Message = fmt.Sprintf("No active DataNode found to execute backup: %v; next attempt at %s", err, nextTime.Format("2006-01-02 15:04:05"))
			return ctrl.Result{RequeueAfter: time.Until(nextTime)}, r.Status().Update(ctx, backup)
		}
		if backup.Status.StartTime != nil && time.Since(backup.Status.StartTime.Time) < 2*time.Minute {
			backup.Status.Message = fmt.Sprintf("Awaiting active online DataNode (%v)...", err)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, backup)
		}
		backup.Status.Phase = v1alpha1.BackupPhaseFailed
		backup.Status.Message = fmt.Sprintf("No active DataNode found to execute backup: %v", err)
		nowMeta := metav1.Now()
		backup.Status.CompletionTime = &nowMeta
		return ctrl.Result{}, r.Status().Update(ctx, backup)
	}

	backup.Status.Phase = v1alpha1.BackupPhaseRunning
	backupDir := backup.Spec.BackupPath
	if backupDir == "" {
		backupDir = fmt.Sprintf("/tmp/backup/%s/%s", cluster.Name, backup.Name)
	}
	backup.Status.BackupPath = backupDir

	// Execute backup across specified databases
	totalChunks := 0
	force := backup.Spec.Force
	parallel := backup.Spec.Parallel
	snapshot := true
	if backup.Spec.Snapshot != nil {
		snapshot = *backup.Spec.Snapshot
	}

	for _, db := range backup.Spec.Databases {
		table := ""
		if len(backup.Spec.Tables) > 0 {
			table = backup.Spec.Tables[0]
		}
		chunks, err := cli.Backup(ctx, activeDN, backupDir, db, table, force, parallel, snapshot)
		if err != nil {
			nowMeta := metav1.Now()
			backup.Status.Phase = v1alpha1.BackupPhaseFailed
			backup.Status.CompletionTime = &nowMeta
			if cronSched != nil {
				nextTime := cronSched.Next(time.Now())
				backup.Status.NextBackupTime = &metav1.Time{Time: nextTime}
				backup.Status.Message = fmt.Sprintf("Backup failed for DB %s: %v; next attempt at %s", db, err, nextTime.Format("2006-01-02 15:04:05"))
				requeueWait := time.Until(nextTime)
				if requeueWait <= 0 {
					requeueWait = 5 * time.Second
				}
				return ctrl.Result{RequeueAfter: requeueWait}, r.Status().Update(ctx, backup)
			}
			backup.Status.Message = fmt.Sprintf("Backup failed for DB %s: %v", db, err)
			return ctrl.Result{}, r.Status().Update(ctx, backup)
		}
		totalChunks += chunks
	}

	nowMeta := metav1.Now()
	backup.Status.Phase = v1alpha1.BackupPhaseCompleted
	backup.Status.Node = activeDN
	backup.Status.TotalChunks = totalChunks
	backup.Status.CompletionTime = &nowMeta
	backup.Status.LastBackupTime = &nowMeta

	metrics.BackupTotalChunks.WithLabelValues(backup.Spec.Cluster, backup.Namespace, backup.Name).Set(float64(totalChunks))

	if cronSched != nil {
		nextTime := cronSched.Next(nowMeta.Time)
		backup.Status.NextBackupTime = &metav1.Time{Time: nextTime}
		backup.Status.Message = fmt.Sprintf("Backup succeeded (%d chunk(s) stored in %s via %s); next at %s",
			totalChunks, backupDir, activeDN, nextTime.Format("2006-01-02 15:04:05"))
		requeueWait := time.Until(nextTime)
		if requeueWait <= 0 {
			requeueWait = 1 * time.Second
		}
		return ctrl.Result{RequeueAfter: requeueWait}, r.Status().Update(ctx, backup)
	}

	backup.Status.Message = fmt.Sprintf("Backup succeeded (%d chunk(s) stored in %s via %s)", totalChunks, backupDir, activeDN)
	return ctrl.Result{}, r.Status().Update(ctx, backup)
}

func (r *DDBBackupReconciler) getDDBClient(ctx context.Context, cluster *v1alpha1.DolphinDBCluster) *ddbclient.Client {
	targetPodName := fmt.Sprintf("%s-ctl-0", cluster.Name)
	port := int32(8900)
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		targetPodName = fmt.Sprintf("%s-single-0", cluster.Name)
		port = 8848
		if cluster.Spec.Standalone != nil && cluster.Spec.Standalone.Port > 0 {
			port = cluster.Spec.Standalone.Port
		}
	} else if cluster.Spec.Controller != nil && cluster.Spec.Controller.Port > 0 {
		port = cluster.Spec.Controller.Port
	}

	pod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: targetPodName}, pod); err != nil {
		return nil
	}
	if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		return nil
	}

	baseURL := fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port)
	user, pass := credentials.Resolve(ctx, r.Client, cluster.Namespace, cluster.Spec.Security.AdminSecretName)
	return ddbclient.NewClient(baseURL, user, pass)
}

func (r *DDBBackupReconciler) findActiveDataNode(ctx context.Context, cli *ddbclient.Client, cluster *v1alpha1.DolphinDBCluster) (string, error) {
	if cluster.Spec.Mode == v1alpha1.ModeStandalone {
		return fmt.Sprintf("%s-single-0", cluster.Name), nil
	}

	perf, err := cli.GetClusterPerf(ctx)
	if err != nil {
		return "", err
	}

	for _, n := range perf {
		if n.State == 1 && (n.Mode == 3 || strings.Contains(n.Name, "-dn-")) {
			return n.Name, nil
		}
	}

	return "", fmt.Errorf("zero online datanodes in cluster perf")
}

func (r *DDBBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DDBBackup{}).
		Complete(r)
}

// DDBRestoreReconciler reconciles a DDBRestore object
type DDBRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dolphindb.io,resources=ddbbackups,verbs=get;list;watch

func (r *DDBRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	restore := &v1alpha1.DDBRestore{}
	if err := r.Get(ctx, req.NamespacedName, restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if restore.Status.Phase == v1alpha1.RestorePhaseCompleted || restore.Status.Phase == v1alpha1.RestorePhaseFailed {
		return ctrl.Result{}, nil
	}

	if restore.Status.Phase == "" {
		restore.Status.Phase = v1alpha1.RestorePhasePending
		now := metav1.Now()
		restore.Status.StartTime = &now
		restore.Status.Message = "Initialized restore request"
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, restore)
	}

	logger.Info("Reconciling DDBRestore", "name", restore.Name, "cluster", restore.Spec.Cluster, "db", restore.Spec.Database)

	// Fetch target cluster
	cluster := &v1alpha1.DolphinDBCluster{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: restore.Namespace, Name: restore.Spec.Cluster}, cluster); err != nil {
		if errors.IsNotFound(err) {
			restore.Status.Phase = v1alpha1.RestorePhaseFailed
			restore.Status.Message = fmt.Sprintf("Cluster %q not found", restore.Spec.Cluster)
			now := metav1.Now()
			restore.Status.CompletionTime = &now
			return ctrl.Result{}, r.Status().Update(ctx, restore)
		}
		return ctrl.Result{}, err
	}

	// Resolve backup path and node
	backupDir := restore.Spec.BackupPath
	targetNode := restore.Spec.Node
	if restore.Spec.BackupName != "" {
		sourceBackup := &v1alpha1.DDBBackup{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: restore.Namespace, Name: restore.Spec.BackupName}, sourceBackup); err == nil {
			if backupDir == "" {
				backupDir = sourceBackup.Status.BackupPath
			}
			if targetNode == "" {
				targetNode = sourceBackup.Status.Node
			}
		}
	}

	if backupDir == "" {
		restore.Status.Phase = v1alpha1.RestorePhaseFailed
		restore.Status.Message = "Neither spec.backupPath nor valid spec.backupName provided"
		now := metav1.Now()
		restore.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, restore)
	}

	// Obtain client
	bReconciler := &DDBBackupReconciler{Client: r.Client, Scheme: r.Scheme}
	cli := bReconciler.getDDBClient(ctx, cluster)
	if cli == nil {
		restore.Status.Message = "Awaiting DolphinDB controller endpoint..."
		return ctrl.Result{RequeueAfter: 3 * time.Second}, r.Status().Update(ctx, restore)
	}

	activeDN := targetNode
	var err error
	if activeDN == "" {
		activeDN, err = bReconciler.findActiveDataNode(ctx, cli, cluster)
		if err != nil {
			restore.Status.Phase = v1alpha1.RestorePhaseFailed
			restore.Status.Message = fmt.Sprintf("No active DataNode found to execute restore: %v", err)
			now := metav1.Now()
			restore.Status.CompletionTime = &now
			return ctrl.Result{}, r.Status().Update(ctx, restore)
		}
	}

	restore.Status.Phase = v1alpha1.RestorePhaseRunning
	err = cli.Restore(ctx, activeDN, backupDir, restore.Spec.Database, restore.Spec.Table, restore.Spec.TargetDatabase, restore.Spec.TargetTable)
	if err != nil {
		restore.Status.Phase = v1alpha1.RestorePhaseFailed
		restore.Status.Message = fmt.Sprintf("Restore failed: %v", err)
		now := metav1.Now()
		restore.Status.CompletionTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, restore)
	}

	target := restore.Spec.Database
	if restore.Spec.TargetDatabase != "" {
		target = restore.Spec.TargetDatabase
	}

	now := metav1.Now()
	restore.Status.Phase = v1alpha1.RestorePhaseCompleted
	restore.Status.CompletionTime = &now
	restore.Status.Message = fmt.Sprintf("Successfully restored %s from %s into target %s via %s", restore.Spec.Database, backupDir, target, activeDN)
	return ctrl.Result{}, r.Status().Update(ctx, restore)
}

func (r *DDBRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.DDBRestore{}).
		Complete(r)
}
