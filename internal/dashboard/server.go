package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"dolphindb.io/operator/api/v1alpha1"
)

//go:embed ui/*
var uiFS embed.FS

// Server provides the HTTP dashboard and REST API
type Server struct {
	client    client.Client
	listen    string
	namespace string
}

func NewServer(cli client.Client, listen string, defaultNamespace string) *Server {
	return &Server{
		client:    cli,
		listen:    listen,
		namespace: defaultNamespace,
	}
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// API Routes
	mux.HandleFunc("/api/healthz", s.handleHealthz)
	mux.HandleFunc("/api/clusters", s.handleClusters)
	mux.HandleFunc("/api/clusters/", s.handleClusterSubRoutes)
	mux.HandleFunc("/api/operations", s.handleOperations)
	mux.HandleFunc("/api/backups", s.handleBackups)
	mux.HandleFunc("/api/restores", s.handleRestores)

	// Embedded UI Static Assets
	subUI, err := fs.Sub(uiFS, "ui")
	if err == nil {
		fileServer := http.FileServer(http.FS(subUI))
		mux.Handle("/", fileServer)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("DolphinDB Operator Dashboard UI unavailable"))
		})
	}

	server := &http.Server{
		Addr:         s.listen,
		Handler:      corsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErr:
		return err
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func jsonResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func errorResponse(w http.ResponseWriter, statusCode int, message string) {
	jsonResponse(w, statusCode, map[string]string{"error": message})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

// ClusterSummary represents high-level metrics for dashboard cards
type ClusterSummary struct {
	Name                 string               `json:"name"`
	Namespace            string               `json:"namespace"`
	Mode                 v1alpha1.ClusterMode `json:"mode"`
	Version              string               `json:"version"`
	Health               string               `json:"health"`
	DesiredNodes         int32                `json:"desiredNodes"`
	ReadyNodes           int32                `json:"readyNodes"`
	DataNodes            int32                `json:"dataNodes"`
	ComputeNodes         int32                `json:"computeNodes"`
	DfsReplicationFactor int32                `json:"dfsReplicationFactor"`
	CreationTimestamp    metav1.Time          `json:"creationTimestamp"`
	ControllerPort       int32                `json:"controllerPort"`
}

func getDfsReplicationFactor(c *v1alpha1.DolphinDBCluster) int32 {
	if c.Spec.Mode == v1alpha1.ModeStandalone {
		return 1
	}
	repFactor := int32(2)
	if c.Spec.Controller != nil {
		if rfStr, ok := c.Spec.Controller.Config["dfsReplicationFactor"]; ok {
			if rf, err := strconv.Atoi(rfStr); err == nil && rf > 0 {
				return int32(rf)
			}
		}
	}
	return repFactor
}

func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ctx := r.Context()
	clusterList := &v1alpha1.DolphinDBClusterList{}
	listOpts := []client.ListOption{}
	ns := r.URL.Query().Get("namespace")
	if ns == "" && s.namespace != "" {
		ns = s.namespace
	}
	if ns != "" {
		listOpts = append(listOpts, client.InNamespace(ns))
	}

	if err := s.client.List(ctx, clusterList, listOpts...); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list clusters: %v", err))
		return
	}

	summaries := make([]ClusterSummary, 0, len(clusterList.Items))
	for _, c := range clusterList.Items {
		ctlPort := int32(8900)
		if c.Spec.Mode == v1alpha1.ModeStandalone {
			ctlPort = 8848
			if c.Spec.Standalone != nil && c.Spec.Standalone.Port > 0 {
				ctlPort = c.Spec.Standalone.Port
			}
		} else if c.Spec.Controller != nil && c.Spec.Controller.Port > 0 {
			ctlPort = c.Spec.Controller.Port
		}

		repl := getDfsReplicationFactor(&c)

		dataNodes := int32(0)
		if c.Spec.DataNode != nil {
			dataNodes = c.Spec.DataNode.Replicas
		}

		computeNodes := int32(0)
		if c.Spec.ComputeNode != nil {
			computeNodes = c.Spec.ComputeNode.Replicas
		}

		desired := c.Status.Controller.Desired + c.Status.DataNode.Desired + c.Status.ComputeNode.Desired
		ready := c.Status.Controller.Ready + c.Status.DataNode.Ready + c.Status.ComputeNode.Ready
		if c.Spec.Mode == v1alpha1.ModeStandalone {
			desired = 1
			if c.Status.Phase == v1alpha1.PhaseReady {
				ready = 1
			}
		}

		health := string(c.Status.Phase)
		if health == "" {
			health = "Creating"
		}

		summaries = append(summaries, ClusterSummary{
			Name:                 c.Name,
			Namespace:            c.Namespace,
			Mode:                 c.Spec.Mode,
			Version:              c.Spec.Version,
			Health:               health,
			DesiredNodes:         desired,
			ReadyNodes:           ready,
			DataNodes:            dataNodes,
			ComputeNodes:         computeNodes,
			DfsReplicationFactor: repl,
			CreationTimestamp:    c.CreationTimestamp,
			ControllerPort:       ctlPort,
		})
	}

	jsonResponse(w, http.StatusOK, summaries)
}

func (s *Server) handleClusterSubRoutes(w http.ResponseWriter, r *http.Request) {
	// Pattern: /api/clusters/{namespace}/{name}[/{action}]
	path := strings.TrimPrefix(r.URL.Path, "/api/clusters/")
	parts := strings.Split(strings.Trim(path, "/"), "/")

	if len(parts) < 2 {
		errorResponse(w, http.StatusBadRequest, "Invalid path: expected /api/clusters/{namespace}/{name}")
		return
	}

	namespace := parts[0]
	name := parts[1]

	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			s.getClusterDetail(w, r, namespace, name)
			return
		}
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	action := parts[2]
	switch action {
	case "restart":
		if r.Method == http.MethodPost {
			s.triggerRollingRestart(w, r, namespace, name)
			return
		}
	case "scale":
		if r.Method == http.MethodPost {
			s.scaleCluster(w, r, namespace, name)
			return
		}
	case "rebalance":
		if r.Method == http.MethodPost {
			s.triggerRebalance(w, r, namespace, name)
			return
		}
	default:
		errorResponse(w, http.StatusNotFound, "Action not found")
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

// ClusterDetail represents comprehensive topology view for a single cluster
type ClusterDetail struct {
	Cluster    v1alpha1.DolphinDBCluster `json:"cluster"`
	Nodes      []NodeInfo                `json:"nodes"`
	Operations []v1alpha1.DDBOperation   `json:"operations"`
	Backups    []v1alpha1.DDBBackup      `json:"backups"`
}

type NodeInfo struct {
	Name        string            `json:"name"`
	Role        v1alpha1.NodeRole `json:"role"`
	Phase       string            `json:"phase"`
	PodIP       string            `json:"podIP"`
	HostIP      string            `json:"hostIP"`
	Port        int32             `json:"port"`
	Ready       bool              `json:"ready"`
	ChunksCount int32             `json:"chunksCount"`
	DDBState    int32             `json:"ddbState"`
}

func (s *Server) getClusterDetail(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found: %v", namespace, name, err))
		return
	}

	// Fetch DDBNodes
	ddbNodeList := &v1alpha1.DDBNodeList{}
	_ = s.client.List(ctx, ddbNodeList, client.InNamespace(namespace))

	nodes := make([]NodeInfo, 0, len(ddbNodeList.Items))
	for _, n := range ddbNodeList.Items {
		if n.Spec.ClusterName != name {
			continue
		}
		podIP := n.Status.PodIP
		hostIP := n.Status.HostIP
		ready := false
		if n.Status.Phase == v1alpha1.NodePhaseRunning && n.Status.DDBState == 1 {
			ready = true
		}

		nodes = append(nodes, NodeInfo{
			Name:        n.Name,
			Role:        n.Spec.Role,
			Phase:       string(n.Status.Phase),
			PodIP:       podIP,
			HostIP:      hostIP,
			Port:        n.Spec.Port,
			Ready:       ready,
			ChunksCount: n.Status.ChunksCount,
			DDBState:    n.Status.DDBState,
		})
	}

	// Fetch Operations for this cluster
	opList := &v1alpha1.DDBOperationList{}
	_ = s.client.List(ctx, opList, client.InNamespace(namespace))
	clusterOps := make([]v1alpha1.DDBOperation, 0)
	for _, op := range opList.Items {
		if op.Spec.Cluster == name {
			clusterOps = append(clusterOps, op)
		}
	}
	sort.Slice(clusterOps, func(i, j int) bool {
		return clusterOps[i].CreationTimestamp.After(clusterOps[j].CreationTimestamp.Time)
	})

	// Fetch Backups for this cluster
	backupList := &v1alpha1.DDBBackupList{}
	_ = s.client.List(ctx, backupList, client.InNamespace(namespace))
	clusterBackups := make([]v1alpha1.DDBBackup, 0)
	for _, b := range backupList.Items {
		if b.Spec.Cluster == name {
			clusterBackups = append(clusterBackups, b)
		}
	}
	sort.Slice(clusterBackups, func(i, j int) bool {
		return clusterBackups[i].CreationTimestamp.After(clusterBackups[j].CreationTimestamp.Time)
	})

	jsonResponse(w, http.StatusOK, ClusterDetail{
		Cluster:    *cluster,
		Nodes:      nodes,
		Operations: clusterOps,
		Backups:    clusterBackups,
	})
}

type RollingRestartRequest struct {
	NodeType string `json:"nodeType,omitempty"`
}

func (s *Server) triggerRollingRestart(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req RollingRestartRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	opName := fmt.Sprintf("restart-%s-%d", name, time.Now().Unix())
	params := map[string]string{}
	if req.NodeType != "" {
		params["nodeType"] = req.NodeType
	}

	op := &v1alpha1.DDBOperation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opName,
			Namespace: namespace,
			Labels: map[string]string{
				"dolphindb.io/cluster-name": name,
			},
		},
		Spec: v1alpha1.DDBOperationSpec{
			Cluster:    name,
			Type:       v1alpha1.OpRollingRestart,
			Parameters: params,
		},
	}

	if err := s.client.Create(ctx, op); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create DDBOperation: %v", err))
		return
	}

	jsonResponse(w, http.StatusCreated, op)
}

type ScaleRequest struct {
	DataNodes    *int32 `json:"dataNodes,omitempty"`
	ComputeNodes *int32 `json:"computeNodes,omitempty"`
}

func (s *Server) scaleCluster(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	cluster := &v1alpha1.DolphinDBCluster{}
	if err := s.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
		errorResponse(w, http.StatusNotFound, fmt.Sprintf("Cluster %s/%s not found", namespace, name))
		return
	}

	// Verify dfsReplicationFactor safety guard
	repl := getDfsReplicationFactor(cluster)

	if req.DataNodes != nil {
		if *req.DataNodes < repl {
			errorResponse(w, http.StatusBadRequest, fmt.Sprintf(
				"Cannot scale DataNodes to %d: cluster dfsReplicationFactor is %d. DataNodes must be >= %d to prevent chunk replica insufficiency.",
				*req.DataNodes, repl, repl))
			return
		}
		if cluster.Spec.DataNode == nil {
			cluster.Spec.DataNode = &v1alpha1.DataNodeGroupSpec{Replicas: *req.DataNodes}
		} else {
			cluster.Spec.DataNode.Replicas = *req.DataNodes
		}
	}

	if req.ComputeNodes != nil {
		if *req.ComputeNodes < 0 {
			errorResponse(w, http.StatusBadRequest, "computeNodes cannot be negative")
			return
		}
		if cluster.Spec.ComputeNode == nil {
			cluster.Spec.ComputeNode = &v1alpha1.ComputeNodeGroupSpec{Replicas: *req.ComputeNodes}
		} else {
			cluster.Spec.ComputeNode.Replicas = *req.ComputeNodes
		}
	}

	if err := s.client.Update(ctx, cluster); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to scale cluster: %v", err))
		return
	}

	jsonResponse(w, http.StatusOK, cluster)
}

func (s *Server) triggerRebalance(w http.ResponseWriter, r *http.Request, namespace, name string) {
	ctx := r.Context()
	opName := fmt.Sprintf("rebalance-%s-%d", name, time.Now().Unix())
	op := &v1alpha1.DDBOperation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opName,
			Namespace: namespace,
			Labels: map[string]string{
				"dolphindb.io/cluster-name": name,
			},
		},
		Spec: v1alpha1.DDBOperationSpec{
			Cluster: name,
			Type:    v1alpha1.OpRebalanceChunks,
		},
	}

	if err := s.client.Create(ctx, op); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create DDBOperation: %v", err))
		return
	}

	jsonResponse(w, http.StatusCreated, op)
}

func (s *Server) handleOperations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method != http.MethodGet {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	opList := &v1alpha1.DDBOperationList{}
	listOpts := []client.ListOption{}
	ns := r.URL.Query().Get("namespace")
	if ns == "" && s.namespace != "" {
		ns = s.namespace
	}
	if ns != "" {
		listOpts = append(listOpts, client.InNamespace(ns))
	}

	if err := s.client.List(ctx, opList, listOpts...); err != nil {
		errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list operations: %v", err))
		return
	}

	clusterFilter := r.URL.Query().Get("cluster")
	filtered := make([]v1alpha1.DDBOperation, 0, len(opList.Items))
	for _, op := range opList.Items {
		if clusterFilter == "" || op.Spec.Cluster == clusterFilter {
			filtered = append(filtered, op)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreationTimestamp.After(filtered[j].CreationTimestamp.Time)
	})

	jsonResponse(w, http.StatusOK, filtered)
}

type CreateBackupRequest struct {
	Cluster    string   `json:"cluster"`
	Namespace  string   `json:"namespace"`
	Name       string   `json:"name,omitempty"`
	Databases  []string `json:"databases"`
	Tables     []string `json:"tables,omitempty"`
	BackupPath string   `json:"backupPath,omitempty"`
	Schedule   string   `json:"schedule,omitempty"`
	Force      bool     `json:"force,omitempty"`
}

func (s *Server) handleBackups(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		bList := &v1alpha1.DDBBackupList{}
		listOpts := []client.ListOption{}
		ns := r.URL.Query().Get("namespace")
		if ns == "" && s.namespace != "" {
			ns = s.namespace
		}
		if ns != "" {
			listOpts = append(listOpts, client.InNamespace(ns))
		}

		if err := s.client.List(ctx, bList, listOpts...); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list backups: %v", err))
			return
		}
		sort.Slice(bList.Items, func(i, j int) bool {
			return bList.Items[i].CreationTimestamp.After(bList.Items[j].CreationTimestamp.Time)
		})
		jsonResponse(w, http.StatusOK, bList.Items)
		return
	}

	if r.Method == http.MethodPost {
		var req CreateBackupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			errorResponse(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		if req.Cluster == "" || len(req.Databases) == 0 {
			errorResponse(w, http.StatusBadRequest, "spec.cluster and spec.databases are required")
			return
		}

		ns := req.Namespace
		if ns == "" {
			ns = s.namespace
			if ns == "" {
				ns = "default"
			}
		}

		bName := req.Name
		if bName == "" {
			bName = fmt.Sprintf("backup-%s-%d", req.Cluster, time.Now().Unix())
		}

		backup := &v1alpha1.DDBBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bName,
				Namespace: ns,
				Labels: map[string]string{
					"dolphindb.io/cluster-name": req.Cluster,
				},
			},
			Spec: v1alpha1.DDBBackupSpec{
				Cluster:    req.Cluster,
				Databases:  req.Databases,
				Tables:     req.Tables,
				BackupPath: req.BackupPath,
				Schedule:   req.Schedule,
				Force:      req.Force,
			},
		}

		if err := s.client.Create(ctx, backup); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create backup: %v", err))
			return
		}

		jsonResponse(w, http.StatusCreated, backup)
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}

type CreateRestoreRequest struct {
	Cluster        string `json:"cluster"`
	Namespace      string `json:"namespace"`
	Name           string `json:"name,omitempty"`
	BackupName     string `json:"backupName,omitempty"`
	BackupPath     string `json:"backupPath,omitempty"`
	Database       string `json:"database"`
	Table          string `json:"table,omitempty"`
	TargetDatabase string `json:"targetDatabase,omitempty"`
	TargetTable    string `json:"targetTable,omitempty"`
}

func (s *Server) handleRestores(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		rList := &v1alpha1.DDBRestoreList{}
		listOpts := []client.ListOption{}
		ns := r.URL.Query().Get("namespace")
		if ns == "" && s.namespace != "" {
			ns = s.namespace
		}
		if ns != "" {
			listOpts = append(listOpts, client.InNamespace(ns))
		}

		if err := s.client.List(ctx, rList, listOpts...); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list restores: %v", err))
			return
		}
		sort.Slice(rList.Items, func(i, j int) bool {
			return rList.Items[i].CreationTimestamp.After(rList.Items[j].CreationTimestamp.Time)
		})
		jsonResponse(w, http.StatusOK, rList.Items)
		return
	}

	if r.Method == http.MethodPost {
		var req CreateRestoreRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			errorResponse(w, http.StatusBadRequest, "Invalid request body")
			return
		}

		if req.Cluster == "" || req.Database == "" {
			errorResponse(w, http.StatusBadRequest, "spec.cluster and spec.database are required")
			return
		}

		ns := req.Namespace
		if ns == "" {
			ns = s.namespace
			if ns == "" {
				ns = "default"
			}
		}

		rName := req.Name
		if rName == "" {
			rName = fmt.Sprintf("restore-%s-%d", req.Cluster, time.Now().Unix())
		}

		restore := &v1alpha1.DDBRestore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rName,
				Namespace: ns,
				Labels: map[string]string{
					"dolphindb.io/cluster-name": req.Cluster,
				},
			},
			Spec: v1alpha1.DDBRestoreSpec{
				Cluster:        req.Cluster,
				BackupName:     req.BackupName,
				BackupPath:     req.BackupPath,
				Database:       req.Database,
				Table:          req.Table,
				TargetDatabase: req.TargetDatabase,
				TargetTable:    req.TargetTable,
			},
		}

		if err := s.client.Create(ctx, restore); err != nil {
			errorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create restore: %v", err))
			return
		}

		jsonResponse(w, http.StatusCreated, restore)
		return
	}

	errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
}
